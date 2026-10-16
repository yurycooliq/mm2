"""Persistence acceptance: actual sampled events survive an empty recovered session."""
from copy import deepcopy
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from mm2_console.app import App
from mm2_console.audit_projection import observer_identity
from mm2_console.comparison import comparison
from mm2_console.config import Config
from mm2_console.engine import Reply
from mm2_console.store import ACTION_RETENTION_MS, MAX_SAMPLED_ACTIONS, Store

NOW = 1_791_460_800_000  # 2026-10-08T12:00:00Z
BOOT = "00000000-0000-4000-8000-000000000001"
NEXT_BOOT = "00000000-0000-4000-8000-000000000002"


def state(at=NOW, boot=BOOT, locator="a" * 64, sequence=10, hash_value="b" * 64):
    return {"command_version": 7, "execution_mode": "simulation", "feed_source": "public", "mode": "paused",
        "published_at_ms": at, "boot_id": boot, "reset_epoch": 0, "feed_ready": False,
        "simulation_market_condition": "market-A", "market": {"condition_id": "market-A", "title": "Saved sample"},
        "journal": {"committed_prefix": {"schema_version": 1, "source_kind": "engine", "ledger_id": locator,
            "next_sequence": sequence, "last_hash": hash_value, "byte_offset": 100, "manifest_bytes": 20,
            "segment": {"name": "commands.jsonl", "index": 0, "start_sequence": 0, "previous_hash": "0" * 64}}},
        "simulation": {"simulated": True, "now_ms": at, "audit_sequence": 2, "audit_evicted": 0,
            "account": {"initial_cash_micros": 100_000_000, "spent_pico": "2000000000000",
                "reserved_pico": "0", "available_pico": "98000000000000"},
            "orders": [], "venue_orders": [{"id": 2, "quote": {"outcome": "up", "price_micros": 450001}}],
            "recent_audit": [{"sequence": 1, "at_ms": at - 2, "kind": "post_accepted", "order_id": 2, "quantity_micros": None},
                             {"sequence": 2, "at_ms": at - 1, "kind": "venue_fill", "order_id": 2, "quantity_micros": 1_000_001}]},
        "paper_portfolio": {"initial_cash_pico": "100000000000000", "cash_pico": "98000000000000", "spent_pico": "2000000000000",
            "fees_pico": "0", "returned_pico": "0", "realized_pnl_pico": "0", "reserved_pico": "0", "positions": [], "position_count": 0}}


class Engine:
    def __init__(self, value):
        self.value, self.calls = value, []

    def request(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        return Reply(200, self.value)


class ActionHistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = self.root / "secondary.sqlite3"
        self.store = Store(self.path)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def lane(self, value, at=NOW, period="1h"):
        return comparison({"state": value, "observed_at_ms": at, "connected": True}, self.store, self.root / "no-view", at, period)["own"]

    def config(self):
        return Config("http://127.0.0.1:8088", "e" * 32, "o" * 32, self.root, self.root, frozenset())

    def test_paused_recovered_engine_and_reopened_console_retain_exact_sampled_ids(self):
        engine = Engine(state())
        app = App(self.config(), engine, self.store)
        with patch("mm2_console.app.time.time", return_value=NOW / 1000):
            app.sample_once()
            before = app.comparison()["own"]
        self.assertEqual(len(before["actions"]), 2)
        ids = [action["id"] for action in before["actions"]]
        seen = [action["observed_at_ms"] for action in before["actions"]]
        # A restored paused engine retains the model portfolio but constructs no
        # PaperSession/ring until a later Resume. No Reset or invented backfill.
        app.close()
        self.store = Store(self.path)
        recovered = state(NOW + 1000, NEXT_BOOT)
        recovered.update(simulation=None, simulation_market_condition=None, market=None)
        app = App(self.config(), Engine(recovered), self.store)
        with patch("mm2_console.app.time.time", return_value=(NOW + 1000) / 1000):
            app.sample_once()
            after = app.comparison()["own"]
        self.assertEqual([action["id"] for action in after["actions"]], ids)
        self.assertEqual([action["observed_at_ms"] for action in after["actions"]], seen)
        self.assertEqual({action["boot_id"] for action in after["actions"]}, {BOOT})
        self.assertIn("BOOT_CONTINUITY_UNPROVEN", after["warnings"])
        self.assertEqual(after["model_result"]["cash_usd"], "98")
        self.assertIsNone(after["metrics"]["net_pnl_usd"]["value"])
        self.assertFalse(after["coverage"]["financial_complete"])
        self.assertFalse(after["coverage"]["actions_complete"])
        self.assertEqual(app.engine.calls, [("GET", "/v1/state", None)])
        # Do not close twice: the test-owned Store closes in tearDown.

    def test_first_observation_is_immutable_and_dedup_survives_console_restart(self):
        value = state()
        self.assertTrue(self.store.action_sample(NOW, value))
        original = self.lane(value)["actions"]
        later = deepcopy(value); later["simulation"]["now_ms"] = NOW + 1000
        self.assertTrue(self.store.action_sample(NOW + 1000, later))
        self.store.close(); self.store = Store(self.path)
        actions = self.lane(later, NOW + 1000)["actions"]
        self.assertEqual([row["id"] for row in actions], [row["id"] for row in original])
        self.assertEqual([row["observed_at_ms"] for row in actions], [NOW, NOW])
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM sampled_actions").fetchone()[0], 2)

    def test_different_locator_profile_endpoint_and_unbound_boot_do_not_mix(self):
        value = state(); self.store.action_sample(NOW, value)
        for mutation in (lambda row: row["journal"]["committed_prefix"].update(ledger_id="c" * 64),
                         lambda row: row.update(execution_mode="emulator"), lambda row: row.update(reset_epoch=1)):
            changed = deepcopy(value); mutation(changed); changed["simulation"] = None
            self.assertEqual(self.lane(changed)["actions"], [])
        self.store.configure_action_observer(observer_identity("http://different-engine:8088"))
        recovered = deepcopy(value); recovered["simulation"] = None
        self.assertEqual(self.lane(recovered)["actions"], [])
        unbound = state(); unbound.pop("journal")
        self.store.action_sample(NOW, unbound)
        same = deepcopy(unbound); same["simulation"] = None
        self.assertEqual(len(self.lane(same)["actions"]), 2)
        self.assertIn("ACTION_HISTORY_LOCATOR_UNBOUND", self.lane(same)["warnings"])
        same["boot_id"] = NEXT_BOOT
        self.assertEqual(self.lane(same)["actions"], [])

    def test_prefix_rollback_hash_conflict_and_session_counter_reset_split_generation(self):
        for kind in ("prefix", "hash", "audit"):
            with self.subTest(kind=kind):
                value = state(locator={"prefix": "d", "hash": "e", "audit": "f"}[kind] * 64)
                self.store.action_sample(NOW, value)
                old_ids = {row["id"] for row in self.lane(value)["actions"]}
                changed = deepcopy(value)
                changed["simulation"]["recent_audit"] = [changed["simulation"]["recent_audit"][0]]
                if kind == "prefix":
                    changed["journal"]["committed_prefix"]["next_sequence"] = 9
                elif kind == "hash":
                    changed["journal"]["committed_prefix"]["last_hash"] = "1" * 64
                else:
                    changed["simulation"]["audit_sequence"] = 1
                preview = self.lane(changed)
                self.assertEqual(len(preview["actions"]), 1)
                self.assertTrue(old_ids.isdisjoint({row["id"] for row in preview["actions"]}))
                self.assertIn("ACTION_HISTORY_GENERATION_CHANGED", preview["warnings"])
                self.assertTrue(self.store.action_sample(NOW, changed))
                persisted = self.lane(changed)
                self.assertEqual([row["id"] for row in preview["actions"]], [row["id"] for row in persisted["actions"]])

    def test_source_conflict_and_future_event_cannot_replace_a_verified_observation(self):
        value = state(); self.store.action_sample(NOW, value)
        conflict = deepcopy(value)
        conflict["simulation"]["recent_audit"][1]["quantity_micros"] = 9_000_000
        self.store.action_sample(NOW + 1, conflict)
        lane = self.lane(conflict, NOW + 1)
        self.assertEqual(lane["actions"][0]["quantity_shares"], "1.000001")
        self.assertIn("ACTION_HISTORY_INVALID_SOURCE", lane["warnings"])
        future = state(locator="2" * 64)
        future["simulation"]["now_ms"] = NOW + 1000
        for row in future["simulation"]["recent_audit"]:
            row["at_ms"] = NOW + 1
        self.store.action_sample(NOW, future)
        self.assertEqual(self.lane(future)["actions"], [])

    def test_logical_clock_uses_first_observed_period_and_does_not_invent_utc(self):
        value = state(); value["simulation"]["now_ms"] = 25
        for row in value["simulation"]["recent_audit"]:
            row["at_ms"] = row["sequence"]
        self.store.action_sample(NOW - 7_200_000, value)
        # An unchanged ring returned now cannot reanchor the old first observation.
        self.assertEqual(self.lane(value)["actions"], [])
        self.store.action_sample(NOW, value)
        self.assertEqual(self.lane(value)["actions"], [])
        value["simulation"] = None
        self.assertEqual(self.lane(value)["actions"], [])
        lane = self.lane(value, period="24h")
        self.assertEqual(len(lane["actions"]), 2)
        self.assertTrue(all(row["at_ms"] is None and row["clock_kind"] == "logical" for row in lane["actions"]))
        self.assertIn("SIMULATION_CLOCK_NOT_WALL_TIME", lane["warnings"])

    def test_capture_precedes_raw_compaction_and_database_failure_does_not_stop_sampler(self):
        value = state(); value["large_unused_detail"] = "x" * 100_000
        app = App(self.config(), Engine(value), self.store)
        with patch("mm2_console.app.time.time", return_value=NOW / 1000):
            app.sample_once()
            self.assertEqual(len(app.comparison()["own"]["actions"]), 2)
        self.assertTrue(self.store.history(1)[0]["state"]["history_projection"]["compacted"])
        with patch.object(self.store, "action_sample", side_effect=sqlite3.OperationalError("private fixture error")):
            with patch("mm2_console.app.time.time", return_value=NOW / 1000):
                app.sample_once()
                lane = app.comparison()["own"]
                self.assertTrue(app.state()["connected"])
        self.assertIn("SECONDARY_ACTION_HISTORY_UNAVAILABLE", lane["warnings"])
        self.assertEqual(len(app.engine.calls), 2)
        self.assertNotIn("private fixture error", str(lane))
        with patch.object(self.store, "action_history", side_effect=sqlite3.OperationalError("unavailable")):
            with patch("mm2_console.app.time.time", return_value=NOW / 1000):
                self.assertEqual(len(app.comparison()["own"]["actions"]), 2)

    def test_legacy_retained_samples_backfill_only_actual_valid_audit(self):
        value = state()
        self.store.sample(NOW, value)
        self.store.sample(NOW + 1, {"execution_mode": "simulation", "history_projection": {"compacted": True}})
        app = App(self.config(), Engine(value), self.store)
        recovered = state(NOW + 2, BOOT); recovered["simulation"] = None
        with patch("mm2_console.app.time.time", return_value=(NOW + 2) / 1000):
            app.engine.value = recovered; app.sample_once()
            lane = app.comparison()["own"]
        self.assertEqual(len(lane["actions"]), 2)
        self.assertIn("ACTION_HISTORY_SAMPLE_GAPS", lane["warnings"])
        self.assertEqual({row["observed_at_ms"] for row in lane["actions"]}, {NOW})

    def test_legacy_unknown_observer_history_cannot_be_attributed_to_another_boot(self):
        self.store.sample(NOW, state())
        recovered = state(NOW + 2, NEXT_BOOT); recovered["simulation"] = None
        app = App(self.config(), Engine(recovered), self.store)
        with patch("mm2_console.app.time.time", return_value=(NOW + 2) / 1000):
            app.sample_once()
            self.assertEqual(app.comparison()["own"]["actions"], [])
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM sampled_actions").fetchone()[0], 0)

    def test_evicted_logical_ring_is_not_reinserted_at_a_new_first_observation(self):
        value = state(); value["simulation"]["now_ms"] = 25
        for row in value["simulation"]["recent_audit"]:
            row["at_ms"] = row["sequence"]
        self.store.action_sample(NOW, value)
        later = NOW + ACTION_RETENTION_MS + 1
        self.store.action_sample(later, value)  # Expire first observations.
        self.store.action_sample(later + 1000, value)  # Unchanged ring remains old.
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM sampled_actions").fetchone()[0], 0)
        self.assertEqual(self.lane(value, later + 1000)["actions"], [])

    def test_finite_row_retention_tail_and_known_eviction_are_separate(self):
        value = state()
        for batch in range(17):
            at = NOW + batch * 1000
            first = batch * 256 + 1
            value["simulation"].update(now_ms=at, audit_sequence=first + 255,
                recent_audit=[{"sequence": first + index, "at_ms": at, "kind": "book", "order_id": None, "quantity_micros": None}
                              for index in range(256)])
            self.assertTrue(self.store.action_sample(at, value))
        lane = self.lane(value, NOW + 16_000)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM sampled_actions").fetchone()[0], MAX_SAMPLED_ACTIONS)
        self.assertEqual(len(lane["actions"]), 256)
        self.assertEqual(lane["coverage"]["observed_actions"], MAX_SAMPLED_ACTIONS)
        self.assertEqual(lane["coverage"]["omitted_actions"], MAX_SAMPLED_ACTIONS - 256)
        self.assertEqual(lane["coverage"]["retention_omitted_observations"], 256)
        self.assertIn("ACTION_HISTORY_RETENTION_LIMIT", lane["warnings"])
        value["simulation"] = None
        self.store.action_sample(NOW + ACTION_RETENTION_MS + 20_000, value)
        self.assertEqual(self.lane(value, NOW + ACTION_RETENTION_MS + 20_000)["actions"], [])

    def test_uint64_source_sequences_do_not_overflow_sqlite_and_ids_do_not_truncate(self):
        value = state(sequence=2**64 - 1)
        value["reset_epoch"] = 2**64 - 1
        value["simulation"]["audit_sequence"] = 2**64 - 1
        value["simulation"]["recent_audit"] = [{"sequence": 2**64 - 1, "at_ms": NOW, "kind": "book", "order_id": None, "quantity_micros": None}]
        self.assertTrue(self.store.action_sample(NOW, value))
        row = self.lane(value)["actions"][0]
        self.assertEqual(len(row["id"]), 70)
        self.assertEqual(row["sequence"], str(2**64 - 1))
        self.assertEqual(row["reset_epoch"], str(2**64 - 1))
        self.assertEqual(self.store.db.execute("SELECT prefix_sequence FROM action_scopes").fetchone()[0], str(2**64 - 1))
        value["boot_id"] = NEXT_BOOT
        self.assertTrue(self.store.action_sample(NOW, value))
        self.assertEqual(len({row["id"] for row in self.lane(value)["actions"]}), 2)


if __name__ == "__main__":
    unittest.main()
