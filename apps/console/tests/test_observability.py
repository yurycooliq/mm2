from dataclasses import replace
import json
from pathlib import Path
import tempfile
import time
import unittest

from mm2_console.app import App
from mm2_console.config import Config
from mm2_console.engine import Reply
from mm2_console.observability import execution_summary, journal_summary, money, health_identity, health_log
from mm2_console.telegram import TelegramBot


class MockEngine:
    def __init__(self, state):
        self.state = state
        self.calls = []

    def request(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        return Reply(200, self.state)


def state(profile="both"):
    return {"command_version": 7, "published_at_ms": int(time.time() * 1000),
            "mode": "shadow", "execution_mode": profile, "feed_ready": True,
            "source_status": {"phase": "public_ready"}, "simulation_error": None,
            "live": {"simulated": False, "phase": "running", "account_reconciled": True,
                     "user_stream_connected": True, "can_resume": True,
                     "collateral_micros": 80_000_001, "available_micros": 70_000_001,
                     "reserved_pico": "10000000000000", "daily_loss_micros": 2_000_000,
                     "daily_loss_metric": "conservative_gross_purchase_loss_bound", "fault": None},
            "simulation": {"account": {"initial_cash_micros": 100_000_000,
                           "spent_pico": "20000000000000", "reserved_pico": "5000000000000",
                           "available_pico": "75000000000000"}},
            "paper_portfolio": {"cash_pico": "102000000000001", "reserved_pico": "0",
                                "available_pico": "102000000000001", "halted": None},
            "parallel_simulation": {"mode": "shadow", "fault": None},
            "journal": {"total_bytes": 500, "total_max_bytes": 1000,
                        "archive_segments": 1, "max_segments": 100,
                        "owner_commands": 1, "max_commands": 100,
                        "execution_operations": 1, "max_execution_operations": 100}}


class ObservabilityTests(unittest.TestCase):
    def test_closeout_status_is_bounded_and_does_not_expose_signed_wire_or_infer_net_pnl(self):
        sample = state("trading")
        baseline = health_identity(sample)
        sample["live"].update({"returned_collateral_micros": 5_000_000, "chain_pending_hash": "0x" + "a" * 64,
                               "last_chain_error": "RPC outcome unknown", "session_gas_cost_wei": "1000",
                               "raw_transaction": "private-signed-material", "chain_pending_kind": "batch",
                               "relayer": {"body": "private-signed-batch", "signature": "private-owner-signature"}})
        summary = execution_summary(sample)
        self.assertEqual(summary["real"]["returned_collateral_usd"], "5")
        self.assertIsNone(summary["real"]["pnl_usd"])
        self.assertNotIn("private-signed-material", json.dumps(summary))
        self.assertNotIn("private-signed-batch", json.dumps(summary))
        self.assertNotIn("private-owner-signature", json.dumps(summary))
        self.assertEqual(summary["real"]["chain_pending_kind"], "batch")
        self.assertNotEqual(baseline, health_identity(sample))
        level, text = health_log(sample)
        self.assertEqual(level, "WARN")
        self.assertIn("closeout", text)
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.config = Config("http://127.0.0.1:1", "mock-engine-" + "e" * 32,
                             "mock-owner-" + "o" * 32, root, root, frozenset())
        self.engine = MockEngine(state())
        self.app = App(self.config, self.engine)

    def tearDown(self):
        self.app.close()
        self.temp.cleanup()

    def bot(self, transport=None):
        self.app.config = replace(self.app.config, telegram_token="offline-mock-token", telegram_owner=42)
        return TelegramBot(self.app, transport=transport or object())

    def test_modes_separate_balances_and_do_not_relabel_loss_bound_as_pnl(self):
        for profile in ("emulator", "simulation", "shadow", "trading", "both"):
            with self.subTest(profile=profile):
                self.engine.state = state(profile)
                self.app.sample_once()
                summary = self.app.state()["execution"]
                text = self.bot()._status()
                self.assertIn(f"profile {profile}", text)
                self.assertEqual(summary["real"]["selected"], profile in {"trading", "both"})
                self.assertEqual(summary["paper"]["selected"], profile in {"emulator", "simulation", "both"})
                self.assertIsNone(summary["real"]["pnl_usd"])
                self.assertIsNone(summary["paper"]["pnl_usd"])
                self.assertIn("Economic PnL: unknown", text)
                if profile in {"trading", "both"}:
                    self.assertIn("REAL funds USD: collateral 80.000001", text)
                    self.assertIn("conservative gross purchase loss bound", text)
                    self.assertNotIn("live disabled", text)
                if profile in {"emulator", "simulation", "both"}:
                    self.assertIn("global cash 102.000000000001", text)
                    self.assertIn("current market available 75", text)

    def test_unreconciled_stale_or_nonreal_payload_never_claims_known_real_funds(self):
        for change in ({"account_reconciled": False}, {"simulated": True}, {"collateral_micros": True}, {"available_micros": -1}):
            self.engine.state = state("trading")
            self.engine.state["live"].update(change)
            self.app.sample_once()
            self.assertIn("REAL funds USD: unknown", self.bot()._status())
        self.engine.state = state("trading")
        self.engine.state["published_at_ms"] -= 4000
        self.app.sample_once()
        text = self.bot()._status()
        self.assertIn("STALE", text)
        self.assertIn("REAL funds USD: unknown", text)
        self.assertEqual(self.app.store.logs(1)[0]["level"], "WARN")
        self.assertIn("publication stale", self.app.store.logs(1)[0]["message"])

    def test_material_lane_fault_and_journal_capacity_changes_log_once(self):
        self.app.sample_once()
        count = len(self.app.store.logs(100))
        # Changing balances every fill is not a health transition.
        self.engine.state["live"]["collateral_micros"] -= 1
        self.app.sample_once()
        self.assertEqual(len(self.app.store.logs(100)), count)
        self.engine.state["live"]["fault"] = "live reconciliation failed"
        self.app.sample_once()
        latest = self.app.store.logs(1)[0]
        self.assertEqual(latest["level"], "ERROR")
        self.assertIn("live reconciliation failed", latest["message"])
        self.app.sample_once()
        self.assertEqual(len(self.app.store.logs(100)), count + 1)
        self.engine.state["live"]["fault"] = None
        self.engine.state["simulation_error"] = "parallel paper dropped feed event"
        self.app.sample_once()
        self.assertIn("parallel paper dropped feed event", self.app.store.logs(1)[0]["message"])
        self.engine.state["simulation_error"] = None
        self.engine.state["journal"]["total_bytes"] = 800
        self.app.sample_once()
        self.assertEqual(self.app.store.logs(1)[0]["level"], "WARN")
        self.assertIn("journal warning", self.app.store.logs(1)[0]["message"])
        self.assertIn("800 / 1000 bytes | warning", self.bot()._status())

    def test_secondary_history_compacts_large_details_without_changing_current_snapshot(self):
        self.engine.state["live"]["orders"] = [{"id": str(i), "detail": "x" * 200} for i in range(1000)]
        self.app.sample_once()
        current = self.app.state()
        self.assertEqual(len(current["state"]["live"]["orders"]), 1000)
        historical = self.app.store.history(1)[0]["state"]
        self.assertTrue(historical["history_projection"]["compacted"])
        self.assertEqual(historical["execution_summary"]["real"]["collateral_usd"], "80.000001")
        self.assertIsNone(historical["pnl"])
        self.assertLess(len(json.dumps(historical).encode()), 65_536)
        self.assertEqual(current["telemetry"]["history_compacted"], 1)
        self.assertEqual(current["telemetry"]["history_dropped"], 0)

    def test_status_loop_uses_mock_transport_and_never_requests_execution(self):
        self.app.sample_once()
        class MockTransport:
            def __init__(self):
                self.calls = []
                self.bot = None
            def call(self, method, payload):
                self.calls.append((method, payload))
                if method == "getUpdates":
                    return [{"update_id": 0, "message": {"from": {"id": 42},
                             "chat": {"id": 42, "type": "private"}, "text": "/status"}}]
                self.bot.stop.set()
                return {"message_id": 1}
        transport = MockTransport()
        bot = self.bot(transport)
        transport.bot = bot
        bot._loop()
        self.assertEqual([method for method, _ in transport.calls], ["getUpdates", "sendMessage"])
        text = transport.calls[1][1]["text"]
        self.assertIn("REAL funds", text)
        self.assertIn("VIRTUAL funds", text)
        self.assertEqual(len(self.engine.calls), 1)
        self.assertEqual(self.app.store.get_meta("telegram_offset"), "1")

    def test_invalid_or_uninitialized_state_does_not_crash_sampler_or_fabricate_money(self):
        self.engine.state = {"command_version": 0, "published_at_ms": int(time.time() * 1000),
                             "execution_mode": "trading", "source_status": None, "live": None}
        self.app.sample_once()
        self.assertIn("REAL funds USD: unknown", self.bot()._status())
        self.assertIsNone(money(True))
        self.assertIsNone(money(0.1))
        self.assertIsNone(money("-1"))
        self.assertEqual(money("100000000000001", 1_000_000_000_000), "100.000000000001")
        self.engine.state = {"command_version": -1}
        self.app.sample_once()
        self.assertFalse(self.app.state()["connected"])

    def test_journal_identity_capacity_warns_even_when_byte_count_is_small(self):
        self.engine.state["journal"]["owner_commands"] = 95
        self.assertEqual(journal_summary(self.engine.state)["stage"], "critical")
        self.engine.state["journal"]["owner_commands"] = 100
        self.assertEqual(journal_summary(self.engine.state)["stage"], "full")
        self.assertEqual(journal_summary({"journal_bytes": 0})["stage"], "unknown")

    def test_secondary_lane_capacity_stages_and_prepare_error_log_once(self):
        self.engine.state["live"].update(journal_bytes=100, journal_max_bytes=1000,
                                        last_prepare_error=None)
        self.engine.state["parallel_simulation"]["journal"] = {"total_bytes": 100, "total_max_bytes": 1000}
        self.app.sample_once()
        for lane, container in (("live", self.engine.state["live"]),
                                ("paper", self.engine.state["parallel_simulation"]["journal"])):
            key = "journal_bytes" if lane == "live" else "total_bytes"
            for used, expected in ((800, "warning"), (950, "critical")):
                with self.subTest(lane=lane, used=used):
                    before = len(self.app.store.logs(100))
                    container[key] = used
                    self.app.sample_once()
                    self.assertEqual(self.app.store.logs(1)[0]["level"], "WARN")
                    self.assertIn(f"{lane} journal {expected}", self.app.store.logs(1)[0]["message"])
                    self.assertEqual(len(self.app.store.logs(100)), before + 1)
                    self.app.sample_once()
                    self.assertEqual(len(self.app.store.logs(100)), before + 1)
                    self.assertEqual(self.app.state()["execution"]["real" if lane == "live" else "paper"]["journal"]["stage"], expected)
            container[key] = 100
            self.app.sample_once()
        self.engine.state["live"]["last_prepare_error"] = "venue tick changed; intent not submitted"
        before = len(self.app.store.logs(100))
        self.app.sample_once()
        self.assertEqual(self.app.store.logs(1)[0]["level"], "WARN")
        self.assertIn("live prepare", self.app.store.logs(1)[0]["message"])
        self.app.sample_once()
        self.assertEqual(len(self.app.store.logs(100)), before + 1)
        text = self.bot()._status()
        self.assertIn("Live journal: 100 / 1000 bytes | normal", text)
        self.assertIn("Paper journal: 100 / 1000 bytes | normal", text)
        self.assertIn("Live preparation: venue tick changed", text)
        self.assertIn("Economic PnL: unknown", text)


if __name__ == "__main__":
    unittest.main()
