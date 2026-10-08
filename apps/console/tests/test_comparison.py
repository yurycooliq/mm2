"""Read-only comparison contract, exact values and failure boundaries."""
from copy import deepcopy
import hashlib
import json
import sqlite3
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mm2_console.comparison import comparison, decimal, empty_lane, load_reference, stamp
from mm2_console.store import Store
from mm2_console.telegram_reports import valuation_context
from tools.research import archive
from tools.research.archive import publish_bundle, read_member
from tools.research.console_comparison import build_reference, publish_reference
from tools.research.observe_mo_money import normalize_activity, position_snapshot
from tools.research.schema import ResearchError, digest, load_json, quality

NOW = stamp("2026-10-08T12:00:00Z")
ACCOUNT = "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


class ComparisonTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = Store(self.root / "secondary.sqlite3")
        self.state = {"execution_mode": "simulation", "mode": "paused", "published_at_ms": NOW,
            "reset_epoch": 0, "simulation_market_condition": "market-A", "feed_ready": False,
            "simulation": {"simulated": True, "now_ms": NOW, "audit_sequence": 4, "audit_evicted": 2,
                "account": {"initial_cash_micros": 100_000_000, "spent_pico": "2000000000000",
                    "reserved_pico": "1000000000000", "available_pico": "97000000000000",
                    "up_inventory_micros": 0, "down_inventory_micros": 0},
                "recent_audit": [{"sequence": 4, "at_ms": NOW - 1000, "kind": "venue_fill", "order_id": 2, "quantity_micros": 1_000_001}]},
            "paper_portfolio": {"cash_pico": "98000000000001", "reserved_pico": "0", "positions": [], "position_count": 0}}
        self.envelope = {"state": self.state, "observed_at_ms": NOW, "connected": True}

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def artifact(self, mutate=None, source_at="2026-10-08T11:59:00Z", published="2026-10-08T11:59:00Z"):
        view = {"schema_version": 1, "account_id": ACCOUNT, "source_bundle_id": "b" * 64, "source_at_utc": source_at,
            "coverage": {"actions_complete": True, "financial_complete": False, "reason": "PUBLIC_ACTIVITY_NOT_AUTHENTICATED_FILLS", "observed_actions": 2, "omitted_actions": 0},
            "metrics": {"net_pnl_usd": {"value": "999", "scope": "public-wallet source period", "reason": "INCOMPLETE_ACCOUNTING"},
                "turnover_usd": {"value": "15.000000000001", "scope": "public-wallet source period", "reason": "PUBLIC_ACTIVITY_NOT_AUTHENTICATED_FILLS"}},
            "actions": [{"id": "activity:a:0", "at_ms": NOW - 120_000, "kind": "TRADE", "market": "market-public", "side": "BUY", "outcome": "Up", "quantity_shares": "2.000001", "amount_usd": "1.123456789012"},
                {"id": "activity:b:0", "at_ms": NOW - 7_200_000, "kind": "REWARD", "market": None, "side": None, "outcome": None, "quantity_shares": None, "amount_usd": "3"}],
            "positions": [{"market": "public-held", "outcome": "Down", "quantity_shares": "2", "value_usd": "1.2"}], "warnings": ["PUBLIC_WALLET_NOT_BOT_ATTRIBUTED"]}
        if mutate:
            mutate(view)
        raw = canonical(view) + b"\n"
        manifest = {"schema_version": 1, "created_at_utc": published, "utc_range": {"start": "2026-10-08T00:00:00Z", "end": "2026-10-08T11:59:00Z"},
            "scope": {"kind": "console-comparison-reference", "lane": "public-benchmark", "account_id": ACCOUNT}, "quality": {},
            "source_revision": "fixture", "config_hash": None, "files": [{"path": "view.json", "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw), "rows": 1}]}
        manifest["bundle_id"] = hashlib.sha256(canonical(manifest)).hexdigest()
        target = self.root / "views" / manifest["bundle_id"]
        target.mkdir(parents=True)
        (target / "view.json").write_bytes(raw)
        (target / "manifest.json").write_bytes(canonical(manifest))
        (target.parent / "current.json").write_bytes(canonical({"bundle_id": target.name}))
        return target

    def test_unavailable_reference_and_missing_engine_do_not_become_zero_results(self):
        result = comparison({}, self.store, self.root / "missing", NOW)
        self.assertEqual(result["reference"]["status"], "UNAVAILABLE")
        self.assertEqual(result["own"]["status"], "UNAVAILABLE")
        self.assertIsNone(result["own"]["metrics"]["net_pnl_usd"]["value"])
        self.assertEqual(result["comparison"]["status"], "INCONCLUSIVE")

    def test_simulated_current_cash_and_observed_fill_are_exact_not_real_profit(self):
        result = comparison(self.envelope, self.store, self.root / "missing", NOW)["own"]
        self.assertEqual(result["metrics"]["cash_usd"]["value"], "98.000000000001")
        self.assertEqual(result["metrics"]["available_usd"]["value"], "97")
        self.assertEqual(result["actions"][0]["quantity_shares"], "1.000001")
        self.assertIsNone(result["actions"][0]["amount_usd"])
        self.assertIsNone(result["metrics"]["net_pnl_usd"]["value"])
        self.assertFalse(result["coverage"]["actions_complete"])
        self.assertEqual(result["coverage"]["omitted_actions"], 2)

    def test_stale_engine_retains_labeled_actions_but_not_current_balances(self):
        self.envelope["connected"] = False
        result = comparison(self.envelope, self.store, self.root / "missing", NOW)["own"]
        self.assertEqual(result["status"], "STALE")
        self.assertIsNone(result["metrics"]["cash_usd"]["value"])
        self.assertEqual(len(result["actions"]), 1)

    def test_demo_clock_does_not_become_wall_time_or_false_period_filter(self):
        self.state["simulation"]["now_ms"] = 25
        self.state["simulation"]["recent_audit"][0]["at_ms"] = 24
        own = comparison(self.envelope, self.store, self.root / "missing", NOW)["own"]
        self.assertIsNone(own["actions"][0]["at_ms"])
        self.assertIn("SIMULATION_CLOCK_NOT_WALL_TIME", own["warnings"])

    def test_equity_context_reset_gap_and_null_prevent_comparative_gain_claim(self):
        context = valuation_context(self.state)
        self.store.equity_sample(NOW - 60_000, 9, 10**14 + 1, context)
        self.store.equity_sample(NOW, 9, 10**14 + 2, context)
        own = comparison(self.envelope, self.store, self.root / "missing", NOW)["own"]
        self.assertEqual(own["metrics"]["equity_change_usd"]["value"], "0.000000000001")
        self.state["reset_epoch"] = 1
        own = comparison(self.envelope, self.store, self.root / "missing", NOW)["own"]
        self.assertIsNone(own["metrics"]["equity_change_usd"]["value"])
        self.assertIsNone(own["metrics"]["net_pnl_usd"]["value"])

    def test_reference_source_window_and_requested_window_remain_distinct(self):
        self.artifact()
        result = comparison(self.envelope, self.store, self.root / "views", NOW, "1h")["reference"]
        self.assertEqual(result["status"], "AVAILABLE")
        self.assertEqual(len(result["actions"]), 1)
        self.assertEqual(result["coverage"]["observed_actions"], 2)
        self.assertEqual(result["metrics"]["turnover_usd"]["value"], "15.000000000001")
        self.assertIsNone(result["metrics"]["net_pnl_usd"]["value"])
        self.assertIn("REFERENCE_OUTSIDE_REQUESTED_PERIOD", result["warnings"])

    def test_republishing_old_source_does_not_make_it_fresh(self):
        self.artifact(source_at="2026-10-08T11:59:00Z", published="2026-10-08T13:59:00Z")
        result = load_reference(self.root / "views", NOW + 7_200_000, None)
        self.assertEqual(result["status"], "STALE")
        self.assertEqual(result["freshness"]["age_ms"], 7_260_000)

    def test_pointer_traversal_and_member_tampering_fail_without_raw_content(self):
        target = self.artifact()
        (target / "view.json").write_text('"private-source-details"')
        result = load_reference(self.root / "views", NOW, None)
        self.assertEqual(result["status"], "INVALID")
        self.assertNotIn("private-source-details", json.dumps(result))
        (target.parent / "current.json").write_text('{"bundle_id":"../../private"}')
        self.assertEqual(load_reference(target.parent, NOW, None)["status"], "INVALID")

    def test_wrong_account_future_action_and_oversized_projection_fail_closed(self):
        for mutate in (lambda view: view.update(account_id="0x" + "1" * 40),
                       lambda view: view["actions"][0].update(at_ms=NOW + 1),
                       lambda view: view.update(actions=view["actions"] * 130)):
            with self.subTest(mutate=mutate):
                target = self.artifact(mutate)
                self.assertEqual(load_reference(target, NOW, None)["status"], "INVALID")

    def test_period_and_exact_decimal_contracts(self):
        with self.assertRaises(ValueError):
            comparison(self.envelope, self.store, self.root / "missing", NOW, "7d")
        self.assertEqual(decimal(-1), "-0.000000000001")
        self.assertIsNone(decimal(True))
        self.assertIsNone(decimal(10**40))

    def test_subset_cannot_promote_incomplete_or_different_period_financial_result(self):
        for whole_complete, selected_first in ((False, NOW - 3_600_000), (True, NOW - 3_600_000)):
            def claimed(view):
                view["coverage"]["financial_complete"] = whole_complete
                view["periods"] = {"1h": {"period": {"start_ms": selected_first, "end_ms": NOW - 60_000},
                    "financial_complete": True, "net_pnl_usd": "999", "observed_actions": 2}}
            self.artifact(claimed)
            result = load_reference(self.root / "views", NOW, NOW - 3_600_000, "1h")
            self.assertEqual(result["status"], "AVAILABLE")
            self.assertFalse(result["coverage"]["financial_complete"])
            self.assertIsNone(result["metrics"]["net_pnl_usd"]["value"])

    def observer_bundle(self, repetitions=2):
        fixtures = Path(archive.__file__).resolve().parent / "tests" / "fixtures"
        captured = load_json(fixtures / "mo-money-activity.json")
        raw_rows, events = [], []
        for index in range(repetitions):
            raw = deepcopy(captured["rows"][index % 2])
            raw["timestamp"] = 1791280800 + index  # 2026-10-06 10:00:00 UTC onward
            raw["transactionHash"] = "0x" + format(index + 1, "064x")
            raw["title"] = "Captured public market"
            item = {"row": raw, "receipt_hash": digest(captured["receipt"]), "observed_at_utc": "2026-10-06T10:05:01Z"}
            raw_rows.append(item)
            events.append(normalize_activity(item, ACCOUNT))
        position_capture = load_json(fixtures / "mo-money-positions.json")
        closing = position_snapshot({"rows": [{"row": row, "observed_at_utc": "2026-10-06T10:05:01Z"} for row in position_capture["rows"]],
            "complete": False, "reasons": ["CAPTURED_TWO_ROW_PAGE_ONLY"]}, ACCOUNT)
        return publish_bundle(self.root / "observer", {"events.jsonl": events, "closing.json": closing,
            "coverage.json": {"activity_complete": False, "financial_complete": False}, "source-activity.jsonl": raw_rows},
            scope={"kind": "one-public-account", "lane": "public-benchmark", "account_id": ACCOUNT, "chain_id": 137},
            utc_range={"start": "2026-10-06T09:00:00Z", "end": "2026-10-06T10:05:00Z"},
            quality=quality("PARTIAL", ["PROFILE_ATTRIBUTION_REVERIFY_REQUIRED"]), created_at_utc="2026-10-06T10:05:01Z")

    def test_verified_captured_observer_adapter_connects_to_console_without_profit_claim(self):
        observer = self.observer_bundle()
        target = publish_reference(observer, self.root / "published")
        (target.parent / "current.json").write_bytes(canonical({"bundle_id": target.name}))
        result = comparison(self.envelope, self.store, target.parent, NOW, "1h")["reference"]
        self.assertEqual(result["status"], "STALE")
        self.assertEqual(len(result["actions"]), 2)
        self.assertEqual(result["actions"][0]["title"], "Captured public market")
        self.assertIn("PROFILE_ATTRIBUTION_REVERIFY_REQUIRED", result["warnings"])
        self.assertEqual(result["metrics"]["observed_events"]["value"], "2")
        self.assertEqual(result["metrics"]["position_value_usd"]["value"], "82.402")
        self.assertIsNone(result["metrics"]["nav_usd"]["value"])
        self.assertIsNone(result["metrics"]["net_pnl_usd"]["value"])

    def test_full_event_period_aggregates_do_not_count_only_the_bounded_action_tail(self):
        observer = self.observer_bundle(300)
        view, _ = build_reference(observer)
        self.assertEqual(len(view["actions"]), 256)
        self.assertEqual(view["periods"]["1h"]["observed_events"], "300")
        self.assertEqual(view["periods"]["1h"]["activity_summary"]["buy_count"], "300")
        target = publish_reference(observer, self.root / "published")
        reference = load_reference(target, NOW, NOW - 3_600_000, "1h")
        self.assertEqual(reference["coverage"]["observed_actions"], 300)
        self.assertEqual(reference["coverage"]["omitted_actions"], 44)
        self.assertFalse(reference["coverage"]["actions_complete"])
        self.assertEqual(reference["metrics"]["observed_events"]["value"], "300")
        self.assertEqual(len(reference["actions"]), 256)

    def test_wrong_observer_identity_or_tampered_member_never_publishes_view(self):
        observer = self.observer_bundle()
        (observer / "coverage.json").write_text('{"financial_complete":true}')
        with self.assertRaises(ResearchError):
            publish_reference(observer, self.root / "not-published")
        self.assertFalse((self.root / "not-published").exists())

    def test_paper_quote_requires_exact_retained_id_same_market_and_no_conflict(self):
        self.state["market"] = {"condition_id": "market-A", "title": "Own synthetic market"}
        self.state["simulation"]["venue_orders"] = [{"id": 2, "quote": {"outcome": "up", "price_micros": 450001}}]
        action = comparison(self.envelope, self.store, self.root / "missing", NOW)["own"]["actions"][0]
        self.assertEqual((action["order_id"], action["side"], action["outcome"], action["price_usd"]), ("2", "BUY", "up", "0.450001"))
        self.assertIsNone(action["amount_usd"])
        self.state["simulation"]["orders"] = [{"id": 2, "quote": {"outcome": "down", "price_micros": 550001}}]
        action = comparison(self.envelope, self.store, self.root / "missing", NOW)["own"]["actions"][0]
        self.assertIsNone(action["price_usd"])
        self.state["market"]["condition_id"] = "market-B"
        action = comparison(self.envelope, self.store, self.root / "missing", NOW)["own"]["actions"][0]
        self.assertIsNone(action["price_usd"])
        self.assertIsNone(action["title"])

    def test_secondary_equity_failure_does_not_hide_current_simulation_or_request_engine(self):
        with patch.object(self.store, "equity_history", side_effect=sqlite3.OperationalError("fixture storage unavailable")):
            own = comparison(self.envelope, self.store, self.root / "missing", NOW)["own"]
        self.assertEqual(own["status"], "AVAILABLE")
        self.assertEqual(own["metrics"]["cash_usd"]["value"], "98.000000000001")
        self.assertEqual(own["equity"], [])
        self.assertIn("SECONDARY_EQUITY_UNAVAILABLE", own["warnings"])
