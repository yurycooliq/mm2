"""Reports must preserve financial uncertainty and agree across both views."""
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

from tools.research.archive import publish_bundle
from tools.research.control_report import build_report, render_full, render_telegram
from tools.research.schema import ResearchError, quality

ROOT = Path(__file__).resolve().parents[3]


class ControlReportTests(unittest.TestCase):
    def setUp(self):
        (ROOT / "output").mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=ROOT / "output", prefix="report-test-")
        self.accounts = {name: {"lane": "public-benchmark", "event_count": 19, "net_pnl_pico": None,
                               "quality": quality("PARTIAL", ["OPENING_STATE_UNKNOWN"]),
                               "components": {"conservative_cost_bound_pico": "0"},
                               "utc_range": {"start": "2026-10-06T00:00:00Z", "end": "2026-10-07T00:00:00Z"}}
                         for name in ("mm2-public", "mo-money")}

    def tearDown(self):
        self.temp.cleanup()

    def bundle(self, accounts=None):
        return publish_bundle(Path(self.temp.name), {"accounting-summary.json": accounts or self.accounts,
                              "diagnostics.json": {"incidents": [{"category": "oom"}] * 19},
                              "health.json": {"connected": True}, "provenance.json": {"diagnostic_backlog": 3}},
                              scope={"kind": "research-packet", "lane": "public-benchmark"},
                              utc_range=self.accounts["mm2-public"]["utc_range"],
                              quality=quality("PARTIAL"), created_at_utc="2026-10-07T00:00:01Z")

    def test_legacy_zero_costs_incident_rows_and_connected_snapshot_never_invent_truth(self):
        report = build_report(self.bundle(), cutoff_utc="2026-10-07T00:01:00Z")
        self.assertIsNone(report["account_observations"]["mm2-public"]["declared_event_cost_bound"]["value"])
        self.assertIsNone(report["diagnostics"]["crash_episode_count"])
        self.assertIsNone(report["engine_snapshot"]["current_health"])
        self.assertIsNone(report["metrics"]["live_bot_net_pnl"]["value"])
        self.assertEqual(report["profitability_verdict"], "INCONCLUSIVE")

    def test_positive_complete_wallet_result_cannot_become_mm2_profit(self):
        accounts = deepcopy(self.accounts)
        accounts["mm2-public"].update(quality=quality("COMPLETE", accounting=True), net_pnl_pico="2000000000000")
        report = build_report(self.bundle(accounts), cutoff_utc="2026-10-07T00:01:00Z")
        self.assertEqual(report["account_observations"]["mm2-public"]["wallet_net_pnl"]["value"], "2000000000000")
        self.assertIsNone(report["metrics"]["live_bot_net_pnl"]["value"])
        self.assertEqual(report["profitability_verdict"], "INCONCLUSIVE")
        self.assertIn("это не прибыль MM2", render_full(report))

    def test_full_and_telegram_views_share_cutoff_identity_and_unknown_live_result(self):
        report = build_report(self.bundle(), cutoff_utc="2026-10-07T00:01:00Z")
        for text in (render_full(report), render_telegram(report)):
            self.assertIn(report["report_id"], text)
            self.assertIn(report["display_cutoff"], text)
            self.assertIn("неизвестно", text)
        self.assertLess(len(render_telegram(report)), 4096)
        self.assertEqual(report["display_cutoff"], "2026-10-07T01:01:00+01:00")

    def test_packet_and_controller_observed_after_cutoff_are_rejected(self):
        bundle = self.bundle()
        with self.assertRaises(ResearchError):
            build_report(bundle, cutoff_utc="2026-10-07T00:00:00Z")
        with self.assertRaises(ResearchError):
            build_report(bundle, cutoff_utc="2026-10-07T00:01:00Z",
                         controller={"captured_at_utc": "2026-10-07T00:02:00Z", "status": {}})

    def test_complete_declared_bound_requires_new_coverage_and_preserves_zero(self):
        accounts = deepcopy(self.accounts)
        accounts["mm2-public"]["components"]["cost_evidence_coverage"] = {
            "source_window_complete": True, "missing_cost_bound": 0, "cost_bound_records": 19}
        report = build_report(self.bundle(accounts), cutoff_utc="2026-10-07T00:01:00Z")
        self.assertEqual(report["account_observations"]["mm2-public"]["declared_event_cost_bound"]["value"], "0")


if __name__ == "__main__":
    unittest.main()
