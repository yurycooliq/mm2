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
    def test_expired_access_is_separate_from_engine_and_precedes_historical_faults(self):
        access = {"captured_at_utc": "2026-10-08T00:00:30Z", "profile": "mm2",
                  "region": "eu-west-1", "status": "EXPIRED", "identity_arn": "quarantined"}
        capture = {"captured_at_utc": "2026-10-07T00:00:30Z", "hours": 1,
                   "incidents": [{"source": "docker_engine", "message": "diagnostic_code=feed_queue_unavailable source=binance fatal=true"}]}
        bundle = self.bundle()
        report = build_report(bundle, cutoff_utc="2026-10-08T00:01:00Z", access=access, diagnostic_snapshot=capture)
        without_access = build_report(bundle, cutoff_utc="2026-10-08T00:01:00Z", diagnostic_snapshot=capture)
        self.assertNotEqual(report["report_id"], without_access["report_id"])
        self.assertEqual(report["research_access"]["age_seconds"], 30)
        self.assertEqual(report["recent_diagnostics"]["age_seconds"], 86430)
        self.assertIn("AWS-доступ", report["next_actions"][0]["action"])
        self.assertIsNone(report["engine_snapshot"]["current_health"])
        self.assertNotIn("quarantined", str(report))
        for text in (render_full(report), render_telegram(report)):
            self.assertIn("EXPIRED", text)
            self.assertIn("86430", text)
            self.assertIn("не доказывает остановку engine", text)
            self.assertNotIn("свеж", text.lower())
        self.assertLess(len(render_telegram(report)), 4096)

    def test_future_or_invalid_access_snapshot_is_rejected(self):
        access = {"captured_at_utc": "2026-10-07T00:00:30Z", "profile": "mm2", "region": "eu-west-1", "status": "AUTHENTICATED"}
        bundle = self.bundle()
        for change in ({"captured_at_utc": "2026-10-07T00:02:00Z"}, {"status": "HEALTHY"},
                       {"profile": "untrusted\ntext"}, {"region": "untrusted"}):
            with self.subTest(change=change), self.assertRaises(ResearchError):
                build_report(bundle, cutoff_utc="2026-10-07T00:01:00Z", access={**access, **change})

    def test_authenticated_access_never_proves_engine_health(self):
        access = {"captured_at_utc": "2026-10-07T00:00:30+00:00", "profile": "mm2", "region": "eu-west-1", "status": "AUTHENTICATED"}
        report = build_report(self.bundle(), cutoff_utc="2026-10-07T00:01:00Z", access=access)
        self.assertIsNone(report["engine_snapshot"]["current_health"])
        self.assertIsNone(report["metrics"]["live_bot_net_pnl"]["value"])
        self.assertNotIn("AWS-доступ", report["next_actions"][0]["action"])

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
        bundle = self.bundle()
        report = build_report(bundle, cutoff_utc="2026-10-07T00:01:00Z")
        later = build_report(bundle, cutoff_utc="2026-10-07T00:02:00Z")
        self.assertNotEqual(report["report_id"], later["report_id"])
        self.assertEqual(report["packet_age_seconds"], 59)
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

    def test_allowlisted_engine_snapshot_has_time_and_never_becomes_daily_uptime_or_profit(self):
        engine = {"captured_at_utc": "2026-10-07T00:00:30+00:00", "health": {"mode": "paused", "execution_mode": "simulation",
                  "api_ready": True, "feed_ready": True, "unapproved_internal_field": "quarantined"},
                  "geoblock": {"blocked": True, "country": "IE", "region": "L", "ip": "quarantined"}}
        report = build_report(self.bundle(), cutoff_utc="2026-10-07T00:01:00Z", engine=engine)
        self.assertEqual(report["engine_snapshot"]["age_seconds"], 30)
        self.assertIn("simulation / paused", render_full(report))
        self.assertNotIn("quarantined", str(report))
        self.assertIsNone(report["engine_snapshot"]["current_health"])
        self.assertIsNone(report["metrics"]["live_bot_net_pnl"]["value"])

    def test_recent_diagnostics_prioritize_fatal_feed_failure_without_claiming_crashes(self):
        capture = {"captured_at_utc": "2026-10-07T00:00:30Z", "unit": "mm2.service", "hours": 1,
                   "systemd": {"NRestarts": "19", "private_field": "quarantined"},
                   "incidents": [{"source": "docker_engine", "message": "diagnostic_code=feed_queue_unavailable source=binance fatal=true"}] * 10}
        report = build_report(self.bundle(), cutoff_utc="2026-10-07T00:01:00Z", diagnostic_snapshot=capture)
        self.assertEqual(report["recent_diagnostics"]["observed_rows"], 10)
        self.assertEqual(report["recent_diagnostics"]["systemd"], {"NRestarts": "19"})
        self.assertIsNone(report["recent_diagnostics"]["crash_episode_count"])
        self.assertIn("fatal feed", report["next_actions"][0]["action"])
        self.assertIn("NRestarts=19", render_full(report))
        self.assertIn("fatal feed", render_telegram(report))
        self.assertNotIn("quarantined", str(report))
        with self.assertRaises(ResearchError):
            build_report(self.bundle(), cutoff_utc="2026-10-07T00:00:10Z", diagnostic_snapshot=capture)


if __name__ == "__main__":
    unittest.main()
