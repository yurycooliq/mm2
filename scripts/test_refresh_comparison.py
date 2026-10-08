from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.refresh_comparison import MAX_STORAGE, refresh, storage_usage
from tools.research.comparison_cache import initialize
from tools.research.archive import verify_bundle
from tools.research.registry import DEFAULT_ACCOUNTS
from tools.research.schema import ResearchError, load_json, quality


class Client:
    def __init__(self, receipts=True):
        self.receipts = [{"endpoint": "activity"}] if receipts else []
        self.request_count, self.bytes_read = 1, 2


def observed():
    return {"events": [], "activity_rows": [], "position_rows": [],
        "snapshot": {"schema_version": 1, "account_id": DEFAULT_ACCOUNTS["mo-money"]["account_id"],
            "lane": "public-benchmark", "at_utc": "2026-10-08T01:00:01Z", "cash_pico": None,
            "reserved_pico": None, "positions": [], "positions_complete": True,
            "financial_complete": False, "quality": quality("PARTIAL", ["PUBLIC_CASH_UNKNOWN"])},
        "coverage": {"activity_complete": True, "positions_page_complete": True,
            "gaps": [], "invalid_rows": [], "financial_complete": False},
        "quality": quality("PARTIAL", ["PUBLIC_CASH_UNKNOWN", "PROFILE_ATTRIBUTION_REVERIFY_REQUIRED"])}


class RefreshComparisonTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "view"
        self.clock = datetime(2026, 10, 8, 1, tzinfo=timezone.utc)
    def tearDown(self):
        self.temp.cleanup()

    def capture(self, client=None):
        with patch("scripts.refresh_comparison.observe_account", return_value=observed()):
            return refresh(self.root, hours=1, now=self.clock, client=client or Client())

    def test_verified_success_switches_pointer_and_retains_source_evidence(self):
        result = self.capture()
        pointer = load_json(self.root / "current.json")
        self.assertEqual(pointer["bundle_id"], result["bundle_id"])
        manifest = verify_bundle(self.root / pointer["bundle_id"])
        self.assertEqual(manifest["utc_range"]["end"], "2026-10-08T01:00:00Z")
        self.assertEqual(verify_bundle(self.root / "hot-observers" / result["observer_bundle_id"])["scope"]["chain_id"], 137)
        self.assertEqual(result["live_orders"], 0)
        self.assertFalse(result["coverage"]["financial_complete"])

    def test_failed_activity_never_replaces_last_view_with_fresh_zero(self):
        self.capture()
        previous = (self.root / "current.json").read_bytes()
        with self.assertRaisesRegex(ResearchError, "activity unavailable"):
            self.capture(Client(receipts=False))
        self.assertEqual((self.root / "current.json").read_bytes(), previous)

    def test_invalid_view_publication_cannot_switch_existing_pointer(self):
        self.capture()
        previous = (self.root / "current.json").read_bytes()
        with patch("scripts.refresh_comparison.publish_reference", side_effect=ResearchError("fixture")):
            with self.assertRaises(ResearchError):
                self.capture()
        self.assertEqual((self.root / "current.json").read_bytes(), previous)

    def test_storage_quota_stops_before_network_without_removing_evidence(self):
        initialize(self.root)
        sentinel = self.root / "keep"
        sentinel.write_text("preserve")
        with patch("scripts.refresh_comparison.preflight", side_effect=ResearchError("comparison cache capacity exhausted")), \
                patch("scripts.refresh_comparison.observe_account") as network:
            with self.assertRaisesRegex(ResearchError, "capacity exhausted"):
                refresh(self.root, now=self.clock)
        network.assert_not_called()
        self.assertEqual(sentinel.read_text(), "preserve")

    def test_invalid_existing_pointer_retained_for_investigation(self):
        initialize(self.root)
        (self.root / "current.json").write_text('{"bundle_id":"../../escape"}')
        with self.assertRaisesRegex(ResearchError, "pointer not indexed"):
            self.capture()
        self.assertIn("../../escape", (self.root / "current.json").read_text())

    def test_invalid_window_and_non_utc_clock_fail_before_capture(self):
        with patch("scripts.refresh_comparison.observe_account") as network:
            for hours in (0, 25, True):
                with self.assertRaises(ResearchError):
                    refresh(self.root, hours=hours, now=self.clock)
            with self.assertRaisesRegex(ResearchError, "requires UTC"):
                refresh(self.root, now=datetime(2026, 10, 24, tzinfo=timezone(timedelta(hours=1))))
        network.assert_not_called()

    def test_storage_root_must_be_directory(self):
        self.root.write_text("keep")
        with self.assertRaises((ResearchError, OSError)):
            storage_usage(self.root)


if __name__ == "__main__":
    unittest.main()
