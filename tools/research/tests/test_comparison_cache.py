from datetime import timedelta
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.research.archive import publish_bundle, verify_bundle
from tools.research.comparison_cache import ACCOUNT, MAX_STORAGE, commit, initialize, preflight, storage_usage
from tools.research.schema import ResearchError, load_json, quality, utc


class ComparisonCacheTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.root = self.base / "dedicated-secondary"
        initialize(self.root)

    def tearDown(self):
        self.temp.cleanup()

    def capture(self, at, root=None):
        root = root or self.root
        end = utc(at)
        source_range = {"start": (end - timedelta(hours=24)).isoformat().replace("+00:00", "Z"), "end": at}
        observer = publish_bundle(root / "hot-observers", {"events.jsonl": [], "closing.json": {"account_id": ACCOUNT},
            "coverage.json": {"activity_complete": False}, "source-activity.jsonl": [{"row": {"sample": at}}],
            "source-positions.jsonl": [], "requests.jsonl": [{"endpoint": "activity"}]},
            scope={"kind": "one-public-account", "lane": "public-benchmark", "account_id": ACCOUNT, "chain_id": 137},
            utc_range=source_range, quality=quality("PARTIAL", ["FIXTURE_ONLY"]), created_at_utc=at)
        view = publish_bundle(root, {"view.json": {"schema_version": 1, "account_id": ACCOUNT,
            "source_bundle_id": observer.name, "source_at_utc": at, "actions": [], "positions": []}},
            scope={"kind": "console-comparison-reference", "lane": "public-benchmark", "account_id": ACCOUNT},
            utc_range=source_range, quality=quality("PARTIAL", ["FIXTURE_ONLY"]), created_at_utc=at)
        return observer, view

    def current(self):
        pointer = load_json(self.root / "current.json")
        return verify_bundle(self.root / pointer["bundle_id"])

    def test_initialize_never_adopts_existing_archive_or_real_source(self):
        legacy = self.base / "legacy"
        legacy.mkdir()
        sentinel = legacy / "events.jsonl"
        sentinel.write_text("preserve original source")
        with self.assertRaisesRegex(ResearchError, "empty root"):
            initialize(legacy)
        self.assertEqual(sentinel.read_text(), "preserve original source")
        with self.assertRaisesRegex(ResearchError, "empty root"):
            initialize(self.root)

    def test_keeps_two_hot_captures_and_full_first_daily_source_immutable(self):
        captures = [self.capture(f"2026-10-08T12:{minute:02}:00Z") for minute in (0, 5, 10)]
        first_bytes = {path.name: path.read_bytes() for path in captures[0][0].iterdir()}
        for observer, view in captures:
            result = commit(self.root, observer, view)
        self.assertEqual(result["hot_capture_count"], 2)
        self.assertEqual(result["deleted_secondary_hot_bundles"], 2)
        self.assertFalse(captures[0][0].exists())
        self.assertFalse(captures[0][1].exists())
        self.assertTrue(all(observer.exists() and view.exists() for observer, view in captures[1:]))
        checkpoint = self.root / "checkpoints" / "2026-10-08"
        preserved = checkpoint / "observer" / captures[0][0].name
        self.assertEqual({path.name: path.read_bytes() for path in preserved.iterdir()}, first_bytes)
        self.assertEqual(self.current()["bundle_id"], captures[-1][1].name)
        self.assertEqual(result["checkpoint_count"], 1)
        self.assertFalse(result["calendar_day_complete"])
        self.assertEqual(result["remote_archive"], "UNVERIFIED")
        self.assertFalse(result["primary_evidence_deleted"])
        preflight(self.root, reserve_bytes=0)

    def test_daily_checkpoint_reports_actual_rolling_window_gap(self):
        for at in ("2026-10-08T00:05:00Z", "2026-10-09T00:07:00Z"):
            observer, view = self.capture(at)
            result = commit(self.root, observer, view)
        self.assertEqual(result["checkpoint_count"], 2)
        self.assertEqual(result["checkpoint_gap_ms"], 120_000)
        receipt = load_json(self.root / "checkpoints" / "2026-10-09" / "checkpoint.json")
        self.assertEqual(receipt["source_range"]["start"], "2026-10-08T00:07:00Z")
        self.assertFalse(receipt["calendar_day_complete"])

    def test_external_sources_are_read_only_and_cannot_be_pruned(self):
        external = self.base / "real-source-fixture"
        observer, view = self.capture("2026-10-08T12:00:00Z", external)
        before = {str(path): path.read_bytes() for path in external.rglob("*") if path.is_file()}
        with self.assertRaisesRegex(ResearchError, "dedicated hot copies"):
            commit(self.root, observer, view)
        self.assertEqual({str(path): path.read_bytes() for path in external.rglob("*") if path.is_file()}, before)

    def test_capacity_exhaustion_retains_pointer_and_audit(self):
        observer, view = self.capture("2026-10-08T12:00:00Z")
        commit(self.root, observer, view)
        previous = (self.root / "current.json").read_bytes()
        with self.assertRaisesRegex(ResearchError, "capacity exhausted"):
            preflight(self.root, reserve_bytes=MAX_STORAGE)
        observer2, view2 = self.capture("2026-10-08T12:05:00Z")
        with patch("tools.research.comparison_cache.storage_usage", return_value=MAX_STORAGE):
            with self.assertRaisesRegex(ResearchError, "capacity exhausted"):
                commit(self.root, observer2, view2)
        self.assertEqual((self.root / "current.json").read_bytes(), previous)
        self.assertTrue(observer.exists())
        self.assertTrue((self.root / "checkpoints" / "2026-10-08" / "checkpoint.json").exists())

    def test_invalid_checkpoint_blocks_pointer_change_and_hot_pruning(self):
        first = self.capture("2026-10-08T12:00:00Z")
        commit(self.root, *first)
        second = self.capture("2026-10-08T12:05:00Z")
        commit(self.root, *second)
        checkpoint_source = self.root / "checkpoints" / "2026-10-08" / "observer" / first[0].name
        (checkpoint_source / "events.jsonl").write_bytes(b"corrupt checkpoint")
        previous = (self.root / "current.json").read_bytes()
        third = self.capture("2026-10-08T12:10:00Z")
        with patch("tools.research.comparison_cache._remove_bundle") as pruning:
            with self.assertRaises(ResearchError):
                commit(self.root, *third)
        pruning.assert_not_called()
        self.assertEqual((self.root / "current.json").read_bytes(), previous)
        self.assertTrue(first[0].exists())

    def test_pruning_failure_preserves_verified_new_current_and_checkpoint(self):
        captures = [self.capture(f"2026-10-08T12:{minute:02}:00Z") for minute in (0, 5, 10)]
        for pair in captures[:2]:
            commit(self.root, *pair)
        with patch("tools.research.comparison_cache._remove_bundle", side_effect=PermissionError("fixture hot lock")):
            with self.assertRaises(PermissionError):
                commit(self.root, *captures[2])
        self.assertEqual(self.current()["bundle_id"], captures[2][1].name)
        self.assertTrue((self.root / "checkpoints" / "2026-10-08" / "checkpoint.json").exists())
        self.assertTrue(captures[0][0].exists())
        preflight(self.root, reserve_bytes=0)
        fourth = self.capture("2026-10-08T12:15:00Z")
        recovered = commit(self.root, *fourth)
        self.assertEqual(recovered["hot_capture_count"], 2)
        self.assertEqual(recovered["deleted_secondary_hot_bundles"], 4)
        self.assertEqual(self.current()["bundle_id"], fourth[1].name)

    def test_link_is_rejected_without_following_or_removing_target(self):
        target = self.base / "outside"
        target.mkdir()
        sentinel = target / "keep"
        sentinel.write_text("untouched")
        link = self.root / "unknown-source-link"
        try:
            link.symlink_to(target, target_is_directory=True)
        except OSError:
            self.skipTest("native symlink unavailable; Linux CI covers link boundary")
        with self.assertRaisesRegex(ResearchError, "link rejected"):
            storage_usage(self.root)
        self.assertEqual(sentinel.read_text(), "untouched")

    def test_unknown_files_and_interrupted_staging_never_enter_prune_plan(self):
        sentinel = self.root / "unknown-source.txt"
        sentinel.write_text("keep unknown evidence")
        staging = self.root / "publishing-owner-investigation"
        staging.mkdir()
        (staging / "keep.json").write_text("{}")
        for minute in (0, 5, 10):
            commit(self.root, *self.capture(f"2026-10-08T12:{minute:02}:00Z"))
        self.assertEqual(sentinel.read_text(), "keep unknown evidence")
        self.assertEqual((staging / "keep.json").read_text(), "{}")

    def test_writer_lock_and_backward_capture_do_not_replace_current(self):
        first = self.capture("2026-10-08T12:05:00Z")
        commit(self.root, *first)
        previous = (self.root / "current.json").read_bytes()
        older = self.capture("2026-10-08T12:00:00Z")
        with self.assertRaisesRegex(ResearchError, "backwards"):
            commit(self.root, *older)
        lock = self.root / "comparison-cache.lock"
        lock.write_text("existing writer")
        newer = self.capture("2026-10-08T12:10:00Z")
        with self.assertRaisesRegex(ResearchError, "writer lock"):
            commit(self.root, *newer)
        self.assertEqual((self.root / "current.json").read_bytes(), previous)


if __name__ == "__main__":
    unittest.main()
