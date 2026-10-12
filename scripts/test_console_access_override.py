"""Console-only overlay safety: exact source, IDs and no engine/proxy operations."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import console_access_override as overlay


class ConsoleAccessOverrideTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.release = Path(self.temp.name) / ("a" * 40)
        self.release.mkdir()
        (self.release / "release-manifest.json").write_bytes(b"{}")
        self.settings = {"schema": 1, "base_commit": "a" * 40, "console_commit": "b" * 40,
            "base_manifest_sha256": hashlib.sha256(b"{}").hexdigest(), "profile_sha256": hashlib.sha256(b"profile").hexdigest(),
            "engine_image": "sha256:" + "1" * 64, "old_console_image": "sha256:" + "2" * 64,
            "console_image": "sha256:" + "3" * 64, "proxy_image": "sha256:" + "4" * 64,
            "owner_id": 42, "additional_ids": [1401626578]}
        self.before = {role: {"id": role, "image": self.settings["old_console_image" if role == "console" else role + "_image"],
                              "started_at": "original", "running": True} for role in overlay.ROLES}
        self.after = copy.deepcopy(self.before)
        self.after["console"].update(id="new-console", image=self.settings["console_image"], started_at="new")
        self.access = {"owner_id": 42, "users": [42, 1401626578]}
        self.calls = []

    def tearDown(self):
        self.temp.cleanup()

    def fake_run(self, args, **kwargs):
        self.calls.append(args)
        return self.settings["console_commit"] if args[:3] == ["docker", "image", "inspect"] else ""

    def reconcile(self, before=None, after=None, access=None, runtime=None, overlay_bytes=None):
        payload = {"services": {"console": {"image": self.settings["console_image"], "environment": {
            "MM2_TELEGRAM_ALLOWED_IDS": "1401626578"}}}}
        encoded = (json.dumps(payload, sort_keys=True) + "\n").encode("ascii")
        runtime = runtime or b"MM2_TELEGRAM_OWNER_ID=42\nMM2_TELEGRAM_BOT_TOKEN=offline-private-fixture\n"
        reads = lambda path, bound: (overlay_bytes if overlay_bytes is not None else encoded) if str(path) == "overlay" else runtime
        collect = Mock(side_effect=[before or self.before, after or self.after])
        profile = Mock(); profile.read_bytes.return_value = b"profile"
        with patch.object(overlay, "Path", return_value=profile), patch.object(overlay, "private_file", side_effect=reads):
            return overlay.reconcile(self.settings, self.release, Path("runtime"), Path("overlay"),
                                     run=self.fake_run, collect=collect, access=lambda cid, run: access or self.access)

    def test_only_console_is_recreated_and_primary_identities_are_preserved(self):
        result = self.reconcile()
        self.assertTrue(result["changed"])
        self.assertFalse(result["engine_touched"])
        self.assertEqual(result["before"]["engine"], result["after"]["engine"])
        commands = [args for args in self.calls if args[:2] == ["docker", "compose"]]
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0][-7:], ["up", "--detach", "--no-deps", "--no-build", "--pull", "never", "console"])
        self.assertIn("--project-name", commands[0])
        self.assertNotIn("down", commands[0])

    def test_matching_console_is_a_noop(self):
        result = self.reconcile(before=self.after)
        self.assertFalse(result["changed"])
        self.assertEqual(self.calls, [])

    def test_unrelated_engine_or_console_release_is_never_modified(self):
        for role in ("engine", "console"):
            before = copy.deepcopy(self.before)
            before[role]["image"] = "sha256:" + "f" * 64
            self.assertFalse(self.reconcile(before=before)["applicable"])
        self.assertEqual(self.calls, [])

    def test_wrong_owner_or_overlay_aborts_before_compose(self):
        for values in ({"runtime": b"MM2_TELEGRAM_OWNER_ID=99\nMM2_TELEGRAM_BOT_TOKEN=offline\n"}, {"overlay_bytes": b"{}"}):
            with self.assertRaises(ValueError):
                self.reconcile(**values)
        self.assertFalse(any(args[:2] == ["docker", "compose"] for args in self.calls))

    def test_primary_change_during_console_update_is_reported(self):
        after = copy.deepcopy(self.after)
        after["engine"]["started_at"] = "changed-externally"
        with self.assertRaisesRegex(ValueError, "primary identity"):
            self.reconcile(after=after)

    def test_malformed_ids_and_mutable_images_are_rejected(self):
        for key, value in (("additional_ids", [True]), ("additional_ids", [{}]), ("additional_ids", [1, 1]),
                           ("owner_id", 0), ("console_image", "latest"), ("console_commit", "main")):
            invalid = {**self.settings, key: value}
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                overlay.checked_settings(invalid)


if __name__ == "__main__":
    unittest.main()
