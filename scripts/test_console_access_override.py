"""Console-only overlay safety: exact source, IDs and no engine/proxy operations."""
import copy
from contextlib import nullcontext
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
                                     run=self.fake_run, collect=collect, access=lambda cid, run: access or self.access,
                                     guard=lambda settings, run: nullcontext())

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
        self.assertFalse(any(args[:2] != ["docker", "ps"] for args in self.calls))

    def test_unrelated_engine_or_console_release_is_never_modified(self):
        for role in ("engine", "console"):
            before = copy.deepcopy(self.before)
            before[role]["image"] = "sha256:" + "f" * 64
            self.assertFalse(self.reconcile(before=before)["applicable"])
        self.assertFalse(any(args[:2] != ["docker", "ps"] for args in self.calls))

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

    def test_guard_is_never_started_has_no_compose_hash_and_is_removed_on_failure(self):
        calls = []
        def fake(args, **kwargs):
            calls.append(args)
            return "d" * 64 if args[:2] == ["docker", "create"] else ""
        sleep = Mock()
        with self.assertRaisesRegex(ValueError, "fixture failure"):
            with overlay.update_guard(self.settings, fake, sleep):
                raise ValueError("fixture failure")
        sleep.assert_called_once_with(2.5)
        self.assertEqual(calls[0][:2], ["docker", "create"])
        self.assertIn("com.docker.compose.service=console", calls[0])
        self.assertFalse(any("config-hash" in item for item in calls[0]))
        self.assertNotIn("--mount", calls[0])
        self.assertEqual(calls[-1], ["docker", "container", "rm", "d" * 64])
        self.assertFalse(any("start" in args or "stop" in args or "pause" in args for args in calls))

    def test_stale_guard_cleanup_requires_exact_owned_stopped_container(self):
        labels = {overlay.GUARD_LABEL: self.settings["console_commit"], "com.docker.compose.project": overlay.PROJECT,
                  "com.docker.compose.service": "console", "com.docker.compose.oneoff": "False"}
        value = {"name": "/" + overlay.GUARD_NAME, "image": self.settings["console_image"], "running": False, "labels": labels}
        for valid in (False, True):
            state = copy.deepcopy(value)
            state["running"] = not valid
            calls = []
            def fake(args, **kwargs):
                calls.append(args)
                return "d" * 64 if args[:2] == ["docker", "ps"] else json.dumps(state)
            if valid:
                overlay.cleanup_guard(self.settings, fake)
                self.assertEqual(calls[-1], ["docker", "container", "rm", "d" * 64])
            else:
                with self.assertRaises(ValueError):
                    overlay.cleanup_guard(self.settings, fake)
                self.assertFalse(any("rm" in args for args in calls))

    def test_inventory_ignores_only_a_valid_stopped_guard(self):
        values = {character * 64: {**self.before[role], "name": "/fixture-" + role, "guard": "", "compose_hash": "fixture"}
                  for role, character in (("engine", "e"), ("console", "f"), ("proxy", "c"))}
        values["d" * 64] = {"id": "d" * 64, "image": self.settings["console_image"], "running": False,
            "name": "/" + overlay.GUARD_NAME, "guard": self.settings["console_commit"], "compose_hash": ""}
        rows = "\n".join(cid + " " + role for cid, role in (("e" * 64, "engine"), ("f" * 64, "console"),
                                                          ("c" * 64, "proxy"), ("d" * 64, "console")))
        def fake(args, **kwargs):
            self.assertFalse(any("label!=" in item for item in args))
            return rows if args[:2] == ["docker", "ps"] else json.dumps(values[args[-1]])
        self.assertEqual(set(overlay.inventory(fake)), overlay.ROLES)
        values["d" * 64]["running"] = True
        with self.assertRaises(ValueError):
            overlay.inventory(fake)


if __name__ == "__main__":
    unittest.main()
