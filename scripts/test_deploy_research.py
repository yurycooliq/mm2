"""Public release tampering/boundary tests; no actual /opt or systemd changes."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))
SPEC = importlib.util.spec_from_file_location("deploy_research", SCRIPTS / "deploy_research.py")
deploy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(deploy)
REVISION = "a" * 40
FILES = ("tools/research/example.py", "infra/aws/mm2-research.service", "infra/aws/mm2-research.timer")


class ResearchDeploymentTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / "opt/mm2-research/releases").mkdir(parents=True)
        (self.root / "etc/systemd/system").mkdir(parents=True)
        self.files_patch = patch.object(deploy, "FILES", FILES)
        self.files_patch.start()

    def tearDown(self):
        self.files_patch.stop()
        self.temp.cleanup()

    def mapped_path(self, value, *parts):
        value = str(value)
        if value.startswith("/opt/") or value.startswith("/etc/"):
            return self.root / value.lstrip("/") / Path(*parts)
        return Path(value, *parts)

    def archive(self, *, content=b"safe committed source", revision=REVISION, extras=(), bad_hash=False):
        bodies = {name: content for name in FILES}
        manifest = {"schema_version": 1, "source_revision": revision, "files": {
            name: {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()} for name, data in bodies.items()}}
        if bad_hash:
            manifest["files"][FILES[0]]["sha256"] = "f" * 64
        bodies["research-release.json"] = json.dumps(manifest).encode()
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode="w:gz") as archive:
            for name, data in (*bodies.items(), *extras):
                member = tarfile.TarInfo(name)
                member.size = len(data)
                archive.addfile(member, io.BytesIO(data))
        return output.getvalue()

    def install(self, data):
        source = self.root / "release.tar.gz"
        source.write_bytes(data)
        with patch.object(Path, "symlink_to", autospec=True) as link, patch.object(deploy.os, "replace") as replace, patch.object(deploy.subprocess, "run", side_effect=self.inactive_command) as command:
            result = deploy.install(source, REVISION, hashlib.sha256(data).hexdigest(),
                                    releases=self.root / "opt/mm2-research/releases", units=self.root / "etc/systemd/system", run=command)
        return result, link, replace, command

    @staticmethod
    def inactive_command(args, **_):
        return subprocess.CompletedProcess(args, 3 if "is-active" in args else 0, stdout="")

    def test_exact_public_release_installs_after_all_hashes_verified(self):
        result, link, replace, command = self.install(self.archive())
        self.assertTrue(result["verified"])
        self.assertFalse(result["started"])
        self.assertEqual((self.root / "opt/mm2-research/releases" / REVISION / FILES[0]).read_bytes(), b"safe committed source")
        self.assertEqual(link.call_count, 1)
        self.assertEqual(replace.call_count, 1)
        self.assertFalse(any("start" in call.args[0] for call in command.call_args_list))

    def test_bad_member_hash_identity_and_traversal_never_activate(self):
        for data in (self.archive(bad_hash=True), self.archive(revision="b" * 40), self.archive(extras=[("../../private.env", b"evil")])):
            with self.subTest(hash=hashlib.sha256(data).hexdigest()):
                with self.assertRaises(ValueError):
                    self.install(data)
                self.assertFalse((self.root / "opt/mm2-research/releases" / REVISION).exists())

    def test_duplicate_tar_member_rejected_before_writing(self):
        with self.assertRaises(ValueError):
            self.install(self.archive(extras=[(FILES[0], b"duplicate")]))
        self.assertFalse((self.root / "opt/mm2-research/releases" / REVISION).exists())

    def test_existing_release_never_overwritten(self):
        destination = self.root / "opt/mm2-research/releases" / REVISION
        destination.mkdir()
        (destination / "retained.txt").write_text("keep")
        with self.assertRaises(ValueError):
            self.install(self.archive())
        self.assertEqual((destination / "retained.txt").read_text(), "keep")

    def test_mutating_input_after_hash_cannot_change_verified_executable_bytes(self):
        source = self.root / "release.tar.gz"
        original = self.archive()
        source.write_bytes(original)
        actual_open = tarfile.open
        mutated = False
        replacement = self.archive(content=b"untrusted replacement")

        def open_after_mutation(*args, **kwargs):
            nonlocal mutated
            if not mutated:
                mutated = True
                source.write_bytes(replacement)
            return actual_open(*args, **kwargs)

        with patch.object(Path, "symlink_to", autospec=True), patch.object(deploy.os, "replace"), patch.object(deploy.tarfile, "open", side_effect=open_after_mutation):
            deploy.install(source, REVISION, hashlib.sha256(original).hexdigest(),
                           releases=self.root / "opt/mm2-research/releases", units=self.root / "etc/systemd/system", run=self.inactive_command)
        self.assertEqual((self.root / "opt/mm2-research/releases" / REVISION / FILES[0]).read_bytes(), b"safe committed source")

    def test_pack_reads_commit_objects_instead_of_racing_workspace(self):
        for name in FILES:
            source = self.root / name
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_bytes(b"uncommitted race")

        def command(args, **kwargs):
            output = "" if "status" in args else (REVISION if "rev-parse" in args else "committed source")
            return subprocess.CompletedProcess(args, 0, stdout=output if kwargs.get("text") else output.encode())

        target = self.root / "packed.tar.gz"
        with patch.object(deploy, "ROOT", self.root), patch.object(deploy.subprocess, "run", side_effect=command):
            deploy.pack(target)
        with tarfile.open(target, "r:gz") as archive:
            self.assertEqual(archive.extractfile(FILES[0]).read(), b"committed source")

    def test_active_service_blocks_before_release_write_or_activation(self):
        source = self.root / "release.tar.gz"
        data = self.archive()
        source.write_bytes(data)
        with patch.object(deploy.os, "replace") as replace:
            with self.assertRaises(ValueError):
                deploy.install(source, REVISION, hashlib.sha256(data).hexdigest(),
                               releases=self.root / "opt/mm2-research/releases", units=self.root / "etc/systemd/system",
                               run=lambda args, **_: subprocess.CompletedProcess(args, 0))
        replace.assert_not_called()
        self.assertFalse((self.root / "opt/mm2-research/releases" / REVISION).exists())


if __name__ == "__main__":
    unittest.main()
