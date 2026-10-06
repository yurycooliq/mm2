import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.research import patches
from tools.research.schema import ResearchError, digest


def text_patch(path="tools/research/example.py"):
    return (f"diff --git a/{path} b/{path}\n"
            "index 1234567..2345678 100644\n"
            f"--- a/{path}\n+++ b/{path}\n"
            "@@ -1 +1 @@\n-old\n+new\n").encode()


class PatchBoundaryTest(unittest.TestCase):
    def test_scoped_regular_text_proposal_accepted(self):
        result = patches.inspect_patch(text_patch())
        self.assertEqual(result["paths"], ["tools/research/example.py"])
        self.assertEqual(len(result["patch_hash"]), 64)

    def test_execution_policy_broker_and_path_escape_rejected(self):
        for path in ("crates/mm2-engine/src/runtime.rs", "infra/aws/mm2.service", ".github/workflows/checks.yml",
                     "research/policies/autonomy.json", "tools/research/jobs.py", "tools/research/publish.py",
                     "tools/research/patches.py", "tools/research/../private.py", "docs/secret.md"):
            with self.subTest(path=path), self.assertRaises(ResearchError):
                patches.inspect_patch(text_patch(path))

    def test_delete_binary_rename_and_symlink_rejected(self):
        cases = [text_patch().replace(b"+++ b/tools/research/example.py", b"+++ /dev/null"),
                 text_patch().replace(b"index 1234567..2345678 100644", b"index 1234567..2345678 120000"),
                 text_patch().replace(b"index 1234567..2345678 100644", b"deleted file mode 100644"),
                 text_patch().replace(b"index 1234567..2345678 100644", b"GIT binary patch"),
                 text_patch().replace(b"diff --git a/tools/research/example.py b/tools/research/example.py",
                                      b"diff --git a/tools/research/example.py b/tools/research/renamed.py")]
        for data in cases:
            with self.assertRaises(ResearchError):
                patches.inspect_patch(data)

    def test_offline_container_has_no_host_credentials_or_writable_mount(self):
        args = patches.docker_command(patches.VALIDATION_IMAGE, Path("/scratch/candidate"), Path("/scratch/trusted"), ["python", "-B", "-m", "unittest"])
        for pair in (("--network", "none"), ("--cap-drop", "ALL"), ("--security-opt", "no-new-privileges"),
                     ("--user", "65532:65532"), ("--pull", "never")):
            index = args.index(pair[0])
            self.assertEqual(args[index + 1], pair[1])
        self.assertIn("--read-only", args)
        mounts = [args[i + 1] for i, value in enumerate(args) if value == "--mount"]
        self.assertEqual(len(mounts), 2)
        self.assertTrue(all(value.endswith(",readonly") for value in mounts))
        self.assertFalse(any("docker.sock" in value or "/home" in value or ".aws" in value for value in mounts))
        with self.assertRaises(ResearchError):
            patches.docker_command("python:latest", "/a", "/b", ["python"])
        with self.assertRaises(ResearchError):
            patches.docker_command(patches.VALIDATION_IMAGE, "/a,target=/host", "/b", ["python"])

    def test_preview_binds_exact_repository_base_issue_and_patch(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "proposal.patch"
            path.write_bytes(text_patch())
            with patch.object(patches, "git", side_effect=[b"a" * 40 + b"\n", b"https://github.com/yurycooliq/mm2.git\n"]):
                result = patches.preview(temp, path, baseline_sha="a" * 40, issue_number=3,
                                         issue_spec_hash="b" * 64, policy_hash="c" * 64)
            self.assertEqual(result["baseline_sha"], "a" * 40)
            self.assertEqual(result["issue_number"], 3)
            self.assertFalse(result["automatic_merge"])
            with patch.object(patches, "git", side_effect=[b"a" * 40 + b"\n", b"https://github.com/attacker/mm2.git\n"]):
                with self.assertRaises(ResearchError):
                    patches.preview(temp, path, baseline_sha="a" * 40, issue_number=3,
                                    issue_spec_hash="b" * 64, policy_hash="c" * 64)

    def test_dirty_baseline_waits_without_creating_worktree(self):
        plan = {"baseline_sha": "a" * 40, "issue_number": 3, "issue_spec_hash": "b" * 64,
                "policy_hash": "c" * 64, "proposal_id": "d" * 64}
        with patch.object(patches, "preview", return_value=plan), patch.object(patches, "git", return_value=b" M README.md\n") as command:
            with self.assertRaises(ResearchError):
                patches.prepare("/repo", "/patch", plan, "/worktrees")
        self.assertEqual(command.call_count, 1)

    def test_stale_validation_cannot_publish(self):
        body = {"status": "PASS", "candidate_sha": "a" * 40, "baseline_sha": "b" * 40,
                "proposal_id": "c" * 64, "patch_hash": "d" * 64, "policy_hash": "e" * 64,
                "check_principal": "trusted-offline-patch-broker"}
        receipt = {**body, "receipt_hash": digest(body)}
        prepared = {"candidate_sha": "f" * 40, "worktree": "/worktree",
                    "proposal": {"baseline_sha": "b" * 40, "proposal_id": "c" * 64,
                                 "patch_hash": "d" * 64, "policy_hash": "e" * 64}}
        with patch.object(patches, "git") as git, patch.object(patches, "run_bounded") as run:
            with self.assertRaises(ResearchError):
                patches.publish_draft(prepared, receipt, expected_policy_hash="e" * 64, outbox_root="/outbox")
        git.assert_not_called()
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
