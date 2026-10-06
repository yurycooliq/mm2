import copy
from pathlib import Path
import posixpath
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
        self.assertEqual(args[args.index("--workdir") + 1], "/trusted/output")
        self.assertEqual(args[args.index("--memory") + 1], "768m")
        mounts = [args[i + 1] for i, value in enumerate(args) if value == "--mount"]
        self.assertEqual(len(mounts), 2)
        self.assertTrue(all(value.endswith(",readonly") for value in mounts))
        self.assertFalse(any("docker.sock" in value or "/home" in value or ".aws" in value for value in mounts))
        with self.assertRaises(ResearchError):
            patches.docker_command("python:latest", "/a", "/b", ["python"])
        with self.assertRaises(ResearchError):
            patches.docker_command(patches.VALIDATION_IMAGE, "/a,target=/host", "/b", ["python"])

    def test_trusted_runner_relocates_only_exact_fixture_roots(self):
        namespace = {}
        # Exercise the bootstrap's fixture adapter, never run/discover a fake suite.
        with patch.object(tempfile, "mkdtemp", return_value="/fixture") as underlying, patch.object(patches.os.path, "normpath", posixpath.normpath):
            exec(patches.TRUSTED_TEST_RUNNER.split("suite=", 1)[0], namespace)
            namespace["fixture_mkdtemp"](prefix="case-", dir="/trusted")
            self.assertEqual(underlying.call_args.kwargs["dir"], "/trusted/output")
            namespace["fixture_mkdtemp"](prefix="case-", dir="/trusted/output/archive")
            self.assertEqual(underlying.call_args.kwargs["dir"], "/trusted/output/archive")
            namespace["fixture_mkdtemp"](prefix="case-", dir="/etc/mm2")
            self.assertEqual(underlying.call_args.kwargs["dir"], "/etc/mm2")

    def test_preview_binds_exact_repository_base_issue_and_patch(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "proposal.patch"
            path.write_bytes(text_patch())
            with patch.object(patches, "git", side_effect=[b"a" * 40 + b"\n", b"https://github.com/yurycooliq/mm2.git\n",
                                                           b"codex/telegram-owner-miniapp\n", b"a" * 40 + b"\n"]):
                result = patches.preview(temp, path, baseline_sha="a" * 40, issue_number=3,
                                         issue_spec_hash="b" * 64, policy_hash="c" * 64)
            self.assertEqual(result["baseline_sha"], "a" * 40)
            self.assertEqual(result["issue_number"], 3)
            self.assertEqual(result["baseline_branch"], "codex/telegram-owner-miniapp")
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

    def publication(self):
        plan = {"baseline_sha": "b" * 40, "baseline_branch": "codex/telegram-owner-miniapp", "proposal_id": "c" * 64,
                "patch_hash": "d" * 64, "policy_hash": "e" * 64, "issue_number": 3, "paths": ["tools/research/example.py"]}
        prepared = {"candidate_sha": "a" * 40, "worktree": "/worktree", "branch": "codex/research-3-123456789abc", "proposal": plan}
        body = {"status": "PASS", "candidate_sha": prepared["candidate_sha"], **{key: plan[key] for key in
                ("baseline_sha", "baseline_branch", "proposal_id", "patch_hash", "policy_hash")},
                "check_principal": "trusted-offline-patch-broker"}
        return prepared, {**body, "receipt_hash": digest(body)}

    def git_for_publication(self, remote_line):
        def command(repo, *args, **_):
            if args[:2] == ("rev-parse", "HEAD"):
                return b"a" * 40 + b"\n"
            if args[0] == "status":
                return b""
            if args[0] == "merge-base":
                return b"b" * 40 + b"\n"
            if args[0] == "diff-tree":
                return b"tools/research/example.py\n"
            if args[0] == "ls-remote":
                return remote_line
            if args[0] == "push":
                return b""
            raise AssertionError("unexpected broker command")
        return command

    def test_missing_or_changed_remote_baseline_blocks_before_push_or_pr(self):
        prepared, receipt = self.publication()
        for line in (b"", b"f" * 40 + b"\trefs/heads/codex/telegram-owner-miniapp\n",
                     b"b" * 40 + b"\trefs/heads/main\n"):
            with tempfile.TemporaryDirectory() as temp, patch.object(patches, "git", side_effect=self.git_for_publication(line)) as git, patch.object(patches, "run_bounded") as run:
                with self.assertRaises(ResearchError):
                    patches.publish_draft(prepared, receipt, expected_policy_hash="e" * 64, outbox_root=temp)
                self.assertFalse(any(call.args[1] == "push" for call in git.call_args_list))
                run.assert_not_called()

    def test_pr_targets_exact_verified_baseline_branch_instead_of_main(self):
        import json
        prepared, receipt = self.publication()
        observed = {"headRefOid": "a" * 40, "baseRefName": "codex/telegram-owner-miniapp", "baseRefOid": "b" * 40}
        replies = [b"[]", b"https://github.com/yurycooliq/mm2/pull/3\n", json.dumps(observed).encode()]
        remote = b"b" * 40 + b"\trefs/heads/codex/telegram-owner-miniapp\n"
        with tempfile.TemporaryDirectory() as temp, patch.object(patches, "git", side_effect=self.git_for_publication(remote)) as git, patch.object(patches, "run_bounded", side_effect=replies) as run:
            result = patches.publish_draft(prepared, receipt, expected_policy_hash="e" * 64, outbox_root=temp)
            self.assertEqual(result["status"], "CONFIRMED")
            create = next(call.args[0] for call in run.call_args_list if "create" in call.args[0])
            self.assertEqual(create[create.index("--base") + 1], prepared["proposal"]["baseline_branch"])
            pushes = [call for call in git.call_args_list if call.args[1] == "push"]
            self.assertEqual(len(pushes), 1)
            self.assertTrue(pushes[0].args[-1].endswith(":refs/heads/" + prepared["branch"]))

    def test_existing_pr_on_wrong_base_is_not_confirmed_or_recreated(self):
        import json
        prepared, receipt = self.publication()
        prior = [{"headRefOid": "a" * 40, "baseRefName": "main", "baseRefOid": "b" * 40,
                  "url": "https://github.com/yurycooliq/mm2/pull/3", "state": "OPEN"}]
        remote = b"b" * 40 + b"\trefs/heads/codex/telegram-owner-miniapp\n"
        with tempfile.TemporaryDirectory() as temp, patch.object(patches, "git", side_effect=self.git_for_publication(remote)) as git, patch.object(patches, "run_bounded", return_value=json.dumps(prior).encode()) as run:
            with self.assertRaises(ResearchError):
                patches.publish_draft(prepared, receipt, expected_policy_hash="e" * 64, outbox_root=temp)
            self.assertEqual(run.call_count, 1)
            self.assertFalse(any(call.args[1] == "push" for call in git.call_args_list))


if __name__ == "__main__":
    unittest.main()
