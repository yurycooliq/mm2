import copy
from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location("pr_hygiene", Path(__file__).with_name("pr_hygiene.py"))
policy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(policy)
NOW = datetime(2026, 10, 16, 12, tzinfo=timezone.utc)
STAMP = "2026-10-16T10:00:00Z"
HEAD, BASE = "a" * 40, "b" * 40


def pr(number=1, base="main", draft=True, names=()):
    return {"number": number, "state": "open", "draft": draft, "head": {"ref": f"codex/pr-{number}"},
            "base": {"ref": base}, "labels": [{"name": name} for name in names], "created_at": STAMP, "updated_at": STAMP}


def ready_pr():
    return {"number": 1, "state": "OPEN", "isDraft": False, "baseRefName": "main", "baseRefOid": BASE,
            "headRefOid": HEAD, "labels": [], "author": {"login": "yurycooliq"},
            "statusCheckRollup": [{"name": name, "status": "COMPLETED", "conclusion": "SUCCESS"} for name in policy.REQUIRED_CHECKS],
            "reviews": [{"state": "COMMENTED", "commit": {"oid": HEAD}, "author": {"login": "yurycooliq"}, "submittedAt": STAMP,
                         "body": f"MM2 independent review\nHead: {HEAD}\nBase: {BASE}\nReviewer: agent:/root/independent_reviewer\nResult: PASS\n"}]}


class InventoryTests(unittest.TestCase):
    def codes(self, prs, main_at=STAMP):
        return {item["code"] for item in policy.inventory(prs, main_at, NOW)}

    def test_three_draft_experimental_prs_can_be_retained_without_promoting_them(self):
        prs = [pr(names=("experimental",)), pr(2, "codex/pr-1", names=("experimental",)), pr(3, "codex/pr-2", names=("experimental",))]
        self.assertEqual(self.codes(prs), set())

    def test_fourth_open_dependency_is_reported(self):
        prs = [pr(), pr(2, "codex/pr-1"), pr(3, "codex/pr-2"), pr(4, "codex/pr-3")]
        issues = policy.inventory(prs, STAMP, NOW)
        self.assertIn({"pr": 4, "code": "deep_stack", "severity": "error", "message": "Stack exceeds three open PRs; integrate the ready prerequisite before extending it."}, issues)

    def test_ready_dependency_and_experimental_hold_are_reported(self):
        self.assertIn("ready_on_dependency", self.codes([pr(), pr(2, "codex/pr-1", False)]))
        for name in policy.HOLD_LABELS:
            self.assertIn("hold_is_ready", self.codes([pr(draft=False, names=(name,))]))

    def test_closed_or_deleted_prerequisite_requires_retargeting(self):
        self.assertIn("missing_dependency", self.codes([pr(2, "codex/pr-1")]))

    def test_cycle_is_reported_without_recursion_or_hanging(self):
        self.assertIn("dependency_cycle", self.codes([pr(1, "codex/pr-2"), pr(2, "codex/pr-1")]))

    def test_old_main_with_pending_work_is_reported_but_empty_repository_is_quiet(self):
        old = "2026-10-01T00:00:00Z"
        self.assertIn("stale_main", self.codes([pr()], old))
        self.assertEqual(self.codes([], old), set())

    def test_old_ready_is_an_error_and_old_draft_is_a_warning(self):
        for draft, severity, code in [(False, "error", "stale_ready"), (True, "warning", "stale_draft")]:
            item = pr(draft=draft)
            item["updated_at"] = "2026-10-01T00:00:00Z"
            self.assertTrue(any(row["code"] == code and row["severity"] == severity for row in policy.inventory([item], STAMP, NOW)))

    def test_unknown_identity_or_timestamp_never_becomes_a_clean_inventory(self):
        cases = [[pr(), pr()], [dict(pr(), draft=None)], [dict(pr(), updated_at="unknown")],
                 [dict(pr(), head={})], [dict(pr(), labels=None)], [dict(pr(), number=True)]]
        for rows in cases:
            with self.subTest(rows=rows), self.assertRaises((policy.PolicyError, KeyError)):
                policy.inventory(rows, STAMP, NOW)


class ReadinessTests(unittest.TestCase):
    def test_exact_named_agent_review_and_all_required_checks_are_ready(self):
        result = policy.readiness(ready_pr(), BASE)
        self.assertEqual(result["status"], "READY")
        self.assertFalse(result["mutations_performed"])

    def test_human_approval_must_be_another_contributor_at_the_current_head(self):
        item = ready_pr()
        item["reviews"][0].update(state="APPROVED", author={"login": "reviewer"})
        self.assertEqual(policy.readiness(item, BASE)["status"], "READY")
        item["reviews"][0]["author"]["login"] = item["author"]["login"]
        self.assertEqual(policy.readiness(item, BASE)["status"], "HOLD")

    def test_requested_changes_are_not_erased_by_an_agent_comment(self):
        item = ready_pr()
        requested = dict(item["reviews"][0], state="CHANGES_REQUESTED", author={"login": "reviewer"}, submittedAt="2026-10-15T10:00:00Z")
        item["reviews"].insert(0, requested)
        self.assertEqual(policy.readiness(item, BASE)["status"], "HOLD")
        approved = dict(requested, state="APPROVED", submittedAt=STAMP)
        item["reviews"].append(approved)
        self.assertEqual(policy.readiness(item, BASE)["status"], "READY")

    def test_latest_negative_agent_receipt_blocks_old_pass_and_other_agents(self):
        for result in ("FAIL", "HOLD"):
            item = ready_pr()
            prior = item["reviews"][0]
            negative = dict(prior, body=prior["body"].replace("Result: PASS", "Result: " + result), submittedAt="2026-10-16T11:00:00Z")
            item["reviews"].append(negative)
            item["reviews"].append(dict(prior, body=prior["body"].replace("independent_reviewer", "another_reviewer")))
            self.assertEqual(policy.readiness(item, BASE)["status"], "HOLD")
            item["reviews"].append(dict(prior, submittedAt="2026-10-16T12:00:00Z"))
            self.assertEqual(policy.readiness(item, BASE)["status"], "READY")

    def test_malformed_or_ambiguous_current_receipt_never_keeps_old_pass(self):
        for suffix in ("Result: FAIL\n", "Head: " + HEAD + "\n"):
            item = ready_pr()
            prior = item["reviews"][0]
            item["reviews"].append(dict(prior, body=prior["body"] + suffix, submittedAt="2026-10-16T11:00:00Z"))
            with self.assertRaises(policy.PolicyError):
                policy.readiness(item, BASE)

    def test_untrusted_publisher_cannot_spoof_agent_reconciliation(self):
        item = ready_pr()
        original = item["reviews"][0]
        item["reviews"].append(dict(original, body=original["body"].replace("Result: PASS", "Result: FAIL"), submittedAt="2026-10-16T11:00:00Z"))
        for publisher in ({"login": "unrelated-commenter"}, None, {}):
            spoof = dict(original, author=publisher, submittedAt="2026-10-16T12:00:00Z")
            with self.subTest(publisher=publisher):
                candidate = copy.deepcopy(item)
                candidate["reviews"].append(spoof)
                self.assertEqual(policy.readiness(candidate, BASE)["status"], "HOLD")

    def test_fork_author_or_missing_author_is_not_a_trusted_agent_publisher(self):
        for publisher in ({"login": "fork-author"}, None, {}):
            item = ready_pr()
            item["author"]["login"] = "fork-author"
            item["reviews"][0]["author"] = publisher
            self.assertEqual(policy.readiness(item, BASE)["status"], "HOLD")

    def test_missing_failed_pending_skipped_or_billing_blocked_checks_hold(self):
        for change in ("missing", "FAILURE", "SKIPPED", "pending"):
            item = ready_pr()
            if change == "missing":
                item["statusCheckRollup"].pop()
            elif change == "pending":
                item["statusCheckRollup"][0]["status"] = "QUEUED"
            else:
                item["statusCheckRollup"][0]["conclusion"] = change
            self.assertEqual(policy.readiness(item, BASE)["status"], "HOLD")

    def test_stale_head_or_base_and_duplicate_review_bindings_are_rejected(self):
        for change in ("head", "base", "duplicate", "missing", "same-head-new-main"):
            item = ready_pr()
            review = item["reviews"][0]
            main = BASE
            if change == "head":
                review["commit"]["oid"] = "c" * 40
            elif change == "base":
                review["body"] = review["body"].replace(BASE, "c" * 40)
            elif change == "duplicate":
                review["body"] += "Result: PASS\n"
            elif change == "missing":
                item["reviews"] = []
            else:
                main = "c" * 40
            if change == "duplicate":
                with self.assertRaises(policy.PolicyError):
                    policy.readiness(item, main)
            else:
                self.assertEqual(policy.readiness(item, main)["status"], "HOLD")

    def test_draft_stacked_closed_and_experimental_work_cannot_be_ready(self):
        for key, value in [("isDraft", True), ("baseRefName", "codex/dependency"), ("state", "CLOSED"),
                           ("labels", [{"name": "experimental"}]), ("labels", [{"name": "merge-hold"}])]:
            item = ready_pr()
            item[key] = value
            self.assertEqual(policy.readiness(item, BASE)["status"], "HOLD")

    def test_unknown_check_or_revision_evidence_fails_closed(self):
        for key in ("headRefOid", "baseRefOid", "statusCheckRollup", "labels"):
            item = copy.deepcopy(ready_pr())
            del item[key]
            with self.assertRaises(policy.PolicyError):
                policy.readiness(item, BASE)


if __name__ == "__main__":
    unittest.main()
