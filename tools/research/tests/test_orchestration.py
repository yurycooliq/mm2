import copy
import concurrent.futures
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import tempfile
import threading
import unittest

from tools.research.archive import publish_bundle, verify_bundle
from tools.research.controller import DEFAULT_POLICY, bundle_inputs, enqueue, load_policy, prepare_draft, ready_code, enqueue_coding
from tools.research.experiments import (candidate_receipt, coding_ticket, register_dataset,
                                       register_experiment, score_experiment, start_experiment)
from tools.research.github import render_issue, validate_issue
from tools.research.jobs import JobStore
from tools.research.publish import publish, reconcile
from tools.research.retention import apply, initialize_root, preview
from tools.research.schema import ResearchError, canonical, digest

REPO_ROOT = Path(__file__).resolve().parents[3]
POLICY_HASH = "a" * 64


class Fixture(unittest.TestCase):
    def setUp(self):
        (REPO_ROOT / "output").mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="orchestration-tests-", dir=REPO_ROOT / "output")
        self.root = Path(self.temp.name)
        self.now = int(datetime(2026, 9, 30, tzinfo=timezone.utc).timestamp())
        self.store = JobStore(self.root / "jobs.sqlite3", clock=lambda: self.now)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def lease(self, stage="analysis", inputs=None):
        self.store.enqueue(stage, inputs or {"bundle_id": "b" * 64}, POLICY_HASH)
        return self.store.claim(stage, "desktop", "run-1", lease_seconds=120)

    def bundle(self):
        return publish_bundle(self.root / "archive", {"diagnostics.json": {"safe": True}},
                              scope={"kind": "diagnostic"}, utc_range={"start": "2026-10-01T00:00:00Z", "end": "2026-10-02T00:00:00Z"},
                              quality={"status": "PARTIAL", "eligibility": {}, "reasons": ["finite-tail"]})


class JobsTest(Fixture):
    def test_enqueue_dedup_is_content_addressed_and_durable(self):
        a = self.store.enqueue("analysis", {"value": 1}, POLICY_HASH)
        b = self.store.enqueue("analysis", {"value": 1}, POLICY_HASH)
        c = self.store.enqueue("analysis", {"value": 2}, POLICY_HASH)
        self.assertEqual(a["id"], b["id"])
        self.assertNotEqual(a["id"], c["id"])
        with JobStore(self.root / "jobs.sqlite3") as reopened:
            self.assertEqual(len(reopened.list()), 2)

    def test_two_connections_claim_one_generation(self):
        self.store.enqueue("analysis", {"value": 1}, POLICY_HASH)
        barrier = threading.Barrier(2)

        def claim(owner):
            with JobStore(self.root / "jobs.sqlite3", clock=lambda: self.now) as other:
                barrier.wait()
                return other.claim("analysis", owner, "parallel-run")

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            values = list(pool.map(claim, ["worker-a", "worker-b"]))
        self.assertEqual(sum(value is not None for value in values), 1)

    def test_expired_worker_is_fenced_from_checkpoint_finish_and_dispatch(self):
        old = self.lease()
        effect = self.store.prepare_publication(old, "yurycooliq/mm2", "c" * 64, {"title": "x", "body": "y", "labels": []})
        self.now += 121
        new = self.store.claim("analysis", "replacement", "run-2", lease_seconds=120)
        self.assertEqual(new["generation"], old["generation"] + 1)
        for action in (lambda: self.store.checkpoint(old, "stale", {}),
                       lambda: self.store.finish(old, "SUCCEEDED", {}),
                       lambda: self.store.dispatch_publication(old, effect["id"])):
            with self.assertRaises(ResearchError):
                action()
        adopted = self.store.prepare_publication(new, "yurycooliq/mm2", "c" * 64, {"title": "x", "body": "y", "labels": []})
        self.assertEqual(adopted["generation"], new["generation"])

    def test_dispatched_unknown_blocks_reclaim_and_finishing(self):
        ticket = self.lease()
        effect = self.store.prepare_publication(ticket, "yurycooliq/mm2", "c" * 64, {"title": "x", "body": "y", "labels": []})
        self.store.dispatch_publication(ticket, effect["id"])
        with self.assertRaises(ResearchError):
            self.store.finish(ticket, "SUCCEEDED", {})
        self.now += 121
        self.assertIsNone(self.store.claim("analysis", "replacement", "run-2"))
        self.store.publication_unknown(effect["id"])
        self.assertEqual(self.store.get(ticket["job_id"])["status"], "WAITING_ACCESS")
        with self.assertRaises(ResearchError):
            self.store.wake(ticket["job_id"], "blind-retry")

    def test_checkpoint_is_immutable(self):
        ticket = self.lease()
        self.store.checkpoint(ticket, "plan", {"value": 1})
        self.store.checkpoint(ticket, "plan", {"value": 1})
        with self.assertRaises(ResearchError):
            self.store.checkpoint(ticket, "plan", {"value": 2})

    def test_no_model_fallback_or_financial_stage(self):
        self.store.enqueue("analysis", {}, POLICY_HASH)
        with self.assertRaises(ResearchError):
            self.store.claim("analysis", "desktop", "run", model="gpt-6-luna")
        with self.assertRaises(ResearchError):
            self.store.enqueue("funded-trading", {}, POLICY_HASH)

    def test_attempt_budget_preserves_waiting_job(self):
        ticket = self.lease()
        for attempt in (2, 3):
            self.now += 121
            ticket = self.store.claim("analysis", "desktop", f"run-{attempt}", lease_seconds=120)
        self.now += 121
        self.assertIsNone(self.store.claim("analysis", "desktop", "run-4"))
        self.assertEqual(self.store.get(ticket["job_id"])["status"], "WAITING_REVIEW")

    def test_crash_fingerprint_ignores_report_date_but_tracks_epoch(self):
        a = self.store.register_crash("transport", "epoch-a", "b" * 64, {"time": 1})
        b = self.store.register_crash("transport", "epoch-a", "b" * 64, {"time": 2})
        c = self.store.register_crash("transport", "epoch-b", "b" * 64, {"time": 2})
        self.assertEqual(a["fingerprint"], b["fingerprint"])
        self.assertEqual(b["observed_count"], 2)
        self.assertNotEqual(a["fingerprint"], c["fingerprint"])


class FakeGitHub:
    repo = "yurycooliq/mm2"
    timeout = 30

    def __init__(self, lose_response=False):
        self.remote = []
        self.posts = 0
        self.lose_response = lose_response

    def all_issues(self):
        return copy.deepcopy(self.remote)

    def create_issue(self, request):
        self.posts += 1
        remote = {**request, "number": self.posts, "html_url": f"https://github.com/{self.repo}/issues/{self.posts}", "state": "closed"}
        self.remote.append(remote)
        if self.lose_response:
            raise ResearchError("response lost after remote commit")
        return remote


class PublicationTest(Fixture):
    def publication(self):
        ticket = self.lease()
        fingerprint = "c" * 64
        request = {"title": "Bootstrap gap", "body": f"<!-- mm2-research:{fingerprint} -->\nEvidence", "labels": ["research"]}
        effect = self.store.prepare_publication(ticket, "yurycooliq/mm2", fingerprint, request)
        return ticket, effect

    def test_lost_post_response_reconciles_closed_issue_without_second_post(self):
        ticket, effect = self.publication()
        client = FakeGitHub(lose_response=True)
        self.assertEqual(publish(self.store, client, ticket, effect["id"])["status"], "UNKNOWN")
        self.assertEqual(reconcile(self.store, client, effect["id"])["status"], "CONFIRMED")
        self.assertEqual(publish(self.store, client, ticket, effect["id"])["status"], "CONFIRMED")
        self.assertEqual(client.posts, 1)

    def test_negative_listing_never_retries_unknown_post(self):
        ticket, effect = self.publication()
        client = FakeGitHub(lose_response=True)
        publish(self.store, client, ticket, effect["id"])
        client.remote = []  # Simulate delayed listing visibility.
        self.assertEqual(publish(self.store, client, ticket, effect["id"])["status"], "WAITING_ACCESS")
        self.assertEqual(client.posts, 1)

    def test_marker_alone_does_not_confirm_modified_body(self):
        ticket, effect = self.publication()
        client = FakeGitHub(lose_response=True)
        publish(self.store, client, ticket, effect["id"])
        client.remote[0]["body"] += "\nattacker instructions"
        with self.assertRaises(ResearchError):
            reconcile(self.store, client, effect["id"])

    def test_stale_worker_cannot_publish_after_lease_reclaim(self):
        ticket, effect = self.publication()
        self.now += 121
        self.store.claim("analysis", "replacement", "run-2")
        client = FakeGitHub()
        with self.assertRaises(ResearchError):
            publish(self.store, client, ticket, effect["id"])
        self.assertEqual(client.posts, 0)


class ContractTest(Fixture):
    def issue(self, manifest, policy_hash):
        return {"schema_version": 1, "policy_hash": policy_hash, "title": "Trace startup failure stage safely",
                "scope": {"account_id": "owner", "lane": "shadow", "component": "feeds"},
                "failure_family": "bootstrap", "version_epoch": "revision-a", "mechanism_id": "missing-stage",
                "evidence": [{"bundle_id": manifest["bundle_id"], "artifact": "diagnostics.json", "sha256": manifest["files"][0]["sha256"]}],
                "labels": ["reliability", "needs-evidence"],
                "sections": {name: "Finite evidence; cause not confirmed; implement bounded stage counters and fault tests."
                             for name in ("problem", "confirmed", "hypothesis", "unknowns", "alternative", "mechanism", "metric", "implementation", "recovery_risk", "tests", "acceptance", "rollout", "rollback")}}

    def test_verified_partial_bundle_can_propose_measurement_without_profit_or_readiness(self):
        path = self.bundle()
        manifest = verify_bundle(path)
        policy, policy_hash = load_policy(DEFAULT_POLICY)
        job = enqueue(self.store, policy, policy_hash, "analysis", [path])
        ticket = self.store.claim("analysis", "desktop", "run-1")
        effect = prepare_draft(self.store, ticket, self.issue(manifest, policy_hash), policy, policy_hash)
        self.assertEqual(effect["status"], "PENDING")
        self.assertIn("Financial authorization remains separate", effect["body"]["body"])
        self.assertEqual(job["id"], ticket["job_id"])

    def test_untrusted_ready_labels_and_wrong_evidence_rejected(self):
        manifest = verify_bundle(self.bundle())
        spec = self.issue(manifest, POLICY_HASH)
        spec["labels"] = ["ready-for-code"]
        with self.assertRaises(ResearchError):
            validate_issue(spec, {manifest["bundle_id"]: manifest}, POLICY_HASH)
        spec["labels"] = ["needs-evidence"]
        spec["evidence"][0]["sha256"] = "f" * 64
        with self.assertRaises(ResearchError):
            validate_issue(spec, {manifest["bundle_id"]: manifest}, POLICY_HASH)

    def test_worktree_without_enforced_denials_is_not_coding_sandbox(self):
        with self.assertRaises(ResearchError):
            coding_ticket({"baseline_sha": "a" * 40, "implementation": {}}, sandbox_receipt={"status": "PASS", "broker_verified": True},
                          policy_hash=POLICY_HASH, baseline_clean=True, current_baseline_sha="a" * 40)

    def test_coding_requires_registered_independent_exact_issue_review(self):
        path = self.bundle()
        manifest = verify_bundle(path)
        policy, policy_hash = load_policy(DEFAULT_POLICY)
        enqueue(self.store, policy, policy_hash, "analysis", [path])
        ticket = self.store.claim("analysis", "desktop-author", "run-1")
        spec = self.issue(manifest, policy_hash)
        effect = prepare_draft(self.store, ticket, spec, policy, policy_hash)
        self.store.confirm_publication(effect["id"], "2", "https://github.com/yurycooliq/mm2/issues/2", effect["body_hash"])
        a1 = {**policy, "autonomy": "A1", "enabled_stages": ["analysis", "reliability", "retention", "coding"]}
        current_hash = digest(a1)
        review = {"effect_id": effect["id"], "body_hash": effect["body_hash"], "spec_hash": digest(spec),
                  "source_policy_hash": policy_hash, "policy_hash": current_hash, "baseline_sha": "a" * 40,
                  "paths": ["tools/research/example.py"], "status": "PASS", "broker_verified": True,
                  "principal": "trusted-research-review-broker", "model": "gpt-6-astra", "actual_settings_verified": True,
                  "artifact_hash": "b" * 64}
        with self.assertRaises(ResearchError):
            ready_code(self.store, effect["id"], {**review, "principal": "desktop-author"}, baseline_sha="a" * 40,
                       paths=review["paths"], policy=a1, policy_hash=current_hash)
        ready = ready_code(self.store, effect["id"], review, baseline_sha="a" * 40, paths=review["paths"], policy=a1, policy_hash=current_hash)
        job = enqueue_coding(self.store, ready["ready_id"], a1, current_hash)
        self.assertEqual(job["id"], enqueue_coding(self.store, ready["ready_id"], a1, current_hash)["id"])
        self.assertEqual(job["inputs"]["ready"]["issue_number"], 2)
        with self.assertRaises(ResearchError):
            enqueue_coding(self.store, "f" * 64, a1, current_hash)


class RetentionTest(Fixture):
    def secondary(self):
        root = self.root / "diagnostics"
        initialize_root(root, "diagnostic")
        (root / "old.log").write_text("safe-old", encoding="utf-8")
        (root / "pinned.log").write_text("retained", encoding="utf-8")
        for path in (root / "old.log", root / "pinned.log"):
            os.utime(path, (1, 1))
        return root

    def test_selected_only_deleted_and_receipt_preserves_unselected(self):
        root = self.secondary()
        plan = preview(root, policy_hash=POLICY_HASH, cutoff_epoch=100, pins=["pinned.log"], lease_paths=[])
        result = apply(plan, self.root / "receipts", approved_plan_id=plan["plan_id"], current_pins=["pinned.log"], current_lease_paths=[], policy_hash=POLICY_HASH)
        self.assertEqual(result["status"], "VERIFIED")
        self.assertFalse((root / "old.log").exists())
        self.assertEqual((root / "pinned.log").read_text(), "retained")
        repeated = apply(plan, self.root / "receipts", approved_plan_id=plan["plan_id"], current_pins=["pinned.log"], current_lease_paths=[], policy_hash=POLICY_HASH)
        self.assertEqual(result, repeated)

    def test_mutation_or_new_pin_blocks_cleanup(self):
        root = self.secondary()
        plan = preview(root, policy_hash=POLICY_HASH, cutoff_epoch=100, pins=[], lease_paths=[])
        with self.assertRaises(ResearchError):
            apply(plan, self.root / "receipts", approved_plan_id=plan["plan_id"], current_pins=["old.log"], current_lease_paths=[], policy_hash=POLICY_HASH)
        (root / "old.log").write_text("changed")
        with self.assertRaises(ResearchError):
            apply(plan, self.root / "receipts", approved_plan_id=plan["plan_id"], current_pins=[], current_lease_paths=[], policy_hash=POLICY_HASH)
        self.assertTrue((root / "old.log").exists())

    def test_primary_and_financial_paths_never_eligible(self):
        with self.assertRaises(ResearchError):
            initialize_root(self.root / "ledger", "diagnostic")
        root = self.secondary()
        (root / "paper.journal").write_text("must-preserve")
        with self.assertRaises(ResearchError):
            preview(root, policy_hash=POLICY_HASH, cutoff_epoch=100, pins=[], lease_paths=[])


class ExperimentTest(Fixture):
    def spec(self, family="quote-width"):
        dataset_ids = {}
        for kind, start, end in (("development", "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z"),
                                 ("holdout", "2026-10-03T00:00:00Z", "2026-10-04T00:00:00Z"),
                                 ("forward", "2026-10-05T00:00:00Z", "2026-10-06T00:00:00Z")):
            dataset_ids[kind] = register_dataset(self.store, {"schema_version": 1, "kind": kind,
                "utc_range": {"start": start, "end": end}, "bundle_ids": [] if kind == "forward" else [digest({"kind": kind, "family": family})],
                "universe_hash": "b" * 64, "evaluator_hash": "c" * 64, "opening_state_hash": "d" * 64,
                "quality_eligible": kind != "forward", "prospective": kind == "forward"})
        return {"schema_version": 1, "kind": "strategy", "family": family, "author": "developer",
                "candidate_sha": "a" * 40, "baseline_sha": "b" * 40, "config_hash": "a" * 64,
                "evaluator_hash": "c" * 64, "policy_hash": POLICY_HASH, "emulator_hash": "f" * 64,
                "criteria": [{"metric": "net_delta_pico", "op": "gte", "threshold": "10"}],
                "required_receipts": {kind: "trusted-" + kind for kind in ("ci", "review", "replay", "forward", "performance", "recovery")},
                "datasets": dataset_ids, "embargo_seconds": 3600, "carryover_policy": "preserve-full-inventory-and-cash",
                "max_family_trials": 3}

    def test_consumed_holdout_cannot_be_reused_and_negative_result_retained(self):
        spec = self.spec()
        experiment = register_experiment(self.store, spec)
        start_experiment(self.store, experiment)
        result = score_experiment(self.store, experiment, {"declared_status": "FAIL", "metrics": {"net_delta_pico": "-10"}}, spec["candidate_sha"])
        self.assertEqual(result["status"], "FAIL")
        self.assertFalse(result["promotion_allowed"])
        self.assertEqual(register_experiment(self.store, spec), experiment)  # Identical registration is idempotent.
        changed = {**spec, "candidate_sha": "c" * 40}
        with self.assertRaises(ResearchError):
            register_experiment(self.store, changed)

    def test_author_or_stale_sha_cannot_attest_candidate(self):
        spec = self.spec()
        receipt = {**{name: spec[name] for name in ("candidate_sha", "baseline_sha", "config_hash", "evaluator_hash", "policy_hash", "emulator_hash", "datasets")},
                   "kind": "review", "principal": "developer", "status": "PASS", "broker_verified": True, "artifact_hash": "e" * 64}
        with self.assertRaises(ResearchError):
            candidate_receipt(spec, "review", receipt, spec["candidate_sha"])
        receipt["principal"] = spec["required_receipts"]["review"]
        with self.assertRaises(ResearchError):
            candidate_receipt(spec, "review", receipt, "c" * 40)

    def test_unknown_profit_cannot_pass_from_declared_success(self):
        spec = self.spec()
        experiment = register_experiment(self.store, spec)
        start_experiment(self.store, experiment)
        result = score_experiment(self.store, experiment, {"declared_status": "PASS", "metrics": {}, "evidence_quality_eligible": False}, spec["candidate_sha"])
        self.assertEqual(result["status"], "INCONCLUSIVE")

    def test_second_active_strategy_trial_is_blocked(self):
        one = register_experiment(self.store, self.spec("one"))
        two = register_experiment(self.store, self.spec("two"))
        start_experiment(self.store, one)
        with self.assertRaises(ResearchError):
            start_experiment(self.store, two)

    def test_chronology_violation_rejected(self):
        spec = self.spec()
        spec["embargo_seconds"] = 100_000
        with self.assertRaises(ResearchError):
            register_experiment(self.store, spec)

    def test_forward_must_be_registered_and_started_before_observations(self):
        spec = self.spec()
        experiment = register_experiment(self.store, spec)
        self.now = int(datetime(2026, 10, 5, tzinfo=timezone.utc).timestamp())
        with self.assertRaises(ResearchError):
            start_experiment(self.store, experiment)
        with self.assertRaises(ResearchError):
            register_experiment(self.store, {**spec, "candidate_sha": "c" * 40})

    def test_verified_exact_sha_trial_can_pass_without_financial_authority(self):
        spec = self.spec()
        experiment = register_experiment(self.store, spec)
        start_experiment(self.store, experiment)
        self.now = int(datetime(2026, 10, 6, tzinfo=timezone.utc).timestamp())
        receipts = {}
        for kind, principal in spec["required_receipts"].items():
            receipt = {**{name: spec[name] for name in ("candidate_sha", "baseline_sha", "config_hash", "evaluator_hash", "policy_hash", "emulator_hash", "datasets")},
                       "kind": kind, "principal": principal, "status": "PASS", "broker_verified": True, "artifact_hash": "e" * 64}
            if kind == "forward":
                receipt.update({"observed_bundle_ids": ["d" * 64], "bundle_hashes_verified": True})
            receipts[kind] = receipt
        result = score_experiment(self.store, experiment, {"declared_status": "PASS", "metrics": {"net_delta_pico": "11"},
                                  "evidence_quality_eligible": True, "receipts": receipts}, spec["candidate_sha"])
        self.assertEqual(result["status"], "PASS")
        self.assertFalse(result["promotion_allowed"])
        self.assertFalse(result["funded_authorization"])


if __name__ == "__main__":
    unittest.main()
