import base64
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import research_bridge, research_desktop
from tools.research.archive import publish_bundle, verify_bundle
from tools.research.controller import DEFAULT_POLICY, enqueue, load_policy, prepare_draft
from tools.research.github import REQUIRED_SECTIONS
from tools.research.jobs import JobStore
from tools.research.schema import ResearchError, canonical, digest, load_json


class DesktopBridgeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.state = self.root / "state"
        self.bundle = publish_bundle(self.state / "archive", {"facts.json": {"safe": True}},
                                     scope={"kind": "diagnostic"}, utc_range={"start": "2026-10-01T00:00:00Z", "end": "2026-10-02T00:00:00Z"},
                                     quality={"status": "PARTIAL", "eligibility": {}, "reasons": ["finite-tail"]})
        self.manifest = verify_bundle(self.bundle)
        self.policy, self.policy_hash = load_policy(DEFAULT_POLICY)
        self.config = {"state": str(self.state), "policy": str(DEFAULT_POLICY)}
        with JobStore(self.state / "jobs.sqlite3") as store:
            enqueue(store, self.policy, self.policy_hash, "analysis", [self.bundle])

    def tearDown(self):
        self.temp.cleanup()

    def request(self, command, **fields):
        return research_bridge.handle({"schema_version": 1, "command": command, **fields}, self.config)

    def claim(self):
        return self.request("claim", stage="analysis", owner="desktop", run_id="run-1", model="gpt-6-astra", reasoning="max")["ticket"]

    def effect(self, unknown=False):
        ticket = self.claim()
        spec = {"schema_version": 1, "policy_hash": self.policy_hash, "title": "Fixture bounded diagnostics",
                "scope": {"account_id": "owner", "lane": "shadow", "component": "diagnostics"},
                "failure_family": "fixture", "version_epoch": "epoch-fixture", "mechanism_id": "bounded-stage",
                "evidence": [{"bundle_id": self.manifest["bundle_id"], "artifact": "facts.json", "sha256": self.manifest["files"][0]["sha256"]}],
                "labels": ["needs-evidence"], "sections": {name: "Test fixture only; bounded evidence and no economic claim." for name in REQUIRED_SECTIONS}}
        with JobStore(self.state / "jobs.sqlite3") as store:
            effect = prepare_draft(store, ticket, spec, self.policy, self.policy_hash)
            if unknown:
                store.dispatch_publication(ticket, effect["id"])
                store.publication_unknown(effect["id"])
            else:
                store.confirm_publication(effect["id"], "9", "https://github.com/yurycooliq/mm2/issues/9", effect["body_hash"])
        return ticket, effect, spec

    def test_overlapping_bridge_claims_and_no_model_fallback(self):
        first = self.claim()
        self.assertIn("job_id", first)
        self.assertIsNone(self.claim())
        with self.assertRaises(ResearchError):
            self.request("claim", stage="analysis", owner="desktop", run_id="run-2", model="gpt-6-luna", reasoning="max")

    def test_bridge_rejects_command_fields_and_bundle_path_injection(self):
        with self.assertRaises(ResearchError):
            self.request("status", shell="cat /etc/mm2/private.env")
        with self.assertRaises(ResearchError):
            self.request("bundle", bundle_id="../../etc/mm2/private.env")

    def test_confirm_cannot_bind_arbitrary_repository_url(self):
        ticket = self.claim()
        with JobStore(self.state / "jobs.sqlite3") as store:
            effect = store.prepare_publication(ticket, "yurycooliq/mm2", "a" * 64, {"title": "x", "body": "y", "labels": []})
        with self.assertRaises(ResearchError):
            self.request("confirm", effect_id=effect["id"], remote_id="1", remote_url="https://github.com/attacker/mm2/issues/1", verified_body_hash=effect["body_hash"])

    def test_pull_is_durable_and_repeated_partial_resume_keeps_exact_bundles(self):
        def remote(payload):
            return research_bridge.handle({"schema_version": 1, **payload}, self.config)
        with patch.object(research_desktop, "ROOT", self.root), patch.object(research_desktop, "bridge", side_effect=remote):
            pulled = research_desktop.pull("analysis")
            self.assertTrue(Path(pulled["ticket"]).is_file())
            resumed = research_desktop.pull(None, pulled["ticket"])
            self.assertEqual(pulled["bundles"], resumed["bundles"])
            self.assertFalse(resumed["model_claim_is_provider_attestation"])
            self.assertEqual(verify_bundle(pulled["bundles"][0]), self.manifest)

    def test_unknown_effect_does_not_post_again(self):
        effect = {"id": "a" * 64, "marker": "mm2-research:" + "b" * 64, "status": "UNKNOWN"}
        class Client:
            def all_issues(self):
                return []
            def create_issue(self, _):
                raise AssertionError("unknown response must not repeat POST")
        with patch.object(research_desktop, "bridge", return_value=effect):
            with self.assertRaises(ResearchError):
                research_desktop.publish({}, {}, Client())

    def test_server_effect_context_and_status_recover_frozen_review_inputs(self):
        ticket, effect, spec = self.effect()
        recovered = self.request("effect", effect_id=effect["id"])
        self.assertEqual(recovered["issue_spec"], spec)
        self.assertEqual(recovered["issue_spec_hash"], digest(spec))
        self.assertEqual(recovered["parent_inputs"], ticket["inputs"])
        self.assertEqual(recovered["parent_inputs_hash"], ticket["inputs_hash"])
        status = self.request("status")
        self.assertEqual(status["confirmed_effects"][0]["id"], effect["id"])

    def test_ready_code_bridge_passes_keyword_contract_and_exposes_registry(self):
        ticket, effect, spec = self.effect()
        review = {"effect_id": effect["id"], "body_hash": effect["body_hash"], "spec_hash": digest(spec),
                  "source_policy_hash": self.policy_hash, "policy_hash": self.policy_hash, "baseline_sha": "b" * 40,
                  "paths": ["tools/research/example.py"], "status": "PASS", "broker_verified": True,
                  "principal": "trusted-research-review-broker", "model": "gpt-6-astra", "actual_settings_verified": True,
                  "artifact_hash": "c" * 64}
        ready = self.request("ready-code", effect_id=effect["id"], review=review, baseline_sha="b" * 40, paths=review["paths"])
        status = self.request("status")
        self.assertEqual(status["ready_code"][0]["ready_id"], ready["ready_id"])
        self.assertEqual(status["ready_code"][0]["issue_number"], 9)
        with self.assertRaises(ResearchError):
            self.request("ready-code", effect_id=effect["id"], review={**review, "principal": "desktop"}, baseline_sha="b" * 40, paths=review["paths"])

    def test_unknown_reconciliation_is_lease_independent_and_never_resends(self):
        ticket, effect, spec = self.effect(unknown=True)
        with self.assertRaises(ResearchError):
            self.request("wake-publication", effect_id=effect["id"])
        remote = {**effect["body"], "number": 9, "html_url": "https://github.com/yurycooliq/mm2/issues/9", "state": "closed"}
        class Client:
            def all_issues(self):
                return [remote]
            def create_issue(self, _):
                raise AssertionError("reconciliation must never POST")
        def remote_bridge(payload):
            return research_bridge.handle({"schema_version": 1, **payload}, self.config)
        with patch.object(research_desktop, "bridge", side_effect=remote_bridge):
            result = research_desktop.reconcile(effect["id"], Client())
        self.assertEqual(result["status"], "CONFIRMED")
        self.assertEqual(result["parent"]["job"]["status"], "PENDING")
        self.assertIsNotNone(self.claim())

    def test_negative_remote_visibility_leaves_unknown_parent_waiting(self):
        ticket, effect, _ = self.effect(unknown=True)
        class Client:
            def all_issues(self):
                return []
            def create_issue(self, _):
                raise AssertionError("no retry allowed")
        with patch.object(research_desktop, "bridge", side_effect=lambda payload: research_bridge.handle({"schema_version": 1, **payload}, self.config)):
            result = research_desktop.reconcile(effect["id"], Client())
        self.assertEqual(result["status"], "WAITING_ACCESS")
        self.assertFalse(result["blind_retry_allowed"])
        self.assertIsNone(self.claim())


if __name__ == "__main__":
    unittest.main()
