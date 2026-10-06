import base64
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import research_bridge, research_desktop
from tools.research.archive import publish_bundle, verify_bundle
from tools.research.controller import DEFAULT_POLICY, enqueue, load_policy
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


if __name__ == "__main__":
    unittest.main()
