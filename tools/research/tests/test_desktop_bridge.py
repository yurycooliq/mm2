import base64
import copy
import io
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts import research_bridge, research_desktop
from tools.research.archive import publish_bundle, verify_bundle
from tools.research.controller import DEFAULT_POLICY, enqueue, load_policy, prepare_draft
from tools.research.github import REQUIRED_SECTIONS
from tools.research.jobs import JobStore
from tools.research.schema import ResearchError, canonical, digest, load_json, parse_json


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

    def large_packet(self):
        bundle = publish_bundle(self.state / "archive", {"events.jsonl": [{"line": "x" * 80, "number": n} for n in range(4096)]},
                                scope={"kind": "diagnostic"}, utc_range=self.manifest["utc_range"], quality=self.manifest["quality"])
        manifest = verify_bundle(bundle)
        content = parse_json(canonical(self.request("bundle", bundle_id=manifest["bundle_id"])))
        return bundle, manifest, content

    def test_large_member_chunk_transport_roundtrips_and_saves_verified_bundle(self):
        bundle, manifest, content = self.large_packet()
        member = manifest["files"][0]
        self.assertGreater(member["bytes"], 256 * 1024)
        self.assertGreater(len(content["files"][member["path"]]), 1)
        self.assertTrue(all(len(chunk) <= 128 * 1024 for chunk in content["files"][member["path"]]))
        self.assertEqual(research_desktop.decode_packet(content)[member["path"]], (bundle / member["path"]).read_bytes())
        ticket = self.claim()
        ticket["inputs"]["bundles"] = [{"bundle_id": manifest["bundle_id"], "manifest_hash": digest(manifest)}]
        def remote(payload):
            if payload["command"] == "claim":
                return {"ticket": ticket, "policy": self.policy}
            return parse_json(canonical(research_bridge.handle({"schema_version": 1, **payload}, self.config)))
        with patch.object(research_desktop, "ROOT", self.root), patch.object(research_desktop, "bridge", side_effect=remote):
            result = research_desktop.pull("analysis")
        self.assertEqual(verify_bundle(result["bundles"][0]), manifest)

    def test_chunk_tampering_count_and_aggregate_quota_fail_before_save(self):
        _, _, content = self.large_packet()
        name = content["manifest"]["files"][0]["path"]
        bad = copy.deepcopy(content)
        first = bad["files"][name][0]
        bad["files"][name][0] = ("A" if first[0] != "A" else "B") + first[1:]
        with self.assertRaises(ResearchError):
            research_desktop.decode_packet(bad)
        bad = copy.deepcopy(content)
        bad["files"][name].pop()
        with self.assertRaises(ResearchError):
            research_desktop.decode_packet(bad)
        bad = copy.deepcopy(content)
        bad["manifest"]["files"] = [{"path": "one.jsonl", "bytes": research_desktop.PACKET_BYTES, "sha256": "a" * 64},
                                     {"path": "two.jsonl", "bytes": 1, "sha256": "b" * 64}]
        # Declared aggregate is refused before any decoding of untrusted data.
        bad["files"] = {"one.jsonl": ["invalid"], "two.jsonl": ["invalid"]}
        with self.assertRaisesRegex(ResearchError, "aggregate quota"):
            research_desktop.decode_packet(bad)
        bad = copy.deepcopy(content)
        bad["files"]["extra.json"] = []
        with self.assertRaisesRegex(ResearchError, "differ from manifest"):
            research_desktop.decode_packet(bad)

    def test_status_exposes_service_failure_even_without_pending_jobs(self):
        ticket = self.claim()
        with JobStore(self.state / "jobs.sqlite3") as store:
            store.finish(ticket, "SUCCEEDED", {"status": "fixture"})
        health = {"unit": "mm2-research.service", "status": "FAILED", "fingerprint": "c" * 64}
        with patch.object(research_bridge, "service_health", return_value=health):
            self.assertEqual(self.request("status")["service_health"], health)


class ServiceHealthTest(unittest.TestCase):
    def fixture(self, **updates):
        values = {"ActiveState": "inactive", "SubState": "dead", "Result": "success", "ExecMainStatus": "0",
                  "ExecMainStartTimestamp": "Tue 2026-10-06 10:00:00 UTC", "ExecMainExitTimestamp": "Tue 2026-10-06 10:00:01 UTC"}
        values.update(updates)
        return "\n".join(f"{key}={value}" for key, value in values.items()).encode("ascii") + b"\n"

    def test_inactive_success_oneshot_is_healthy_and_failure_is_stable(self):
        healthy = research_bridge.service_health(lambda: (self.fixture(), 0))
        self.assertEqual(healthy["status"], "HEALTHY")
        self.assertEqual(healthy["last_exited_utc"], "2026-10-06T10:00:01Z")
        failure = research_bridge.service_health(lambda: (self.fixture(ActiveState="failed", SubState="failed", Result="exit-code", ExecMainStatus="1"), 0))
        repeated = research_bridge.service_health(lambda: (self.fixture(ActiveState="failed", SubState="failed", Result="exit-code", ExecMainStatus="1", ExecMainExitTimestamp="Tue 2026-10-06 10:15:01 UTC"), 0))
        self.assertEqual(failure["status"], "FAILED")
        self.assertEqual(failure["fingerprint"], repeated["fingerprint"])

    def test_unknown_unit_malformed_enum_and_timestamp_are_unavailable(self):
        cases = [(b"ActiveState=inactive\n", 0), (self.fixture() + b"Result=success\n", 0),
                 (self.fixture(Result=""), 0), (self.fixture(SubState="unknown-code"), 0),
                 (self.fixture(ExecMainStatus="256"), 0), (self.fixture(ExecMainStartTimestamp="Tue 2026-99-06 10:00:00 UTC"), 0),
                 (self.fixture(ExecMainStartTimestamp="Mon 2026-10-06 10:00:00 UTC"), 0),
                 (self.fixture(), 1), (b"x" * (research_bridge.HEALTH_BYTES + 1), 0)]
        for raw, code in cases:
            with self.subTest(raw=raw[:32], code=code):
                health = research_bridge.service_health(lambda: (raw, code))
                self.assertEqual(health["status"], "UNAVAILABLE")
                self.assertNotIn("last_started_utc", health)
        def timeout():
            raise subprocess.TimeoutExpired("must-not-escape", 3)
        self.assertEqual(research_bridge.service_health(timeout)["reason"], "QUERY_TIMEOUT")

    def test_fixed_unprivileged_query_has_minimal_environment_and_bounded_read(self):
        raw = self.fixture()
        class Process:
            stdout = io.BytesIO(raw)
            def poll(self): return 0
            def wait(self, timeout): return 0
            def kill(self): raise AssertionError("finished process must not be killed")
        with patch.object(research_bridge.subprocess, "Popen", return_value=Process()) as launch:
            health = research_bridge.service_health()
        self.assertEqual(health["status"], "HEALTHY")
        args, kwargs = launch.call_args
        self.assertEqual(args[0][:3], ["/usr/bin/systemctl", "show", "mm2-research.service"])
        self.assertEqual(args[0][-1], "--property=" + ",".join(research_bridge.SERVICE_FIELDS))
        self.assertEqual(kwargs["env"], {"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C", "TZ": "UTC"})
        self.assertEqual(kwargs["stderr"], subprocess.DEVNULL)


if __name__ == "__main__":
    unittest.main()
