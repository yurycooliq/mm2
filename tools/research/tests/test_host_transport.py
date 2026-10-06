"""Host transport boundaries: immutable versions, bounded reads and no raw uploads.

All remote operations use FakeS3. No AWS credential/provider, wallet or network is read.
"""
from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from tools.research import storage
from tools.research.archive import publish_bundle, read_member, verify_bundle
from tools.research.observe_mo_money import observe_account
from tools.research.registry import load_registry
from tools.research.schema import ResearchError, canonical, quality

IDENTITY = "1" * 64
RANGE = {"start": "2026-10-06T00:00:00Z", "end": "2026-10-07T00:00:00Z"}
PRIVATE_CANARY = "PRIVATE_CANARY_DO_NOT_TRANSPORT"


class Body(io.BytesIO):
    def __init__(self, data):
        super().__init__(data)
        self.requested_reads = []

    def read(self, amount=-1):
        self.requested_reads.append(amount)
        return super().read(amount)


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.puts = []
        self.gets = []
        self.bodies = []
        self.reported_version = None
        self.length_adjustment = 0

    def put_object(self, **arguments):
        self.puts.append(arguments)
        identity = (arguments["Bucket"], arguments["Key"])
        self.objects[identity] = bytes(arguments["Body"])
        return {"VersionId": "version-1"}

    def get_object(self, **arguments):
        self.gets.append(arguments)
        data = self.objects[(arguments["Bucket"], arguments["Key"])]
        body = Body(data)
        self.bodies.append(body)
        return {"ContentLength": len(data) + self.length_adjustment, "Body": body,
                "VersionId": self.reported_version or arguments.get("VersionId", "version-1")}

    def seed_bundle(self, directory, prefix="research/"):
        manifest = verify_bundle(directory)
        identity = manifest["bundle_id"]
        for item in manifest["files"] + [{"path": "manifest.json"}]:
            self.objects[("private-fixture-bucket", storage.safe_key(prefix, identity, item["path"]))] = (directory / item["path"]).read_bytes()
        return identity


def host_module():
    # Module imports must work from repo root; no sys.path fallback that hides a CLI bug.
    from scripts import research_export
    return research_export


def diagnostics():
    return {"schema_version": 1, "captured_at_utc": RANGE["end"], "unit": "mm2.service", "hours": 1,
        "read_only": True, "systemd": {"NRestarts": "7", "ActiveState": "active"}, "containers": [],
        "incidents": [{"category": "source_bootstrap", "message": "diagnostic_code=native_snapshot_bootstrap_timeout source=clob fatal=false",
                       "source": "systemd_journal", "fingerprint": "2" * 64, "observed_timestamp_us": "1791244800000000"}],
        "quality": {"status": "PARTIAL", "reasons": ["bounded_recent_journal_not_complete_history"]}}


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=Path.cwd(), prefix="host-transport-test-")
        self.root = Path(self.temporary.name)
        self.s3 = FakeS3()

    def tearDown(self):
        self.temporary.cleanup()

    def bundle(self, files=None, scope=None):
        return publish_bundle(self.root / "archive", files or {"accounting.json": {"schema_version": 1, "net_pnl_pico": None}},
            scope=scope or {"kind": "research-accounting"}, utc_range=RANGE, quality=quality("PARTIAL", ["UNKNOWN_NAV"]),
            created_at_utc=RANGE["end"])

    def test_conditional_upload_hash_version_and_private_encryption(self):
        key = storage.safe_key("research/", IDENTITY, "accounting.json")
        payload = canonical({"net_pnl_pico": None})
        receipt = storage.put_immutable(self.s3, "private-fixture-bucket", key, payload)
        self.assertEqual(receipt["sha256"], hashlib.sha256(payload).hexdigest())
        self.assertEqual(receipt["version_id"], "version-1")
        self.assertEqual(self.s3.puts[0]["IfNoneMatch"], "*")
        self.assertEqual(self.s3.puts[0]["ServerSideEncryption"], "AES256")
        self.assertEqual(self.s3.gets[0]["VersionId"], "version-1")
        self.assertTrue(self.s3.bodies[0].closed)

    def test_requested_version_mismatch_and_unversioned_response_fail(self):
        for version in ("version-other", "null"):
            with self.subTest(version=version):
                self.s3.reported_version = version
                with self.assertRaises(ResearchError):
                    storage.put_immutable(self.s3, "private-fixture-bucket", storage.safe_key("research/", IDENTITY, "result.json"), b"{}")

    def test_truncated_content_length_cannot_create_remote_receipt(self):
        self.s3.length_adjustment = 10
        with self.assertRaises(ResearchError):
            storage.put_immutable(self.s3, "private-fixture-bucket", storage.safe_key("research/", IDENTITY, "result.json"), b"{}")
        self.assertTrue(self.s3.bodies[0].closed)

    def test_remote_quota_rejects_before_body_read_and_closes(self):
        key = storage.safe_key("research/", IDENTITY, "result.json")
        self.s3.objects[("private-fixture-bucket", key)] = b"12345"
        with self.assertRaises(ResearchError):
            storage.remote_bytes(self.s3, "private-fixture-bucket", key, maximum=4)
        self.assertEqual(self.s3.bodies[0].requested_reads, [])
        self.assertTrue(self.s3.bodies[0].closed)

    def test_namespace_traversal_and_arbitrary_private_payload_refused(self):
        for prefix, name in (("exports/other/", "a.json"), ("research/", "../raw.json"), ("research/", "sub/file.json")):
            with self.assertRaises(ResearchError):
                storage.safe_key(prefix, IDENTITY, name)
        arbitrary = self.bundle({"raw.json": {"message": PRIVATE_CANARY}}, {"kind": "arbitrary-input"})
        with self.assertRaises(ResearchError):
            storage.upload_bundle(self.s3, "private-fixture-bucket", arbitrary, "exports/mm2/")
        self.assertEqual(self.s3.puts, [])

    def test_manifest_is_last_and_remote_members_are_hash_bound(self):
        bundle = self.bundle()
        receipt = storage.upload_bundle(self.s3, "private-fixture-bucket", bundle, "research/")
        self.assertTrue(self.s3.puts[-1]["Key"].endswith("/manifest.json"))
        self.assertTrue(receipt["verified_remote"])
        self.assertFalse(receipt["primary_cleanup_allowed"])
        downloaded = storage.download_bundle(self.s3, "private-fixture-bucket", "research/", receipt["bundle_id"], self.root / "downloads")
        self.assertEqual(verify_bundle(downloaded), verify_bundle(bundle))
        key = storage.safe_key("research/", receipt["bundle_id"], "accounting.json")
        self.s3.objects[("private-fixture-bucket", key)] = b'{"net_pnl_pico":"100"}\n'
        with self.assertRaises(ResearchError):
            storage.download_bundle(self.s3, "private-fixture-bucket", "research/", receipt["bundle_id"], self.root / "corrupt-downloads")

    def test_download_declared_byte_budget_checked_before_member_request(self):
        bundle = self.bundle()
        identity = self.s3.seed_bundle(bundle)
        before = len(self.s3.gets)
        with patch.object(storage, "MAX_OBJECT", 4):
            with self.assertRaises(ResearchError):
                storage.download_bundle(self.s3, "private-fixture-bucket", "research/", identity, self.root / "downloads")
        self.assertEqual(len(self.s3.gets) - before, 1, "Oversized declared member must not trigger GET")

    def test_entire_declared_bundle_budget_is_preflighted(self):
        bundle = self.bundle({"first.json": {"value": "ok"}, "last.json": {"value": "x" * 100}})
        identity = self.s3.seed_bundle(bundle)
        first_bytes = verify_bundle(bundle)["files"][0]["bytes"]
        before = len(self.s3.gets)
        with patch.object(storage, "MAX_OBJECT", first_bytes + 1):
            with self.assertRaises(ResearchError):
                storage.download_bundle(self.s3, "private-fixture-bucket", "research/", identity, self.root / "downloads")
        self.assertEqual(len(self.s3.gets) - before, 1, "Reject cumulative declarations before requesting any member")

    def test_existing_cache_requires_exact_remote_manifest_identity(self):
        bundle = self.bundle()
        identity = self.s3.seed_bundle(bundle)
        downloaded = storage.download_bundle(self.s3, "private-fixture-bucket", "research/", identity, self.root / "downloads")
        remote_key = ("private-fixture-bucket", storage.safe_key("research/", identity, "manifest.json"))
        changed = json.loads(self.s3.objects[remote_key])
        changed["source_revision"] = "tampered"
        self.s3.objects[remote_key] = canonical(changed)
        with self.assertRaises(ResearchError):
            storage.download_bundle(self.s3, "private-fixture-bucket", "research/", identity, downloaded.parent)


class HostProducerTests(unittest.TestCase):
    def setUp(self):
        self.module = host_module()
        self.temporary = tempfile.TemporaryDirectory(dir=Path.cwd(), prefix="host-producer-test-")
        self.root = Path(self.temporary.name)
        self.primary = self.root / "primary"
        self.primary.mkdir()
        self.source = self.primary / "simulation"
        self.source.mkdir()
        self.config = {"spool": str(self.root / "spool"), "source": str(self.source), "source_id": "fixture-paper-host",
            "lane": "public-paper", "bucket": "private-fixture-bucket", "region": "eu-west-1", "source_revision": "a" * 40}
        self.state = {"execution_mode": "simulation", "mode": "paused", "api_ready": True,
                      "live": {"signed_intents": PRIVATE_CANARY}, "fault_message": PRIVATE_CANARY}
        self.s3 = FakeS3()

    def tearDown(self):
        self.temporary.cleanup()

    def test_unknown_metadata_signed_state_and_fault_text_never_uploaded(self):
        with patch.object(self.module, "STATE_ROOT", self.primary.resolve()):
            result = self.module.run_export(self.config, self.s3, self.state, diagnostics())
        self.assertFalse(result["financial_complete"])
        self.assertFalse(result["primary_mutations"])
        self.assertNotIn(PRIVATE_CANARY.encode(), b"".join(p["Body"] for p in self.s3.puts))

    def test_unrecognized_diagnostic_message_is_not_arbitrary_raw_upload(self):
        raw = diagnostics()
        raw["incidents"].append({"category": "transport", "message": PRIVATE_CANARY,
            "source": "systemd_journal", "fingerprint": "3" * 64})
        raw["raw_log"] = PRIVATE_CANARY
        with patch.object(self.module, "STATE_ROOT", self.primary.resolve()):
            self.module.run_export(self.config, self.s3, self.state, raw)
        self.assertNotIn(PRIVATE_CANARY.encode(), b"".join(p["Body"] for p in self.s3.puts))

    def test_spool_symlink_ancestor_cannot_enter_primary_root(self):
        alias = self.root / "alias"
        try:
            alias.symlink_to(self.primary, target_is_directory=True)
        except OSError:
            self.skipTest("Host does not grant fixture symlink creation")
        config = {**self.config, "spool": str(alias / "nested-spool")}
        with patch.object(self.module, "STATE_ROOT", self.primary.resolve()):
            with self.assertRaises(ValueError):
                self.module.run_export(config, self.s3, self.state, diagnostics())
        self.assertFalse((self.primary / "nested-spool").exists())
        self.assertEqual(self.s3.puts, [])

    def test_direct_spool_overlap_is_rejected_before_primary_mkdir(self):
        config = {**self.config, "spool": str(self.primary / "new-secondary-spool")}
        with patch.object(self.module, "STATE_ROOT", self.primary.resolve()):
            with self.assertRaises(ValueError):
                self.module.run_export(config, self.s3, self.state, diagnostics())
        self.assertFalse((self.primary / "new-secondary-spool").exists(), "Configuration rejection must not create primary state")
        self.assertEqual(self.s3.puts, [])

    def test_real_lane_cannot_export_owner_paper_prefix_as_live_ledger(self):
        config = {**self.config, "lane": "real"}
        state = {**self.state, "execution_mode": "trading", "journal": {"committed_prefix": {"source_kind": "paper"}}}
        with patch.object(self.module, "STATE_ROOT", self.primary.resolve()), patch.object(self.module, "export_prefix") as exporter:
            self.module.run_export(config, self.s3, state, diagnostics())
        exporter.assert_not_called()

    def test_public_owner_account_is_reconciliation_not_mm2_profit(self):
        class EmptyPublicAPI:
            def get(self, endpoint, params):
                return [], {"endpoint": endpoint, "observed_at_utc": RANGE["end"], "rows": 0}
        owner = next(item for item in load_registry()["accounts"] if item["name"] == "mm2-public")
        captured = observe_account(EmptyPublicAPI(), owner, RANGE["start"], RANGE["end"])
        self.assertFalse(captured["coverage"]["mm2_order_attribution"])
        self.assertIn("PUBLIC_OWNER_ACTIVITY_MM2_ATTRIBUTION_UNPROVEN", captured["quality"]["reasons"])
        self.assertIsNone(captured["snapshot"]["cash_pico"])
        self.assertFalse(captured["quality"]["eligibility"]["accounting"])


if __name__ == "__main__":
    unittest.main()
