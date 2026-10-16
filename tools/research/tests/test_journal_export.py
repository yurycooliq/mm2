import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("journal_export", Path(__file__).parents[1] / "journal_export.py")
exporter = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = exporter
SPEC.loader.exec_module(exporter)


def record(sequence, previous, body, live=False):
    fields = {"sequence": sequence, "previous_hash": previous}
    parts = [sequence, previous, *([body["reason"], body["ledger"]] if live else [body])]
    fields["hash"] = exporter.sha(exporter.encoded(parts))
    fields.update(body if live else {"entry": body})
    return fields, exporter.encoded(fields) + b"\n"


def engine_fixture(source):
    limits = {"initial_collateral_usd": "100", "max_imbalance_shares": "10", "lot_shares": "5", "half_width": "0.04", "max_feed_age_ms": 1000}
    first, a = record(0, exporter.ZERO_HASH, {"kind": "bootstrap", "limits": limits})
    second, b = record(1, first["hash"], {"kind": "execution", "operations": [{"kind": "advance", "now_ms": 7}]})
    (source / "commands.jsonl").write_bytes(a + b)
    return {"schema_version": 1, "source_kind": "engine", "ledger_id": "a" * 64,
            "segment": {"name": "commands.jsonl", "index": 0, "start_sequence": 0, "previous_hash": exporter.ZERO_HASH},
            "byte_offset": len(a + b), "next_sequence": 2, "last_hash": second["hash"]}


class JournalExportTests(unittest.TestCase):
    def test_public_fee_evidence_is_strict_and_keeps_exact_rate(self):
        policy = {"condition_id": "0x" + "a" * 64, "up_token": "1", "down_token": "2",
            "rate": "0.07", "exponent": 1, "taker_only": True, "observed_at_ms": 1000,
            "response_sha256": "b" * 64}
        self.assertEqual(exporter._operation({"kind": "fee_rules", "policy": policy})["policy"], policy)
        for key in policy:
            partial = dict(policy)
            del partial[key]
            with self.assertRaises(exporter.ExportError):
                exporter._operation({"kind": "fee_rules", "policy": partial})
        with self.assertRaises(exporter.ExportError):
            exporter._operation({"kind": "fee_rules", "policy": {**policy, "api_token": "secret"}})

    def setUp(self):
        # Stay inside the verified repository root on managed Windows sandbox hosts.
        self.temp = tempfile.TemporaryDirectory(prefix=".journal-export-test-", dir=Path(__file__).resolve().parents[3])
        self.base = Path(self.temp.name)
        self.source = self.base / "source"
        self.source.mkdir()
        self.destination = self.base / "bundle"

    def tearDown(self):
        self.temp.cleanup()

    def test_committed_prefix_ignores_complete_uncommitted_and_partial_tails(self):
        watermark = engine_fixture(self.source)
        third, data = record(2, watermark["last_hash"], {"kind": "execution", "operations": [{"kind": "advance", "now_ms": 999}]})
        with (self.source / "commands.jsonl").open("ab") as stream:
            stream.write(data + b'{"partial":')
        original = (self.source / "commands.jsonl").read_bytes()
        manifest = exporter.export_prefix(self.source, self.destination, watermark, "aws-mm2-paper")
        self.assertEqual(manifest["source_records"], 2)
        self.assertFalse(manifest["financial_effects_complete"])
        self.assertNotIn("999", (self.destination / "events.jsonl").read_text())
        self.assertEqual((self.source / "commands.jsonl").read_bytes(), original)
        self.assertEqual(exporter.export_prefix(self.source, self.destination, watermark, "aws-mm2-paper"), manifest)

    def test_rotation_resolves_pinned_old_index_and_rejects_wrong_active_segment(self):
        watermark = engine_fixture(self.source)
        (self.source / "commands.jsonl").rename(self.source / "commands-00000000.jsonl")
        _, next_data = record(2, watermark["last_hash"], {"kind": "execution", "operations": [{"kind": "advance", "now_ms": 8}]})
        (self.source / "commands.jsonl").write_bytes(next_data)
        exporter.export_prefix(self.source, self.destination, watermark, "aws-mm2-paper")
        (self.source / "commands-00000000.jsonl").unlink()
        with self.assertRaises(exporter.ExportError):
            exporter.export_prefix(self.source, self.base / "wrong", watermark, "aws-mm2-paper")

    def test_failed_publication_leaves_staging_and_retry_preserves_events(self):
        watermark = engine_fixture(self.source)
        with patch.object(exporter.os, "rename", side_effect=OSError("fixture interruption")):
            with self.assertRaises(OSError):
                exporter.export_prefix(self.source, self.destination, watermark, "aws-mm2-paper")
        self.assertFalse(self.destination.exists())
        staging = list(self.base.glob(".incomplete-export-*"))
        self.assertEqual(len(staging), 1)
        self.assertTrue((staging[0] / "INCOMPLETE").exists())
        with self.assertRaises(exporter.ExportError):
            exporter.verify_bundle(staging[0])
        manifest = exporter.export_prefix(self.source, self.destination, watermark, "aws-mm2-paper")
        self.assertEqual(manifest["event_count"], 2)

    def test_hash_corruption_byte_and_record_budgets_fail_closed(self):
        watermark = engine_fixture(self.source)
        raw = (self.source / "commands.jsonl").read_bytes()
        (self.source / "commands.jsonl").write_bytes(raw.replace(b'"now_ms":7', b'"now_ms":8'))
        with self.assertRaises(exporter.ExportError):
            exporter.export_prefix(self.source, self.destination, watermark, "aws-mm2-paper")
        (self.source / "commands.jsonl").write_bytes(raw)
        for limits in (exporter.ExportLimits(max_input_bytes=10), exporter.ExportLimits(max_output_bytes=10), exporter.ExportLimits(max_records=1)):
            with self.assertRaises(exporter.ExportError):
                exporter.export_prefix(self.source, self.destination, watermark, "aws-mm2-paper", limits)

    def test_live_projection_excludes_signed_pending_and_free_form_errors(self):
        ledger = {"phase": "paused", "orders": {}, "positions": {}, "collateral_micros": 100_000_000,
                  "session_spent_pico": "0", "signed_intents": {"wire": {"secret": "FAKE_PRIVATE_SENTINEL"}},
                  "chain_pending": {"raw": "FAKE_PRIVATE_SENTINEL"}, "fault": "FAKE_PRIVATE_SENTINEL"}
        first, data = record(0, exporter.ZERO_HASH, {"reason": "startup_paused_recovery", "ledger": ledger}, live=True)
        (self.source / "live-0000000000000000.jsonl").write_bytes(data)
        watermark = {"schema_version": 1, "source_kind": "live", "ledger_id": "b" * 64,
                     "segment": {"name": "live-0000000000000000.jsonl", "start_sequence": 0, "previous_hash": exporter.ZERO_HASH},
                     "byte_offset": len(data), "next_sequence": 1, "last_hash": first["hash"]}
        exporter.export_prefix(self.source, self.destination, watermark, "aws-mm2-live")
        text = (self.destination / "events.jsonl").read_text()
        self.assertNotIn("FAKE_PRIVATE_SENTINEL", text)
        event = json.loads(text)
        self.assertIsNone(event["event_time_ms"])
        self.assertIsNone(event["native_fill_identity"])
        self.assertEqual(event["data"]["classifications"]["fill_timestamps"], "unavailable")
        ledger["unknown_financial_effect"] = 1
        bad, data = record(0, exporter.ZERO_HASH, {"reason": "startup_paused_recovery", "ledger": ledger}, live=True)
        (self.source / "live-0000000000000000.jsonl").write_bytes(data)
        watermark.update(byte_offset=len(data), last_hash=bad["hash"])
        with self.assertRaises(exporter.ExportError):
            exporter.export_prefix(self.source, self.base / "unknown", watermark, "aws-mm2-live")

    def test_unknown_paper_semantics_are_not_silently_filtered(self):
        first, data = record(0, exporter.ZERO_HASH, {"kind": "execution", "operations": [{"kind": "new_fee", "amount": 1}]})
        (self.source / "commands.jsonl").write_bytes(data)
        watermark = {"schema_version": 1, "source_kind": "engine", "ledger_id": "a" * 64,
                     "segment": {"name": "commands.jsonl", "index": 0, "start_sequence": 0, "previous_hash": exporter.ZERO_HASH},
                     "byte_offset": len(data), "next_sequence": 1, "last_hash": first["hash"]}
        with self.assertRaises(exporter.ExportError):
            exporter.export_prefix(self.source, self.destination, watermark, "aws-mm2-paper")

    def test_live_sealed_predecessor_requires_matching_nonempty_successor(self):
        ledger = {"phase": "paused", "orders": {}, "positions": {}, "collateral_micros": 0, "session_spent_pico": "0"}
        first, data = record(0, exporter.ZERO_HASH, {"reason": "startup_paused_recovery", "ledger": ledger}, live=True)
        (self.source / "live-0000000000000000.jsonl").write_bytes(data)
        (self.source / "live-0000000000000001.jsonl").write_bytes(b"")
        self.assertEqual(exporter.discover_sealed(self.source, "live", "b" * 64), [])
        _, successor = record(1, first["hash"], {"reason": "owner_control", "ledger": ledger}, live=True)
        (self.source / "live-0000000000000001.jsonl").write_bytes(successor)
        prefixes = exporter.discover_sealed(self.source, "live", "b" * 64)
        self.assertEqual(len(prefixes), 1)
        self.assertEqual(prefixes[0]["next_sequence"], 1)
        exporter.export_prefix(self.source, self.destination, prefixes[0], "aws-mm2-live")
        (self.source / "live-0000000000000000.jsonl").write_bytes(data[:-1])
        with self.assertRaises(exporter.ExportError):
            exporter.discover_sealed(self.source, "live", "b" * 64)

    def test_paths_and_existing_bundle_hashes_are_checked(self):
        watermark = engine_fixture(self.source)
        with self.assertRaises(exporter.ExportError):
            exporter.export_prefix(self.source, self.source / "bad", watermark, "aws-mm2-paper")
        exporter.export_prefix(self.source, self.destination, watermark, "aws-mm2-paper")
        (self.destination / "events.jsonl").write_text("tampered\n")
        with self.assertRaises(exporter.ExportError):
            exporter.export_prefix(self.source, self.destination, watermark, "aws-mm2-paper")


if __name__ == "__main__":
    unittest.main()
