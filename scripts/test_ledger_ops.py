"""Recovery fixtures exercise fencing, exact evidence copying and fail-closed restore."""
import json
from pathlib import Path
import tempfile
import unittest

from ledger_ops import (LedgerError, backup, digest, engine_fence, restore,
                        file_fence, verify_backup, verify_journal)


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"


def fixture(directory, segmented=False, live=False):
    limits = {"initial_collateral_usd": "1000", "max_imbalance_shares": "25",
              "lot_shares": "5", "half_width": "0.045", "max_feed_age_ms": 1000}
    entries = [{"kind": "bootstrap", "limits": limits},
               {"kind": "command", "command": {"kind": "control", "id": "owner-1",
                                                "expected_version": 0, "action": "pause"},
                "receipt": {"command_id": "owner-1", "applied_version": 1, "mode_at_ack": "paused"},
                "operations": [{"kind": "advance", "now_ms": 10}]},
               {"kind": "execution", "operations": [{"kind": "advance", "now_ms": 20}]}]
    previous = "0" * 64
    records = []
    for sequence, entry in enumerate(entries):
        current = digest([sequence, previous, entry])
        records.append(encoded({"sequence": sequence, "previous_hash": previous,
                                "hash": current, "entry": entry}))
        previous = current
    if segmented:
        data = b"".join(records[:2])
        (directory / "commands-00000000.jsonl").write_bytes(data)
        last_hash = json.loads(records[1])["hash"]
        seal = {"segment": 0, "bytes": len(data), "next_sequence": 2, "last_hash": last_hash,
                "hash": digest([0, len(data), 2, last_hash])}
        (directory / "journal-manifest.jsonl").write_bytes(encoded(seal))
        (directory / "commands.jsonl").write_bytes(records[2])
    else:
        (directory / "commands.jsonl").write_bytes(b"".join(records))
    (directory / "execution-profile.json").write_text('{"mode":"simulation"}\n')
    if live:
        (directory / "live").mkdir()
        (directory / "live/live-identity.json").write_text("a" * 64)
        ledger = {"phase": "paused", "orders": [], "cash": "1000"}
        record = {"sequence": 0, "previous_hash": "0" * 64,
                  "hash": digest([0, "0" * 64, "bootstrap", ledger]),
                  "reason": "bootstrap", "ledger": ledger}
        (directory / "live/live-0000000000000000.jsonl").write_bytes(encoded(record))


class LedgerOperationsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.source = self.base / "state"
        self.source.mkdir()

    def tearDown(self):
        self.temporary.cleanup()

    def test_backup_and_restore_preserve_receipts_segments_and_live_ledger_exactly(self):
        fixture(self.source, segmented=True, live=True)
        (self.source / "paper").mkdir()
        fixture(self.source / "paper", segmented=True)
        verification = verify_journal(self.source)
        snapshot = self.base / "backup"
        self.assertEqual(backup(self.source, snapshot), verification)
        target = self.base / "restore"
        self.assertEqual(restore(snapshot, target), verification)
        self.assertEqual(verification["owner_commands"], 1)
        self.assertEqual(verification["live"]["checkpoint_records"], 1)
        self.assertEqual(verification["paper"]["execution_operations"], 2)
        for source in self.source.rglob("*"):
            if source.is_file() and source.name not in {"engine.lock", "live.lock"}:
                self.assertEqual(source.read_bytes(), (target / source.relative_to(self.source)).read_bytes())

    def test_process_fence_rejects_backup_while_engine_owns_state(self):
        fixture(self.source)
        with engine_fence(self.source):
            with self.assertRaises(LedgerError):
                backup(self.source, self.base / "backup")
        self.assertFalse((self.base / "backup").exists())

    def test_companion_paper_or_live_owner_also_fences_backup(self):
        fixture(self.source, live=True)
        (self.source / "paper").mkdir()
        fixture(self.source / "paper")
        for directory, name in ((self.source / "paper", "engine.lock"),
                                (self.source / "live", "live.lock")):
            with self.subTest(name=name), file_fence(directory, name):
                with self.assertRaises(LedgerError):
                    backup(self.source, self.base / "backup")
        self.assertFalse((self.base / "backup").exists())

    def test_only_a_final_empty_live_rotation_segment_is_recoverable(self):
        fixture(self.source, live=True)
        empty = self.source / "live/live-0000000000000001.jsonl"
        empty.write_bytes(b"")
        self.assertEqual(verify_journal(self.source)["live"]["checkpoint_records"], 1)
        (self.source / "live/live-0000000000000002.jsonl").write_bytes(b"")
        with self.assertRaises(LedgerError):
            verify_journal(self.source)

    def shared_account_fixture(self):
        fixture(self.source, live=True)
        shared = self.base / "account-live"
        (self.source / "live").rename(shared)
        (self.source / "execution-profile.json").write_text(json.dumps({
            "schema": 1, "mode": "both", "feed_source": "public",
            "live_state_dir": "/data/account-live"}))
        return shared

    def test_shared_account_backup_requires_sidecar_and_restores_to_two_new_directories(self):
        shared = self.shared_account_fixture()
        snapshot = self.base / "backup"
        with self.assertRaises(LedgerError):
            backup(self.source, snapshot)
        self.assertFalse(snapshot.exists())
        verified = backup(self.source, snapshot, shared)
        manifest = json.loads((snapshot / "backup-manifest.json").read_bytes())
        self.assertEqual(manifest["live_layout"], "sidecar")
        self.assertEqual(verified["live"]["checkpoint_records"], 1)
        target, account = self.base / "restored-profile", self.base / "restored-account"
        with self.assertRaises(LedgerError):
            restore(snapshot, target)
        self.assertFalse(target.exists())
        self.assertEqual(restore(snapshot, target, account), verified)
        self.assertFalse((target / "live").exists())
        for source in shared.iterdir():
            if source.name != "live.lock":
                self.assertEqual(source.read_bytes(), (account / source.name).read_bytes())
        self.assertEqual((target / "execution-profile.json").read_bytes(),
                         (self.source / "execution-profile.json").read_bytes())

    def test_shared_account_fence_and_destination_boundaries_are_enforced(self):
        shared = self.shared_account_fixture()
        with file_fence(shared, "live.lock"):
            with self.assertRaises(LedgerError):
                backup(self.source, self.base / "backup", shared)
        with self.assertRaises(LedgerError):
            backup(self.source, shared / "nested-backup", shared)
        self.assertFalse((shared / "nested-backup").exists())
        snapshot = self.base / "backup"
        backup(self.source, snapshot, shared)
        target = self.base / "restored-profile"
        with self.assertRaises(LedgerError):
            restore(snapshot, target, shared)
        self.assertFalse(target.exists())

    def test_embedded_and_external_account_sources_cannot_be_combined(self):
        fixture(self.source, live=True)
        shared = self.base / "account-live"
        shared.mkdir()
        (shared / "live-identity.json").write_text("a" * 64)
        with self.assertRaises(LedgerError):
            backup(self.source, self.base / "backup", shared)

    def test_existing_destinations_and_nested_destinations_are_never_modified(self):
        fixture(self.source)
        existing = self.base / "existing"
        existing.mkdir()
        (existing / "keep").write_text("preserve")
        for path in (existing, self.source / "nested"):
            with self.assertRaises(LedgerError):
                backup(self.source, path)
        self.assertEqual((existing / "keep").read_text(), "preserve")

    def test_partial_command_or_manifest_or_missing_archive_refuses_backup(self):
        for corruption in ("command", "manifest", "archive"):
            with self.subTest(corruption=corruption):
                state = self.base / corruption
                state.mkdir()
                fixture(state, segmented=True)
                if corruption == "command":
                    with (state / "commands.jsonl").open("ab") as output:
                        output.write(b'{"partial":')
                elif corruption == "manifest":
                    with (state / "journal-manifest.jsonl").open("ab") as output:
                        output.write(b'{"partial":')
                else:
                    (state / "commands-00000000.jsonl").unlink()
                with self.assertRaises(LedgerError):
                    backup(state, self.base / (corruption + "-backup"))

    def test_complete_pending_rename_is_verified_without_modifying_source(self):
        fixture(self.source, segmented=True)
        active = self.source / "commands.jsonl"
        active.unlink()
        (self.source / "commands-00000000.jsonl").rename(active)
        before = active.read_bytes()
        self.assertTrue(verify_journal(self.source)["pending_rotation"])
        self.assertEqual(active.read_bytes(), before)
        self.assertFalse((self.source / "commands-00000000.jsonl").exists())

    def test_changed_backup_refuses_restore_before_creating_target(self):
        fixture(self.source, live=True)
        snapshot = self.base / "backup"
        backup(self.source, snapshot)
        (snapshot / "live/live-identity.json").write_text("b" * 64)
        target = self.base / "restore"
        with self.assertRaises(LedgerError):
            restore(snapshot, target)
        self.assertFalse(target.exists())

    def test_partial_live_checkpoint_and_unknown_live_file_refuse_backup(self):
        fixture(self.source, live=True)
        unknown = self.source / "live/unrecognized-state.json"
        unknown.write_text("{}")
        with self.assertRaises(LedgerError):
            verify_journal(self.source)
        unknown.unlink()
        with (self.source / "live/live-0000000000000000.jsonl").open("ab") as output:
            output.write(b'{"partial":')
        with self.assertRaises(LedgerError):
            verify_journal(self.source)

    def test_backup_path_traversal_and_incomplete_marker_are_rejected(self):
        fixture(self.source)
        snapshot = self.base / "backup"
        backup(self.source, snapshot)
        manifest = snapshot / "backup-manifest.json"
        original = manifest.read_bytes()
        value = json.loads(original)
        value["files"][0]["name"] = "../state/commands.jsonl"
        manifest.write_bytes(encoded(value))
        with self.assertRaises(LedgerError):
            verify_backup(snapshot)
        manifest.write_bytes(original)
        (snapshot / "INCOMPLETE").write_text("unfinished")
        with self.assertRaises(LedgerError):
            verify_backup(snapshot)


if __name__ == "__main__":
    unittest.main()
