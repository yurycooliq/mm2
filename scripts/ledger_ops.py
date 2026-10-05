#!/usr/bin/env python3
"""Stopped-engine journal verify, backup and restore. Never replaces existing state."""
from __future__ import annotations

import argparse
from contextlib import ExitStack, contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import time

MAX_TOTAL_BYTES = 1024 * 1024 * 1024
MAX_SEGMENTS = 4096
MAX_COMMANDS = 1_000_000
MAX_OPERATIONS = 1_000_000
ARCHIVE = re.compile(r"commands-[0-9]{8}\.jsonl\Z")
LIVE_ARCHIVE = re.compile(r"live-[0-9]{16}\.jsonl\Z")
IDENTIFIER = re.compile(r"[a-zA-Z0-9_-]{1,64}\Z")
ACK_MODES = {"paused", "shadow", "killed", "simulation", "trading", "both", "live"}


class LedgerError(Exception):
    pass


def digest(value):
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise LedgerError("duplicate JSON field")
        result[key] = value
    return result


def json_value(raw):
    try:
        return json.loads(raw, object_pairs_hook=unique_object,
                          parse_constant=lambda _: (_ for _ in ()).throw(LedgerError("nonfinite JSON")))
    except (ValueError, UnicodeError) as error:
        raise LedgerError("invalid JSON evidence") from error


def exact_fields(value, fields):
    if not isinstance(value, dict) or set(value) != set(fields):
        raise LedgerError("unexpected evidence schema")


def integer(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def regular_file(path):
    if path.is_symlink() or not path.is_file():
        raise LedgerError("evidence path must be a regular file")
    return path


def lines(path, limit):
    with regular_file(path).open("rb") as source:
        while True:
            raw = source.readline(limit + 1)
            if not raw:
                return
            if len(raw) > limit or not raw.endswith(b"\n"):
                raise LedgerError("partial or oversized evidence record")
            yield json_value(raw)


def evidence_files(directory, depth=0):
    if depth > 1:
        raise LedgerError("unsupported nested execution state directory")
    names = []
    for path in directory.iterdir():
        if path.name.startswith("commands-") and not ARCHIVE.fullmatch(path.name):
            raise LedgerError("unexpected journal archive name")
        if path.name == "commands.jsonl" or ARCHIVE.fullmatch(path.name) or path.name in {
            "journal-manifest.jsonl", "execution-profile.json"
        }:
            regular_file(path)
            names.append(path.name)
        elif path.name not in {"engine.lock", "paper", "live", "backup-manifest.json", "INCOMPLETE"}:
            raise LedgerError("unrecognized engine state file; backup needs an updated schema")
    if "commands.jsonl" not in names:
        raise LedgerError("active command journal is missing")
    paper = directory / "paper"
    if paper.exists():
        if paper.is_symlink() or not paper.is_dir():
            raise LedgerError("paper ledger directory must not be linked")
        names.extend("paper/" + name for name in evidence_files(paper, depth + 1))
    live = directory / "live"
    if live.exists():
        names.extend("live/" + name for name in live_evidence_files(live))
    return sorted(names)


def live_evidence_files(directory):
    if directory.is_symlink() or not directory.is_dir():
        raise LedgerError("live ledger directory must not be linked")
    names = []
    for path in directory.iterdir():
        if path.name == "live.lock":
            regular_file(path)
        elif path.name == "live-identity.json" or LIVE_ARCHIVE.fullmatch(path.name):
            regular_file(path)
            names.append(path.name)
            if len(names) > MAX_SEGMENTS + 1:
                raise LedgerError("live journal archive count exceeds backup bound")
        else:
            raise LedgerError("unrecognized live state file; backup needs an updated schema")
    return sorted(names)


def verify_live_at(directory, names):
    """Validate checkpoint chain. Live Rust startup additionally validates ledger invariants."""
    files = sorted(name for name in names if name.endswith(".jsonl"))
    identity = directory / "live-identity.json"
    if files and not identity.exists():
        raise LedgerError("live account identity is missing")
    if identity.exists():
        if regular_file(identity).stat().st_size > 65_536:
            raise LedgerError("live identity exceeds byte bound")
        raw_identity = identity.read_bytes()
        if not re.fullmatch(rb"[0-9a-f]{64}\n?", raw_identity):
            raise LedgerError("invalid live identity schema")
    sequence, previous = 0, "0" * 64
    for index, name in enumerate(files):
        start = int(Path(name).stem.removeprefix("live-"))
        if start != sequence:
            raise LedgerError("live journal segment ordering mismatch")
        records = 0
        for record in lines(directory / name, 1024 * 1024):
            exact_fields(record, {"sequence", "previous_hash", "hash", "reason", "ledger"})
            if (not integer(record["sequence"]) or record["sequence"] != sequence
                    or record["previous_hash"] != previous or not isinstance(record["reason"], str)
                    or not isinstance(record["ledger"], dict)
                    or record["hash"] != digest([sequence, previous, record["reason"], record["ledger"]])):
                raise LedgerError("live checkpoint sequence or hash mismatch")
            sequence += 1
            records += 1
            previous = record["hash"]
        if not records and index != len(files) - 1:
            raise LedgerError("live journal segment is empty")
    return {"checkpoint_records": sequence, "segments": len(files), "last_hash": previous}


def verify_live(directory, names):
    return verify_live_at(directory / "live", [name.removeprefix("live/")
                          for name in names if name.startswith("live/")])


def live_source_path(source, requested):
    profile = source / "execution-profile.json"
    if profile.exists():
        if regular_file(profile).stat().st_size > 4096:
            raise LedgerError("execution profile exceeds byte bound")
        identity = json_value(profile.read_bytes())
        if isinstance(identity, dict) and identity.get("live_state_dir") and requested is None:
            raise LedgerError("this profile uses a shared account ledger; provide --live-state-dir")
    if requested is None:
        return None
    live = Path(requested).resolve(strict=True)
    if not live.is_dir() or live == source or source in live.parents or live in source.parents:
        raise LedgerError("shared live ledger must be a separate directory")
    if (source / "live").exists():
        raise LedgerError("embedded and shared account ledgers cannot be mixed")
    if "live-identity.json" not in live_evidence_files(live):
        raise LedgerError("shared live account identity is missing")
    return live


def source_objects(source, live=None):
    objects = [(name, source / name) for name in evidence_files(source)]
    if live is not None:
        objects.extend(("live/" + name, live / name) for name in live_evidence_files(live))
    return sorted(objects)


def verify_source(source, live=None):
    verification = verify_journal(source)
    if live is not None:
        names = live_evidence_files(live)
        verification["live"] = verify_live_at(live, names)
        verification["total_bytes"] += sum((live / name).stat().st_size for name in names)
        if verification["total_bytes"] > MAX_TOTAL_BYTES:
            raise LedgerError("combined backup evidence exceeds byte bound")
    return verification


def verify_journal(directory, depth=0):
    """Integrity, sequence, receipt and rotation checks; engine replay checks domain state."""
    directory = Path(directory)
    names = evidence_files(directory, depth)
    total = sum((directory / name).stat().st_size for name in names)
    if total > MAX_TOTAL_BYTES:
        raise LedgerError("evidence exceeds supported backup byte bound")
    seals = []
    manifest = directory / "journal-manifest.jsonl"
    if manifest.exists():
        if manifest.stat().st_size > 2 * 1024 * 1024:
            raise LedgerError("segment manifest exceeds byte bound")
        for seal in lines(manifest, 1024):
            exact_fields(seal, {"segment", "bytes", "next_sequence", "last_hash", "hash"})
            if (not integer(seal["segment"]) or seal["segment"] != len(seals)
                    or len(seals) >= MAX_SEGMENTS or not integer(seal["bytes"])
                    or seal["bytes"] == 0 or not integer(seal["next_sequence"])
                    or seal["next_sequence"] == 0 or not isinstance(seal["last_hash"], str)
                    or len(seal["last_hash"]) != 64
                    or seal["hash"] != digest([seal["segment"], seal["bytes"],
                                              seal["next_sequence"], seal["last_hash"]])):
                raise LedgerError("invalid segment seal")
            seals.append(seal)
    expected_archives = {f"commands-{index:08}.jsonl" for index in range(len(seals))}
    actual_archives = {name for name in names if ARCHIVE.fullmatch(name)}
    if actual_archives - expected_archives:
        raise LedgerError("unsealed journal archive")
    sequence, previous, command_count, operation_count = 0, "0" * 64, 0, 0
    command_ids = set()

    def replay(path):
        nonlocal sequence, previous, command_count, operation_count
        for record in lines(path, 65_536):
            exact_fields(record, {"sequence", "previous_hash", "hash", "entry"})
            entry = record["entry"]
            if (not integer(record["sequence"]) or record["sequence"] != sequence
                    or record["previous_hash"] != previous
                    or record["hash"] != digest([sequence, previous, entry])):
                raise LedgerError("journal sequence or hash chain mismatch")
            if not isinstance(entry, dict):
                raise LedgerError("invalid journal entry")
            if entry.get("kind") == "bootstrap" and sequence == 0:
                exact_fields(entry, {"kind", "limits"})
                if not isinstance(entry["limits"], dict):
                    raise LedgerError("invalid bootstrap limits")
            elif entry.get("kind") in {"command", "execution"} and sequence > 0:
                fields = {"kind", "operations"}
                if entry["kind"] == "command":
                    fields |= {"command", "receipt"}
                    command, receipt = entry.get("command"), entry.get("receipt")
                    if not isinstance(command, dict) or not isinstance(receipt, dict):
                        raise LedgerError("invalid durable owner command")
                    command_fields = {"kind", "id", "expected_version"}
                    if command.get("kind") == "control":
                        command_fields.add("action")
                        if command.get("action") not in {"pause", "resume", "kill", "reset"}:
                            raise LedgerError("invalid durable owner action")
                    elif command.get("kind") == "limits":
                        command_fields.add("limits")
                    else:
                        raise LedgerError("unknown durable owner command")
                    exact_fields(command, command_fields)
                    exact_fields(receipt, {"command_id", "applied_version", "mode_at_ack"})
                    if (command_count >= MAX_COMMANDS or not isinstance(command["id"], str)
                            or not IDENTIFIER.fullmatch(command["id"])
                            or command["id"] in command_ids
                            or not integer(command["expected_version"])
                            or command["expected_version"] != command_count
                            or not integer(receipt["applied_version"])
                            or receipt["applied_version"] != command_count + 1
                            or receipt["command_id"] != command["id"]
                            or receipt["mode_at_ack"] not in ACK_MODES):
                        raise LedgerError("invalid or duplicate durable owner receipt")
                    command_ids.add(command["id"])
                    command_count += 1
                exact_fields(entry, fields)
                operations = entry["operations"]
                if (not isinstance(operations, list) or len(operations) > 32
                        or (entry["kind"] == "execution" and not operations)):
                    raise LedgerError("invalid execution transaction")
                operation_count += len(operations)
                if operation_count > MAX_OPERATIONS:
                    raise LedgerError("execution history exceeds supported backup bound")
            else:
                raise LedgerError("invalid bootstrap ordering")
            sequence += 1
            previous = record["hash"]

    pending_rotation = False
    for seal in seals:
        name = f"commands-{seal['segment']:08}.jsonl"
        path = directory / name
        if not path.exists():
            if seal["segment"] != len(seals) - 1:
                raise LedgerError("historical journal archive is missing")
            path = directory / "commands.jsonl"
            pending_rotation = True
        replay(path)
        if (path.stat().st_size != seal["bytes"] or sequence != seal["next_sequence"]
                or previous != seal["last_hash"]):
            raise LedgerError("journal archive does not match its durable seal")
    if not pending_rotation:
        replay(directory / "commands.jsonl")
    if not sequence:
        raise LedgerError("journal lacks durable bootstrap")
    return {"total_bytes": total, "archive_segments": len(seals), "record_count": sequence,
            "owner_commands": command_count, "execution_operations": operation_count,
            "last_hash": previous, "pending_rotation": pending_rotation,
            "live": verify_live(directory, names),
            "paper": verify_journal(directory / "paper", depth + 1)
                     if (directory / "paper").exists() else None}


@contextmanager
def file_fence(directory, name):
    path = directory / name
    if path.is_symlink():
        raise LedgerError("engine fence must not be a symbolic link")
    with path.open("a+b") as fence:
        try:
            if os.name == "nt":
                import msvcrt
                fence.seek(0)
                msvcrt.locking(fence.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fence.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise LedgerError("engine owns this directory; stop it before ledger maintenance") from error
        try:
            yield
        finally:
            if os.name == "nt":
                fence.seek(0)
                msvcrt.locking(fence.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(fence.fileno(), fcntl.LOCK_UN)


@contextmanager
def engine_fence(directory, live_directory=None):
    with ExitStack() as fences:
        fences.enter_context(file_fence(directory, "engine.lock"))
        paper = directory / "paper"
        if paper.exists():
            if paper.is_symlink() or not paper.is_dir() or (paper / "paper").exists():
                raise LedgerError("paper state directory must not be linked or nested")
            fences.enter_context(file_fence(paper, "engine.lock"))
        live = live_directory if live_directory is not None else directory / "live"
        if live.exists():
            if live.is_symlink() or not live.is_dir():
                raise LedgerError("live state directory must not be linked")
            fences.enter_context(file_fence(live, "live.lock"))
        yield


def checksum(path):
    with regular_file(path).open("rb") as source:
        result = hashlib.file_digest(source, "sha256")
    return result.hexdigest()


def sync_directory(directory):
    if os.name != "nt":
        descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def copy_durable(source, destination):
    with regular_file(source).open("rb") as incoming, destination.open("xb") as outgoing:
        shutil.copyfileobj(incoming, outgoing, 1024 * 1024)
        outgoing.flush()
        os.fsync(outgoing.fileno())


def distinct_paths(source, destination):
    source = Path(source).resolve(strict=True)
    destination = Path(destination).absolute()
    if destination.exists() or destination.is_symlink():
        raise LedgerError("destination must be a new path; existing files are never replaced")
    parent = destination.parent.resolve(strict=True)
    destination = parent / destination.name
    if destination == source or source in destination.parents or destination in source.parents:
        raise LedgerError("source and destination must be separate directories")
    if not source.is_dir():
        raise LedgerError("source must be a directory")
    return source, destination


def backup(source, destination, live_state_dir=None):
    source, destination = distinct_paths(source, destination)
    live = live_source_path(source, live_state_dir)
    if live is not None and (destination == live or live in destination.parents or destination in live.parents):
        raise LedgerError("backup destination must be separate from the shared account ledger")
    with engine_fence(source, live):
        verification = verify_source(source, live)
        source_files = source_objects(source, live)
        destination.mkdir(mode=0o700)
        marker = destination / "INCOMPLETE"
        marker.write_text("Incomplete backup; preserve and investigate.\n", encoding="utf-8")
        objects = []
        for name, source_path in source_files:
            (destination / name).parent.mkdir(mode=0o700, exist_ok=True)
            copy_durable(source_path, destination / name)
            objects.append({"name": name, "bytes": source_path.stat().st_size,
                            "sha256": checksum(destination / name)})
            if objects[-1]["sha256"] != checksum(source_path):
                raise LedgerError("state changed during the fenced backup")
        if source_objects(source, live) != source_files or verify_journal(destination) != verification:
            raise LedgerError("backup replay does not match the source ledger")
        layout = "sidecar" if live is not None else "embedded" if any(
            name.startswith("live/") for name, _ in source_files) else "none"
        manifest = {"format": "mm2-offline-backup-v2", "created_unix": int(time.time()),
                    "files": objects, "journal": verification, "live_layout": layout}
        with (destination / "backup-manifest.json").open("xb") as output:
            output.write(json.dumps(manifest, sort_keys=True, indent=2).encode("utf-8") + b"\n")
            output.flush()
            os.fsync(output.fileno())
        marker.unlink()
        sync_directory(destination)
        sync_directory(destination.parent)
        return verification


def verify_backup(directory):
    directory = Path(directory).resolve(strict=True)
    if (directory / "INCOMPLETE").exists():
        raise LedgerError("backup is incomplete")
    path = regular_file(directory / "backup-manifest.json")
    if path.stat().st_size > 2 * 1024 * 1024:
        raise LedgerError("backup manifest exceeds bound")
    manifest = json_value(path.read_bytes())
    fields = {"format", "created_unix", "files", "journal"}
    if manifest.get("format") == "mm2-offline-backup-v2":
        fields.add("live_layout")
    exact_fields(manifest, fields)
    if (manifest["format"] not in {"mm2-offline-backup-v1", "mm2-offline-backup-v2"}
            or not isinstance(manifest["files"], list)
            or manifest.get("live_layout", "embedded") not in {"none", "embedded", "sidecar"}):
        raise LedgerError("unsupported backup format")
    names = evidence_files(directory)
    if len(manifest["files"]) != len(names):
        raise LedgerError("backup file set changed")
    recorded = set()
    for item in manifest["files"]:
        exact_fields(item, {"name", "bytes", "sha256"})
        if item["name"] not in names or item["name"] in recorded:
            raise LedgerError("unsafe or duplicate backup file path")
        recorded.add(item["name"])
        path = directory / item["name"]
        if path.stat().st_size != item["bytes"] or checksum(path) != item["sha256"]:
            raise LedgerError("backup object checksum mismatch")
    verification = verify_journal(directory)
    if verification != manifest["journal"]:
        raise LedgerError("backup journal verification mismatch")
    return verification, names


def restore(source, destination, live_destination=None):
    source, destination = distinct_paths(source, destination)
    verification, names = verify_backup(source)
    original_manifest = (source / "backup-manifest.json").read_bytes()
    manifest = json_value(original_manifest)
    object_hashes = {item["name"]: item["sha256"] for item in manifest["files"]}
    sidecar = manifest.get("live_layout") == "sidecar"
    if sidecar != (live_destination is not None):
        raise LedgerError("shared account backup requires an explicit new --live-destination; embedded backups do not")
    live = None
    if sidecar:
        _, live = distinct_paths(source, live_destination)
        if destination == live or destination in live.parents or live in destination.parents:
            raise LedgerError("restored profile and shared account directories must be separate")
    destination.mkdir(mode=0o700)
    if sidecar:
        live.mkdir(mode=0o700)
    elif any(name.startswith("live/") for name in names):
        (destination / "live").mkdir(mode=0o700)
    if any(name.startswith("paper/") for name in names):
        (destination / "paper").mkdir(mode=0o700)
    with engine_fence(destination, live):
        for name in names:
            target = live / name.removeprefix("live/") if sidecar and name.startswith("live/") else destination / name
            copy_durable(source / name, target)
            if checksum(target) != object_hashes[name]:
                raise LedgerError("backup changed during restore")
        if (source / "backup-manifest.json").read_bytes() != original_manifest:
            raise LedgerError("backup manifest changed during restore")
        if verify_source(destination, live) != verification:
            raise LedgerError("restored journal does not match backup")
        sync_directory(destination)
        sync_directory(destination.parent)
        if live is not None:
            sync_directory(live)
            sync_directory(live.parent)
    return verification


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    for action in ("verify", "backup", "restore", "verify-backup"):
        command = sub.add_parser(action)
        command.add_argument("--source", required=True, type=Path)
        if action in {"backup", "restore"}:
            command.add_argument("--destination", required=True, type=Path)
        if action in {"verify", "backup"}:
            command.add_argument("--live-state-dir", type=Path)
        if action == "restore":
            command.add_argument("--live-destination", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.action == "verify":
            source = args.source.resolve(strict=True)
            live = live_source_path(source, args.live_state_dir)
            with engine_fence(source, live):
                result = verify_source(source, live)
        elif args.action == "backup":
            result = backup(args.source, args.destination, args.live_state_dir)
        elif args.action == "restore":
            result = restore(args.source, args.destination, args.live_destination)
        else:
            result, _ = verify_backup(args.source)
        print(json.dumps({"operation": args.action, "verified": True, "journal": result}, sort_keys=True))
        return 0
    except (LedgerError, OSError) as error:
        print(f"ledger maintenance refused: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
