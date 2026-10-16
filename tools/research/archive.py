"""Immutable, content-addressed bundles. This module never deletes source data."""
import argparse
import hashlib
import os
from pathlib import Path
import re
import uuid
from .schema import VERSION, MAX_FILE, ResearchError, canonical, digest, load_json, parse_json, utc, utc_now, window

MAX_BUNDLE_BYTES = 64 * 1024 * 1024
MAX_FILES = 32


def _name(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}", value) or value in {"manifest.json", ".", ".."}:
        raise ResearchError("bundle filename must be a flat safe basename")
    return value


def _sync_write(path, data):
    with path.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def _sync_directory(path):
    if os.name == "nt":
        return  # Directory fsync is unavailable through Python's Windows API.
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_bundle(root, files, *, scope, utc_range, quality, source_revision="unknown", config_hash=None, created_at_utc=None):
    """files maps basenames to JSON objects or lists (JSONL for .jsonl)."""
    if not isinstance(files, dict) or not 1 <= len(files) <= MAX_FILES:
        raise ResearchError("invalid bundle files")
    window(utc_range["start"], utc_range["end"])
    root = Path(root)
    if root.is_symlink():
        raise ResearchError("archive root symlink rejected")
    root.mkdir(parents=True, exist_ok=True)
    data_files, total = {}, 0
    for name, value in files.items():
        _name(name)
        if name.endswith(".jsonl"):
            if not isinstance(value, list):
                raise ResearchError("JSONL input must be a bounded record list")
            data = b"".join(canonical(row) + b"\n" for row in value)
            rows = len(value)
        elif name.endswith(".json"):
            data, rows = canonical(value) + b"\n", 1
        else:
            raise ResearchError("only canonical JSON/JSONL allowed")
        total += len(data)
        if total > MAX_BUNDLE_BYTES:
            raise ResearchError("bundle byte quota exceeded")
        data_files[name] = (data, rows)
    created_at_utc = created_at_utc or utc_now()
    utc(created_at_utc)
    manifest = {"schema_version": VERSION, "created_at_utc": created_at_utc, "utc_range": utc_range,
                "scope": scope, "quality": quality, "source_revision": source_revision,
                "config_hash": config_hash, "files": [{"path": name, "sha256": hashlib.sha256(data).hexdigest(),
                    "bytes": len(data), "rows": rows} for name, (data, rows) in sorted(data_files.items())]}
    bundle_id = digest(manifest)
    manifest["bundle_id"] = bundle_id
    final = root / bundle_id
    if final.exists():
        existing = verify_bundle(final)
        if existing != manifest:
            raise ResearchError("content-addressed bundle collision")
        return final
    # An interrupted staging directory is recoverable garbage; never remove it here.
    staging = root / ("publishing-" + uuid.uuid4().hex)
    # One exclusive mkdir. tempfile's Windows PermissionError retry loop can spin
    # through thousands of attempts on an actually inaccessible destination.
    staging.mkdir()
    for name, (data, _) in data_files.items():
        _sync_write(staging / name, data)
    _sync_write(staging / "manifest.json", canonical(manifest) + b"\n")
    _sync_directory(staging)
    try:
        os.rename(staging, final)
    except OSError:
        # Destination-exists errors are platform dependent: Windows may return
        # AccessDenied for a nonempty directory, Linux may return ENOTEMPTY.
        # Adopt a competing publication only after full content verification.
        if not final.is_dir():
            raise
        if verify_bundle(final) != manifest:
            raise ResearchError("concurrent publication conflict")
    _sync_directory(root)
    return final


def verify_bundle(path, max_bytes=MAX_BUNDLE_BYTES):
    path = Path(path)
    if path.is_symlink() or not path.is_dir():
        raise ResearchError("bundle directory missing or symlinked")
    manifest = load_json(path / "manifest.json", 512 * 1024)
    if manifest.get("schema_version") != VERSION:
        raise ResearchError("unsupported manifest schema")
    body = {key: value for key, value in manifest.items() if key != "bundle_id"}
    if manifest.get("bundle_id") != digest(body):
        raise ResearchError("manifest content hash mismatch")
    utc(manifest["created_at_utc"])
    window(**manifest["utc_range"])
    files = manifest.get("files")
    if not isinstance(files, list) or not 1 <= len(files) <= MAX_FILES:
        raise ResearchError("manifest file quota exceeded")
    seen, total = set(), 0
    for entry in files:
        name = _name(entry.get("path"))
        if name in seen:
            raise ResearchError("duplicate manifest path")
        seen.add(name)
        target = path / name
        if target.is_symlink() or not target.is_file():
            raise ResearchError("manifest member missing or symlinked")
        length = target.stat().st_size
        total += length
        if type(entry.get("bytes")) is not int or length != entry["bytes"] or total > max_bytes:
            raise ResearchError("manifest size mismatch/quota exceeded")
        data = target.read_bytes()
        if hashlib.sha256(data).hexdigest() != entry.get("sha256"):
            raise ResearchError("manifest member hash mismatch")
        if name.endswith(".jsonl"):
            if data and not data.endswith(b"\n"):
                raise ResearchError("incomplete JSONL member")
            lines = data.splitlines()
            if len(lines) != entry.get("rows"):
                raise ResearchError("manifest row count mismatch")
            for line in lines:
                parse_json(line)
        else:
            parse_json(data)
    actual = {p.name for p in path.iterdir()}
    if actual != seen | {"manifest.json"}:
        raise ResearchError("unmanifested bundle members")
    return manifest


def read_member(path, name):
    manifest = verify_bundle(path)
    if name not in {f["path"] for f in manifest["files"]}:
        raise ResearchError("member not in manifest")
    data = (Path(path) / name).read_bytes()
    return [parse_json(line) for line in data.splitlines()] if name.endswith(".jsonl") else parse_json(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle")
    args = parser.parse_args()
    print(canonical(verify_bundle(args.bundle)).decode())


if __name__ == "__main__":
    main()
