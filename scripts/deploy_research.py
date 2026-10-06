#!/usr/bin/env python3
"""Reviewed public-only research code artifact, verified before host activation."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile

from deploy_package import PUBLIC_FILES, ROOT

MAX_ARCHIVE = 4 * 1024 * 1024
FILES = tuple(name for name in PUBLIC_FILES if name.startswith(("tools/research/", "research/")) or name in {
    "scripts/research_bridge.py", "infra/aws/mm2-research.service", "infra/aws/mm2-research.timer"})
RELEASES = Path("/opt/mm2-research/releases")
UNITS = Path("/etc/systemd/system")


def pack(destination):
    if destination.exists() or destination.is_symlink():
        raise ValueError("public artifact destination must be new")
    if subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, check=True, capture_output=True, text=True).stdout.strip():
        raise ValueError("commit reviewed research source first")
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True).stdout.strip()
    files = {}
    for name in FILES:
        source = ROOT / name
        if source.is_symlink() or not source.is_file():
            raise ValueError("required public member unavailable")
        files[name] = subprocess.run(["git", "show", revision + ":" + name], cwd=ROOT,
            check=True, capture_output=True, timeout=30).stdout.replace(b"\r\n", b"\n")
    manifest = {"schema_version": 1, "source_revision": revision, "files": {
        name: {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()} for name, data in files.items()}}
    files["research-release.json"] = json.dumps(manifest, sort_keys=True).encode() + b"\n"
    with tarfile.open(destination, "x:gz") as archive:
        for name, data in files.items():
            member = tarfile.TarInfo(name); member.size = len(data); member.mode = 0o644
            archive.addfile(member, io.BytesIO(data))
    if destination.stat().st_size > MAX_ARCHIVE:
        raise ValueError("public artifact exceeds quota; preserve incomplete artifact")
    return {"source_revision": revision, "sha256": hashlib.sha256(destination.read_bytes()).hexdigest(), "bytes": destination.stat().st_size}


def install(source, revision, archive_hash, *, releases=None, units=None, run=None):
    run = run or subprocess.run
    if not re.fullmatch(r"[0-9a-f]{40}", revision) or not re.fullmatch(r"[0-9a-f]{64}", archive_hash):
        raise ValueError("exact revision/checksum required")
    if source.is_symlink() or not source.is_file() or source.stat().st_size > MAX_ARCHIVE:
        raise ValueError("public artifact checksum/quota mismatch")
    with source.open("rb") as stream:
        source_bytes = stream.read(MAX_ARCHIVE + 1)
    if len(source_bytes) > MAX_ARCHIVE or hashlib.sha256(source_bytes).hexdigest() != archive_hash:
        raise ValueError("public artifact checksum/quota mismatch")
    root = releases or RELEASES
    units = units or UNITS
    for parent in (root, units, *root.parents, *units.parents):
        if parent.exists() and (parent.is_symlink() or getattr(parent.lstat(), "st_file_attributes", 0) & 0x400):
            raise ValueError("activation root ancestor is linked")
    for unit in ("mm2-research.timer", "mm2-research.service"):
        code = run(["systemctl", "is-active", "--quiet", unit], check=False, timeout=10).returncode
        if code == 0:
            raise ValueError("stop research timer/service before activation")
        if code not in {3, 4}:
            raise ValueError("research lifecycle state could not be verified")
    for name in ("mm2-research.service", "mm2-research.timer"):
        unit_path = units / name
        if unit_path.is_symlink() or (unit_path.exists() and (not unit_path.is_file() or unit_path.stat().st_nlink != 1)):
            raise ValueError("research unit target is indirect")
    destination = root / revision
    if destination.exists() or destination.is_symlink():
        raise ValueError("retain existing release; choose a new exact source revision")
    with tarfile.open(fileobj=io.BytesIO(source_bytes), mode="r:gz") as archive:
        members = archive.getmembers()
        names = [member.name for member in members]
        if len(names) != len(set(names)) or set(names) != set(FILES) | {"research-release.json"} or any(not member.isfile() for member in members):
            raise ValueError("public artifact allowlist/type mismatch")
        if sum(member.size for member in members) > MAX_ARCHIVE:
            raise ValueError("expanded public artifact quota exceeded")
        manifest = json.load(archive.extractfile("research-release.json"))
        if manifest.get("source_revision") != revision or set(manifest.get("files", {})) != set(FILES):
            raise ValueError("research release identity mismatch")
        data = {}
        for name in FILES:
            body = archive.extractfile(name).read(MAX_ARCHIVE + 1)
            expected = manifest["files"][name]
            if expected != {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}:
                raise ValueError("public member hash mismatch")
            data[name] = body
    # Verify every member before creating anything executable or switching current.
    destination.mkdir(mode=0o755)
    for name, body in data.items():
        target = destination / name; target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as stream:
            stream.write(body); stream.flush(); os.fsync(stream.fileno())
        target.chmod(0o644)
    (destination / "research-release.json").write_text(json.dumps(manifest, sort_keys=True) + "\n")
    for name in ("mm2-research.service", "mm2-research.timer"):
        (units / name).write_bytes((destination / "infra/aws" / name).read_bytes())
    run(["systemctl", "daemon-reload"], check=True, timeout=15)
    pending = root.parent / "current.pending"
    pending.symlink_to(destination, target_is_directory=True)
    os.replace(pending, root.parent / "current")
    return {"source_revision": revision, "verified": True, "started": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("pack"); p.add_argument("--destination", type=Path, required=True)
    p = commands.add_parser("install"); p.add_argument("--source", type=Path, required=True)
    p.add_argument("--revision", required=True); p.add_argument("--sha256", required=True)
    args = parser.parse_args()
    try:
        result = pack(args.destination) if args.command == "pack" else install(args.source, args.revision, args.sha256)
        print(json.dumps(result, sort_keys=True)); return 0
    except Exception:
        print("research_deployment_failed; retained controller/archive/releases are preserved", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
