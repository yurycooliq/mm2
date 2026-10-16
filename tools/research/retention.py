"""Manifest-only cleanup of dedicated disposable secondary directories.

Financial bundles and primary journals are never eligible. The plan and durable
receipts live outside the selected root and survive partial/interrupted deletion.
"""
import hashlib
import os
from pathlib import Path
import stat
import time
from .jobs import sha
from .schema import ResearchError, canonical, digest, load_json

MARKER = ".mm2-secondary-root.json"
CLASSES = {"diagnostic", "cache", "build"}
ROOT_NAMES = {"diagnostics", "secondary-cache", "build-cache", "exports-cache"}
FORBIDDEN = {".git", "secrets", "private", "wallet", "journals", "ledger", "releases", "mm2-state"}
MAX_FILES = 10_000
MAX_BYTES = 512 * 1024 * 1024


def _plain(path):
    # On Windows junctions/reparse points are not all reported as symlinks.
    if path.is_symlink():
        raise ResearchError("retention symlink rejected")
    info = path.lstat()
    if getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
        raise ResearchError("retention reparse point rejected")


def _root(path):
    path = Path(path).absolute()
    if path.name not in ROOT_NAMES or any(part.lower() in FORBIDDEN for part in path.parts):
        raise ResearchError("retention root is outside dedicated secondary scope")
    for member in (path, *path.parents):
        if member.exists():
            _plain(member)
    if not path.is_dir():
        raise ResearchError("retention root missing")
    return path.resolve(strict=True)


def initialize_root(path, retention_class):
    path = Path(path).absolute()
    if retention_class not in CLASSES or path.name not in ROOT_NAMES:
        raise ResearchError("financial/unknown retention class is not disposable")
    if any(part.lower() in FORBIDDEN for part in path.parts):
        raise ResearchError("primary/private scope cannot become a secondary root")
    # Inspect ancestors before mkdir; never create through a linked parent.
    for parent in path.parents:
        if parent.exists():
            _plain(parent)
    path.mkdir(parents=True, exist_ok=True)
    path = _root(path)
    value = {"schema_version": 1, "root": str(path), "retention_class": retention_class,
             "primary_deletion_allowed": False}
    marker = path / MARKER
    if marker.exists():
        if load_json(marker) != value:
            raise ResearchError("secondary root identity/class changed")
    else:
        _write_exclusive(marker, value)
    return value


def _write_exclusive(path, value):
    with Path(path).open("xb") as stream:
        stream.write(canonical(value) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def _inventory(root):
    result, total = [], 0
    for current, directories, filenames in os.walk(root, followlinks=False):
        for name in directories:
            _plain(Path(current) / name)
            if name.lower() in FORBIDDEN:
                raise ResearchError("protected member found in secondary root")
        for name in filenames:
            path = Path(current) / name
            _plain(path)
            if path.name == MARKER:
                continue
            relative = path.relative_to(root).as_posix()
            if any(part.lower() in FORBIDDEN for part in path.relative_to(root).parts):
                raise ResearchError("protected member found in secondary root")
            if ("journal" in path.name.lower() or "ledger" in path.name.lower()
                    or path.suffix.lower() in {".key", ".env", ".sqlite3", ".db"} or path.name == "manifest.json"):
                raise ResearchError("financial, credential or control state is not secondary garbage")
            info = path.stat()
            total += info.st_size
            if total > MAX_BYTES or len(result) >= MAX_FILES:
                raise ResearchError("retention inspection exceeds bounded workload")
            result.append({"path": relative, "bytes": info.st_size, "mtime_ns": info.st_mtime_ns,
                           "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    return sorted(result, key=lambda item: item["path"])


def preview(path, *, policy_hash, cutoff_epoch, pins, lease_paths):
    sha(policy_hash, "retention policy hash")
    root = _root(path)
    marker = load_json(root / MARKER)
    if marker.get("root") != str(root) or marker.get("retention_class") not in CLASSES or marker.get("primary_deletion_allowed") is not False:
        raise ResearchError("root lacks valid disposable identity")
    if type(cutoff_epoch) is not int or not 0 <= cutoff_epoch <= int(time.time()) - 30 * 86400:
        raise ResearchError("invalid finite retention cutoff")
    if not isinstance(pins, list) or not isinstance(lease_paths, list):
        raise ResearchError("explicit experiment pins and active lease paths required")
    protected = set(pins + lease_paths)
    if any(not isinstance(name, str) or name.startswith("/") or ".." in Path(name).parts for name in protected):
        raise ResearchError("invalid protected relative path")
    inventory = _inventory(root)
    selected = [item for item in inventory if item["mtime_ns"] < cutoff_epoch * 1_000_000_000
                and not any(item["path"] == name or item["path"].startswith(name.rstrip("/") + "/") for name in protected)]
    body = {"schema_version": 1, "root": str(root), "root_identity_hash": digest(marker),
            "retention_class": marker["retention_class"], "policy_hash": policy_hash,
            "cutoff_epoch": cutoff_epoch, "pins": sorted(pins), "lease_paths": sorted(lease_paths),
            "inventory": inventory, "selected": selected, "inventory_hash": digest(inventory),
            "primary_deletion_allowed": False}
    return {**body, "plan_id": digest(body), "selected_bytes": sum(item["bytes"] for item in selected)}


def apply(plan, receipt_root, *, approved_plan_id, current_pins, current_lease_paths, policy_hash):
    sha(policy_hash, "retention policy hash")
    if plan.get("plan_id") != approved_plan_id:
        raise ResearchError("retention requires exact immutable plan approval")
    body = {key: value for key, value in plan.items() if key not in {"plan_id", "selected_bytes"}}
    if digest(body) != plan["plan_id"] or plan.get("primary_deletion_allowed") is not False or plan.get("policy_hash") != policy_hash:
        raise ResearchError("retention manifest/policy changed")
    if type(plan.get("cutoff_epoch")) is not int or not 0 <= plan["cutoff_epoch"] <= int(time.time()) - 30 * 86400:
        raise ResearchError("retention must preserve at least thirty days of disposable diagnostics")
    root = _root(plan["root"])
    receipt_root = Path(receipt_root).absolute()
    if receipt_root == root or root in receipt_root.parents:
        raise ResearchError("deletion receipts must survive outside selected root")
    for parent in (receipt_root, *receipt_root.parents):
        if parent.exists():
            _plain(parent)
    receipt_root.mkdir(parents=True, exist_ok=True)
    marker = load_json(root / MARKER)
    if digest(marker) != plan["root_identity_hash"]:
        raise ResearchError("retention root identity changed")
    if sorted(current_pins) != plan["pins"] or sorted(current_lease_paths) != plan["lease_paths"]:
        raise ResearchError("pins/leases changed since preview")
    selected = {item["path"]: item for item in plan["selected"]}
    original = {item["path"]: item for item in plan["inventory"]}
    if any(original.get(name) != item for name, item in selected.items()):
        raise ResearchError("selected object outside preview inventory")
    for item in selected.values():
        if item["mtime_ns"] >= plan["cutoff_epoch"] * 1_000_000_000:
            raise ResearchError("selected object is younger than frozen cutoff")
    prepared = receipt_root / f"{plan['plan_id']}.prepared.json"
    if prepared.exists():
        if load_json(prepared).get("plan") != plan:
            raise ResearchError("conflicting prepared retention receipt")
    else:
        if _inventory(root) != plan["inventory"]:
            raise ResearchError("secondary tree changed since preview")
        _write_exclusive(prepared, {"schema_version": 1, "status": "PREPARED", "plan": plan})
    current = {item["path"]: item for item in _inventory(root)}
    unselected = {name: item for name, item in original.items() if name not in selected}
    if any(current.get(name) != item for name, item in unselected.items()) or set(current) - set(original):
        raise ResearchError("unselected tree changed during cleanup/recovery")
    removed = []
    for name, item in selected.items():
        if name not in current:
            # Only an already durable PREPARED receipt allows idempotent recovery.
            removed.append(name)
            continue
        if current[name] != item:
            raise ResearchError("selected member identity/hash changed")
        target = root / name
        if root not in target.resolve(strict=True).parents:
            raise ResearchError("selected member escapes canonical root")
        for parent in (target, *target.parents):
            if parent == root.parent:
                break
            _plain(parent)
        target.unlink()
        removed.append(name)
    after = {item["path"]: item for item in _inventory(root)}
    if after != unselected:
        raise ResearchError("selected absence or unselected preservation failed")
    result = {"schema_version": 1, "plan_id": plan["plan_id"], "status": "VERIFIED",
              "removed": sorted(removed), "unselected_hash": digest(sorted(unselected.values(), key=lambda item: item["path"])),
              "primary_deletion_allowed": False}
    completed = receipt_root / f"{plan['plan_id']}.verified.json"
    if completed.exists():
        if load_json(completed) != result:
            raise ResearchError("completed retention receipt changed")
    else:
        _write_exclusive(completed, result)
    return result
