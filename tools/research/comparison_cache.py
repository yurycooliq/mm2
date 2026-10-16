"""Dedicated disposable public comparison hot cache with immutable daily copies.

Only initialize a new empty root. This module never deletes caller source paths,
primary research archives, financial ledgers, checkpoints, or unknown staging.
Daily checkpoints preserve exact rolling source windows, not proof of complete
calendar-day coverage or an S3 upload. Capacity exhaustion stops publication.
"""
from contextlib import contextmanager
from datetime import timedelta
import os
from pathlib import Path
import re
import uuid

from .archive import MAX_BUNDLE_BYTES, _sync_directory, verify_bundle
from .schema import ResearchError, canonical, digest, load_json, utc

ACCOUNT = "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3"
MAX_STORAGE = 512 * 1024 * 1024
MAX_ENTRIES = 10_000
DEFAULT_RESERVE = 2 * MAX_BUNDLE_BYTES + 2 * 1_048_576
MARKER = "comparison-cache.json"
INDEX = "cache-index.json"
HASH = re.compile(r"[0-9a-f]{64}")


def _plain(path):
    path = Path(path).absolute()
    if any(item.is_symlink() or getattr(item, "is_junction", lambda: False)() for item in (path, *path.parents)):
        raise ResearchError("comparison cache link rejected")
    return path


def _write(path, value):
    data = canonical(value) + b"\n"
    with path.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def _replace(path, value):
    _plain(path)
    temporary = path.parent / ("pending-" + uuid.uuid4().hex + ".json")
    _write(temporary, value)
    os.replace(temporary, path)
    _sync_directory(path.parent)


def initialize(root, max_bytes=MAX_STORAGE):
    """Create a marker only in an empty new dedicated root; never adopt an archive."""
    root = _plain(root)
    if type(max_bytes) is not int or not 1024 <= max_bytes <= MAX_STORAGE:
        raise ResearchError("invalid comparison cache quota")
    if root.exists() and (not root.is_dir() or any(root.iterdir())):
        raise ResearchError("comparison cache initialization requires empty root")
    root.mkdir(parents=True, exist_ok=True)
    _write(root / MARKER, {"schema_version": 1, "purpose": "disposable-secondary-public-comparison-cache",
        "absolute_root": str(root.resolve()), "account_id": ACCOUNT, "max_bytes": max_bytes,
        "hot_captures": 2, "immutable_daily_checkpoints": True, "remote_archive": "UNVERIFIED"})
    (root / "hot-observers").mkdir()
    (root / "checkpoints").mkdir()
    _write(root / INDEX, {"schema_version": 1, "captures": []})
    _sync_directory(root)
    return {"schema_version": 1, "status": "INITIALIZED", "root": str(root), "max_bytes": max_bytes}


def _marker(root):
    root = _plain(root)
    marker = load_json(_plain(root / MARKER), 4096)
    if (not isinstance(marker, dict) or marker.get("schema_version") != 1
            or marker.get("purpose") != "disposable-secondary-public-comparison-cache"
            or marker.get("absolute_root") != str(root.resolve()) or marker.get("account_id") != ACCOUNT
            or marker.get("hot_captures") != 2 or marker.get("immutable_daily_checkpoints") is not True
            or type(marker.get("max_bytes")) is not int or not 1024 <= marker["max_bytes"] <= MAX_STORAGE):
        raise ResearchError("comparison cache marker invalid")
    return root, marker


def storage_usage(root):
    root, marker = _marker(root)
    total, entries, pending = 0, 0, [root]
    while pending:
        for path in pending.pop().iterdir():
            entries += 1
            _plain(path)
            if entries > MAX_ENTRIES:
                raise ResearchError("comparison cache entry quota")
            if path.is_dir():
                pending.append(path)
            elif path.is_file():
                total += path.stat().st_size
            else:
                raise ResearchError("comparison cache special file")
            if total > marker["max_bytes"]:
                raise ResearchError("comparison cache byte quota")
    return total


def _bundle(path, parent, kind):
    path, parent = _plain(path), _plain(parent)
    if path.parent != parent or HASH.fullmatch(path.name) is None:
        raise ResearchError("comparison cache bundle outside dedicated hot root")
    manifest = verify_bundle(path)
    expected = {"kind": kind, "lane": "public-benchmark", "account_id": ACCOUNT}
    if kind == "one-public-account":
        expected["chain_id"] = 137
    if manifest["bundle_id"] != path.name or manifest.get("scope") != expected:
        raise ResearchError("comparison cache bundle scope mismatch")
    for member in manifest["files"]:
        _plain(path / member["path"])
    return manifest


def _pair(root, observer_id, view_id):
    observer = _bundle(root / "hot-observers" / observer_id, root / "hot-observers", "one-public-account")
    view = _bundle(root / view_id, root, "console-comparison-reference")
    data = load_json(root / view_id / "view.json", 524_288)
    if (not isinstance(data, dict) or {item["path"] for item in view["files"]} != {"view.json"}
            or data.get("source_bundle_id") != observer_id or data.get("account_id") != ACCOUNT
            or data.get("source_at_utc") != observer["created_at_utc"]
            or view["utc_range"] != observer["utc_range"]
            or utc(view["created_at_utc"]) < utc(observer["created_at_utc"])):
        raise ResearchError("comparison view source linkage mismatch")
    return observer, view


def _index(root):
    value = load_json(_plain(root / INDEX), 64_000)
    captures = value.get("captures") if isinstance(value, dict) else None
    if (not isinstance(value, dict) or value.get("schema_version") != 1 or not isinstance(captures, list) or len(captures) > 64):
        raise ResearchError("comparison cache index invalid")
    seen = set()
    for row in captures:
        if (not isinstance(row, dict) or set(row) != {"observer_id", "view_id", "captured_at_utc"}
                or HASH.fullmatch(str(row["observer_id"])) is None or HASH.fullmatch(str(row["view_id"])) is None
                or (row["observer_id"], row["view_id"]) in seen):
            raise ResearchError("comparison cache capture identity invalid")
        seen.add((row["observer_id"], row["view_id"]))
        utc(row["captured_at_utc"])
        observer, _ = _pair(root, row["observer_id"], row["view_id"])
        if observer["created_at_utc"] != row["captured_at_utc"]:
            raise ResearchError("comparison cache capture clock mismatch")
    pointer = root / "current.json"
    if pointer.exists():
        current = load_json(_plain(pointer), 256)
        if (not isinstance(current, dict) or set(current) != {"bundle_id"}
                or current["bundle_id"] not in {row["view_id"] for row in captures}):
            raise ResearchError("comparison cache pointer not indexed")
    return captures


def _checkpoints(root):
    result = []
    parent = _plain(root / "checkpoints")
    paths = sorted(parent.iterdir())
    if len(paths) > 366:
        raise ResearchError("comparison checkpoint count quota")
    for path in paths:
        _plain(path)
        if not path.is_dir() or re.fullmatch(r"\d{4}-\d{2}-\d{2}", path.name) is None:
            raise ResearchError("comparison checkpoint requires investigation")
        receipt = load_json(path / "checkpoint.json", 16_384)
        if not isinstance(receipt, dict) or receipt.get("checkpoint_id") != digest({key: value for key, value in receipt.items() if key != "checkpoint_id"}):
            raise ResearchError("comparison checkpoint receipt integrity")
        expected_keys = {"schema_version", "utc_day", "observer_id", "view_id", "source_range", "captured_at_utc",
                         "gap_from_previous_checkpoint_ms", "calendar_day_complete", "remote_archive", "checkpoint_id"}
        gap = receipt.get("gap_from_previous_checkpoint_ms")
        if (set(receipt) != expected_keys or receipt.get("schema_version") != 1 or receipt.get("utc_day") != path.name
                or receipt.get("calendar_day_complete") is not False or receipt.get("remote_archive") != "UNVERIFIED"
                or HASH.fullmatch(str(receipt.get("observer_id"))) is None or HASH.fullmatch(str(receipt.get("view_id"))) is None
                or (gap is not None and (type(gap) is not int or gap < 0))):
            raise ResearchError("comparison checkpoint semantics invalid")
        observer = _bundle(path / "observer" / receipt["observer_id"], path / "observer", "one-public-account")
        view = _bundle(path / "view" / receipt["view_id"], path / "view", "console-comparison-reference")
        data = load_json(path / "view" / receipt["view_id"] / "view.json", 524_288)
        if (receipt["source_range"] != observer["utc_range"] or receipt["captured_at_utc"] != observer["created_at_utc"]
                or data.get("source_bundle_id") != observer["bundle_id"] or view["utc_range"] != observer["utc_range"]
                or utc(receipt["captured_at_utc"]).date().isoformat() != path.name):
            raise ResearchError("comparison checkpoint source mismatch")
        if set(path.iterdir()) != {path / "checkpoint.json", path / "observer", path / "view"}:
            raise ResearchError("comparison checkpoint unexpected member")
        result.append(receipt)
    return result


def preflight(root, reserve_bytes=DEFAULT_RESERVE):
    """Validate audit copies before a collector fetch; count abandoned staging too."""
    root, marker = _marker(root)
    if type(reserve_bytes) is not int or reserve_bytes < 0:
        raise ResearchError("invalid cache capacity reservation")
    used = storage_usage(root)
    _index(root)
    checkpoints = _checkpoints(root)
    if used + reserve_bytes > marker["max_bytes"]:
        raise ResearchError("comparison cache capacity exhausted; checkpoints retained")
    return {"used_bytes": used, "max_bytes": marker["max_bytes"], "checkpoint_count": len(checkpoints),
            "reserved_bytes": reserve_bytes, "remote_archive": "UNVERIFIED"}


@contextmanager
def _lock(root):
    path = root / "comparison-cache.lock"
    try:
        with path.open("xb") as stream:
            stream.write(b"comparison-cache-single-writer\n")
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as error:
        raise ResearchError("comparison cache writer lock; investigation required") from error
    try:
        yield
    finally:
        _plain(path)
        if path.read_bytes() != b"comparison-cache-single-writer\n":
            raise ResearchError("comparison cache writer lock changed")
        path.unlink()


def _copy_bundle(source, parent, manifest):
    parent.mkdir()
    destination = parent / manifest["bundle_id"]
    destination.mkdir()
    for member in manifest["files"] + [{"path": "manifest.json"}]:
        name = member["path"]
        raw = _plain(source / name).read_bytes()
        with (destination / name).open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    if verify_bundle(destination) != manifest:
        raise ResearchError("comparison checkpoint copy mismatch")
    _sync_directory(destination)


def _remove_bundle(path, parent, manifest):
    """Unlink only verified flat files from an indexed obsolete hot copy."""
    path, parent = _plain(path), _plain(parent)
    if path.parent != parent or path.resolve().parent != parent.resolve() or HASH.fullmatch(path.name) is None:
        raise ResearchError("comparison deletion outside dedicated hot root")
    if verify_bundle(path) != manifest:
        raise ResearchError("comparison obsolete hot copy changed")
    for member in manifest["files"] + [{"path": "manifest.json"}]:
        target = _plain(path / member["path"])
        if target.resolve().parent != path.resolve():
            raise ResearchError("comparison deletion member escaped bundle")
        target.unlink()
    path.rmdir()


def commit(root, observer, view):
    """Own pointer publication and retain two verified hot captures plus daily copies.

    Caller publishes observer under root/hot-observers and view directly in root.
    Caller must preflight before network/publication. Existing legacy roots must
    remain separate. A failed prune retains the verified current pointer. A
    partial deletion fails index verification on the next preflight; unchanged
    obsolete copies can be retried after complete verification.
    """
    root, marker = _marker(root)
    observer, view = _plain(observer), _plain(view)
    if observer.parent != root / "hot-observers" or view.parent != root:
        raise ResearchError("comparison commit requires dedicated hot copies")
    with _lock(root):
        if storage_usage(root) + 32_768 > marker["max_bytes"]:
            raise ResearchError("comparison cache metadata capacity exhausted; current retained")
        previous = _index(root)
        checkpoints = _checkpoints(root)
        source_manifest, view_manifest = _pair(root, observer.name, view.name)
        captured = utc(source_manifest["created_at_utc"])
        if previous and captured < max(utc(row["captured_at_utc"]) for row in previous):
            raise ResearchError("comparison capture clock moved backwards")
        day = captured.date().isoformat()
        gap_ms = None
        checkpoint = next((row for row in checkpoints if row["utc_day"] == day), None)
        if checkpoint is None:
            additional = sum(member["bytes"] for item in (source_manifest, view_manifest) for member in item["files"])
            additional += len(canonical(source_manifest)) + len(canonical(view_manifest)) + 32_768
            if storage_usage(root) + additional > marker["max_bytes"]:
                raise ResearchError("comparison checkpoint capacity exhausted; current retained")
            if checkpoints:
                previous_end = max(utc(row["source_range"]["end"]) for row in checkpoints)
                gap_ms = max(0, int((utc(source_manifest["utc_range"]["start"]) - previous_end) / timedelta(milliseconds=1)))
            destination = root / "checkpoints" / day
            destination.mkdir()
            _copy_bundle(observer, destination / "observer", source_manifest)
            _copy_bundle(view, destination / "view", view_manifest)
            checkpoint = {"schema_version": 1, "utc_day": day, "observer_id": observer.name, "view_id": view.name,
                "source_range": source_manifest["utc_range"], "captured_at_utc": source_manifest["created_at_utc"],
                "gap_from_previous_checkpoint_ms": gap_ms, "calendar_day_complete": False, "remote_archive": "UNVERIFIED"}
            checkpoint["checkpoint_id"] = digest(checkpoint)
            _write(destination / "checkpoint.json", checkpoint)
            _sync_directory(destination)
            _checkpoints(root)
        else:
            gap_ms = checkpoint["gap_from_previous_checkpoint_ms"]
        row = {"observer_id": observer.name, "view_id": view.name, "captured_at_utc": source_manifest["created_at_utc"]}
        captures = [item for item in previous if (item["observer_id"], item["view_id"]) != (observer.name, view.name)] + [row]
        captures.sort(key=lambda item: (utc(item["captured_at_utc"]), item["view_id"]))
        if captures[-1] != row:
            raise ResearchError("ambiguous simultaneous comparison capture")
        _replace(root / INDEX, {"schema_version": 1, "captures": captures})
        _replace(root / "current.json", {"bundle_id": view.name})
        kept, deleted = captures[-2:], 0
        protected_observers = {item["observer_id"] for item in kept}
        protected_views = {item["view_id"] for item in kept}
        obsolete_views, obsolete_observers = {}, {}
        for old in captures[:-2]:
            old_observer, old_view = _pair(root, old["observer_id"], old["view_id"])
            if old["view_id"] not in protected_views:
                obsolete_views[old["view_id"]] = old_view
            if old["observer_id"] not in protected_observers:
                obsolete_observers[old["observer_id"]] = old_observer
        # Multiple views may share one source after interrupted publication.
        # Plan before unlinking and remove each verified hot identity once.
        for identity, manifest in obsolete_views.items():
            _remove_bundle(root / identity, root, manifest)
            deleted += 1
        for identity, manifest in obsolete_observers.items():
            _remove_bundle(root / "hot-observers" / identity, root / "hot-observers", manifest)
            deleted += 1
        _replace(root / INDEX, {"schema_version": 1, "captures": kept})
        used = storage_usage(root)
        return {"schema_version": 1, "status": "PUBLISHED", "bundle_id": view.name,
            "observer_bundle_id": observer.name, "used_bytes": used, "max_bytes": marker["max_bytes"],
            "hot_capture_count": len(kept), "checkpoint_count": len(_checkpoints(root)),
            "checkpoint_gap_ms": gap_ms, "deleted_secondary_hot_bundles": deleted,
            "checkpoint_id": checkpoint["checkpoint_id"], "calendar_day_complete": False,
            "remote_archive": "UNVERIFIED", "primary_evidence_deleted": False}
