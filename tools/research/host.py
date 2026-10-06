"""Isolated observer, sanitized archive ingest, accounting and durable job scheduler."""
import argparse
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import queue
import re
import signal
import shutil
import subprocess
import sys
import threading
import time
from .accounting import account_bundle
from .archive import MAX_BUNDLE_BYTES, publish_bundle, read_member, verify_bundle
from .compare import build_comparison
from .controller import bundle_inputs, enqueue, load_policy
from .jobs import MAX_JOBS, JobStore, bounded
from .registry import DEFAULT_ACCOUNTS
from .schema import ResearchError, canonical, digest, load_json, parse_json, quality, utc
from .storage import client, download_bundle, upload_bundle

MAX_TREE = 8 * 1024**3
MAX_TREE_ENTRIES = 100_000
MAX_METADATA = 30_000
MAX_INDEX_KEYS = 10_000
MAX_MANIFESTS = 5000
MAX_INGEST = 16
MAX_PROCESS_OUTPUT = 512 * 1024
# Reserve the worst-case growth of ingest, both source/accounting bundles, packet
# and controller database before starting work. Exhaustion pauses only research.
MAX_CYCLE_GROWTH = (MAX_INGEST + 5) * (MAX_BUNDLE_BYTES + 512 * 1024) + 128 * 1024**2
MAX_CYCLE_ENTRIES = 1000


def tree_budget(root, *, reserve_entries=0):
    total, entries = 0, 0
    for item in root.rglob("*"):
        entries += 1
        if item.is_symlink():
            raise ResearchError("research state symlink")
        if item.is_file():
            total += item.stat().st_size
        if total > MAX_TREE or entries > MAX_TREE_ENTRIES - reserve_entries:
            raise ResearchError("research archive quota requires retention/compaction review")
    return total


def export_ingest(s3, bucket, root):
    manifests, pages, keys = set(), 0, 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="exports/mm2/", PaginationConfig={"PageSize": 500}):
        pages += 1
        if pages > 20:
            raise ResearchError("export index quota requires compaction")
        contents = page.get("Contents", [])
        if not isinstance(contents, list) or len(contents) > 500:
            raise ResearchError("export index page quota exceeded")
        keys += len(contents)
        if keys > MAX_INDEX_KEYS:
            raise ResearchError("export index quota requires compaction")
        for item in contents:
            match = re.fullmatch(r"exports/mm2/([0-9a-f]{64})/manifest.json", item.get("Key", ""))
            if match:
                manifests.add(match.group(1))
                if len(manifests) > MAX_MANIFESTS:
                    raise ResearchError("export manifest quota requires compaction")
    fresh, diagnostics = 0, []
    for identity in sorted(manifests):
        target = root / identity
        if not target.exists():
            if fresh >= MAX_INGEST:
                continue
            target = download_bundle(s3, bucket, "exports/mm2/", identity, root)
            fresh += 1
        manifest = load_json(target / "manifest.json", 512 * 1024)
        if "publication_id" not in manifest:
            verified = verify_bundle(target)
            if verified["scope"].get("kind") == "sanitized-host-diagnostics":
                diagnostics.append((utc(verified["utc_range"]["end"]), target))
    diagnostics.sort(key=lambda item: item[0])
    return {"new": fresh, "indexed": len(manifests), "latest_diagnostics": diagnostics[-1][1] if diagnostics else None,
            "diagnostic_windows": [item[1] for item in diagnostics],
            "ingest_backlog": sum(not (root / name).exists() for name in manifests)}


def bounded_observer(arguments, *, deadline=130, maximum=MAX_PROCESS_OUTPUT):
    """One shared stdout/stderr quota; DNS hangs and descendants are OS-fenced."""
    options = {"start_new_session": True} if os.name != "nt" else {
        "creationflags": subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW}
    process = subprocess.Popen(arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **options)
    records, stop = queue.Queue(maxsize=8), threading.Event()

    def reader(name, stream):
        try:
            while not stop.is_set():
                data = stream.read(16 * 1024)
                while not stop.is_set():
                    try:
                        records.put((name, data), timeout=0.05)
                        break
                    except queue.Full:
                        pass
                if not data:
                    return
        except OSError:
            pass

    readers = [threading.Thread(target=reader, args=(name, stream), daemon=True)
               for name, stream in (("out", process.stdout), ("err", process.stderr))]
    for worker in readers:
        worker.start()
    output, total, finished = bytearray(), 0, set()
    until, completed = time.monotonic() + deadline, False
    try:
        while len(finished) < 2:
            remaining = until - time.monotonic()
            if remaining <= 0:
                raise ResearchError("observer global deadline exceeded; published evidence preserved")
            try:
                name, data = records.get(timeout=min(0.05, remaining))
            except queue.Empty:
                continue
            if not data:
                finished.add(name)
                continue
            total += len(data)
            if total > maximum:
                raise ResearchError("observer combined output quota exceeded; published evidence preserved")
            if name == "out":
                output.extend(data)
        try:
            code = process.wait(timeout=max(0.001, until - time.monotonic()))
        except subprocess.TimeoutExpired:
            raise ResearchError("observer global deadline exceeded; published evidence preserved") from None
        completed = True
        if code:
            raise ResearchError("observer capture failed; published evidence preserved")
        return bytes(output)
    finally:
        stop.set()
        if not completed:
            if os.name != "nt":
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            else:
                # A dedicated, hidden process tree is created above; no shell interpolation.
                try:
                    subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2, check=False,
                        creationflags=subprocess.CREATE_NO_WINDOW)
                except (OSError, subprocess.SubprocessError):
                    pass
            if process.poll() is None:
                process.kill()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
        for worker, stream in zip(readers, (process.stdout, process.stderr)):
            worker.join(timeout=1)
            # BufferedReader.close may block behind a reader whose descendant owns
            # the pipe. Never wait without a bound if platform tree cleanup failed.
            if not worker.is_alive():
                stream.close()


def collect_public(root, revision, start, end):
    arguments = [sys.executable, "-B", "-m", "tools.research.observe_mo_money", "--start", start,
        "--end", end, "--out", str(root / "archive"), "--state", str(root / "observer-cursor.json"),
        "--max-seconds", "110", "--max-requests", "240", "--source-revision", revision]
    # Process output is bounded in memory and never written unbounded to disk.
    summary = parse_json(bounded_observer(arguments))
    return summary["bundles"]


def _metadata_insert(store, key, value):
    if store.db.execute("SELECT count(*) FROM metadata").fetchone()[0] >= MAX_METADATA:
        raise ResearchError("research metadata quota requires compaction review")
    store.db.execute("INSERT INTO metadata VALUES (?,?)", (key, bounded(value)))


def _reserve_job(store, key, packet, policy_hash, metadata):
    reference = bundle_inputs([packet])[0]
    with store.transaction():
        row = store.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        if row:
            # Previously deployed markers store the completed job identity alone.
            if re.fullmatch(r"[0-9a-f]{64}", row[0]):
                old = store.get(row[0])
                if not old or old["inputs"].get("metadata") != metadata:
                    raise ResearchError("legacy frozen window job cannot be verified")
                references = old["inputs"].get("bundles", [])
                if len(references) != 1:
                    raise ResearchError("legacy frozen job input cardinality changed")
                return {"job_id": old["id"], "policy_hash": old["policy_hash"],
                        "metadata": metadata, "reference": references[0]}
            record = parse_json(row[0])
            if record.get("schema_version") != 1 or record.get("metadata") != metadata:
                raise ResearchError("frozen research reservation mismatch")
            return record
        record = {"schema_version": 1, "reference": reference, "policy_hash": policy_hash,
                  "metadata": metadata, "job_id": None}
        _metadata_insert(store, key, record)
        return record


def _finish_reserved_job(store, key, record, policy, policy_hash, stage):
    if record["policy_hash"] != policy_hash:
        raise ResearchError("frozen research policy changed; explicit review required")
    if record["job_id"] is not None:
        old = store.get(record["job_id"])
        if (not old or old["stage"] != stage or old["policy_hash"] != policy_hash
                or old["inputs"] != {"bundles": [record["reference"]], "metadata": record["metadata"]}):
            raise ResearchError("completed frozen research job cannot be verified")
        return record["job_id"], False
    reference = record["reference"]
    if bundle_inputs([reference["path"]])[0] != reference:
        raise ResearchError("reserved research packet changed")
    job = enqueue(store, policy, policy_hash, stage, [reference["path"]], metadata=record["metadata"])
    # Crash after enqueue is harmless: retry uses exactly the same frozen inputs.
    with store.transaction():
        row = store.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        current = parse_json(row[0])
        if current.get("job_id") is not None:
            if current["job_id"] != job["id"]:
                raise ResearchError("frozen research job identity changed")
            return job["id"], False
        if current != record:
            raise ResearchError("frozen research reservation changed")
        current["job_id"] = job["id"]
        store.db.execute("UPDATE metadata SET value=? WHERE key=? AND value=?",
                         (bounded(current), key, row[0]))
        return job["id"], True


def _crash_observation(store, incident, diagnostic_manifest, packet):
    scope = diagnostic_manifest["scope"]
    component = {"docker_engine": "engine", "docker_console": "secondary-console", "docker_proxy": "secondary-proxy",
                 "systemd_journal": "deployment-unit"}.get(incident.get("source"), "unknown")
    source = {"source_id": scope.get("source_id"), "lane": scope.get("lane"), "component": component}
    # Current-container OCI identity cannot attribute old lines in a rolling tail.
    epoch = "unverified:" + digest(source)[:24]
    revision = scope.get("engine_source_revision")
    if component != "engine" or not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
        revision = None
    stamp = incident.get("observed_timestamp_us")
    stamp = stamp if isinstance(stamp, str) and re.fullmatch(r"[0-9]{1,20}", stamp) else None
    identity = digest({"scope": source, "family": incident["category"], "signature": incident["fingerprint"],
                       "origin": incident.get("source"), "timestamp_us": stamp})
    fingerprint = digest({"family": incident["category"], "epoch": epoch, "signature": incident["fingerprint"]})
    evidence = {"bundle_id": packet.name, "diagnostic_bundle_id": diagnostic_manifest["bundle_id"],
                "incident": incident, "historic_root_cause_proven": False,
                "component": component,
                "current_engine_source_revision": revision, "incident_engine_source_revision": None,
                "incident_epoch_attribution": "UNVERIFIED_HISTORICAL_TAIL",
                "unique_incident_occurrence_verified": stamp is not None}
    with store.transaction():
        key = "incident-observed-" + identity
        if not store.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone():
            exists = store.db.execute("SELECT fingerprint FROM crash_signatures WHERE fingerprint=?", (fingerprint,)).fetchone()
            if not exists and store.db.execute("SELECT count(*) FROM crash_signatures").fetchone()[0] >= MAX_JOBS:
                raise ResearchError("crash signature quota exhausted")
            now = store.clock()
            store.db.execute("INSERT INTO crash_signatures VALUES(?,?,?,?,?,1,?,NULL) "
                "ON CONFLICT(fingerprint) DO UPDATE SET last_seen=excluded.last_seen,count=count+1,evidence=excluded.evidence",
                (fingerprint, incident["category"], epoch, now, now, bounded(evidence)))
            _metadata_insert(store, key, {"fingerprint": fingerprint})
    return fingerprint, epoch


def _pending_diagnostics(database, ingest, latest_manifest):
    if not latest_manifest:
        return [], 0
    scope = latest_manifest["scope"]
    with JobStore(database) as store:
        consumed = {row[0] for row in store.db.execute(
            "SELECT key FROM metadata WHERE key LIKE 'diagnostics-consumed-%'")}
    pending, backlog = [], 0
    for path in ingest["diagnostic_windows"]:
        if "diagnostics-consumed-" + path.name in consumed:
            continue
        manifest = verify_bundle(path)
        if any(manifest["scope"].get(key) != scope.get(key) for key in ("source_id", "lane")):
            # Different source/lane evidence remains archived for a separately scoped cycle.
            continue
        backlog += 1
        if len(pending) < MAX_INGEST:
            data = read_member(path, "diagnostics.json")
            if not isinstance(data, dict) or not isinstance(data.get("incidents", []), list):
                raise ResearchError("diagnostic window incident schema changed")
            pending.append((manifest, data))
    return pending, backlog


def _lifecycle_context(data):
    """Keep each sanitized window's evidence; never borrow the newest counters."""
    from scripts.research_export import safe_diagnostics
    projected = safe_diagnostics(data)
    captured = data.get("captured_at_utc")
    if captured is not None:
        try:
            utc(captured)
        except (ResearchError, TypeError, ValueError):
            captured = None
    return {"captured_at_utc": captured, "unit": projected["unit"],
            "hours": projected["hours"], "systemd": projected["systemd"] or None,
            "containers": projected["containers"] or None,
            "engine_source_revision": projected.get("engine_source_revision"),
            "historic_root_cause_proven": False}


def run_cycle(config, *, s3=None, observe=None, now=None):
    root = Path(config["state"])
    if root.is_symlink():
        raise ResearchError("linked controller state")
    root.mkdir(parents=True, exist_ok=True)
    if tree_budget(root, reserve_entries=MAX_CYCLE_ENTRIES) > MAX_TREE - MAX_CYCLE_GROWTH:
        raise ResearchError("research archive headroom exhausted; retention/compaction review required")
    if shutil.disk_usage(root).free < MAX_CYCLE_GROWTH:
        raise ResearchError("research filesystem headroom exhausted; preserved evidence requires review")
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ResearchError("research cycle requires an explicit timezone")
    now = now.astimezone(timezone.utc).replace(microsecond=0)
    end = now.isoformat().replace("+00:00", "Z")
    start = (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    s3 = s3 or client(config["region"])
    policy, policy_hash = load_policy(config["policy"])
    archive = root / "archive"
    archive.mkdir(exist_ok=True)
    ingest = export_ingest(s3, config["bucket"], root / "exports")
    captures = (observe or collect_public)(root, config["source_revision"], start, end)
    if not isinstance(captures, list) or len(captures) != 2:
        raise ResearchError("exactly two registered public account captures are required")
    accounts, bundles = {}, []
    for capture in captures:
        source = Path(capture["bundle"])
        if not source.resolve().is_relative_to(archive.resolve()):
            raise ResearchError("observer bundle outside trusted archive")
        manifest = verify_bundle(source)
        name = capture.get("account")
        if (name not in DEFAULT_ACCOUNTS or name in accounts
                or manifest["scope"].get("account_id") != DEFAULT_ACCOUNTS[name]["account_id"]
                or manifest["scope"].get("kind") != "one-public-account"
                or manifest["scope"].get("lane") != "public-benchmark"
                or manifest["utc_range"]["end"] != end
                or utc(manifest["utc_range"]["start"]) > utc(start)):
            raise ResearchError("observer account identity, scope or coverage mismatch")
        result = account_bundle(source)
        if capture["account"] == "mm2-public":
            result["mm2_ownership_attribution"] = "UNKNOWN: wallet may include other applications or manual activity"
            result["quality"]["reasons"].append("MM2_ORDER_INTENT_ATTRIBUTION_REQUIRED")
        accounts[capture["account"]] = result
        output = publish_bundle(archive, {"accounting.json": result}, scope=manifest["scope"],
            utc_range=manifest["utc_range"], quality=result["quality"], source_revision=config["source_revision"])
        bundles.extend([source, output])
    if set(accounts) != {"mo-money", "mm2-public"}:
        raise ResearchError("both registered public accounts are required")
    comparison = build_comparison(accounts["mm2-public"], accounts["mo-money"])
    comparison["ownership_attribution"] = "UNVERIFIED: owner-wallet benchmark is not authenticated mm2 results"
    health, incident_data, diagnostic_manifest = {}, {}, None
    if ingest["latest_diagnostics"]:
        diagnostic_manifest = verify_bundle(ingest["latest_diagnostics"])
        health = read_member(ingest["latest_diagnostics"], "health.json")
        incident_data = read_member(ingest["latest_diagnostics"], "diagnostics.json")
    pending_diagnostics, diagnostic_backlog = _pending_diagnostics(root / "jobs.sqlite3", ingest, diagnostic_manifest)
    if pending_diagnostics:
        incident_data = {"schema_version": 1, "read_only": True, "historic_root_cause_proven": False,
            "incidents": [{**incident, "diagnostic_bundle_id": manifest["bundle_id"]}
                for manifest, data in pending_diagnostics for incident in data.get("incidents", [])[:400]
                if isinstance(incident, dict)],
            "windows": [{"bundle_id": manifest["bundle_id"], "scope": manifest["scope"],
                         "utc_range": manifest["utc_range"],
                         "lifecycle_context": _lifecycle_context(data)} for manifest, data in pending_diagnostics]}
    packet = publish_bundle(archive, {"comparison.json": comparison, "accounting-summary.json": accounts,
        "health.json": health, "diagnostics.json": incident_data, "provenance.json": {
            "source_bundles": [verify_bundle(path)["bundle_id"] for path in bundles],
            "diagnostics_bundle": ingest["latest_diagnostics"].name if ingest["latest_diagnostics"] else None,
            "diagnostic_backfill_windows": [manifest["bundle_id"] for manifest, _ in pending_diagnostics],
            "diagnostic_backlog": diagnostic_backlog,
            "ingest_backlog": ingest["ingest_backlog"], "financial_outperformance_established": False}},
        scope={"kind": "research-packet", "lane": "public-benchmark", "account_scope": "one-account-each"},
        utc_range=comparison["utc_range"], quality=quality("PARTIAL", ["ACCOUNTING_AND_ATTRIBUTION_GATES_PENDING"]),
        source_revision=config["source_revision"], config_hash=policy_hash)
    upload_bundle(s3, config["bucket"], packet, "research/")
    jobs = []
    with JobStore(root / "jobs.sqlite3") as store:
        # Freeze the first packet of each six-hour bucket. Later packets stay archived,
        # while repeated polling cannot create another analysis for the same window.
        bucket = now.strftime("%Y-%m-%d") + "-" + str(now.hour // 6)
        key = "analysis-window-" + bucket
        record = _reserve_job(store, key, packet, policy_hash, {"window_key": bucket})
        job, created = _finish_reserved_job(store, key, record, policy, policy_hash, "analysis")
        if created:
            jobs.append(job)
        for window_manifest, window_data in pending_diagnostics:
            seen = set()
            incidents = window_data.get("incidents", [])
            for incident in incidents[:400]:
                if not isinstance(incident, dict):
                    continue
                family, signature = incident.get("category"), incident.get("fingerprint")
                if (family not in {"panic", "oom", "durability", "engine_integrity", "engine_metadata_timeout", "runtime_unknown", "lifecycle_command", "lifecycle_unknown"}
                        or not isinstance(signature, str) or not re.fullmatch(r"[0-9a-f]{64}", signature)):
                    continue
                if incident.get("source") not in {"systemd_journal", "docker_engine", "docker_console", "docker_proxy"}:
                    continue
                occurrence = digest(incident)
                if occurrence in seen:
                    continue
                seen.add(occurrence)
                fingerprint, epoch = _crash_observation(store, incident, window_manifest, packet)
                row = store.db.execute("SELECT issue_job_id FROM crash_signatures WHERE fingerprint=?", (fingerprint,)).fetchone()
                if row[0] is None:
                    key = "reliability-first-" + fingerprint
                    record = _reserve_job(store, key, packet, policy_hash, {
                        "crash_fingerprint": fingerprint, "source_epoch": epoch, "reason": family})
                    job, created = _finish_reserved_job(store, key, record, policy, policy_hash, "reliability")
                    with store.transaction():
                        store.db.execute("UPDATE crash_signatures SET issue_job_id=? WHERE fingerprint=? AND issue_job_id IS NULL",
                            (job, fingerprint))
                    if created:
                        jobs.append(job)
            # A crash before this durable receipt retries the same rows/jobs safely.
            with store.transaction():
                key = "diagnostics-consumed-" + window_manifest["bundle_id"]
                if not store.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone():
                    _metadata_insert(store, key, {"schema_version": 1, "packet": packet.name,
                        "diagnostic_bundle_id": window_manifest["bundle_id"],
                        "processed_incident_rows": min(len(incidents), 400), "historic_root_cause_proven": False})
    tree_budget(root)
    return {"schema_version": 1, "packet": packet.name, "new_jobs": jobs,
        "ingested_exports": ingest["new"], "ingest_backlog": ingest["ingest_backlog"],
        "diagnostic_windows_processed": len(pending_diagnostics),
        "diagnostic_backlog": max(0, diagnostic_backlog - len(pending_diagnostics)),
        "outperformance_established": False, "primary_mutations": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    try:
        print(canonical(run_cycle(load_json(args.config))).decode())
        return 0
    except Exception:
        print("research_cycle_failed; preserved controller/archive require bounded diagnostic review", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
