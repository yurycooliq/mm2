#!/usr/bin/env python3
"""Trusted host-local read-only exporter. No primary mutations or raw log uploads."""
from datetime import datetime, timedelta, timezone
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.incident_capture import collect
from tools.research.archive import publish_bundle
from tools.research.journal_export import ExportLimits, export_prefix, encoded, verify_bundle as verify_projection
from tools.research.schema import canonical, digest, load_json, quality
from tools.research.storage import client, upload_bundle

STATE_ROOT = Path("/srv/mm2/engine")
MAX_SPOOL = 512 * 1024 * 1024


def state_token(path=Path("/etc/mm2/runtime.env")):
    # Only this trusted producer reads the owner API credential. It is never persisted,
    # returned, passed in argv, or made available to the analysis/coding worker.
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            key, separator, value = line.rstrip("\r\n").partition("=")
            if key == "MM2_ENGINE_API_TOKEN" and separator and 24 <= len(value) <= 256 and value.isascii():
                return value
    raise ValueError("export API capability unavailable")


def cached_state(token):
    request = urllib.request.Request("http://127.0.0.1:8088/v1/state", headers={"Authorization": "Bearer " + token})
    with urllib.request.urlopen(request, timeout=2) as response:
        data = response.read(4 * 1024 * 1024 + 1)
    if len(data) > 4 * 1024 * 1024:
        raise ValueError("cached metadata byte quota exceeded")
    return json.loads(data)


def source_directory(path):
    path = Path(path)
    if path.is_symlink():
        raise ValueError("primary source symlink")
    resolved = path.resolve(strict=True)
    if resolved != STATE_ROOT and STATE_ROOT not in resolved.parents:
        raise ValueError("source outside configured primary root")
    return resolved


def safe_health(state):
    safe = {}
    for key in ("schema_version", "published_at_ms", "sequence", "command_version", "reset_epoch",
                "api_ready", "feed_ready", "live_eligible", "committing", "shutting_down"):
        value = state.get(key)
        if value is None or type(value) in (int, bool):
            safe[key] = value
    for key, allowed in {
        "mode": {"paused", "running", "killed"}, "feed_source": {"demo", "public"},
        "execution_mode": {"simulation", "shadow", "trading", "both"},
        "fault_class": {"durability_fault", "capacity_fault", "runtime_fault", "simulation_invariant_fault",
                         "source_fault", "external_acknowledgement_unknown"},
    }.items():
        value = state.get(key)
        safe[key] = value if isinstance(value, str) and value in allowed else None
    diagnostics = state.get("feed_diagnostics")
    if isinstance(diagnostics, dict):
        from scripts.incident_capture import SAFE_CODES
        safe["feed_diagnostics"] = {}
        for source in ("clob", "binance"):
            item = diagnostics.get(source)
            if not isinstance(item, dict):
                continue
            def counter(value):
                return type(value) is int and 0 <= value <= 2**64 - 1
            projected = {key: item[key] for key in ("attempts", "transport_disconnects", "bootstrap_timeouts",
                "backoff_resets", "retry_delay_ms", "last_failure_at_mono_ms")
                if key in item and (counter(item[key]) or (key == "last_failure_at_mono_ms" and item[key] is None))}
            for key in ("snapshots_seen", "snapshots_discarded_before_cutoff", "bootstrap_cutoff_exchange_ms"):
                value = item.get(key)
                if isinstance(value, list) and len(value) == 2 and all(counter(part) for part in value):
                    projected[key] = list(value)
            if type(item.get("last_failure_fatal")) is bool:
                projected["last_failure_fatal"] = item["last_failure_fatal"]
            failure = item.get("last_failure")
            if isinstance(failure, str) and failure in SAFE_CODES:
                projected["last_failure"] = failure
            phase = item.get("phase")
            if isinstance(phase, str) and phase in {"idle", "connecting", "rest_recovery", "awaiting_native_snapshot",
                                                  "streaming", "retrying", "faulted", "stopped"}:
                projected["phase"] = phase
            safe["feed_diagnostics"][source] = projected
    return safe


def spool_bytes(root):
    total, entries = 0, 0
    for path in root.rglob("*"):
        entries += 1
        if entries > 10_000:
            raise ValueError("spool entry quota exceeded")
        if path.is_symlink():
            raise ValueError("spool symlink")
        if path.is_file():
            total += path.stat().st_size
        if total > MAX_SPOOL:
            raise ValueError("spool quota exceeded")
    return total


def safe_diagnostics(value):
    from scripts.incident_capture import CLASSIFIERS, UNITS, diagnostic_message, classify, fingerprint
    result = {"schema_version": 1, "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "unit": value.get("unit") if value.get("unit") in UNITS else None,
        "read_only": True, "hours": value.get("hours") if type(value.get("hours")) is int and 1 <= value["hours"] <= 72 else None,
        "systemd": {}, "containers": [], "incidents": [],
        "quality": {"status": "PARTIAL", "reasons": ["BOUNDED_ALLOWLIST_PROJECTION"]}}
    if isinstance(value.get("engine_source_revision"), str) and re.fullmatch(r"[0-9a-f]{40}", value["engine_source_revision"]):
        result["engine_source_revision"] = value["engine_source_revision"]
    for key, allowed in {"ActiveState": {"active", "inactive", "failed", "activating", "deactivating"},
                         "SubState": {"running", "dead", "failed", "start", "stop", "exited"},
                         "Result": {"success", "exit-code", "signal", "timeout", "oom-kill", "start-limit-hit"}}.items():
        item = value.get("systemd", {}).get(key)
        if item in allowed:
            result["systemd"][key] = item
    for key in ("NRestarts", "ExecMainCode", "ExecMainStatus", "MemoryCurrent", "MemoryPeak"):
        item = value.get("systemd", {}).get(key)
        if isinstance(item, str) and item.isdigit() and len(item) <= 20:
            result["systemd"][key] = item
    for item in value.get("containers", [])[:6]:
        if item.get("service") not in {"engine", "console", "proxy"}:
            continue
        result["containers"].append({"service": item["service"],
            "status": item.get("status") if item.get("status") in {"running", "exited", "dead", "created", "restarting"} else None,
            **{key: val for key in ("exit_code", "restarts", "oom_killed") if (val := item.get(key)) is None or type(val) in (int, bool)}})
    for item in value.get("incidents", [])[:400]:
        message = diagnostic_message(str(item.get("message", "")))
        category = classify(message)
        if category == "other" or item.get("source") not in {"systemd_journal", "docker_engine", "docker_console", "docker_proxy"}:
            continue
        stamp = item.get("observed_timestamp_us")
        result["incidents"].append({"category": category, "message": message, "source": item["source"],
            "observed_timestamp_us": stamp if isinstance(stamp, str) and stamp.isdigit() and len(stamp) <= 20 else None,
            "fingerprint": fingerprint(category, message)})
    return result


def run_export(config, s3=None, state=None, diagnostics=None):
    root = Path(config["spool"])
    if root.is_symlink():
        raise ValueError("analytical spool overlaps primary state")
    for parent in root.parents:
        if parent.exists() and parent.is_symlink():
            raise ValueError("analytical spool has linked ancestor")
    primary = STATE_ROOT.resolve(strict=True)
    prospective = root.resolve(strict=False)
    if prospective == primary or primary in prospective.parents or prospective in primary.parents:
        raise ValueError("analytical spool overlaps primary state")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root = root.resolve(strict=True)
    used = spool_bytes(root)
    if used > MAX_SPOOL - 80 * 1024 * 1024:
        raise ValueError("insufficient analytical spool headroom")
    s3 = s3 or client(config["region"])
    state_gap = False
    if state is None:
        try:
            state = cached_state(state_token())
        except Exception:
            state = {}; state_gap = True
    diagnostics = safe_diagnostics(diagnostics if diagnostics is not None else collect(hours=1))
    now = datetime.now(timezone.utc).replace(microsecond=0)
    period = {"start": (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z"), "end": now.isoformat().replace("+00:00", "Z")}
    watermark = ((state.get("live") or {}).get("journal_committed_prefix") if config["lane"] == "real"
                 else (state.get("journal") or {}).get("committed_prefix"))
    receipts = []
    projection_gap = "committed_prefix_unavailable"
    if isinstance(watermark, dict):
        try:
            source = source_directory(config["source"])
            if watermark.get("source_kind") != ("live" if config["lane"] == "real" else "engine"):
                raise ValueError("source lane identity mismatch")
            identity = hashlib.sha256(encoded({"source_id": config["source_id"], "watermark": watermark, "projection_version": 1})).hexdigest()
            destination = root / identity
            limits = ExportLimits(max_input_bytes=16 * 1024 * 1024, max_output_bytes=64 * 1024 * 1024)
            export_prefix(source, destination, watermark, config["source_id"], limits)
            receipts.append(upload_bundle(s3, config["bucket"], destination, "exports/mm2/"))
            projection_gap = None
        except Exception:
            projection_gap = "projection_unavailable_requires_review"
    packet = publish_bundle(root, {"diagnostics.json": diagnostics, "health.json": safe_health(state),
        "coverage.json": {"active_prefix_available": isinstance(watermark, dict), "financial_complete": False,
            "historical_backfill_required": True, "source_id": config["source_id"], "lane": config["lane"],
            "projection_gap": projection_gap, "cached_state_unavailable": state_gap}},
        scope={"kind": "sanitized-host-diagnostics", "source_id": config["source_id"], "lane": config["lane"],
            "engine_source_revision": diagnostics.get("engine_source_revision"),
            "incident_epoch_attribution": "UNVERIFIED_HISTORICAL_TAIL"},
        utc_range=period, quality=quality("PARTIAL", ["BOUNDED_DIAGNOSTICS", "CHECKPOINT_PROJECTION_ONLY"]),
        source_revision=config["source_revision"], config_hash=digest({key: value for key, value in config.items() if key != "spool"}))
    receipts.append(upload_bundle(s3, config["bucket"], packet, "exports/mm2/"))
    # Receipts enable the explicit secondary-retention broker. This producer never deletes.
    receipt = root / ("receipt-" + packet.name + ".json")
    if not receipt.exists():
        with receipt.open("xb") as stream:
            stream.write(canonical({"schema_version": 1, "uploads": receipts, "primary_cleanup_allowed": False}) + b"\n")
            stream.flush(); os.fsync(stream.fileno())
    return {"uploaded": len(receipts), "diagnostic_bundle": packet.name,
            "financial_complete": False, "primary_mutations": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("/etc/mm2/research-export.json"))
    args = parser.parse_args()
    try:
        print(json.dumps(run_export(load_json(args.config)), sort_keys=True))
        return 0
    except Exception:
        print("research_export_failed; primary state is unchanged; inspect bounded diagnostics", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
