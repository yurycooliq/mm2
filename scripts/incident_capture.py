#!/usr/bin/env python3
"""Bounded read-only process diagnostics. Never reads environments or execution journals."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import threading
from urllib.parse import urlsplit

MAX_BYTES = 256 * 1024
MAX_LINE = 2048
MAX_LINES = 160
CONTAINER_ID = re.compile(r"[0-9a-f]{64}\Z")
UNITS = {"mm2.service", "mm2-research.service"}
SERVICES = {"engine", "console", "proxy"}
SYSTEMD_FIELDS = {
    "ActiveState", "SubState", "Result", "NRestarts", "ExecMainCode", "ExecMainStatus",
    "ActiveEnterTimestamp", "ExecMainStartTimestamp", "MemoryCurrent", "MemoryPeak",
}
CLASSIFIERS = (
    ("runtime_unknown", r"runtime_fault"),
    ("source_input", r"invalid_(?:public|tick|exchange|market|book|websocket)_"),
    ("engine_metadata_timeout", r"engine_metadata_timeout"),
    ("engine_integrity", r"engine_integrity_error|duplicate.*engine|engine changed identity"),
    ("durability", r"journal|durable.*(?:failed|unknown)|fsync|capacity.*(?:reached|exceeded)|No space left"),
    ("oom", r"out of memory|oom|killed process"),
    ("panic", r"panicked|panic|rustls.*CryptoProvider"),
    ("source_bootstrap", r"bootstrap|snapshot|initial.*book"),
    ("transport", r"disconnect|connection|websocket|transport|timeout|timed out"),
    ("lifecycle_command", r"command_failed|command_timeout|invalid_lifecycle_metadata"),
    ("lifecycle_unknown", r"Production lifecycle failed|main process exited|Failed with result"),
)
SAFE_CODES = {
    **{key: key for key, _ in CLASSIFIERS},
    "engine_integrity_error": "engine_integrity", "command_timeout": "lifecycle_command",
    "command_failed": "lifecycle_command", "invalid_lifecycle_metadata": "lifecycle_command",
    "os_error": "lifecycle_command", "subprocess_error": "lifecycle_command",
    "native_snapshot_bootstrap_timeout": "source_bootstrap",
    **{key: "transport" for key in ("connect_unavailable", "rest_book_unavailable", "receive_timeout",
        "transport_disconnected", "transport_send_failed", "transport_unavailable")},
    **{key: "source_input" for key in ("feed_queue_unavailable", "input_capacity_exceeded",
        "invalid_websocket_protocol", "invalid_public_input", "invalid_tick_input",
        "invalid_exchange_clock", "invalid_market_identity", "invalid_book_input", "source_fault")},
    "durability_fault": "durability", "capacity_fault": "durability",
    "runtime_fault": "runtime_unknown", "simulation_invariant_fault": "engine_integrity",
    "external_acknowledgement_unknown": "engine_integrity",
}


def sanitize(text: str) -> str:
    """Conservative text sanitizer. Exact private material stays out of diagnostics."""
    text = str(text)[: MAX_LINE * 4]
    text = re.sub(r"[\x00-\x08\x0b-\x1f\x7f]", "?", text)
    def origin(match):
        try:
            parsed = urlsplit(match.group(0))
            return parsed.scheme + "://" + (parsed.hostname or "redacted") + "/[path-redacted]"
        except ValueError:
            return "[url-redacted]"
    text = re.sub(r"(?:https?|wss?)://[^\s\"'<>]+", origin, text)
    text = re.sub(r"\b\d{5,15}:[A-Za-z0-9_-]{20,}\b", "[bot-token-redacted]", text)
    text = re.sub(r'''(?i)["']?\b(?:authorization|cookie|initdata|seed|private[_ -]?key|api[_ -]?(?:secret|key)|token|password|passphrase)\b["']?\s*[:=]\s*(?:["'][^"']*["']|\[[^\]]*\]|[^\s,}]+)''', "[credential-redacted]", text)
    text = re.sub(r"\b(?:MM2_|AWS_|OPENAI_)[A-Z0-9_]+=[^\s]+", "[environment-redacted]", text)
    text = re.sub(r"(?:0x)?[0-9a-fA-F]{64,}", "[long-hex-redacted]", text)
    text = re.sub(r"(?<![A-Za-z0-9_])[A-Za-z0-9_+/=-]{40,}(?![A-Za-z0-9_])", "[long-value-redacted]", text)
    return text[:MAX_LINE]


def classify(message: str) -> str:
    match = re.match(r"diagnostic_code=([a-z_]+)(?: |$)", message)
    if match:
        return SAFE_CODES.get(match.group(1), "other")
    for category, pattern in CLASSIFIERS:
        if re.search(pattern, message, re.IGNORECASE):
            return category
    return "other"


def diagnostic_message(raw: str) -> str:
    """Only fixed diagnostic codes leave the host; arbitrary log text is quarantined."""
    # Export/report layers may project an already sanitized incident again.
    # Preserve only this exact fixed-code grammar, never append arbitrary text.
    fixed = re.fullmatch(r"diagnostic_code=([a-z_]+)(?: source=(clob|binance))?(?: fatal=(true|false))?", raw)
    if fixed and fixed.group(1) in SAFE_CODES:
        return raw
    start = raw.find("{")
    if start >= 0:
        try:
            row = json.loads(raw[start:])
            if isinstance(row, dict):
                level = row.get("level")
                if isinstance(level, str) and level.lower() in {"debug", "info"}:
                    return ""
                fields = row.get("fields", {})
                if isinstance(fields, dict) and fields.get("code") in SAFE_CODES:
                    result = "diagnostic_code=" + fields["code"]
                    if fields.get("source") in {"clob", "binance"}:
                        result += " source=" + fields["source"]
                    if type(fields.get("fatal")) is bool:
                        result += " fatal=" + str(fields["fatal"]).lower()
                    return result
                parts = [row[key] for key in ("msg", "message", "error") if isinstance(row.get(key), str)]
                category = classify(" ".join(parts))
                return "diagnostic_code=" + category if category != "other" else ""
        except (ValueError, TypeError):
            pass
    # Legacy supervisor output has a known fixed classification but no structured row.
    for code in SAFE_CODES:
        if re.search(r"\b" + re.escape(code) + r"\b", raw):
            return "diagnostic_code=" + code
    category = classify(raw)
    return "diagnostic_code=" + category if category != "other" else ""


def fingerprint(category: str, message: str) -> str:
    normalized = re.sub(r"\b[0-9]+\b", "#", sanitize(message).lower())
    normalized = re.sub(r"\s+", " ", normalized)
    return hashlib.sha256((category + ":" + normalized).encode()).hexdigest()


def bounded_command(argv, run=subprocess.run):
    process = None
    try:
        if run is subprocess.run:
            process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            captured = []
            def reader():
                try:
                    captured.append(process.stdout.read(MAX_BYTES + 1))
                except OSError:
                    captured.append(b"")
            thread = threading.Thread(target=reader, daemon=True)
            thread.start()
            thread.join(12)
            if thread.is_alive() or (captured and len(captured[0]) > MAX_BYTES):
                process.kill()
                process.wait(timeout=2)
                thread.join(2)
                process.stdout.close()
                return None, "output_bound" if captured else "command_unavailable"
            process.wait(timeout=2)
            process.stdout.close()
            if process.returncode:
                return None, "command_failed"
            return captured[0].decode("utf-8", "replace") if captured else "", None
        result = run(argv, capture_output=True, text=True, timeout=12, check=False)
        if len(result.stdout.encode("utf-8", "replace")) > MAX_BYTES:
            return None, "output_bound"
        if result.returncode != 0:
            return None, "command_failed"
        return result.stdout + (result.stderr or ""), None
    except (OSError, subprocess.SubprocessError):
        return None, "command_unavailable"
    finally:
        if process is not None:
            if process.poll() is None:
                process.kill()
                try:
                    process.wait(timeout=2)
                except subprocess.SubprocessError:
                    pass
            if process.stdout is not None:
                process.stdout.close()


def collect(unit="mm2.service", hours=6, run=subprocess.run):
    if unit not in UNITS or type(hours) is not int or not 1 <= hours <= 72:
        raise ValueError("unsupported diagnostic scope")
    output = {"schema_version": 1, "captured_at_utc": datetime.now(timezone.utc).isoformat(),
              "unit": unit, "hours": hours, "read_only": True, "systemd": {},
              "containers": [], "incidents": [], "quality": {"status": "COMPLETE", "reasons": []}}
    def missing(stage, reason):
        output["quality"]["status"] = "PARTIAL"
        output["quality"]["reasons"].append(stage + ":" + reason)
    show, error = bounded_command(["systemctl", "show", unit, "--property=" + ",".join(sorted(SYSTEMD_FIELDS))], run)
    if error:
        missing("systemd", error)
    else:
        for line in show.splitlines():
            key, separator, value = line.partition("=")
            if separator and key in SYSTEMD_FIELDS:
                output["systemd"][key] = sanitize(value)
    journal, error = bounded_command(["journalctl", "--unit", unit, "--since", f"{hours} hours ago",
                                     "--no-pager", "--lines", str(MAX_LINES), "--output=json"], run)
    if error:
        missing("journal", error)
    else:
        output["quality"]["reasons"].append("bounded_recent_journal_not_complete_history")
        for line in journal.splitlines()[:MAX_LINES]:
            try:
                row = json.loads(line)
                message = row.get("MESSAGE")
                if not isinstance(message, str):
                    continue
                message = diagnostic_message(message)
                category = classify(message)
                if category == "other":
                    continue
                stamp = row.get("__REALTIME_TIMESTAMP")
                output["incidents"].append({"category": category, "message": message,
                    "observed_timestamp_us": stamp if isinstance(stamp, str) and stamp.isdigit() else None,
                    "fingerprint": fingerprint(category, message), "source": "systemd_journal"})
            except (ValueError, TypeError):
                missing("journal", "malformed_record")
    inventory, error = bounded_command(["docker", "ps", "--all", "--no-trunc", "--format",
        '{{.ID}} {{.Label "com.docker.compose.service"}}', "--filter",
        "label=com.docker.compose.project=mm2-production", "--filter",
        "label=com.docker.compose.oneoff=False"], run)
    if error:
        missing("containers", error)
    else:
        rows = inventory.splitlines()
        if len(rows) > 6:
            missing("containers", "inventory_bound")
            rows = []
        for line in rows:
            fields = line.split()
            if len(fields) != 2 or not CONTAINER_ID.fullmatch(fields[0]) or fields[1] not in SERVICES:
                missing("containers", "identity_mismatch")
                continue
            identity, service = fields
            state, error = bounded_command(["docker", "inspect", "--format",
                '{{.State.Status}} {{.State.ExitCode}} {{.State.OOMKilled}} {{.RestartCount}} {{.State.StartedAt}} {{.State.FinishedAt}}', identity], run)
            safe = {"container_id": identity, "service": service}
            if error:
                missing(service + "_state", error)
            else:
                parts = state.strip().split()
                if len(parts) == 6 and parts[1].isdigit() and parts[2] in {"true", "false"} and parts[3].isdigit():
                    safe.update(status=sanitize(parts[0]), exit_code=int(parts[1]), oom_killed=parts[2] == "true",
                                restarts=int(parts[3]), started_at=sanitize(parts[4]), finished_at=sanitize(parts[5]))
                else:
                    missing(service + "_state", "invalid_metadata")
            output["containers"].append(safe)
            if service == "engine":
                revision, revision_error = bounded_command(["docker", "inspect", "--format",
                    '{{index .Config.Labels "org.opencontainers.image.revision"}}', identity], run)
                if not revision_error and re.fullmatch(r"[0-9a-f]{40}", revision.strip()):
                    output["engine_source_revision"] = revision.strip()
                else:
                    missing("engine_revision", "unverified")
            logs, error = bounded_command(["docker", "logs", "--timestamps", "--tail", "80", identity], run)
            if error:
                missing(service + "_logs", error)
            else:
                for message in logs.splitlines()[-80:]:
                    cleaned = diagnostic_message(message)
                    category = classify(cleaned)
                    if category != "other":
                        output["incidents"].append({"category": category, "message": cleaned,
                            "fingerprint": fingerprint(category, cleaned), "source": "docker_" + service})
    output["incidents"] = output["incidents"][:400]
    output["quality"]["status"] = "PARTIAL"  # A finite tail cannot prove historic absence of failures.
    return output


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unit", choices=sorted(UNITS), default="mm2.service")
    parser.add_argument("--hours", type=int, default=6)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        encoded = json.dumps(collect(args.unit, args.hours), ensure_ascii=True, sort_keys=True) + "\n"
        if args.output:
            with args.output.open("x", encoding="ascii") as stream:
                stream.write(encoded)
        else:
            sys.stdout.write(encoded)
        return 0
    except (OSError, ValueError):
        print("Read-only diagnostic capture failed; no runtime changes were made.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
