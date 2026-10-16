#!/usr/bin/env python3
"""SSM/SSH authenticated controller bridge. JSON commands never become shell code."""
import base64
from datetime import datetime
import hashlib
from pathlib import Path
import re
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.research.archive import verify_bundle
from tools.research.controller import load_policy, prepare_draft, verified_inputs, ready_code, enqueue_coding
from tools.research.jobs import JobStore, sha
from tools.research.schema import ResearchError, canonical, load_json, parse_json

MAX_REQUEST = 512 * 1024
MAX_RESPONSE = 8 * 1024 * 1024
PACKET_BYTES = 4 * 1024 * 1024
CHUNK_BYTES = 96 * 1024
SERVICE_UNIT = "mm2-research.service"
SERVICE_FIELDS = ("ActiveState", "SubState", "Result", "ExecMainStatus", "ExecMainStartTimestamp", "ExecMainExitTimestamp")
HEALTH_BYTES = 16 * 1024


def _service_query():
    """Read a fixed public unit projection with hard output and elapsed bounds."""
    command = ["/usr/bin/systemctl", "show", SERVICE_UNIT, "--no-pager", "--property=" + ",".join(SERVICE_FIELDS)]
    started = time.monotonic()
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                               env={"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C", "TZ": "UTC"})
    output = []
    reader = threading.Thread(target=lambda: output.append(process.stdout.read(HEALTH_BYTES + 1)), daemon=True)
    reader.start()
    try:
        reader.join(max(0, 3 - (time.monotonic() - started)))
        if reader.is_alive():
            raise subprocess.TimeoutExpired(command, 3)
        if not output or len(output[0]) > HEALTH_BYTES:
            raise ResearchError("QUERY_OUTPUT_QUOTA")
        code = process.wait(timeout=max(0.001, 3 - (time.monotonic() - started)))
        return output[0], code
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=1)
        process.stdout.close()


def service_health(query=None):
    """No logs, command output, inherited credentials, or unknown values escape."""
    base = {"schema_version": 1, "unit": SERVICE_UNIT}
    try:
        raw, code = (query or _service_query)()
        if not isinstance(raw, bytes) or len(raw) > HEALTH_BYTES:
            raise ResearchError("QUERY_OUTPUT_QUOTA")
        if code != 0:
            raise ResearchError("QUERY_FAILED")
        values = {}
        for line in raw.decode("ascii").splitlines():
            name, value = line.split("=", 1)
            if name not in SERVICE_FIELDS or name in values:
                raise ValueError("invalid property")
            values[name] = value
        if set(values) != set(SERVICE_FIELDS):
            raise ValueError("missing property")
        active = values["ActiveState"]
        sub = values["SubState"]
        result = values["Result"]
        if active not in {"active", "reloading", "inactive", "failed", "activating", "deactivating", "maintenance", "refreshing"} or sub not in {
            "dead", "condition", "start-pre", "start", "start-post", "running", "exited", "reload", "reload-signal", "reload-notify",
            "stop", "stop-watchdog", "stop-sigterm", "stop-sigkill", "stop-post", "final-watchdog", "final-sigterm", "final-sigkill",
            "failed", "auto-restart", "auto-restart-queued", "cleaning"} or result not in {
            "success", "resources", "timeout", "exit-code", "signal", "core-dump", "watchdog", "start-limit-hit", "protocol",
            "oom-kill", "exec-condition", "condition", "assert", "dependency"}:
            raise ResearchError("UNSUPPORTED_VALUE")
        if not re.fullmatch(r"(?:0|[1-9][0-9]{0,2})", values["ExecMainStatus"]) or int(values["ExecMainStatus"]) > 255:
            raise ValueError("invalid exit status")
        timestamps = []
        for field in SERVICE_FIELDS[-2:]:
            value = values[field]
            if value == "":
                timestamps.append(None)
                continue
            if not re.fullmatch(r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) [0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} UTC", value):
                raise ValueError("invalid timestamp")
            parsed = datetime.fromisoformat(value[4:-4])
            if value[:3] != ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")[parsed.weekday()]:
                raise ValueError("invalid weekday")
            timestamps.append(parsed.isoformat(timespec="seconds") + "Z")
        exit_status = int(values["ExecMainStatus"])
        status = "FAILED" if active == "failed" or result != "success" or exit_status != 0 else "HEALTHY"
        health = {**base, "status": status, "active_state": active, "sub_state": sub, "result": result,
                  "exit_status": exit_status, "last_started_utc": timestamps[0], "last_exited_utc": timestamps[1]}
    except subprocess.TimeoutExpired:
        health = {**base, "status": "UNAVAILABLE", "reason": "QUERY_TIMEOUT"}
    except (OSError, subprocess.SubprocessError):
        health = {**base, "status": "UNAVAILABLE", "reason": "QUERY_UNAVAILABLE"}
    except ResearchError as exc:
        reason = str(exc)
        health = {**base, "status": "UNAVAILABLE", "reason": reason if reason in {
            "QUERY_OUTPUT_QUOTA", "QUERY_FAILED", "UNSUPPORTED_VALUE"} else "MALFORMED_RESPONSE"}
    except (ValueError, TypeError):
        health = {**base, "status": "UNAVAILABLE", "reason": "MALFORMED_RESPONSE"}
    # Repeated identical failures do not notify on each oneshot timestamp change.
    signature = {key: value for key, value in health.items() if key not in {"last_started_utc", "last_exited_utc"}}
    return {**health, "fingerprint": hashlib.sha256(canonical(signature)).hexdigest()}


def effect_context(store, effect_id):
    effect = store.outbox(effect_id)
    if not effect:
        return None
    parent = store.get(effect["job_id"])
    name = "issue-spec-" + effect["fingerprint"][:16]
    checkpoint = store.db.execute("SELECT body,hash FROM checkpoints WHERE job_id=? AND generation=? AND name=?",
                                  (effect["job_id"], effect["generation"], name)).fetchone()
    return {**effect, "issue_spec": parse_json(checkpoint["body"]) if checkpoint else None,
            "issue_spec_hash": checkpoint["hash"] if checkpoint else None,
            "parent_inputs": parent["inputs"] if parent else None,
            "parent_inputs_hash": parent["inputs_hash"] if parent else None,
            "source_policy_hash": parent["policy_hash"] if parent else None}


def ready_registry(store):
    if not store.db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ready_code'").fetchone():
        return []
    items = []
    for row in store.db.execute("SELECT * FROM ready_code ORDER BY created DESC,id LIMIT 40"):
        body = parse_json(row["body"])
        items.append({"ready_id": row["id"], "status": row["status"], "job_id": row["job_id"],
                      **{key: body[key] for key in ("effect_id", "issue_number", "spec_hash", "baseline_sha", "paths", "policy_hash", "review_hash")}})
    return items


def handle(request, config):
    if not isinstance(request, dict) or request.get("schema_version") != 1:
        raise ResearchError("unsupported bridge schema")
    command = request.get("command")
    permitted = {"schema_version", "command", "stage", "owner", "run_id", "model", "reasoning",
        "ticket", "name", "result", "status", "spec", "effect_id", "remote_id", "remote_url", "verified_body_hash", "bundle_id",
        "review", "baseline_sha", "paths", "ready_id"}
    if set(request) - permitted:
        raise ResearchError("bridge request field outside allowlist")
    root = Path(config["state"])
    policy, policy_hash = load_policy(config["policy"])
    with JobStore(root / "jobs.sqlite3") as store:
        if command == "status":
            confirmed = [effect_context(store, row["id"]) for row in store.db.execute(
                "SELECT id FROM outbox WHERE status='CONFIRMED' ORDER BY updated DESC,id LIMIT 40")]
            return {"jobs": store.list(40), "unknown_effects": store.unknown_effects(),
                    "confirmed_effects": confirmed, "ready_code": ready_registry(store), "policy": policy, "policy_hash": policy_hash,
                    "service_health": service_health()}
        if command == "claim":
            if request["stage"] not in policy["enabled_stages"]:
                raise ResearchError("stage disabled by trusted policy")
            ticket = store.claim(request["stage"], request["owner"], request["run_id"],
                model=request["model"], reasoning=request["reasoning"], expected_policy_hash=policy_hash, lease_seconds=1800)
            if ticket:
                verified_inputs(ticket)
            return {"ticket": ticket, "policy": policy, "policy_hash": policy_hash}
        if command == "renew":
            return store.renew(request["ticket"], 1800)
        if command == "checkpoint":
            return {"artifact_hash": store.checkpoint(request["ticket"], request["name"], request["result"])}
        if command == "complete":
            return store.finish(request["ticket"], request["status"], request["result"])
        if command == "draft":
            return prepare_draft(store, request["ticket"], request["spec"], policy, policy_hash)
        if command == "effect":
            return effect_context(store, request["effect_id"])
        if command == "ready-code":
            return ready_code(store, request["effect_id"], request["review"], baseline_sha=request["baseline_sha"],
                             paths=request["paths"], policy=policy, policy_hash=policy_hash)
        if command == "enqueue-coding":
            return enqueue_coding(store, request["ready_id"], policy, policy_hash)
        if command == "dispatch":
            return store.dispatch_publication(request["ticket"], request["effect_id"])
        if command == "unknown":
            store.publication_unknown(request["effect_id"])
            return {"status": "UNKNOWN"}
        if command == "confirm":
            remote_id = request["remote_id"]
            if not isinstance(remote_id, str) or not remote_id.isdigit() or not 1 <= int(remote_id) <= 1_000_000 or request["remote_url"] != f"https://github.com/{policy['repository']}/issues/{remote_id}":
                raise ResearchError("remote issue receipt identity mismatch")
            return store.confirm_publication(request["effect_id"], request["remote_id"], request["remote_url"], request["verified_body_hash"])
        if command == "wake-publication":
            effect = store.outbox(request["effect_id"])
            if not effect or effect["status"] != "CONFIRMED" or effect["repository"] != policy["repository"]:
                raise ResearchError("only a confirmed scoped external effect may resume its waiting parent")
            parent = store.get(effect["job_id"])
            if parent and parent["status"] == "WAITING_ACCESS":
                store.wake(parent["id"], "remote-effect-confirmed")
            return {"job": store.get(effect["job_id"]), "effect_status": "CONFIRMED"}
        if command == "bundle":
            sha(request["bundle_id"])
            path = root / "archive" / request["bundle_id"]
            manifest = verify_bundle(path)
            response = {"manifest": manifest, "files": {}}
            total = 0
            for member in manifest["files"]:
                total += member["bytes"]
                if total > PACKET_BYTES:
                    raise ResearchError("desktop packet quota exceeded; use a compact research packet")
                with (path / member["path"]).open("rb") as stream:
                    data = stream.read(member["bytes"] + 1)
                if len(data) != member["bytes"] or hashlib.sha256(data).hexdigest() != member["sha256"]:
                    raise ResearchError("verified packet changed during transport")
                response["files"][member["path"]] = [base64.b64encode(data[offset:offset + CHUNK_BYTES]).decode("ascii")
                    for offset in range(0, len(data), CHUNK_BYTES)]
            return response
        raise ResearchError("unsupported bridge operation")


def main():
    try:
        request_raw = sys.stdin.buffer.read(MAX_REQUEST + 1)
        if len(request_raw) > MAX_REQUEST:
            raise ResearchError("bridge request quota")
        result = canonical(handle(parse_json(request_raw), load_json("/etc/mm2-research/config.json")))
        if len(result) > MAX_RESPONSE:
            raise ResearchError("bridge response quota")
        sys.stdout.buffer.write(result + b"\n")
        return 0
    except Exception:
        sys.stderr.write("research_bridge_failed; controller evidence is preserved\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
