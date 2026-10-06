#!/usr/bin/env python3
"""SSM/SSH authenticated controller bridge. JSON commands never become shell code."""
import base64
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.research.archive import verify_bundle
from tools.research.controller import load_policy, prepare_draft, verified_inputs, ready_code, enqueue_coding
from tools.research.jobs import JobStore, sha
from tools.research.schema import ResearchError, canonical, load_json, parse_json

MAX_REQUEST = 512 * 1024
MAX_RESPONSE = 8 * 1024 * 1024


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
            return {"jobs": store.list(40), "unknown_effects": store.unknown_effects(), "policy": policy, "policy_hash": policy_hash}
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
            return store.outbox(request["effect_id"])
        if command == "ready-code":
            return ready_code(store, request["effect_id"], request["review"], request["baseline_sha"], request["paths"], policy, policy_hash)
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
        if command == "bundle":
            sha(request["bundle_id"])
            path = root / "archive" / request["bundle_id"]
            manifest = verify_bundle(path)
            response = {"manifest": manifest, "files": {}}
            total = 0
            for member in manifest["files"]:
                total += member["bytes"]
                if total > MAX_RESPONSE // 2:
                    raise ResearchError("desktop packet quota exceeded; use a compact research packet")
                response["files"][member["path"]] = base64.b64encode((path / member["path"]).read_bytes()).decode()
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
