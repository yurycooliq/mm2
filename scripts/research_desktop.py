#!/usr/bin/env python3
"""Trusted desktop broker. Remote controller is sole database writer; gh stays local."""
import argparse
import base64
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.research.archive import verify_bundle
from tools.research.github import GhClient, remote_request, request_hash
from tools.research.publish import _receipt
from tools.research.schema import ResearchError, canonical, digest, load_json, parse_json

ROOT = Path(__file__).resolve().parents[1]
REMOTE = "sudo -u mm2research /opt/mm2-research/venv/bin/python -B /opt/mm2-research/current/scripts/research_bridge.py"
MAX_RESPONSE = 8 * 1024 * 1024


def bridge(payload, run=subprocess.run):
    payload = {"schema_version": 1, **payload}
    data = canonical(payload)
    if len(data) > 512 * 1024:
        raise ResearchError("desktop request quota exceeded")
    result = run(["ssh", "-F", str(ROOT / "output/deployment-access/ssh-config"), "mm2-research", REMOTE],
        input=data, capture_output=True, timeout=45, check=False)
    if result.returncode or len(result.stdout) > MAX_RESPONSE:
        raise ResearchError("research controller unavailable; preserve tickets and retry after access recovery")
    return parse_json(result.stdout)


def save(path, value):
    data = canonical(value) + b"\n"
    if path.exists():
        if path.is_symlink() or path.read_bytes() != data:
            raise ResearchError("retained desktop artifact changed")
        return
    with path.open("xb") as stream:
        stream.write(data); stream.flush(); os.fsync(stream.fileno())


def pull(stage, existing_ticket=None):
    run_id = "desktop-" + uuid.uuid4().hex
    if existing_ticket:
        ticket = load_json(existing_ticket)
        bridge({"command": "renew", "ticket": ticket})
        result = bridge({"command": "status"})
    else:
        result = bridge({"command": "claim", "stage": stage, "owner": "codex-desktop", "run_id": run_id,
                         "model": "gpt-6-astra", "reasoning": "max"})
        ticket = result["ticket"]
    if ticket is None:
        return {"ticket": None}
    directory = ROOT / "output/research-inbox" / (ticket["job_id"] + "-" + str(ticket["generation"]))
    directory.mkdir(parents=True, exist_ok=True)
    save(directory / "ticket.json", ticket)
    save(directory / "policy.json", result["policy"])
    paths = []
    for reference in ticket["inputs"]["bundles"]:
        content = bridge({"command": "bundle", "bundle_id": reference["bundle_id"]})
        if content["manifest"].get("bundle_id") != reference["bundle_id"] or digest(content["manifest"]) != reference["manifest_hash"]:
            raise ResearchError("desktop packet differs from exact claimed inputs")
        path = directory / reference["bundle_id"]
        pending = directory / (reference["bundle_id"] + ".pending")
        if path.exists():
            verify_bundle(path)
            if load_json(path / "manifest.json") != content["manifest"]:
                raise ResearchError("retained inbox manifest changed")
            paths.append(str(path)); continue
        pending.mkdir(exist_ok=True)
        save(pending / "manifest.json", content["manifest"])
        for member in content["manifest"]["files"]:
            # Flat safe member names are rechecked by the immutable manifest verifier.
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}", member["path"]) or member["path"] in {"manifest.json", ".", ".."}:
                raise ResearchError("unsafe desktop bundle member")
            data = base64.b64decode(content["files"][member["path"]], validate=True)
            target = pending / member["path"]
            if target.exists():
                if target.is_symlink() or target.read_bytes() != data:
                    raise ResearchError("retained pending member changed")
            else:
                with target.open("xb") as stream:
                    stream.write(data); stream.flush(); os.fsync(stream.fileno())
        verify_bundle(pending)
        os.rename(pending, path)
        paths.append(str(path))
    return {"ticket": str(directory / "ticket.json"), "bundles": paths, "model_claim_is_provider_attestation": False}


def publish(ticket, spec, gh=None):
    gh = gh or GhClient()
    effect = bridge({"command": "draft", "ticket": ticket, "spec": spec})
    # Exact broker marker/body check before dispatch; a missing result is not inferred
    # from a truncated GitHub search page. No POST retry after uncertainty.
    issues = gh.all_issues()
    matches = [item for item in issues if f"<!-- {effect['marker']} -->" in (item.get("body") or "")]
    if len(matches) > 1:
        raise ResearchError("ambiguous issue publication marker")
    if matches:
        found = matches[0]
        _receipt(effect, found)
    else:
        if effect["status"] not in {"PENDING"}:
            raise ResearchError("unknown issue response requires reconciliation; no blind resend")
        bridge({"command": "dispatch", "ticket": ticket, "effect_id": effect["id"]})
        try:
            posted = gh.create_issue(effect["body"])
            found = gh.get_issue(posted["number"])
            _receipt(effect, found)
        except Exception:
            bridge({"command": "unknown", "effect_id": effect["id"]})
            raise ResearchError("GitHub issue response unknown; reconcile exact marker before any further send") from None
    return bridge({"command": "confirm", "effect_id": effect["id"], "remote_id": str(found["number"]),
        "remote_url": found["html_url"], "verified_body_hash": effect["body_hash"]})


def reconcile(effect_id, gh=None):
    """Recover a possibly committed POST without a current worker lease or any POST."""
    gh = gh or GhClient()
    effect = bridge({"command": "effect", "effect_id": effect_id})
    if not effect:
        raise ResearchError("unknown durable publication effect")
    matches = [item for item in gh.all_issues() if f"<!-- {effect['marker']} -->" in (item.get("body") or "")]
    if len(matches) > 1:
        raise ResearchError("ambiguous remote markers require owner reconciliation")
    if not matches:
        return {"status": "WAITING_ACCESS", "effect_id": effect_id, "blind_retry_allowed": False}
    remote_id, remote_url = _receipt(effect, matches[0])
    confirmed = bridge({"command": "confirm", "effect_id": effect_id, "remote_id": remote_id,
                        "remote_url": remote_url, "verified_body_hash": effect["body_hash"]})
    resumed = bridge({"command": "wake-publication", "effect_id": effect_id})
    return {"status": "CONFIRMED", "effect": confirmed, "parent": resumed}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status")
    p = commands.add_parser("pull"); p.add_argument("--stage", choices=["analysis", "reliability", "coding"], required=True)
    p = commands.add_parser("resume"); p.add_argument("--ticket", required=True)
    p = commands.add_parser("effect"); p.add_argument("--effect-id", required=True)
    p = commands.add_parser("reconcile"); p.add_argument("--effect-id", required=True)
    p = commands.add_parser("ready-code")
    p.add_argument("--effect-id", required=True); p.add_argument("--review", required=True)
    p.add_argument("--baseline-sha", required=True); p.add_argument("--path", action="append", required=True)
    p = commands.add_parser("enqueue-coding"); p.add_argument("--ready-id", required=True)
    for command in ("renew", "checkpoint", "complete", "publish"):
        p = commands.add_parser(command); p.add_argument("--ticket", required=True)
        if command in {"checkpoint", "complete"}: p.add_argument("--result", required=True)
        if command == "checkpoint": p.add_argument("--name", required=True)
        if command == "complete": p.add_argument("--status", required=True)
        if command == "publish": p.add_argument("--spec", required=True)
    args = parser.parse_args()
    try:
        if args.command == "status": result = bridge({"command": "status"})
        elif args.command == "pull": result = pull(args.stage)
        elif args.command == "resume": result = pull(None, args.ticket)
        elif args.command == "effect": result = bridge({"command": "effect", "effect_id": args.effect_id})
        elif args.command == "reconcile": result = reconcile(args.effect_id)
        elif args.command == "ready-code": result = bridge({"command": "ready-code", "effect_id": args.effect_id,
            "review": load_json(args.review), "baseline_sha": args.baseline_sha, "paths": args.path})
        elif args.command == "enqueue-coding": result = bridge({"command": "enqueue-coding", "ready_id": args.ready_id})
        else:
            ticket = load_json(args.ticket)
            if args.command == "publish": result = publish(ticket, load_json(args.spec))
            else:
                payload = {"command": args.command, "ticket": ticket}
                if args.command in {"checkpoint", "complete"}: payload["result"] = load_json(args.result)
                if args.command == "checkpoint": payload["name"] = args.name
                if args.command == "complete": payload["status"] = args.status
                result = bridge(payload)
        print(canonical(result).decode()); return 0
    except Exception:
        print("research_desktop_operation_failed; preserve immutable tickets/receipts and inspect scoped controller status", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
