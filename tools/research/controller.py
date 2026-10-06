"""Trusted research controller CLI; all worker output is untrusted evidence."""
import argparse
from pathlib import Path
import sys
from .archive import verify_bundle
from .experiments import register_dataset, register_experiment, score_experiment, start_experiment
from .github import DEFAULT_REPOSITORY, GhClient, render_issue, repository, validate_issue
from .jobs import JobStore, MODEL, REASONING, STAGES, WAITING, TERMINAL, sha
from .patches import allowed_path
from .publish import publish, reconcile
from .retention import apply as apply_retention, initialize_root, preview as preview_retention
from .schema import ResearchError, canonical, digest, load_json, parse_json

DEFAULT_POLICY = Path(__file__).resolve().parents[2] / "research" / "policies" / "autonomy.json"


def load_policy(path):
    policy = load_json(path, 64 * 1024)
    if policy.get("schema_version") != 1 or policy.get("autonomy") not in {"A0", "A1"}:
        raise ResearchError("unsupported trusted autonomy policy")
    repository(policy.get("repository"))
    if policy.get("model") != MODEL or policy.get("reasoning") not in REASONING:
        raise ResearchError("policy must pin selected strongest model; no fallback")
    if policy.get("funded_authorization") is not False or policy.get("automatic_merge") is not False:
        raise ResearchError("funded/merge permissions require separate reviewed policy")
    allowed = policy.get("enabled_stages")
    if not isinstance(allowed, list) or not set(allowed) <= STAGES:
        raise ResearchError("policy stage allowlist invalid")
    if policy["autonomy"] == "A0" and set(allowed) & {"coding", "review", "replay"}:
        raise ResearchError("A1 sandbox gate is not active in A0 policy")
    return policy, digest(policy)


def bundle_inputs(paths):
    bundles, seen = [], set()
    for path in paths:
        manifest = verify_bundle(path)
        if manifest["bundle_id"] in seen:
            continue
        seen.add(manifest["bundle_id"])
        bundles.append({"bundle_id": manifest["bundle_id"], "path": str(Path(path).resolve()),
                        "manifest_hash": digest(manifest), "quality": manifest["quality"]})
    if not 1 <= len(bundles) <= 64:
        raise ResearchError("research job requires 1..64 verified immutable bundles")
    return sorted(bundles, key=lambda item: item["bundle_id"])


def verified_inputs(ticket, overrides=()):
    replacements = {}
    for path in overrides:
        manifest = verify_bundle(path)
        replacements[manifest["bundle_id"]] = str(path)
    result = {}
    for reference in ticket["inputs"].get("bundles", []):
        manifest = verify_bundle(replacements.get(reference["bundle_id"], reference["path"]))
        if manifest["bundle_id"] != reference["bundle_id"] or digest(manifest) != reference["manifest_hash"]:
            raise ResearchError("input manifest changed after job enqueue")
        result[manifest["bundle_id"]] = manifest
    if not result:
        raise ResearchError("worker ticket has no verified bundle inputs")
    return result


def enqueue(store, policy, policy_hash, stage, paths, *, metadata=None, candidate_sha=None):
    if stage not in policy["enabled_stages"]:
        raise ResearchError("stage waits for trusted autonomy/sandbox prerequisite")
    metadata = metadata or {}
    if not isinstance(metadata, dict) or set(metadata) - {"window_key", "crash_fingerprint", "source_epoch", "reason"}:
        raise ResearchError("job metadata outside approved controller scope")
    return store.enqueue(stage, {"bundles": bundle_inputs(paths), "metadata": metadata}, policy_hash, candidate_sha)


def prepare_draft(store, ticket, spec, policy, policy_hash, overrides=()):
    with store.transaction():
        store._current(ticket)
    if ticket["policy_hash"] != policy_hash or ticket["stage"] not in {"analysis", "reliability"}:
        raise ResearchError("issue draft has wrong trusted policy/stage")
    manifests = verified_inputs(ticket, overrides)
    fingerprint = validate_issue(spec, manifests, policy_hash)
    request = render_issue(spec, fingerprint)
    store.checkpoint(ticket, "issue-spec-" + fingerprint[:16], spec)
    return store.prepare_publication(ticket, policy["repository"], fingerprint, request)


def ready_code(store, effect_id, review, *, baseline_sha, paths, policy, policy_hash):
    """Trusted independent review broker only; labels/external issue prose grant nothing."""
    from .experiments import git_sha
    from .jobs import bounded
    git_sha(baseline_sha)
    effect = store.outbox(effect_id)
    if not effect or effect["status"] != "CONFIRMED" or effect["repository"] != policy["repository"]:
        raise ResearchError("coding readiness requires independently verified confirmed analysis publication")
    parent = store.get(effect["job_id"])
    if not parent or parent["stage"] not in {"analysis", "reliability"}:
        raise ResearchError("coding readiness belongs to stale or untrusted analysis policy")
    checkpoint_name = "issue-spec-" + effect["fingerprint"][:16]
    row = store.db.execute("SELECT body,hash FROM checkpoints WHERE job_id=? AND generation=? AND name=?",
                           (effect["job_id"], effect["generation"], checkpoint_name)).fetchone()
    if not row:
        raise ResearchError("published issue lacks frozen validated source specification")
    spec = parse_json(row["body"])
    if not isinstance(paths, list) or not 1 <= len(paths) <= 32 or len(set(paths)) != len(paths):
        raise ResearchError("finite exact coding file scope required")
    for path in paths:
        allowed_path(path)
    expected = {"effect_id": effect_id, "body_hash": effect["body_hash"], "spec_hash": row["hash"],
                "policy_hash": policy_hash, "source_policy_hash": parent["policy_hash"], "baseline_sha": baseline_sha, "paths": sorted(paths)}
    if any(review.get(key) != value for key, value in expected.items()):
        raise ResearchError("independent review does not bind current exact issue/spec/baseline/scope")
    principals = policy.get("independent_review_principals", ["trusted-research-review-broker"])
    if (review.get("status") != "PASS" or review.get("broker_verified") is not True
            or review.get("principal") not in principals or review.get("principal") in {parent["owner"], "codex-desktop"}
            or review.get("model") != MODEL or review.get("actual_settings_verified") is not True):
        raise ResearchError("independent trusted review/model receipt missing; labels never authorize coding")
    sha(review.get("artifact_hash"), "independent review artifact hash")
    manifests = verified_inputs(JobStore.ticket(parent))
    issue_number = int(effect["remote_id"])
    if effect["remote_url"] != f"https://github.com/{policy['repository']}/issues/{issue_number}":
        raise ResearchError("confirmed source issue repository/identity mismatch")
    body = {"schema_version": 1, **expected, "issue_number": issue_number, "issue_spec": spec,
            "bundles": parent["inputs"]["bundles"], "review_hash": digest(review),
            "input_manifest_hashes": {key: digest(value) for key, value in manifests.items()}}
    ready_id = digest(body)
    store.db.execute("CREATE TABLE IF NOT EXISTS ready_code(id TEXT PRIMARY KEY,body TEXT NOT NULL,status TEXT NOT NULL,job_id TEXT,created INTEGER NOT NULL)")
    with store.transaction():
        store.db.execute("INSERT OR IGNORE INTO ready_code VALUES(?,?,'READY',NULL,?)", (ready_id, bounded(body), store.clock()))
    return {"ready_id": ready_id, "status": "READY", **body}


def enqueue_coding(store, ready_id, policy, policy_hash):
    sha(ready_id, "ready registry id")
    if policy["autonomy"] != "A1" or "coding" not in policy["enabled_stages"]:
        raise ResearchError("A1 artifact-broker workflow is not activated by trusted policy")
    if not store.db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ready_code'").fetchone():
        raise ResearchError("arbitrary GitHub issue/label cannot enqueue coding")
    row = store.db.execute("SELECT * FROM ready_code WHERE id=?", (ready_id,)).fetchone()
    if not row:
        raise ResearchError("arbitrary GitHub issue/label cannot enqueue coding")
    item = parse_json(row["body"])
    if item["policy_hash"] != policy_hash:
        raise ResearchError("coding readiness review belongs to old policy")
    job = store.enqueue("coding", {"bundles": item["bundles"], "ready": item}, policy_hash, item["baseline_sha"])
    with store.transaction():
        store.db.execute("UPDATE ready_code SET status='DISPATCHED',job_id=? WHERE id=?", (job["id"], ready_id))
    return job


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="trusted controller SQLite; never mount in an untrusted worker")
    parser.add_argument("--policy", default=str(DEFAULT_POLICY), help="read-only trusted controller policy")
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("enqueue")
    p.add_argument("--stage", choices=sorted(STAGES), required=True)
    p.add_argument("--bundle", action="append", required=True)
    p.add_argument("--metadata")
    p.add_argument("--candidate-sha")
    p = commands.add_parser("claim")
    p.add_argument("--stage", choices=sorted(STAGES), required=True)
    p.add_argument("--owner", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--model", default=MODEL)
    p.add_argument("--reasoning", default="max")
    p.add_argument("--lease-seconds", type=int, default=900)
    p = commands.add_parser("status")
    p.add_argument("--job-id")
    p.add_argument("--limit", type=int, default=100)
    for name in ("renew", "checkpoint", "complete", "draft", "publish", "dispatch"):
        p = commands.add_parser(name)
        p.add_argument("--ticket", required=True)
        if name == "renew":
            p.add_argument("--lease-seconds", type=int, default=900)
        elif name == "checkpoint":
            p.add_argument("--name", required=True)
            p.add_argument("--result", required=True)
        elif name == "complete":
            p.add_argument("--status", choices=sorted(TERMINAL | WAITING), required=True)
            p.add_argument("--result", required=True)
        elif name == "draft":
            p.add_argument("--spec", required=True)
            p.add_argument("--bundle", action="append", default=[])
        else:
            p.add_argument("--effect-id", required=True)
    p = commands.add_parser("effect")
    p.add_argument("--effect-id", required=True)
    p = commands.add_parser("unknown")
    p.add_argument("--effect-id", required=True)
    p = commands.add_parser("confirm")
    p.add_argument("--effect-id", required=True)
    p.add_argument("--remote-id", required=True)
    p.add_argument("--remote-url", required=True)
    p.add_argument("--verified-body-hash", required=True)
    p = commands.add_parser("reconcile")
    p.add_argument("--effect-id")
    p = commands.add_parser("wake")
    p.add_argument("--job-id", required=True)
    p.add_argument("--prerequisite", required=True)
    p = commands.add_parser("crash")
    p.add_argument("--family", required=True)
    p.add_argument("--epoch", required=True)
    p.add_argument("--signature", required=True)
    p.add_argument("--evidence", required=True)
    p.add_argument("--actionable", action="store_true")
    p = commands.add_parser("ready-code")
    p.add_argument("--effect-id", required=True)
    p.add_argument("--review", required=True)
    p.add_argument("--baseline-sha", required=True)
    p.add_argument("--path", action="append", required=True)
    p = commands.add_parser("enqueue-coding")
    p.add_argument("--ready-id", required=True)
    for name in ("register-dataset", "register-experiment"):
        p = commands.add_parser(name)
        p.add_argument("--spec", required=True)
    p = commands.add_parser("start-experiment")
    p.add_argument("--experiment-id", required=True)
    p = commands.add_parser("score-experiment")
    p.add_argument("--experiment-id", required=True)
    p.add_argument("--result", required=True)
    p.add_argument("--current-head-sha", required=True)
    p = commands.add_parser("init-secondary-root")
    p.add_argument("--root", required=True)
    p.add_argument("--class", dest="retention_class", choices=["diagnostic", "cache", "build"], required=True)
    p = commands.add_parser("retention-preview")
    p.add_argument("--root", required=True)
    p.add_argument("--cutoff-epoch", type=int, required=True)
    p.add_argument("--protection", required=True, help="trusted current pins and lease_paths JSON")
    p = commands.add_parser("retention-apply")
    p.add_argument("--plan", required=True)
    p.add_argument("--approved-plan-id", required=True)
    p.add_argument("--receipt-root", required=True)
    p.add_argument("--protection", required=True)
    args = parser.parse_args(argv)
    try:
        policy, policy_hash = load_policy(args.policy)
        with JobStore(args.db) as store:
            command = args.command
            if command == "enqueue":
                result = enqueue(store, policy, policy_hash, args.stage, args.bundle,
                                 metadata=load_json(args.metadata) if args.metadata else None, candidate_sha=args.candidate_sha)
            elif command == "claim":
                if args.stage not in policy["enabled_stages"]:
                    raise ResearchError("stage has unmet autonomy prerequisite")
                result = store.claim(args.stage, args.owner, args.run_id, model=args.model,
                                     reasoning=args.reasoning, lease_seconds=args.lease_seconds, expected_policy_hash=policy_hash)
                if result is not None:
                    verified_inputs(result)
            elif command == "status":
                result = store.get(args.job_id) if args.job_id else {"jobs": store.list(args.limit), "unknown_effects": store.unknown_effects(), "policy_hash": policy_hash}
            elif command == "renew":
                result = store.renew(load_json(args.ticket), args.lease_seconds)
            elif command == "checkpoint":
                result = {"artifact_hash": store.checkpoint(load_json(args.ticket), args.name, load_json(args.result))}
            elif command == "complete":
                result = store.finish(load_json(args.ticket), args.status, load_json(args.result))
            elif command == "draft":
                result = prepare_draft(store, load_json(args.ticket), load_json(args.spec), policy, policy_hash, args.bundle)
            elif command == "publish":
                result = publish(store, GhClient(policy["repository"]), load_json(args.ticket), args.effect_id)
            elif command == "dispatch":
                result = store.dispatch_publication(load_json(args.ticket), args.effect_id)
            elif command == "effect":
                result = store.outbox(args.effect_id)
            elif command == "unknown":
                store.publication_unknown(args.effect_id)
                result = store.outbox(args.effect_id)
            elif command == "confirm":
                effect = store.outbox(args.effect_id)
                if not effect or args.remote_url != f"https://github.com/{policy['repository']}/issues/{args.remote_id}":
                    raise ResearchError("remote receipt target outside trusted repository")
                result = store.confirm_publication(args.effect_id, args.remote_id, args.remote_url, args.verified_body_hash)
            elif command == "reconcile":
                effects = [store.outbox(args.effect_id)] if args.effect_id else store.unknown_effects()
                result = [reconcile(store, GhClient(policy["repository"]), item["id"]) for item in effects if item]
            elif command == "wake":
                store.wake(args.job_id, args.prerequisite)
                result = store.get(args.job_id)
            elif command == "crash":
                result = store.register_crash(args.family, args.epoch, args.signature, load_json(args.evidence), args.actionable)
            elif command == "ready-code":
                result = ready_code(store, args.effect_id, load_json(args.review), baseline_sha=args.baseline_sha,
                                    paths=args.path, policy=policy, policy_hash=policy_hash)
            elif command == "enqueue-coding":
                result = enqueue_coding(store, args.ready_id, policy, policy_hash)
            elif command == "register-dataset":
                result = {"dataset_id": register_dataset(store, load_json(args.spec))}
            elif command == "register-experiment":
                result = {"experiment_id": register_experiment(store, load_json(args.spec))}
            elif command == "start-experiment":
                result = start_experiment(store, args.experiment_id)
            elif command == "score-experiment":
                result = score_experiment(store, args.experiment_id, load_json(args.result), args.current_head_sha)
            elif command == "init-secondary-root":
                result = initialize_root(args.root, args.retention_class)
            elif command == "retention-preview":
                protection = load_json(args.protection)
                result = preview_retention(args.root, policy_hash=policy_hash, cutoff_epoch=args.cutoff_epoch,
                                           pins=protection["pins"], lease_paths=protection["lease_paths"])
            else:
                protection = load_json(args.protection)
                result = apply_retention(load_json(args.plan), args.receipt_root, approved_plan_id=args.approved_plan_id,
                                         current_pins=protection["pins"], current_lease_paths=protection["lease_paths"], policy_hash=policy_hash)
        print(canonical(result).decode("utf-8"))
        return 0
    except (ResearchError, KeyError, TypeError, OSError) as exc:
        # Keep error messages categorical. External body/credentials never appear here.
        print(canonical({"status": "BLOCKED", "error": type(exc).__name__, "detail": str(exc) if isinstance(exc, ResearchError) else "invalid controller input"}).decode(), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
