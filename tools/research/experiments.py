"""Immutable preregistration, single strategy trial and consumed holdout tracking."""
import re
from .jobs import bounded, identity, sha
from .schema import ResearchError, digest, integer, parse_json, utc, window

RECEIPTS = {"ci", "review", "replay", "forward", "performance", "recovery"}


def git_sha(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ResearchError("exact Git commit SHA required")
    return value


def initialize(store):
    store.db.executescript("""
    CREATE TABLE IF NOT EXISTS datasets (
      id TEXT PRIMARY KEY,spec TEXT NOT NULL,hash TEXT NOT NULL,viewed INTEGER NOT NULL DEFAULT 0,
      consumed_by TEXT,created INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS experiments (
      id TEXT PRIMARY KEY,spec TEXT NOT NULL,hash TEXT NOT NULL,status TEXT NOT NULL,
      result TEXT,created INTEGER NOT NULL,updated INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS trial_families (
      family TEXT PRIMARY KEY,max_trials INTEGER NOT NULL,created INTEGER NOT NULL);
    """)


def register_dataset(store, spec):
    initialize(store)
    existing_id = digest(spec)
    if store.db.execute("SELECT id FROM datasets WHERE id=?", (existing_id,)).fetchone():
        return existing_id
    if spec.get("schema_version") != 1 or spec.get("kind") not in {"development", "holdout", "forward"}:
        raise ResearchError("invalid dataset contract")
    window(**spec["utc_range"])
    files = spec.get("bundle_ids")
    minimum = 0 if spec["kind"] == "forward" else 1
    if not isinstance(files, list) or not minimum <= len(files) <= 256 or len(set(files)) != len(files):
        raise ResearchError("finite unique dataset bundles required")
    for value in files:
        sha(value, "dataset bundle id")
    for name in ("universe_hash", "evaluator_hash", "opening_state_hash"):
        sha(spec.get(name), name)
    if spec["kind"] == "forward":
        if files or spec.get("prospective") is not True or utc(spec["utc_range"]["start"]).timestamp() <= store.clock():
            raise ResearchError("forward contract must be frozen before its future observation window; no preexisting tape")
    elif spec.get("quality_eligible") is not True:
        raise ResearchError("incomplete dataset is research evidence, not an eligible experiment dataset")
    dataset_id = digest(spec)
    with store.transaction():
        store.db.execute("INSERT OR IGNORE INTO datasets(id,spec,hash,created) VALUES(?,?,?,?)",
                         (dataset_id, bounded(spec), dataset_id, store.clock()))
    return dataset_id


def register_experiment(store, spec):
    initialize(store)
    existing_id = digest(spec)
    if store.db.execute("SELECT id FROM experiments WHERE id=?", (existing_id,)).fetchone():
        return existing_id
    if spec.get("schema_version") != 1 or spec.get("kind") not in {"strategy", "measurement", "reliability"}:
        raise ResearchError("invalid experiment kind/version")
    for name in ("candidate_sha", "baseline_sha"):
        git_sha(spec.get(name))
    for name in ("config_hash", "evaluator_hash", "policy_hash", "emulator_hash"):
        sha(spec.get(name), name)
    identity(spec.get("family"), "hypothesis family")
    identity(spec.get("author"), "author principal")
    criteria = spec.get("criteria")
    if not isinstance(criteria, list) or not 1 <= len(criteria) <= 32:
        raise ResearchError("frozen finite success criteria required")
    for criterion in criteria:
        if set(criterion) != {"metric", "op", "threshold"} or criterion["op"] not in {"gte", "lte"}:
            raise ResearchError("criterion must be exact bounded comparison")
        identity(criterion["metric"], "metric name")
        integer(criterion["threshold"], signed=True)
    required = spec.get("required_receipts")
    if not isinstance(required, dict) or set(required) != RECEIPTS:
        raise ResearchError("all exact candidate validation receipts required")
    for kind, principal in required.items():
        identity(principal, f"{kind} trusted principal")
    if required["review"] == spec["author"] or required["ci"] == spec["author"]:
        raise ResearchError("author cannot attest required review or CI")
    dataset_ids = spec.get("datasets")
    if not isinstance(dataset_ids, dict) or set(dataset_ids) != {"development", "holdout", "forward"}:
        raise ResearchError("chronological development/holdout/forward datasets required")
    datasets = {}
    for kind, dataset_id in dataset_ids.items():
        sha(dataset_id, "dataset id")
        row = store.db.execute("SELECT * FROM datasets WHERE id=?", (dataset_id,)).fetchone()
        if not row:
            raise ResearchError("dataset not registered")
        dataset = parse_json(row["spec"])
        if dataset["kind"] != kind or dataset["evaluator_hash"] != spec["evaluator_hash"]:
            raise ResearchError("dataset kind/evaluator differs from frozen spec")
        if kind == "holdout" and (row["viewed"] or row["consumed_by"]):
            raise ResearchError("viewed or previously consumed holdout cannot be new evidence")
        datasets[kind] = dataset
    if utc(datasets["forward"]["utc_range"]["start"]).timestamp() <= store.clock():
        raise ResearchError("cannot preregister an experiment after its forward window began")
    embargo = spec.get("embargo_seconds")
    if type(embargo) is not int or not 1 <= embargo <= 31_536_000:
        raise ResearchError("positive finite embargo required")
    for left, right in (("development", "holdout"), ("holdout", "forward")):
        gap = (utc(datasets[right]["utc_range"]["start"]) - utc(datasets[left]["utc_range"]["end"])).total_seconds()
        if gap < embargo:
            raise ResearchError("dataset chronology or embargo violated")
    if spec.get("carryover_policy") != "preserve-full-inventory-and-cash":
        raise ResearchError("experiment cannot erase carryover inventory/cash")
    if type(spec.get("max_family_trials")) is not int or not 1 <= spec["max_family_trials"] <= 100:
        raise ResearchError("finite preregistered multiple-trial budget required")
    experiment_id = digest(spec)
    with store.transaction():
        if store.db.execute("SELECT id FROM experiments WHERE id=?", (experiment_id,)).fetchone():
            return experiment_id
        family = store.db.execute("SELECT max_trials FROM trial_families WHERE family=?", (spec["family"],)).fetchone()
        if family and family[0] != spec["max_family_trials"]:
            raise ResearchError("hypothesis trial budget was already frozen; new candidate cannot increase it")
        if not family:
            store.db.execute("INSERT INTO trial_families VALUES(?,?,?)", (spec["family"], spec["max_family_trials"], store.clock()))
        trials = 0
        for row in store.db.execute("SELECT spec FROM experiments"):
            if parse_json(row[0]).get("family") == spec["family"]:
                trials += 1
        if trials >= spec["max_family_trials"]:
            raise ResearchError("hypothesis family trial budget exhausted; retain negative results")
        now = store.clock()
        store.db.execute("INSERT INTO experiments VALUES(?,?,?,'REGISTERED',NULL,?,?)",
                         (experiment_id, bounded(spec), experiment_id, now, now))
    return experiment_id


def start_experiment(store, experiment_id):
    initialize(store)
    sha(experiment_id, "experiment id")
    with store.transaction():
        row = store.db.execute("SELECT * FROM experiments WHERE id=?", (experiment_id,)).fetchone()
        if not row or row["status"] != "REGISTERED":
            raise ResearchError("experiment already started or absent")
        spec = parse_json(row["spec"])
        forward = parse_json(store.db.execute("SELECT spec FROM datasets WHERE id=?", (spec["datasets"]["forward"],)).fetchone()[0])
        if utc(forward["utc_range"]["start"]).timestamp() <= store.clock():
            raise ResearchError("candidate must start before prospective forward observations")
        if spec["kind"] == "strategy":
            for active in store.db.execute("SELECT spec FROM experiments WHERE status='RUNNING'"):
                if parse_json(active[0])["kind"] == "strategy":
                    raise ResearchError("one strategy experiment may run at a time")
        holdout = spec["datasets"]["holdout"]
        changed = store.db.execute("UPDATE datasets SET viewed=1,consumed_by=? WHERE id=? AND viewed=0 AND consumed_by IS NULL",
                                   (experiment_id, holdout)).rowcount
        if changed != 1:
            raise ResearchError("holdout consumed concurrently or viewed before start")
        store.db.execute("UPDATE experiments SET status='RUNNING',updated=? WHERE id=?", (store.clock(), experiment_id))
    return spec


def candidate_receipt(spec, kind, receipt, current_head_sha):
    """Called by a trusted check broker; worker self-attestation is insufficient."""
    if kind not in RECEIPTS or receipt.get("kind") != kind:
        raise ResearchError("unexpected validation receipt")
    git_sha(current_head_sha)
    if current_head_sha != spec["candidate_sha"]:
        raise ResearchError("candidate head changed; prior receipts invalidated")
    required_fields = ("candidate_sha", "baseline_sha", "config_hash", "evaluator_hash", "policy_hash", "emulator_hash", "datasets")
    if any(receipt.get(field) != spec[field] for field in required_fields):
        raise ResearchError("validation receipt belongs to another immutable candidate/input")
    if receipt.get("principal") != spec["required_receipts"][kind] or receipt.get("broker_verified") is not True:
        raise ResearchError("validation receipt lacks expected independent broker principal")
    if receipt.get("status") != "PASS":
        raise ResearchError("failed/skipped/cancelled/inconclusive receipt is not PASS")
    sha(receipt.get("artifact_hash"), "validation artifact hash")
    if kind == "forward":
        observed = receipt.get("observed_bundle_ids")
        if not isinstance(observed, list) or not 1 <= len(observed) <= 256 or receipt.get("bundle_hashes_verified") is not True:
            raise ResearchError("forward receipt requires independently verified fresh observation bundles")
        for bundle_id in observed:
            sha(bundle_id, "forward observation bundle id")
    return True


def score_experiment(store, experiment_id, result, current_head_sha):
    initialize(store)
    sha(experiment_id, "experiment id")
    with store.transaction():
        row = store.db.execute("SELECT * FROM experiments WHERE id=?", (experiment_id,)).fetchone()
        if not row or row["status"] != "RUNNING":
            raise ResearchError("only running preregistered experiment may be scored")
        spec = parse_json(row["spec"])
        if result.get("declared_status") not in {"PASS", "FAIL", "INCONCLUSIVE"}:
            raise ResearchError("invalid observed experiment outcome")
        reasons = []
        forward = parse_json(store.db.execute("SELECT spec FROM datasets WHERE id=?", (spec["datasets"]["forward"],)).fetchone()[0])
        if store.clock() < utc(forward["utc_range"]["end"]).timestamp():
            reasons.append("PROSPECTIVE_FORWARD_WINDOW_NOT_FINISHED")
        passed = True
        metrics = result.get("metrics", {})
        for criterion in spec["criteria"]:
            metric = integer(metrics.get(criterion["metric"]), signed=True, nullable=True)
            threshold = integer(criterion["threshold"], signed=True)
            if metric is None:
                reasons.append("REQUIRED_METRIC_UNKNOWN")
            elif not (metric >= threshold if criterion["op"] == "gte" else metric <= threshold):
                passed = False
        receipts = result.get("receipts", {})
        for kind in RECEIPTS:
            try:
                candidate_receipt(spec, kind, receipts.get(kind, {}), current_head_sha)
            except ResearchError:
                reasons.append(f"{kind.upper()}_RECEIPT_NOT_VERIFIED")
        if result.get("evidence_quality_eligible") is not True:
            reasons.append("ECONOMIC_EVIDENCE_NOT_ELIGIBLE")
        if result["declared_status"] == "INCONCLUSIVE":
            reasons.append("WORKER_REPORTED_INCONCLUSIVE")
        outcome = "FAIL" if not passed or result["declared_status"] == "FAIL" else ("INCONCLUSIVE" if reasons else "PASS")
        frozen = {"schema_version": 1, "experiment_id": experiment_id, "spec_hash": row["hash"],
                  "status": outcome, "reasons": sorted(set(reasons)), "observed": result,
                  "promotion_allowed": False, "funded_authorization": False,
                  "scope": "Preregistered experiment evidence; separate release and financial gates remain."}
        store.db.execute("UPDATE experiments SET status=?,result=?,updated=? WHERE id=?",
                         (outcome, bounded(frozen), store.clock(), experiment_id))
    return frozen


def coding_ticket(spec, *, sandbox_receipt, policy_hash, baseline_clean, current_baseline_sha):
    """A worktree alone never satisfies the capability sandbox contract."""
    required_denials = {"production-network", "runtime-secrets", "home-mounts", "docker-socket", "github-write", "policy-write"}
    if (sandbox_receipt.get("broker_verified") is not True or sandbox_receipt.get("policy_hash") != policy_hash
            or set(sandbox_receipt.get("verified_denials", [])) != required_denials
            or sandbox_receipt.get("status") != "PASS"):
        raise ResearchError("enforced sandbox denial checks required before autonomous coding")
    if baseline_clean is not True or current_baseline_sha != spec.get("baseline_sha"):
        raise ResearchError("dirty or changed baseline requires review; never reset owner work")
    git_sha(current_baseline_sha)
    return {"schema_version": 1, "scope": spec["implementation"], "baseline_sha": current_baseline_sha,
            "policy_hash": policy_hash, "branch_prefix": "codex/si-", "autonomy": "A1",
            "network_permissions": [], "publication": "trusted-broker-only", "funded_authorization": False}
