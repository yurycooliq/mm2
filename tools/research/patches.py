"""Trusted text-patch broker. Candidate Python is executed only in locked Docker.

Native AI proposes an artifact; it is not claimed to be a least-privilege
computer agent. This broker owns Git/validation/publication capabilities.
"""
import argparse
import hashlib
import io
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import threading
import uuid
from .experiments import git_sha
from .github import DEFAULT_REPOSITORY
from .jobs import sha
from .schema import ResearchError, canonical, digest, load_json

MAX_PATCH = 1024 * 1024
MAX_OUTPUT = 512 * 1024
MAX_SNAPSHOT = 64 * 1024 * 1024
VALIDATION_IMAGE = "python:3.13-slim-bookworm@sha256:2325bb286ec344af3e5898cc224b5844e2707ac6e26b1632516fd3edc84a5e26"
PROTECTED = {"tools/research/controller.py", "tools/research/jobs.py", "tools/research/github.py",
             "tools/research/publish.py", "tools/research/patches.py", "tools/research/retention.py",
             "tools/research/experiments.py"}


def allowed_path(path):
    if not isinstance(path, str) or not re.fullmatch(r"[A-Za-z0-9_./-]{1,200}", path):
        raise ResearchError("patch path contains quoting/control/unexpected characters")
    if path.startswith("/") or any(part in {"", ".", ".."} for part in path.split("/")):
        raise ResearchError("patch path is outside safe relative scope")
    if path in PROTECTED or path.startswith(("research/policies/", ".github/", "infra/", "crates/", "config/", "scripts/")):
        raise ResearchError("patch touches protected execution, policy or broker scope")
    if not (path.startswith("tools/research/") or path.startswith("docs/") or path.startswith("research/contracts/")):
        raise ResearchError("patch is outside autonomous research/docs scope")
    if not path.endswith((".py", ".md", ".json")):
        raise ResearchError("only research Python, JSON contracts and documentation text allowed")
    if any(word in path.lower() for word in ("secret", "credential", "private", ".env")):
        raise ResearchError("patch cannot add credential/private material")
    return path


def inspect_patch(data):
    if not isinstance(data, bytes) or not 1 <= len(data) <= MAX_PATCH or b"\0" in data:
        raise ResearchError("patch must be bounded UTF-8 text")
    try:
        content = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ResearchError("binary patch rejected") from exc
    if not content.startswith("diff --git "):
        raise ResearchError("canonical git unified diff required")
    paths, current, old_seen, new_seen = [], None, False, False
    for line in content.splitlines():
        if line.startswith("diff --git "):
            if current and not (old_seen and new_seen):
                raise ResearchError("every patch member must contain text hunks")
            match = re.fullmatch(r"diff --git a/([A-Za-z0-9_./-]+) b/([A-Za-z0-9_./-]+)", line)
            if not match or match[1] != match[2]:
                raise ResearchError("renames/copies/quoted paths are not allowed")
            current = allowed_path(match[1])
            if current in paths or len(paths) >= 32:
                raise ResearchError("duplicate or excessive patch members")
            paths.append(current)
            old_seen, new_seen = False, False
        elif line.startswith(("GIT binary patch", "Binary files ", "deleted file mode ", "rename ", "copy ", "similarity index ", "old mode ", "new mode ")):
            raise ResearchError("binary/delete/rename/mode changes rejected")
        elif line.startswith("new file mode ") and line != "new file mode 100644":
            raise ResearchError("new files must be ordinary nonexecutable regular text")
        elif line.startswith("index ") and line.rsplit(" ", 1)[-1] in {"120000", "160000"}:
            raise ResearchError("symlink/submodule patch members rejected")
        elif line.startswith("--- "):
            if line not in {f"--- a/{current}", "--- /dev/null"}:
                raise ResearchError("old patch header disagrees with member identity")
            old_seen = True
        elif line.startswith("+++ "):
            if line != f"+++ b/{current}":
                raise ResearchError("delete or new patch header disagrees with member identity")
            new_seen = True
    if not paths or not (old_seen and new_seen):
        raise ResearchError("empty/mode-only patch rejected")
    return {"patch_hash": hashlib.sha256(data).hexdigest(), "paths": paths, "bytes": len(data)}


def run_bounded(args, *, cwd=None, timeout=120, maximum=MAX_OUTPUT, env=None):
    """No shell; terminate rather than buffer arbitrary candidate output."""
    process = subprocess.Popen(args, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output, failure = [], []

    def collect():
        try:
            data = process.stdout.read(maximum + 1)
            output.append(data)
            if len(data) > maximum:
                process.kill()
                failure.append("output quota exceeded")
        except OSError:
            failure.append("output unavailable")

    reader = threading.Thread(target=collect, daemon=True)
    reader.start()
    reader.join(timeout)
    if reader.is_alive():
        process.kill()
        reader.join(5)
        failure.append("time quota exceeded")
    process.wait(timeout=10)
    if failure:
        raise ResearchError("bounded broker process " + failure[0])
    data = output[0] if output else b""
    if process.returncode:
        raise ResearchError("broker subprocess failed; output hash=" + hashlib.sha256(data).hexdigest())
    return data


def git(repo, *args, maximum=MAX_OUTPUT):
    return run_bounded(["git", "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false", "-c", "commit.gpgsign=false",
                        "-c", "core.autocrlf=false", "-C", str(repo), *args], maximum=maximum)


def branch_name(value):
    if (not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_./-]{0,200}", value)
            or ".." in value or "//" in value or value.endswith(("/", ".", ".lock"))):
        raise ResearchError("named exact baseline branch required; detached or ambiguous base is blocked")
    return value


def preview(repo, patch_path, *, baseline_sha, issue_number, issue_spec_hash, policy_hash):
    git_sha(baseline_sha)
    sha(issue_spec_hash, "frozen issue spec hash")
    sha(policy_hash, "trusted patch policy hash")
    if type(issue_number) is not int or not 1 <= issue_number <= 1_000_000:
        raise ResearchError("frozen repository issue number required")
    path = Path(patch_path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_PATCH:
        raise ResearchError("patch artifact missing, linked or oversized")
    patch = inspect_patch(path.read_bytes())
    revision = git(repo, "rev-parse", "--verify", baseline_sha + "^{commit}").decode().strip()
    if revision != baseline_sha:
        raise ResearchError("baseline must resolve to exact commit")
    remote = git(repo, "remote", "get-url", "origin").decode().strip()
    if remote not in {f"https://github.com/{DEFAULT_REPOSITORY}", f"https://github.com/{DEFAULT_REPOSITORY}.git", f"git@github.com:{DEFAULT_REPOSITORY}.git"}:
        raise ResearchError("source repository differs from trusted mm2 identity")
    baseline_branch = branch_name(git(repo, "symbolic-ref", "--quiet", "--short", "HEAD").decode().strip())
    if git(repo, "rev-parse", "HEAD").decode().strip() != baseline_sha:
        raise ResearchError("named baseline branch does not point at the frozen baseline SHA")
    body = {"schema_version": 1, "baseline_sha": baseline_sha, "baseline_branch": baseline_branch, "issue_number": issue_number,
            "issue_spec_hash": issue_spec_hash, "policy_hash": policy_hash, **patch,
            "repository": DEFAULT_REPOSITORY, "automatic_merge": False, "funded_authorization": False}
    return {**body, "proposal_id": digest(body)}


def _snapshot(repo, revision, target):
    data = git(repo, "archive", "--format=tar", revision, maximum=MAX_SNAPSHOT)
    target.mkdir(parents=True, exist_ok=False)
    total = 0
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
        members = archive.getmembers()
        if len(members) > 10_000:
            raise ResearchError("source snapshot file quota exceeded")
        for member in members:
            parts = Path(member.name).parts
            if member.name.startswith("/") or ".." in parts or ".git" in parts or not (member.isfile() or member.isdir()):
                raise ResearchError("source snapshot unsafe member or linked file")
            total += member.size
            if total > MAX_SNAPSHOT:
                raise ResearchError("source snapshot byte quota exceeded")
            archive.extract(member, target, filter="data")
    return hashlib.sha256(data).hexdigest()


def prepare(repo, patch_path, plan, work_root):
    fresh = preview(repo, patch_path, baseline_sha=plan["baseline_sha"], issue_number=plan["issue_number"],
                    issue_spec_hash=plan["issue_spec_hash"], policy_hash=plan["policy_hash"])
    if fresh != plan:
        raise ResearchError("patch artifact or frozen proposal changed")
    if git(repo, "status", "--porcelain=v1", "--untracked-files=normal").strip():
        raise ResearchError("owner checkout is dirty; retain proposal and wait for clean baseline")
    if git(repo, "rev-parse", "HEAD").decode().strip() != plan["baseline_sha"]:
        raise ResearchError("owner baseline advanced; proposal requires revalidation")
    root = Path(work_root).absolute()
    for parent in (root, *root.parents):
        if parent.exists() and parent.is_symlink():
            raise ResearchError("disposable worktree parent symlink rejected")
    root.mkdir(parents=True, exist_ok=True)
    worktree = root / plan["proposal_id"]
    branch = f"codex/research-{plan['issue_number']}-{plan['patch_hash'][:12]}"
    if worktree.exists():
        raise ResearchError("existing proposal worktree must be reconciled; never reset or overwrite")
    git(repo, "worktree", "add", "--detach", str(worktree), plan["baseline_sha"])
    git(worktree, "checkout", "-b", branch)
    # --index verifies existing path modes/contents too; metadata parser is not
    # substituted for Git's full hunk/path validation.
    git(worktree, "apply", "--check", "--index", "--whitespace=error", str(Path(patch_path).absolute()))
    git(worktree, "apply", "--index", "--whitespace=error", str(Path(patch_path).absolute()))
    changed = git(worktree, "diff", "--cached", "--name-only").decode().splitlines()
    if sorted(changed) != sorted(plan["paths"]):
        raise ResearchError("staged patch paths differ from validated proposal")
    git(worktree, "-c", "user.name=mm2 research broker", "-c", "user.email=mm2-research@users.noreply.github.com",
        "commit", "-m", f"Research proposal #{plan['issue_number']}: {plan['patch_hash'][:12]}")
    head = git_sha(git(worktree, "rev-parse", "HEAD").decode().strip())
    return {"schema_version": 1, "proposal": plan, "worktree": str(worktree), "branch": branch,
            "candidate_sha": head, "automatic_merge": False, "status": "PREPARED"}


def docker_command(image, candidate, trusted, command):
    if image != VALIDATION_IMAGE:
        raise ResearchError("offline validation requires an exact pinned image digest")
    if any(any(character in str(path) for character in (",", "\n", "\r", "\0")) for path in (candidate, trusted)):
        raise ResearchError("snapshot mount path contains Docker option delimiters")
    return ["docker", "run", "--rm", "--pull", "never", "--network", "none", "--read-only",
            "--log-driver", "none", "--no-healthcheck",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--user", "65532:65532",
            "--cpus", "1", "--memory", "768m", "--pids-limit", "128",
            "--tmpfs", "/tmp:rw,noexec,nosuid,size=128m,uid=65532,gid=65532",
            # Logical scratch headroom must exceed the production archive's
            # worst-case reservation (~1.48 GiB) in fixtures. The unchanged
            # 768 MiB cgroup memory limit still bounds actual tmpfs consumption.
            "--tmpfs", "/trusted/output:rw,noexec,nosuid,size=2g,uid=65532,gid=65532",
            "--mount", f"type=bind,source={candidate},target=/candidate,readonly",
            "--mount", f"type=bind,source={trusted},target=/trusted,readonly",
            "--workdir", "/trusted/output", "--env", "PYTHONDONTWRITEBYTECODE=1", "--env", "PYTHONPATH=/candidate",
            "--env", "HOME=/tmp", image, *command]


def cleanup_validation_worker(name, owner, expected_id=None):
    """Reconcile daemon state, then remove only our immutable container identity."""
    rows = run_bounded(["docker", "container", "ls", "--all", "--no-trunc", "--filter",
                        f"name=^/{name}$", "--format", '{{.ID}} {{.Label "mm2.patch-validation"}}'],
                       timeout=10, maximum=1024).decode("ascii").splitlines()
    if not rows:
        return
    fields = rows[0].split()
    if (len(rows) != 1 or len(fields) != 2 or not re.fullmatch(r"[0-9a-f]{64}", fields[0])
            or fields[1] != owner or (expected_id is not None and fields[0] != expected_id)):
        raise ResearchError("validation worker ownership mismatch; no container removed")
    container_id = fields[0]
    try:
        run_bounded(["docker", "container", "rm", "--force", container_id], timeout=10, maximum=1024)
    except ResearchError:
        # The worker may have exited/been removed concurrently. A failed rm alone
        # is not an absence proof; reconcile by the immutable ID, not its name.
        pass
    remaining = run_bounded(["docker", "container", "ls", "--all", "--no-trunc", "--filter",
                             f"id={container_id}", "--format", "{{.ID}}"], timeout=10, maximum=1024)
    if remaining.strip():
        raise ResearchError("validation worker cleanup failed; owned container remains")


def run_validation_worker(image, candidate, trusted, command, timeout):
    """Create before start so a killed attach client cannot orphan a running worker."""
    owner = uuid.uuid4().hex
    name = "mm2-patch-validation-" + owner
    args = docker_command(image, candidate, trusted, command)
    # No --rm: lifecycle is owned here and confirmed after success and failure.
    create = ["docker", "create", "--name", name, "--label", "mm2.patch-validation=" + owner, *args[3:]]
    container_id = None
    try:
        reply = run_bounded(create, timeout=30, maximum=1024)
        container_id = reply.decode("ascii").strip()
        if not re.fullmatch(r"[0-9a-f]{64}", container_id):
            raise ResearchError("validation worker creation identity unavailable")
        return run_bounded(["docker", "start", "--attach", container_id], timeout=timeout)
    finally:
        try:
            cleanup_validation_worker(name, owner, container_id)
        except (ResearchError, OSError, UnicodeError) as exc:
            # Never issue a PASS receipt when cleanup is unknown. Do not forward
            # arbitrary Docker diagnostics; the owned name is safe to reconcile.
            raise ResearchError("validation worker cleanup unconfirmed: " + name) from exc


DENIAL_PROBE = """import json,os,socket
assert os.getuid()==65532
assert not os.path.exists('/var/run/docker.sock')
assert not os.path.exists('/host')
assert all(k not in os.environ for k in ('AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','GH_TOKEN','GITHUB_TOKEN','MM2_PRIVATE_KEY'))
try:
 open('/candidate/.denied-write','wb')
 raise AssertionError('candidate writable')
except PermissionError: pass
except OSError as e: assert e.errno in (13,30)
s=socket.socket();s.settimeout(0.2)
try:
 s.connect(('1.1.1.1',443))
 raise AssertionError('external network available')
except OSError: pass
finally: s.close()
print(json.dumps({'status':'PASS','denials':['production-network','runtime-secrets','home-mounts','docker-socket','candidate-write','github-write']}))
"""


TRUSTED_TEST_RUNNER = """import os,tempfile,unittest
# Historical Windows fixtures requested a repository-local temp directory.
# Source mounts stay immutable: relocate only those exact fixture roots into
# bounded container scratch. Production archive/quota/path APIs are untouched.
original_mkdtemp=tempfile.mkdtemp
def fixture_mkdtemp(suffix=None,prefix=None,dir=None):
 if dir is not None and os.path.normpath(os.fspath(dir)) in ('/trusted','/candidate'):
  dir='/trusted/output'
 return original_mkdtemp(suffix=suffix,prefix=prefix,dir=dir)
tempfile.mkdtemp=fixture_mkdtemp
suite=unittest.defaultTestLoader.discover('/trusted/tools/research/tests',pattern='test_*.py')
result=unittest.TextTestRunner(verbosity=2).run(suite)
raise SystemExit(0 if result.wasSuccessful() else 1)
"""


def validate(repo, prepared, snapshot_root, image, timeout=180):
    worktree = Path(prepared["worktree"])
    head = git(worktree, "rev-parse", "HEAD").decode().strip()
    if head != prepared["candidate_sha"] or git(worktree, "status", "--porcelain=v1").strip():
        raise ResearchError("candidate changed or dirty; validation receipt invalidated")
    root = Path(snapshot_root).absolute()
    for parent in (root, *root.parents):
        if parent.exists() and parent.is_symlink():
            raise ResearchError("snapshot parent symlink rejected")
    root.mkdir(parents=True, exist_ok=True)
    candidate, trusted = root / "candidate", root / "trusted"
    candidate_hash = _snapshot(worktree, head, candidate)
    baseline_hash = _snapshot(repo, prepared["proposal"]["baseline_sha"], trusted)
    # Docker needs this existing bind-directory mount point for a writable tmpfs;
    # its contents remain absent from the clean source snapshot and host mounts.
    (trusted / "output").mkdir(exist_ok=True)
    # Only clean tracked snapshots are mounted. No ignored output/private.env,
    # .git common directory, owner home, credentials or Docker socket exist here.
    probe = run_validation_worker(image, candidate, trusted, ["python", "-B", "-c", DENIAL_PROBE], timeout=30)
    tests = run_validation_worker(image, candidate, trusted, ["python", "-B", "-c", TRUSTED_TEST_RUNNER], timeout=timeout)
    body = {"schema_version": 1, "status": "PASS", "candidate_sha": head,
            "baseline_sha": prepared["proposal"]["baseline_sha"], "proposal_id": prepared["proposal"]["proposal_id"],
            "baseline_branch": prepared["proposal"].get("baseline_branch"),
            "patch_hash": prepared["proposal"]["patch_hash"], "policy_hash": prepared["proposal"]["policy_hash"],
            "candidate_snapshot_hash": candidate_hash, "trusted_snapshot_hash": baseline_hash, "image": image,
            "denial_probe_output_hash": hashlib.sha256(probe).hexdigest(), "trusted_tests_output_hash": hashlib.sha256(tests).hexdigest(),
            "check_principal": "trusted-offline-patch-broker", "suite": "frozen baseline tools/research/tests",
            "runner_hash": hashlib.sha256(TRUSTED_TEST_RUNNER.encode()).hexdigest(), "automatic_merge": False}
    return {**body, "receipt_hash": digest(body)}


def _durable_file(path, value):
    data = canonical(value) + b"\n"
    if path.exists():
        if path.is_symlink() or path.read_bytes() != data:
            raise ResearchError("patch broker outbox immutable identity changed")
        return
    with path.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def publish_draft(prepared, receipt, *, expected_policy_hash, outbox_root, gh="gh"):
    """Trusted broker only. This function never merges or executes candidate code."""
    body = {key: value for key, value in receipt.items() if key != "receipt_hash"}
    if receipt.get("receipt_hash") != digest(body) or receipt.get("status") != "PASS":
        raise ResearchError("candidate lacks immutable successful broker receipt")
    plan = prepared["proposal"]
    if any(receipt.get(key) != value for key, value in {"candidate_sha": prepared["candidate_sha"],
           "baseline_sha": plan["baseline_sha"], "proposal_id": plan["proposal_id"], "patch_hash": plan["patch_hash"],
           "policy_hash": expected_policy_hash, "check_principal": "trusted-offline-patch-broker"}.items()):
        raise ResearchError("draft PR receipt belongs to another candidate/policy/run")
    baseline_branch = branch_name(plan.get("baseline_branch"))
    if receipt.get("baseline_branch") != baseline_branch:
        raise ResearchError("validation receipt does not bind the exact intended baseline branch")
    worktree = Path(prepared["worktree"])
    if git(worktree, "rev-parse", "HEAD").decode().strip() != prepared["candidate_sha"]:
        raise ResearchError("head changed after validation")
    if git(worktree, "status", "--porcelain=v1").strip():
        raise ResearchError("candidate dirty after validation")
    if git(worktree, "merge-base", plan["baseline_sha"], prepared["candidate_sha"]).decode().strip() != plan["baseline_sha"]:
        raise ResearchError("candidate is not descended from its exact frozen baseline")
    changed = git(worktree, "diff-tree", "--no-commit-id", "--name-only", "-r", plan["baseline_sha"], prepared["candidate_sha"]).decode().splitlines()
    if not changed or sorted(changed) != sorted(plan["paths"]):
        raise ResearchError("candidate includes changes outside the frozen patch scope")
    for path in changed:
        allowed_path(path)

    def verify_remote_base():
        lines = git(worktree, "ls-remote", "--heads", "origin", f"refs/heads/{baseline_branch}").decode().splitlines()
        expected = f"{plan['baseline_sha']}\trefs/heads/{baseline_branch}"
        if lines != [expected]:
            raise ResearchError("remote intended baseline is missing, ambiguous or changed; never fall back to main")

    verify_remote_base()
    outbox = Path(outbox_root).absolute()
    for parent in (outbox, *outbox.parents):
        if parent.exists() and parent.is_symlink():
            raise ResearchError("patch broker outbox parent symlink rejected")
    outbox.mkdir(parents=True, exist_ok=True)
    prefix = plan["proposal_id"]
    dispatched_path = outbox / f"{prefix}.dispatched.json"
    # A stable branch is the external idempotency key; enumerate all PR states
    # before any POST. Unknown gh response requires this reconciliation again.
    prior = run_bounded([gh, "pr", "list", "--repo", DEFAULT_REPOSITORY, "--state", "all", "--head", prepared["branch"],
                         "--json", "url,headRefOid,baseRefName,baseRefOid,state", "--limit", "100"])
    import json
    matches = json.loads(prior)
    if len(matches) > 1:
        raise ResearchError("duplicate proposal PRs need reconciliation")
    if matches:
        if (matches[0].get("headRefOid") != prepared["candidate_sha"] or matches[0].get("baseRefName") != baseline_branch
                or matches[0].get("baseRefOid") != plan["baseline_sha"]):
            raise ResearchError("existing remote PR head/base differs from the exact validated candidate/baseline")
        result = {"status": "CONFIRMED", "url": matches[0]["url"], "candidate_sha": prepared["candidate_sha"],
                  "receipt_hash": receipt["receipt_hash"], "attach_artifact_required": True}
        _durable_file(outbox / f"{prefix}.confirmed.json", result)
        return result
    if dispatched_path.exists():
        # A negative listing after a previous POST is not absence proof.
        return {"status": "UNKNOWN", "branch": prepared["branch"], "blind_retry_allowed": False}
    text = (f"Implements frozen research proposal for #{plan['issue_number']}.\n\n"
            f"Candidate: `{prepared['candidate_sha']}`; baseline: `{plan['baseline_sha']}`.\n"
            f"Offline broker validation: `{receipt['receipt_hash']}`. Frozen baseline tests and sandbox denial probes PASS.\n"
            "Financial/production code, policy and CI are outside this patch scope. Independent human review remains required.\n")
    request = {"proposal_id": prefix, "candidate_sha": prepared["candidate_sha"], "branch": prepared["branch"],
               "receipt_hash": receipt["receipt_hash"], "body": text, "title": f"Research proposal #{plan['issue_number']}",
               "repository": DEFAULT_REPOSITORY, "draft": True, "base": baseline_branch, "base_sha": plan["baseline_sha"]}
    _durable_file(outbox / f"{prefix}.prepared.json", request)
    git(worktree, "push", "origin", f"{prepared['candidate_sha']}:refs/heads/{prepared['branch']}")
    verify_remote_base()
    with tempfile.TemporaryDirectory(prefix="mm2-pr-body-") as temp:
        body_file = Path(temp) / "body.md"
        body_file.write_text(text, encoding="utf-8")
        _durable_file(dispatched_path, {"request_hash": digest(request), "candidate_sha": prepared["candidate_sha"]})
        try:
            url = run_bounded([gh, "pr", "create", "--repo", DEFAULT_REPOSITORY, "--draft", "--base", baseline_branch,
                              "--head", prepared["branch"], "--title", f"Research proposal #{plan['issue_number']}",
                              "--body-file", str(body_file)]).decode().strip()
        except ResearchError:
            return {"status": "UNKNOWN", "branch": prepared["branch"], "blind_retry_allowed": False}
    if not re.fullmatch(r"https://github.com/yurycooliq/mm2/pull/[0-9]+", url):
        raise ResearchError("draft PR response unknown; reconcile stable branch")
    try:
        observed = json.loads(run_bounded([gh, "pr", "view", url, "--repo", DEFAULT_REPOSITORY,
                                          "--json", "headRefOid,baseRefName,baseRefOid"]))
        if (observed.get("headRefOid") != prepared["candidate_sha"] or observed.get("baseRefName") != baseline_branch
                or observed.get("baseRefOid") != plan["baseline_sha"]):
            return {"status": "WAITING_REVIEW", "url": url, "reason": "remote PR head or baseline changed",
                    "blind_retry_allowed": False, "attach_artifact_required": True}
    except (ResearchError, ValueError, TypeError):
        return {"status": "UNKNOWN", "url": url, "branch": prepared["branch"], "blind_retry_allowed": False,
                "attach_artifact_required": True}
    result = {"status": "CONFIRMED", "url": url, "candidate_sha": prepared["candidate_sha"],
              "receipt_hash": receipt["receipt_hash"], "attach_artifact_required": True}
    _durable_file(outbox / f"{prefix}.confirmed.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("preview")
    p.add_argument("--patch", required=True)
    p.add_argument("--baseline-sha", required=True)
    p.add_argument("--issue-number", type=int, required=True)
    p.add_argument("--issue-spec-hash", required=True)
    p.add_argument("--policy-hash", required=True)
    p = commands.add_parser("prepare")
    p.add_argument("--patch", required=True)
    p.add_argument("--plan", required=True)
    p.add_argument("--work-root", required=True)
    p = commands.add_parser("validate")
    p.add_argument("--prepared", required=True)
    p.add_argument("--snapshot-root", required=True)
    p.add_argument("--image", required=True)
    p = commands.add_parser("publish-draft")
    p.add_argument("--prepared", required=True)
    p.add_argument("--receipt", required=True)
    p.add_argument("--policy-hash", required=True)
    p.add_argument("--outbox-root", required=True)
    args = parser.parse_args()
    try:
        if args.command == "preview":
            result = preview(args.repo, args.patch, baseline_sha=args.baseline_sha, issue_number=args.issue_number,
                             issue_spec_hash=args.issue_spec_hash, policy_hash=args.policy_hash)
        elif args.command == "prepare":
            result = prepare(args.repo, args.patch, load_json(args.plan), args.work_root)
        elif args.command == "validate":
            result = validate(args.repo, load_json(args.prepared), args.snapshot_root, args.image)
        else:
            result = publish_draft(load_json(args.prepared), load_json(args.receipt), expected_policy_hash=args.policy_hash, outbox_root=args.outbox_root)
        print(canonical(result).decode())
        return 0
    except (ResearchError, OSError, ValueError, KeyError) as exc:
        print(canonical({"status": "WAITING_REVIEW", "error": str(exc) if isinstance(exc, ResearchError) else "invalid broker input/process"}).decode())
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
