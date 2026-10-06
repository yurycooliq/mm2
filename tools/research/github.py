"""Scoped issue contracts and bounded GitHub transport for the trusted broker."""
import json
import re
import subprocess
from .jobs import sha
from .schema import ResearchError, canonical, digest

DEFAULT_REPOSITORY = "yurycooliq/mm2"
LABELS = {"research", "data-quality", "accounting", "execution", "performance", "reliability",
          "ready-for-code", "needs-evidence", "experiment-running", "validated-paper",
          "needs-live-approval", "rejected", "superseded"}
REQUIRED_SECTIONS = ("problem", "confirmed", "hypothesis", "unknowns", "alternative", "mechanism",
                     "metric", "implementation", "recovery_risk", "tests", "acceptance", "rollout", "rollback")
MAX_ISSUE = 60_000
MAX_REMOTE_BYTES = 10 * 1024 * 1024
SECRET_VALUE = re.compile(r"(?:\b\d{7,12}:[A-Za-z0-9_-]{20,}\b|"
                          r"\b(?:private[_ -]?key|seed phrase|api[_ -]?secret|bot[_ -]?token|authorization)\s*[:=]|"
                          r"https?://[^\s<>]+[?][^\s<>]*(?:token|key|secret|signature|password)=)", re.I)


def repository(value, allowed=DEFAULT_REPOSITORY):
    if value != allowed or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", value):
        raise ResearchError("publication target is outside trusted repository scope")
    return value


def safe_prose(value, name, limit=12_000):
    if not isinstance(value, str) or not value.strip() or len(value.encode("utf-8")) > limit:
        raise ResearchError(f"missing or excessive issue {name}")
    if any(ord(c) < 32 and c not in "\n\t\r" for c in value) or SECRET_VALUE.search(value):
        raise ResearchError("issue contains unsafe control or secret-shaped text")
    if "mm2-research:" in value:
        raise ResearchError("external evidence cannot inject broker markers")
    return value.strip()


def validate_issue(spec, verified_bundle_ids, policy_hash):
    """Labels and external prose cannot make an item ready for code."""
    if spec.get("schema_version") != 1 or spec.get("policy_hash") != policy_hash:
        raise ResearchError("issue contract/policy mismatch")
    allowed = {"schema_version", "policy_hash", "title", "scope", "failure_family", "version_epoch",
               "mechanism_id", "evidence", "labels", "sections", "experiment_id", "dependencies"}
    if set(spec) - allowed:
        raise ResearchError("untrusted issue field is outside approved contract")
    title = safe_prose(spec.get("title"), "title", 220)
    if "\n" in title:
        raise ResearchError("issue title must be one line")
    labels = spec.get("labels", [])
    if not isinstance(labels, list) or len(labels) > 6 or any(label not in LABELS for label in labels):
        raise ResearchError("issue labels outside allowlist")
    # Readiness is a trusted registry transition, never a generated label.
    if set(labels) & {"ready-for-code", "validated-paper"}:
        raise ResearchError("worker cannot assert coding readiness or verified economics")
    sections = spec.get("sections")
    if not isinstance(sections, dict) or set(sections) != set(REQUIRED_SECTIONS):
        raise ResearchError("issue must contain the complete actionable technical contract")
    for name in REQUIRED_SECTIONS:
        safe_prose(sections[name], name)
    scope = spec.get("scope")
    if not isinstance(scope, dict) or set(scope) != {"account_id", "lane", "component"}:
        raise ResearchError("exact account/lane/component issue scope required")
    for name, value in scope.items():
        safe_prose(value, name, 256)
    for name in ("failure_family", "version_epoch", "mechanism_id"):
        safe_prose(spec.get(name), name, 256)
    evidence = spec.get("evidence")
    if not isinstance(evidence, list) or not 1 <= len(evidence) <= 32:
        raise ResearchError("bounded immutable issue evidence required")
    for reference in evidence:
        if not isinstance(reference, dict) or set(reference) != {"bundle_id", "artifact", "sha256"}:
            raise ResearchError("evidence reference must bind manifest and member hash")
        sha(reference["bundle_id"], "evidence bundle id")
        sha(reference["sha256"], "evidence member hash")
        if reference["bundle_id"] not in verified_bundle_ids:
            raise ResearchError("issue references an unverified input bundle")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}", reference["artifact"]):
            raise ResearchError("issue evidence path must be safe bundle member")
        manifest = verified_bundle_ids[reference["bundle_id"]]
        if not any(entry["path"] == reference["artifact"] and entry["sha256"] == reference["sha256"] for entry in manifest["files"]):
            raise ResearchError("issue evidence hash/member absent from verified manifest")
    if spec.get("experiment_id") is not None:
        sha(spec["experiment_id"], "experiment id")
    deps = spec.get("dependencies", [])
    if not isinstance(deps, list) or len(deps) > 32 or any(type(x) is not int or not 1 <= x <= 1_000_000 for x in deps):
        raise ResearchError("issue dependencies must be finite repository issue numbers")
    if len(canonical(spec)) > MAX_ISSUE:
        raise ResearchError("issue contract byte quota exceeded")
    fingerprint = digest({"scope": scope, "failure_family": spec["failure_family"],
                          "version_epoch": spec["version_epoch"], "mechanism_id": spec["mechanism_id"]})
    return fingerprint


def render_issue(spec, fingerprint):
    marker = f"mm2-research:{sha(fingerprint)}"
    body = [f"<!-- {marker} -->", "Research evidence and technical proposal. Financial authorization remains separate."]
    for name in REQUIRED_SECTIONS:
        body += [f"### {name.replace('_', ' ').capitalize()}", spec["sections"][name].strip()]
    body += ["### Immutable evidence", "```json", canonical(spec["evidence"]).decode(), "```",
             f"Policy: `{spec['policy_hash']}`", f"Scope: `{canonical(spec['scope']).decode()}`",
             f"Experiment: `{spec.get('experiment_id') or 'not preregistered; measurement first'}`",
             f"Dependencies: {', '.join('#' + str(n) for n in spec.get('dependencies', [])) or 'none'}"]
    request = {"title": spec["title"].strip(), "body": "\n\n".join(body), "labels": list(dict.fromkeys(["research"] + spec.get("labels", [])))}
    if len(canonical(request)) > MAX_ISSUE:
        raise ResearchError("rendered issue exceeds GitHub body quota")
    return request


def remote_request(issue):
    if not isinstance(issue, dict) or not isinstance(issue.get("labels"), list):
        raise ResearchError("invalid remote issue contract")
    labels = [x.get("name") if isinstance(x, dict) else x for x in issue["labels"]]
    # Compare exact requested labels independent of API's label sort order.
    return {"title": issue.get("title"), "body": issue.get("body"), "labels": sorted(labels)}


def request_hash(request):
    return digest({**request, "labels": sorted(request["labels"])})


class GhClient:
    """Uses gh credentials only inside the trusted broker. No arbitrary endpoints."""
    def __init__(self, repo=DEFAULT_REPOSITORY, executable="gh", timeout=30):
        self.repo = repository(repo)
        self.executable = executable
        self.timeout = timeout

    def _call(self, endpoint, method="GET", body=None):
        args = [self.executable, "api", "--hostname", "github.com", "-H", "Accept: application/vnd.github+json",
                "-H", "X-GitHub-Api-Version: 2022-11-28", "--method", method, endpoint]
        payload = None
        if body is not None:
            args += ["--input", "-"]
            payload = canonical(body)
        try:
            response = subprocess.run(args, input=payload, capture_output=True, timeout=self.timeout, check=False)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ResearchError("GitHub transport unavailable or response unknown") from exc
        if response.returncode or len(response.stdout) > MAX_REMOTE_BYTES:
            # Never expose gh stderr, bearer credentials, URLs or raw API bodies.
            raise ResearchError("GitHub request rejected or finite response bound exceeded")
        try:
            return json.loads(response.stdout)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ResearchError("GitHub response malformed or unknown") from exc

    def all_issues(self):
        result = []
        for page in range(1, 21):
            batch = self._call(f"repos/{self.repo}/issues?state=all&per_page=100&page={page}")
            if not isinstance(batch, list) or len(batch) > 100:
                raise ResearchError("GitHub issue listing contract drift")
            result.extend(item for item in batch if "pull_request" not in item)
            if len(batch) < 100:
                return result
        raise ResearchError("GitHub reconciliation pagination limit reached; absence unproven")

    def create_issue(self, request):
        return self._call(f"repos/{self.repo}/issues", "POST", request)

    def get_issue(self, number):
        if type(number) is not int or not 1 <= number <= 1_000_000:
            raise ResearchError("invalid remote issue number")
        return self._call(f"repos/{self.repo}/issues/{number}")
