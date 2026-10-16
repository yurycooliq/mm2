#!/usr/bin/env python3
"""Read-only PR inventory and exact-revision merge preflight. Never merges or deletes."""
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import subprocess

REPOSITORY = "yurycooliq/mm2"
REQUIRED_CHECKS = {"rust", "python", "strategy-parity", "frontend", "containers"}
HOLD_LABELS = {"experimental", "merge-hold"}
MAX_STACK_DEPTH = 2  # At most three open PRs including the root.


class PolicyError(ValueError):
    pass


def timestamp(value):
    if not isinstance(value, str):
        raise PolicyError("missing observation timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise PolicyError("invalid observation timestamp") from None
    if parsed.tzinfo is None:
        raise PolicyError("observation timestamp requires a timezone")
    return parsed.astimezone(timezone.utc)


def gh_json(arguments):
    result = subprocess.run(["gh", *arguments], capture_output=True, timeout=60, check=False)
    if result.returncode or len(result.stdout) > 4 * 1024 * 1024:
        raise PolicyError("GitHub read failed or exceeded its output bound")
    try:
        return json.loads(result.stdout)
    except (ValueError, UnicodeError):
        raise PolicyError("GitHub read returned invalid JSON") from None


def labels(pr):
    values = pr.get("labels")
    if not isinstance(values, list) or any(not isinstance(item, dict) or not isinstance(item.get("name"), str) for item in values):
        raise PolicyError("missing PR label evidence")
    return {item["name"] for item in values}


def inventory(prs, main_at, now):
    """Evaluate the complete open-PR graph; absent evidence is never zero-filled."""
    if not isinstance(prs, list) or len(prs) > 500:
        raise PolicyError("open PR inventory is unavailable or exceeds its bound")
    issues, numbers, branches = [], set(), {}
    def issue(pr, code, message, severity="error"):
        issues.append({"pr": pr, "code": code, "severity": severity, "message": message})
    for pr in prs:
        if (not isinstance(pr, dict) or type(pr.get("number")) is not int or pr["number"] <= 0
                or type(pr.get("draft")) is not bool or pr.get("state") != "open"):
            raise PolicyError("incomplete open PR identity")
        if pr["number"] in numbers:
            raise PolicyError("duplicate PR identity")
        numbers.add(pr["number"])
        try:
            head, base = pr["head"]["ref"], pr["base"]["ref"]
        except (KeyError, TypeError):
            raise PolicyError("missing PR branch identity") from None
        if any(not isinstance(ref, str) or not ref or len(ref) > 255 or any(ord(c) < 32 for c in ref) for ref in (head, base)):
            raise PolicyError("invalid PR branch identity")
        if head in branches:
            raise PolicyError("ambiguous PR head branch")
        branches[head] = pr
        names = labels(pr)
        age = now - timestamp(pr["updated_at"])
        timestamp(pr["created_at"])
        if names & HOLD_LABELS and not pr["draft"]:
            issue(pr["number"], "hold_is_ready", "Experimental or held work must remain Draft.")
        if base != "main" and not pr["draft"]:
            issue(pr["number"], "ready_on_dependency", "Merge prerequisites and retarget this ready PR to main first.")
        if not pr["draft"] and age > timedelta(days=7):
            issue(pr["number"], "stale_ready", "Ready PR has had no update for over seven days; review its disposition.")
        if pr["draft"] and age > timedelta(days=14):
            issue(pr["number"], "stale_draft", "Draft has had no update for over fourteen days; record the blocker or close it.", "warning")
    for pr in prs:
        visited, current, depth = set(), pr, 0
        while current["base"]["ref"] != "main":
            if current["number"] in visited:
                issue(pr["number"], "dependency_cycle", "Open PR dependencies contain a cycle.")
                break
            visited.add(current["number"])
            parent = branches.get(current["base"]["ref"])
            if parent is None:
                issue(pr["number"], "missing_dependency", "Base has no open prerequisite PR; retarget a merged prerequisite to main.")
                break
            current, depth = parent, depth + 1
        if depth > MAX_STACK_DEPTH:
            issue(pr["number"], "deep_stack", "Stack exceeds three open PRs; integrate the ready prerequisite before extending it.")
    if prs and now - timestamp(main_at) > timedelta(days=7):
        issue(None, "stale_main", "main has not advanced in over seven days while open PRs exist.")
    return issues


def independent_review(review, head, base, author):
    if review.get("commit", {}).get("oid") != head:
        return False
    # Single-owner repositories can record a separate agent's review using the
    # owner's account. This is a named agent receipt, never a human approval.
    if review.get("state") not in {"COMMENTED", "APPROVED"}:
        return False
    body = review.get("body", "")
    if not isinstance(body, str) or len(body) > 16_384:
        return False
    fields = {}
    for line in body.splitlines():
        if line.startswith(("Head: ", "Base: ", "Reviewer: ", "Result: ")):
            key, value = line.split(": ", 1)
            if key in fields:
                return False
            fields[key] = value
    binding = (body.startswith("MM2 independent review\n") and fields.get("Head") == head
               and fields.get("Base") == base and fields.get("Result") == "PASS")
    if review.get("state") == "APPROVED":
        return binding and review.get("author", {}).get("login") not in {None, author}
    return binding and bool(re.fullmatch(r"agent:[A-Za-z0-9_/-]{1,128}", fields.get("Reviewer", "")))


def readiness(pr, main_sha):
    errors = []
    head, base = pr.get("headRefOid"), pr.get("baseRefOid")
    if not all(isinstance(sha, str) and re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (head, base, main_sha)):
        raise PolicyError("missing exact revision evidence")
    if pr.get("state") != "OPEN" or pr.get("isDraft") is not False:
        errors.append("PR is not open and ready.")
    if pr.get("baseRefName") != "main" or base != main_sha:
        errors.append("PR must target the observed current main revision.")
    if labels(pr) & HOLD_LABELS:
        errors.append("Experimental or merge-hold label blocks integration.")
    checks = pr.get("statusCheckRollup")
    if not isinstance(checks, list):
        raise PolicyError("missing current-head check evidence")
    passed = {check.get("name") for check in checks if check.get("status") == "COMPLETED" and check.get("conclusion") == "SUCCESS"}
    if REQUIRED_CHECKS - passed:
        errors.append("Missing successful required checks: " + ", ".join(sorted(REQUIRED_CHECKS - passed)))
    if any((check.get("status") != "COMPLETED" or check.get("conclusion") != "SUCCESS")
           if "conclusion" in check else check.get("state") != "SUCCESS" for check in checks):
        errors.append("A reported check is pending, failed, skipped or otherwise not successful.")
    reviews = pr.get("reviews")
    decisions = {}
    if isinstance(reviews, list):
        for review in sorted((row for row in reviews if row.get("state") in {"APPROVED", "CHANGES_REQUESTED"}),
                             key=lambda row: timestamp(row["submittedAt"])):
            decisions[review.get("author", {}).get("login")] = review["state"]
    if "CHANGES_REQUESTED" in decisions.values():
        errors.append("A contributor still requests changes.")
    if not isinstance(reviews, list) or not any(independent_review(review, head, base, pr.get("author", {}).get("login")) for review in reviews):
        errors.append("No independent PASS review is bound to this exact head and base.")
    return {"status": "HOLD" if errors else "READY", "pr": pr["number"], "head": head, "base": base,
            "errors": errors, "mutations_performed": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    audit = commands.add_parser("audit")
    audit.add_argument("--event", type=Path, help="Optional GitHub event; PR checks focus on that PR so stale-main alerts cannot block their own remedy")
    ready = commands.add_parser("ready")
    ready.add_argument("--pr", required=True, type=int)
    args = parser.parse_args(argv)
    try:
        if args.command == "ready":
            if not 1 <= args.pr <= 1_000_000:
                raise PolicyError("invalid PR number")
            pr = gh_json(["pr", "view", str(args.pr), "--repo", REPOSITORY, "--json",
                          "number,state,isDraft,baseRefName,baseRefOid,headRefOid,labels,statusCheckRollup,reviews,author"])
            base = gh_json(["api", f"repos/{REPOSITORY}/git/ref/heads/main"])["object"]["sha"]
            result = readiness(pr, base)
        else:
            focus = None
            if args.event:
                if args.event.stat().st_size > 1_048_576:
                    raise PolicyError("event exceeds its input bound")
                event = json.loads(args.event.read_text(encoding="utf-8"))
                if "pull_request" in event:
                    focus = event["pull_request"]["number"]
                    if type(focus) is not int or focus <= 0:
                        raise PolicyError("invalid event PR identity")
            pages = gh_json(["api", "--paginate", "--slurp", f"repos/{REPOSITORY}/pulls?state=open&per_page=100"])
            prs = [pr for page in pages for pr in page]
            main_at = gh_json(["api", f"repos/{REPOSITORY}/commits/main"])["commit"]["committer"]["date"]
            issues = inventory(prs, main_at, datetime.now(timezone.utc))
            if focus is not None:
                issues = [dict(item, severity="warning") if item["pr"] is None else item
                          for item in issues if item["pr"] in {None, focus}]
            result = {"status": "HOLD" if any(item["severity"] == "error" for item in issues) else "PASS",
                      "open_prs": len(prs), "main_commit_at": main_at, "issues": issues, "mutations_performed": False}
        print(json.dumps(result, indent=2))
        return 1 if result["status"] == "HOLD" else 0
    except (PolicyError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
        print(json.dumps({"status": "UNKNOWN", "error": "PR policy evidence unavailable or invalid; inspect read-only GitHub access", "mutations_performed": False}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
