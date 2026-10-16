#!/usr/bin/env python3
"""Read-only AWS access preflight: fixed classifications, never logs credentials."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import re
import subprocess


def classify_failure(text):
    text = text.lower()
    if any(item in text for item in ("session has expired", "expiredtoken", "token has expired", "reauthenticate")):
        return "EXPIRED"
    if any(item in text for item in ("could not be found", "unable to locate credentials", "no credentials")):
        return "UNCONFIGURED"
    if any(item in text for item in ("accessdenied", "not authorized", "unauthorized")):
        return "DENIED"
    if any(item in text for item in ("could not connect", "endpointconnection", "timed out", "timeout", "name resolution")):
        return "UNREACHABLE"
    return "AUTH_ERROR"


def check_access(profile, region, *, aws="aws", run=subprocess.run, now=None):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", profile) or not re.fullmatch(r"[a-z]{2}(?:-[a-z]+)+-[0-9]", region):
        raise ValueError("invalid profile or region")
    result = {"schema_version": 1, "profile": profile, "region": region,
              "captured_at_utc": now or datetime.now(timezone.utc).isoformat(),
              "scope": "authentication_only_not_engine_health", "primary_mutations": False,
              "credential_expiry_utc": None, "expiry_reason": "PRIVATE_LOGIN_CACHE_IS_NOT_READ"}
    try:
        completed = run([aws, "sts", "get-caller-identity", "--profile", profile, "--region", region,
                         "--output", "json", "--no-cli-pager"], capture_output=True, text=True, timeout=20, check=False)
    except subprocess.TimeoutExpired:
        result["status"] = "UNREACHABLE"
    except OSError:
        result["status"] = "CLI_UNAVAILABLE"
    else:
        if completed.returncode:
            result["status"] = classify_failure(completed.stderr)
        else:
            try:
                identity = json.loads(completed.stdout)
                arn = identity["Arn"]
                if not isinstance(arn, str) or not re.fullmatch(r"arn:aws(?:-[a-z]+)?:[a-z]+::[0-9]{12}:[A-Za-z0-9_+=,.@:/-]+", arn):
                    raise ValueError("unexpected identity")
                result.update(status="AUTHENTICATED", identity_arn=arn, root_identity=arn.endswith(":root"))
            except (ValueError, KeyError, TypeError):
                result["status"] = "INVALID_IDENTITY_RESPONSE"
    # No time-dependent fingerprint: unchanged errors do not create new alerts.
    result["fingerprint"] = hashlib.sha256(json.dumps({key: result.get(key) for key in
                                                       ("profile", "region", "status", "identity_arn")}, sort_keys=True).encode()).hexdigest()
    result["continuous_worker_readiness"] = "UNVERIFIED; renewal, revocation and worker availability need separate checks"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--region", default="eu-west-1")
    parser.add_argument("--aws", default="aws")
    args = parser.parse_args()
    result = check_access(args.profile, args.region, aws=args.aws)
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "AUTHENTICATED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
