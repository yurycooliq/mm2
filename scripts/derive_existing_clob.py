#!/usr/bin/env python3
"""Retrieve existing owner CLOB credentials through the pinned GET-only utility; never create keys."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import urllib.request
import uuid

from deploy_host import private_environment
from deploy_package import image_identity, verify_release
from provision_private_runtime import NoRedirect, apply_update, decode_payload, expected_signer, unique_fields


class DeriveError(Exception):
    """Fixed public failure classes only; never show transport or credential values."""


class SafeParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_usage(sys.stderr)
        self.exit(2, "Invalid credential setup arguments. Only nonce 0 is supported; secret arguments are refused.\n")


def owner_state(token):
    request = urllib.request.Request("http://127.0.0.1:8088/v1/state", headers={"Authorization": "Bearer " + token})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        with opener.open(request, timeout=3) as response:
            data = response.read(2_097_153)
            if response.status != 200 or len(data) > 2_097_152:
                raise ValueError("invalid response")
        result = json.loads(data, object_pairs_hook=unique_fields)
        if (not isinstance(result, dict) or result.get("mode") != "paused" or result.get("live") is not None
                or result.get("live_eligible") is not False or result.get("api_ready") is not True):
            raise ValueError("unexpected state")
        boot = result.get("boot_id")
        counters = tuple(result.get(name) for name in ("command_version", "reset_epoch"))
        if not isinstance(boot, str) or uuid.UUID(boot).int == 0 or any(type(value) is not int or value < 0 for value in counters):
            raise ValueError("invalid control identity")
        return (boot, *counters)
    except Exception:
        raise DeriveError("paused_owner_state_required") from None


def run_auth_worker(image, payload):
    """Private stdin/stdout only, with owned cleanup even on a disconnected Docker client."""
    owner = uuid.uuid4().hex
    name = "mm2-clob-derive-" + owner[:16]
    command = ["docker", "run", "--rm", "--name", name, "--label", "mm2.clob-derive=" + owner,
               "--interactive", "--log-driver", "none", "--no-healthcheck",
               "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
               "--memory", "512m", "--cpus", "1", "--pids-limit", "64",
               "--tmpfs", "/tmp:rw,noexec,nosuid,size=16m", "--entrypoint", "mm2-credential-auth", image]
    # A remote Docker context must never receive this private stdin. Use the local
    # Linux daemon and only the executable search path; no credential inheritance.
    environment = {"PATH": os.defpath, "DOCKER_HOST": "unix:///var/run/docker.sock"}
    try:
        reply = subprocess.run(command, input=json.dumps(payload).encode("ascii"), stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, env=environment, timeout=35, check=False)
    finally:
        # A disconnected Docker client must not leave an authentication worker behind.
        identity = subprocess.run(["docker", "container", "inspect", "--format",
                                   '{{index .Config.Labels "mm2.clob-derive"}}', name],
                                  stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=environment, timeout=10, check=False)
        if identity.returncode == 0:
            if identity.stdout.decode("ascii").strip() != owner:
                raise DeriveError("temporary_worker_owner_mismatch")
            cleaned = subprocess.run(["docker", "container", "rm", "--force", name],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=environment, timeout=10, check=False)
            if cleaned.returncode != 0:
                raise DeriveError("temporary_worker_cleanup_failed")
    if reply.returncode != 0:
        # Utility diagnostics are deliberately not forwarded: third-party messages may contain secrets.
        public_class = reply.stderr.strip()
        known = {b"clob_existing_credentials_absent", b"clob_auth_rejected", b"clob_existing_derivation_rejected"}
        if public_class in known:
            raise DeriveError(public_class.decode("ascii"))
        raise DeriveError("existing_clob_derivation_failed_no_creation")
    result = decode_payload(reply.stdout)
    if result["kind"] != "clob":
        raise DeriveError("unexpected_credential_response")
    return result


def derive(nonce):
    if sys.platform != "linux" or os.geteuid() != 0:
        raise DeriveError("linux_root_required")
    current = Path("/opt/mm2/current").resolve(strict=True)
    if current.parent != Path("/opt/mm2/releases") or not re.fullmatch(r"[0-9a-f]{40}", current.name):
        raise DeriveError("reviewed_release_required")
    manifest = verify_release(current)
    image = manifest["images"]["engine"]
    image_identity(image, expected_revision=manifest["source_commit"])
    values = private_environment(Path("/etc/mm2/runtime.env"))
    key = values.get("MM2_POLY_PRIVATE_KEY")
    if not key:
        raise DeriveError("owner_wallet_key_missing")
    if any(values.get(name) for name in ("MM2_POLY_API_KEY", "MM2_POLY_API_SECRET", "MM2_POLY_API_PASSPHRASE")):
        raise DeriveError("existing_local_clob_settings_preserved")
    signer = expected_signer(current / "config/trading.toml")
    before = owner_state(values["MM2_ENGINE_API_TOKEN"])
    payload = {"private_key": key, "signer_address": signer, "nonce": nonce}
    # No secret argv/environment/files or persistent Docker stdout logs.
    result = run_auth_worker(image, payload)
    if current != Path("/opt/mm2/current").resolve(strict=True) or before != owner_state(values["MM2_ENGINE_API_TOKEN"]):
        raise DeriveError("owner_control_changed_credentials_not_stored")
    if values != private_environment(Path("/etc/mm2/runtime.env")):
        raise DeriveError("runtime_changed_credentials_not_stored")
    stored = apply_update(result, expected_environment=values)
    if stored != {"kind": "clob", "activation": "unchanged", "restart_required": "none; funded configuration remains a separate gate"}:
        raise DeriveError("unexpected_storage_result")
    print(json.dumps({"ok": True, "kind": "clob", "source": "existing_credentials_GET_only",
                      "signer_address": signer, "nonce": nonce, "activation": "unchanged",
                      "restart_required": False, "new_api_key_created": False}))


def main(argv=None):
    parser = SafeParser(description=__doc__)
    parser.add_argument("--nonce", type=int, default=0)
    args = parser.parse_args(argv)
    if args.nonce != 0:
        print("CLOB derivation refused: invalid_nonce", file=sys.stderr)
        return 1
    try:
        derive(args.nonce)
        return 0
    except DeriveError as error:
        print("CLOB derivation refused: " + str(error), file=sys.stderr)
        return 1
    except Exception:
        print("CLOB derivation refused: safe_setup_or_transport_failed", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
