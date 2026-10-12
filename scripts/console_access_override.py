#!/usr/bin/env python3
"""Reconcile a pinned console-only access overlay; never restart or command the engine."""
try:
    import fcntl
except ImportError:  # Pure validation/command-construction tests also run on Windows.
    fcntl = None
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time

PROJECT = "mm2-production"
SETTINGS = Path("/etc/mm2/console-access.json")
IMAGE = re.compile(r"sha256:[0-9a-f]{64}")
COMMIT = re.compile(r"[0-9a-f]{40}")
ROLES = {"engine", "console", "proxy"}
GUARD_LABEL = "mm2.console-access.guard"
GUARD_NAME = "mm2-console-access-update-guard"


def checked_settings(value):
    fields = {"schema", "base_commit", "console_commit", "base_manifest_sha256", "profile_sha256",
              "engine_image", "old_console_image", "console_image", "proxy_image", "owner_id", "additional_ids"}
    if not isinstance(value, dict) or set(value) != fields or value["schema"] != 1:
        raise ValueError("invalid console access settings")
    if any(not isinstance(value[key], str) or not COMMIT.fullmatch(value[key]) for key in ("base_commit", "console_commit")):
        raise ValueError("invalid source binding")
    if any(not isinstance(value[key], str) or not IMAGE.fullmatch(value[key]) for key in ("engine_image", "old_console_image", "console_image", "proxy_image")):
        raise ValueError("invalid immutable image binding")
    if any(not isinstance(value[key], str) or not re.fullmatch(r"[0-9a-f]{64}", value[key]) for key in ("base_manifest_sha256", "profile_sha256")):
        raise ValueError("invalid public file binding")
    ids = value["additional_ids"]
    if not isinstance(ids, list) or not 1 <= len(ids) <= 16:
        raise ValueError("invalid additional IDs")
    if any(type(user_id) is not int or not 0 < user_id < 2**52 for user_id in [value["owner_id"], *ids]):
        raise ValueError("invalid Telegram user ID")
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate Telegram user ID")
    return value


def private_file(path, bound):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1 or info.st_size > bound:
        raise ValueError("private configuration identity rejected")
    return path.read_bytes()


def call(args, timeout=10, **kwargs):
    reply = subprocess.run(args, check=True, capture_output=True, text=True, timeout=timeout, **kwargs)
    if len(reply.stdout) > 65536:
        raise ValueError("public helper response exceeds bound")
    return reply.stdout.strip()


def inventory(run=call):
    rows = run(["docker", "ps", "--all", "--no-trunc", "--filter", "label=com.docker.compose.project=" + PROJECT,
                "--filter", "label=com.docker.compose.oneoff=False", "--format", '{{.ID}} {{.Label "com.docker.compose.service"}}'])
    result = {}
    for row in rows.splitlines():
        cid, role = row.split()
        if role not in ROLES or not re.fullmatch(r"[0-9a-f]{64}", cid):
            raise ValueError("ambiguous production inventory")
        identity = json.loads(run(["docker", "inspect", "--format",
            '{"id":{{json .Id}},"image":{{json .Image}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}},"name":{{json .Name}},"guard":{{json (index .Config.Labels "mm2.console-access.guard")}},"compose_hash":{{json (index .Config.Labels "com.docker.compose.config-hash")}}}', cid]))
        if identity.get("guard"):
            if (role != "console" or identity.get("name") != "/" + GUARD_NAME or identity["running"]
                    or not COMMIT.fullmatch(identity["guard"]) or identity.get("compose_hash")):
                raise ValueError("unexpected update guard in inventory")
            continue
        if role in result:
            raise ValueError("ambiguous production inventory")
        result[role] = identity
    if set(result) != ROLES:
        raise ValueError("incomplete production inventory")
    return result


def console_access(cid, run=call):
    # Only public integer IDs leave this subprocess. No environment/token dump.
    return json.loads(run(["docker", "exec", cid, "python", "-B", "-c",
        "import json; from mm2_console.config import Config; c=Config.from_env(); "
        "print(json.dumps({'owner_id':c.telegram_owner,'users':sorted(getattr(c,'telegram_users',{c.telegram_owner}))}))"]))


def cleanup_guard(settings, run=call):
    ids = run(["docker", "ps", "--all", "--no-trunc", "--filter", "label=" + GUARD_LABEL,
               "--format", "{{.ID}}"])
    if not ids:
        return
    if not re.fullmatch(r"[0-9a-f]{64}", ids):
        raise ValueError("ambiguous console update guard")
    value = json.loads(run(["docker", "inspect", "--format",
        '{"name":{{json .Name}},"image":{{json .Image}},"running":{{json .State.Running}},"labels":{{json .Config.Labels}}}', ids]))
    labels = value["labels"]
    if (value["name"] != "/" + GUARD_NAME or value["image"] != settings["console_image"] or value["running"]
            or labels.get(GUARD_LABEL) != settings["console_commit"]
            or labels.get("com.docker.compose.project") != PROJECT
            or labels.get("com.docker.compose.service") != "console"
            or labels.get("com.docker.compose.oneoff") != "False"
            or "com.docker.compose.config-hash" in labels):
        raise ValueError("console update guard identity rejected")
    run(["docker", "container", "rm", ids])


@contextmanager
def update_guard(settings, run=call, sleep=time.sleep):
    # The base supervisor degrades duplicate secondaries, without suspending
    # engine supervision. Compose lists only containers carrying config-hash;
    # this never-started guard deliberately lacks that label. It has no mounts.
    cid = run(["docker", "create", "--name", GUARD_NAME, "--network", "none", "--read-only",
               "--label", "com.docker.compose.project=" + PROJECT, "--label", "com.docker.compose.service=console",
               "--label", "com.docker.compose.oneoff=False", "--label", GUARD_LABEL + "=" + settings["console_commit"],
               "--entrypoint", "/bin/true", settings["console_image"]])
    if not re.fullmatch(r"[0-9a-f]{64}", cid):
        raise ValueError("invalid console update guard ID")
    try:
        # The original supervisor polls once/second; allow two full polls to
        # observe degradation and cancel any already pending recovery helper.
        sleep(2.5)
        yield
    finally:
        # No force: the guard must never have started or acquired execution.
        run(["docker", "container", "rm", cid])


def reconcile(settings, release, runtime_path, overlay_path, run=call, collect=inventory, access=console_access,
              guard=update_guard):
    settings = checked_settings(settings)
    cleanup_guard(settings, run)
    if release.resolve(strict=True).name != settings["base_commit"]:
        # A later complete deployment owns its own access configuration.
        return {"applicable": False, "reason": "base_release_changed", "engine_touched": False}
    if hashlib.sha256((release / "release-manifest.json").read_bytes()).hexdigest() != settings["base_manifest_sha256"]:
        raise ValueError("base release manifest changed")
    profile = Path("/etc/mm2/active.toml")
    if hashlib.sha256(profile.read_bytes()).hexdigest() != settings["profile_sha256"]:
        return {"applicable": False, "reason": "profile_changed", "engine_touched": False}
    runtime = private_file(runtime_path, 16384)
    values = dict(line.split("=", 1) for line in runtime.decode("ascii").splitlines() if line and not line.startswith("#"))
    if values.get("MM2_TELEGRAM_OWNER_ID") != str(settings["owner_id"]) or not values.get("MM2_TELEGRAM_BOT_TOKEN"):
        raise ValueError("primary Telegram owner changed")
    before = collect(run)
    if (not before["engine"]["running"] or before["engine"]["image"] != settings["engine_image"]
            or before["proxy"]["image"] != settings["proxy_image"]):
        return {"applicable": False, "reason": "primary_identity_changed", "engine_touched": False}
    if before["console"]["image"] not in {settings["old_console_image"], settings["console_image"]}:
        return {"applicable": False, "reason": "console_release_changed", "engine_touched": False}
    desired = {"owner_id": settings["owner_id"], "users": sorted({settings["owner_id"], *settings["additional_ids"]})}
    if before["console"]["running"] and before["console"]["image"] == settings["console_image"] and access(before["console"]["id"], run) == desired:
        return {"applicable": True, "changed": False, "engine_touched": False, "access": desired}
    label = run(["docker", "image", "inspect", "--format", '{{index .Config.Labels "org.opencontainers.image.revision"}}', settings["console_image"]])
    if label != settings["console_commit"]:
        raise ValueError("console image source differs")
    overlay = {"services": {"console": {"image": settings["console_image"], "environment": {
        "MM2_TELEGRAM_ALLOWED_IDS": ",".join(map(str, settings["additional_ids"]))}}}}
    encoded = (json.dumps(overlay, sort_keys=True) + "\n").encode("ascii")
    if private_file(overlay_path, 4096) != encoded:
        raise ValueError("console overlay changed")
    environment = {key: value for key, value in os.environ.items() if not key.startswith("MM2_")}
    environment.update({"MM2_ENGINE_IMAGE": settings["engine_image"], "MM2_CONSOLE_IMAGE": settings["console_image"], "MM2_PROXY_IMAGE": settings["proxy_image"]})
    with guard(settings, run):
        run(["docker", "compose", "--project-name", PROJECT, "--project-directory", str(release),
             "--env-file", str(runtime_path), "-f", "infra/aws/compose.yaml", "-f", str(overlay_path),
             "up", "--detach", "--no-deps", "--no-build", "--pull", "never", "console"], timeout=35, cwd=release, env=environment)
        after = collect(run)
        if after["engine"] != before["engine"] or after["proxy"] != before["proxy"] or private_file(runtime_path, 16384) != runtime:
            raise ValueError("primary identity or runtime changed during console update")
        if after["console"]["image"] != settings["console_image"] or not after["console"]["running"] or access(after["console"]["id"], run) != desired:
            raise ValueError("console access did not match requested IDs")
    return {"applicable": True, "changed": True, "engine_touched": False, "runtime_unchanged": True,
            "before": before, "after": after, "access": desired, "console_commit": settings["console_commit"]}


def main():
    if fcntl is None or os.geteuid() != 0:
        raise ValueError("root required")
    settings = checked_settings(json.loads(private_file(SETTINGS, 4096)))
    descriptor = os.open("/etc/mm2/.console-access.lock", os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    info = os.fstat(descriptor)
    if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1:
        os.close(descriptor)
        raise ValueError("console access lock rejected")
    with os.fdopen(descriptor, "a", encoding="ascii") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = reconcile(settings, Path("/opt/mm2/current"), Path("/etc/mm2/runtime.env"), Path("/etc/mm2/console-access.compose.json"))
    if result.get("changed") or not result.get("applicable"):
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.SubprocessError):
        print("Console access reconciliation deferred; no engine command or restart was requested.", file=sys.stderr)
        raise SystemExit(1)
