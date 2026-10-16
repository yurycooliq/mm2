#!/usr/bin/env python3
"""Verify, import and select mm2 releases on a prepared Linux host; never resume trading."""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
import subprocess
import sys
import tomllib
import uuid

from deploy_package import image_identity, verify_release
from start import preflight_trading

CONFIG_ROOT = Path("/etc/mm2")
STATE_ROOT = Path("/srv/mm2")
INSTALL_ROOT = Path("/opt/mm2")
PRIVATE_KEYS = {"MM2_ENGINE_API_TOKEN", "MM2_CONSOLE_TOKEN", "MM2_DOMAIN", "MM2_POLY_PRIVATE_KEY", "MM2_POLY_API_KEY",
                "MM2_POLY_API_SECRET", "MM2_POLY_API_PASSPHRASE", "MM2_POLYGON_RPC_URL",
                "MM2_POLY_RELAYER_API_KEY", "MM2_POLY_RELAYER_API_KEY_ADDRESS",
                "MM2_POLY_BUILDER_API_KEY", "MM2_POLY_BUILDER_API_SECRET", "MM2_POLY_BUILDER_API_PASSPHRASE",
                "MM2_TELEGRAM_BOT_TOKEN", "MM2_TELEGRAM_OWNER_ID", "MM2_TELEGRAM_ALLOWED_IDS"}
DOMAIN = re.compile(r"(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}")
MODES = {"simulation": "simulation", "trading": "trading", "both": "both", "shadow": "public", "demo": "demo"}
BUILDER_KEYS = ("MM2_POLY_BUILDER_API_KEY", "MM2_POLY_BUILDER_API_SECRET", "MM2_POLY_BUILDER_API_PASSPHRASE")
RELAYER_KEYS = ("MM2_POLY_RELAYER_API_KEY", "MM2_POLY_RELAYER_API_KEY_ADDRESS")


def relayer_authentication(values, required=False, signer=None):
    """One complete runtime scheme; credentials never appear in diagnostic text."""
    owner = [values.get(key, "") for key in RELAYER_KEYS]
    builder = [values.get(key, "") for key in BUILDER_KEYS]
    if ((any(owner) and not all(owner)) or (any(builder) and not all(builder))
            or (any(owner) and any(builder)) or (required and not any(owner + builder))):
        raise ValueError("Relayer requires exactly one complete owner pair or Builder triplet")
    if all(owner):
        if (len(owner[0]) > 1024 or any(not 33 <= ord(char) <= 126 or char in "$'\"\\`" for char in owner[0])
                or not re.fullmatch(r"0x[0-9a-fA-F]{40}", owner[1]) or int(owner[1][2:], 16) == 0):
            raise ValueError("Relayer owner credentials exceed their supported literal bounds")
        if signer is not None and owner[1].lower() != signer.lower():
            raise ValueError("Relayer API key owner differs from the configured controlling signer")
        return "owner"
    if all(builder):
        try:
            key, secret, passphrase = builder
            if len(key) != 36 or str(uuid.UUID(key)) != key.lower() or uuid.UUID(key).int == 0:
                raise ValueError("invalid UUID")
            if not re.fullmatch(r"[A-Za-z0-9_-]{16,512}={0,2}", secret):
                raise ValueError("invalid base64")
            decoded = base64.b64decode(secret + "=" * (-len(secret) % 4), altchars=b"-_", validate=True)
            canonical = base64.urlsafe_b64encode(decoded).decode("ascii")
            if not 16 <= len(decoded) <= 256 or secret not in (canonical, canonical.rstrip("=")):
                raise ValueError("noncanonical or invalid HMAC key")
            if not 1 <= len(passphrase) <= 256 or any(not 33 <= ord(char) <= 126 or char in "$'\"\\`" for char in passphrase):
                raise ValueError("invalid passphrase")
        except (ValueError, TypeError):
            raise ValueError("Builder credentials require a UUID, canonical URL-safe HMAC secret and bounded literal passphrase") from None
        return "builder"
    return None


def private_environment(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > 16_384:
        raise ValueError("runtime environment must be a root-owned regular mode-0600 file within 16 KiB")
    result = {}
    for line in path.read_text(encoding="ascii").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or key not in PRIVATE_KEYS or key in result or any(char in value for char in "\x00\r\n$'\"\\ `"):
            raise ValueError("runtime environment accepts only documented literal keys/values, without interpolation")
        result[key] = value
    if not DOMAIN.fullmatch(result.get("MM2_DOMAIN", "")):
        raise ValueError("set one exact public ASCII HTTPS domain")
    tokens = [result.get(key, "") for key in ("MM2_ENGINE_API_TOKEN", "MM2_CONSOLE_TOKEN")]
    if any(not re.fullmatch(r"[A-Za-z0-9_-]{32,256}", token) for token in tokens) or tokens[0] == tokens[1]:
        raise ValueError("independent private owner and engine tokens are required")
    if bool(result.get("MM2_TELEGRAM_BOT_TOKEN")) != bool(result.get("MM2_TELEGRAM_OWNER_ID")):
        raise ValueError("Telegram token and owner ID must be configured together")
    additional = result.get("MM2_TELEGRAM_ALLOWED_IDS", "")
    if additional and (not result.get("MM2_TELEGRAM_OWNER_ID") or len(additional) > 272
                       or len(additional.split(",")) > 16
                       or any(not re.fullmatch(r"[1-9][0-9]{0,15}", item) or int(item) >= 2**52
                              for item in additional.split(","))):
        raise ValueError("invalid additional Telegram access IDs")
    relayer_authentication(result)
    return result


def image_environment(manifest):
    return {"MM2_" + role.upper() + "_IMAGE": identity for role, identity in manifest["images"].items()}


def bootstrap(domain, config=CONFIG_ROOT):
    if not DOMAIN.fullmatch(domain):
        raise ValueError("set one exact public ASCII HTTPS domain")
    config.mkdir(mode=0o700, parents=True, exist_ok=True)
    if config.is_symlink() or config.stat().st_uid != 0 or stat.S_IMODE(config.stat().st_mode) != 0o700:
        raise ValueError("host configuration directory must be private and root-owned")
    values = {
        "MM2_ENGINE_API_TOKEN": secrets.token_urlsafe(48), "MM2_CONSOLE_TOKEN": secrets.token_urlsafe(48),
        "MM2_DOMAIN": domain, "MM2_TELEGRAM_BOT_TOKEN": "", "MM2_TELEGRAM_OWNER_ID": "",
        "MM2_POLY_PRIVATE_KEY": "", "MM2_POLY_API_KEY": "", "MM2_POLY_API_SECRET": "", "MM2_POLY_API_PASSPHRASE": "",
        "MM2_POLYGON_RPC_URL": "",
        "MM2_POLY_RELAYER_API_KEY": "", "MM2_POLY_RELAYER_API_KEY_ADDRESS": "",
        "MM2_POLY_BUILDER_API_KEY": "", "MM2_POLY_BUILDER_API_SECRET": "", "MM2_POLY_BUILDER_API_PASSPHRASE": "",
    }
    descriptor = os.open(config / "runtime.env", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="ascii") as stream:
        stream.write("".join(key + "=" + value + "\n" for key, value in values.items()))
        stream.flush()
        os.fsync(stream.fileno())
    print("Created private runtime environment. Existing files are never replaced; token values were not printed.")


def check_state_mount(state=STATE_ROOT):
    if not state.is_dir() or state.is_symlink() or not os.path.ismount(state):
        raise ValueError("/srv/mm2 must be the reviewed mounted persistent EBS filesystem")
    marker = state / ".mm2-state-volume"
    if not marker.is_file() or marker.is_symlink() or marker.stat().st_size > 64 or not re.fullmatch(r"vol-[0-9a-f]{8,32}\n?", marker.read_text(encoding="ascii")):
        raise ValueError("persistent state volume identity is missing")
    volume_id = marker.read_text(encoding="ascii").strip()
    mounted = subprocess.run(["findmnt", "--noheadings", "--output", "SOURCE,OPTIONS", "--target", str(state)],
                             check=True, capture_output=True, text=True, timeout=10).stdout.strip().split()
    if len(mounted) != 2 or not {"nodev", "nosuid", "noexec"}.issubset(mounted[1].split(",")):
        raise ValueError("state mount options differ from the reviewed persistent filesystem")
    identity = subprocess.run(["lsblk", "--noheadings", "--output", "SERIAL,TYPE", mounted[0]],
                              check=True, capture_output=True, text=True, timeout=10).stdout.strip().split()
    if identity != [volume_id.replace("-", ""), "disk"]:
        raise ValueError("mounted device does not match the selected EBS volume serial")
    for lane, uid in (("engine", 10001), ("console", 10002), ("caddy", 0)):
        info = (state / lane).lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != uid:
            raise ValueError("persistent lane directories have incorrect identity or ownership")


def preflight(release, config=CONFIG_ROOT, state=STATE_ROOT, run=subprocess.run):
    manifest = verify_release(release)
    check_state_mount(state)
    values = private_environment(config / "runtime.env")
    profile = config / "active.toml"
    if profile.is_symlink() or not profile.is_file() or profile.stat().st_size > 65_536:
        raise ValueError("active public profile must be a bounded regular file")
    parsed = tomllib.loads(profile.read_text(encoding="utf-8"))
    mode = parsed.get("mode")
    state_path = parsed.get("state_dir")
    if mode not in {"simulation", "trading", "both", "shadow", "emulator"} or not isinstance(state_path, str):
        raise ValueError("unsupported startup profile")
    allowed = {"simulation": "/data/simulation", "trading": "/data/trading", "both": "/data/both", "shadow": "/data", "emulator": "/data"}
    if state_path != allowed[mode]:
        raise ValueError("profile state must preserve its reviewed mode binding")
    if mode in {"trading", "both"}:
        preflight_trading(profile, mode)
        if parsed.get("live_state_dir") != "/data/account-live":
            raise ValueError("funded modes must share the existing account ledger")
        if any(not values.get(key) for key in ("MM2_POLY_PRIVATE_KEY", "MM2_POLY_API_KEY", "MM2_POLY_API_SECRET", "MM2_POLY_API_PASSPHRASE", "MM2_POLYGON_RPC_URL")):
            raise ValueError("funded profile private runtime credentials are incomplete")
        if parsed["live"]["signature_type"] == 3:
            relayer_authentication(values, required=True, signer=parsed["live"]["signer_address"])
    expected = image_environment(manifest)
    release_env = "".join(key + "=" + value + "\n" for key, value in sorted(expected.items()))
    if (config / "release.env").is_symlink() or (config / "release.env").read_text(encoding="ascii") != release_env:
        raise ValueError("selected image identities differ from the release manifest")
    for role, identity in manifest["images"].items():
        if image_identity(identity, run, manifest["source_commit"] if role in {"engine", "console"} else None) != identity:
            raise ValueError("a selected immutable image is unavailable")
    # Parse the engine configuration with no runtime credentials and no network.
    run(["docker", "run", "--rm", "--network", "none", "--read-only", "--cap-drop", "ALL", "--security-opt",
         "no-new-privileges:true", "--mount", "type=bind,source=" + str(profile) + ",target=/app/config.toml,readonly",
         manifest["images"]["engine"], "--config", "/app/config.toml", "--check"], check=True, timeout=30, capture_output=True)
    environment = {key: value for key, value in os.environ.items() if not key.startswith("MM2_")}
    environment.update(expected)
    run(["docker", "compose", "--project-directory", str(release), "--env-file", str(config / "runtime.env"), "-f",
         "infra/aws/compose.yaml", "config", "--quiet"], cwd=release, env=environment, check=True, timeout=30, capture_output=True)
    return manifest


def import_release(source, install=INSTALL_ROOT, run=subprocess.run):
    manifest = verify_release(source)
    target = install / "releases" / manifest["source_commit"]
    if target.exists() or target.is_symlink():
        raise ValueError("release destination already exists; it will not be replaced")
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
    shutil.copytree(source, target)
    verify_release(target)
    run(["docker", "image", "load", "--input", str(target / "images.tar")], check=True, timeout=600, capture_output=True)
    for role, identity in manifest["images"].items():
        if image_identity(identity, run, manifest["source_commit"] if role in {"engine", "console"} else None) != identity:
            raise ValueError("loaded image identity does not match the approved manifest")
    return target


def activate(release, mode, profile=None, config=CONFIG_ROOT, install=INSTALL_ROOT, run=subprocess.run):
    release = release.resolve(strict=True)
    if release.parent != (install / "releases").resolve(strict=True):
        raise ValueError("activate only a verified installed release")
    manifest = verify_release(release)
    service = run(["systemctl", "is-active", "--quiet", "mm2.service"], check=False, capture_output=True, timeout=10)
    if service.returncode == 0:
        raise ValueError("stop the existing mm2 service before activation")
    active = run(["docker", "ps", "--quiet", "--filter", "label=com.docker.compose.project=mm2-production"],
                 check=True, capture_output=True, text=True, timeout=30)
    if active.stdout.strip():
        raise ValueError("pause/reconcile and stop the existing mm2 service before activation")
    source_profile = profile or release / "config" / (MODES[mode] + ".toml")
    if source_profile.is_symlink() or not source_profile.is_file() or source_profile.stat().st_size > 65_536:
        raise ValueError("selected public profile is absent, indirect or oversized")
    selected = tomllib.loads(source_profile.read_text(encoding="utf-8"))
    if selected.get("mode") != {"demo": "emulator"}.get(mode, mode):
        raise ValueError("selected profile does not match the requested execution mode")
    if mode in {"trading", "both"}:
        preflight_trading(source_profile, mode)
    config.mkdir(mode=0o700, parents=True, exist_ok=True)
    # Atomic files; previous release/profile remain retained for explicit rollback.
    previous = config / "active.toml"
    if previous.exists():
        if previous.is_symlink() or not previous.is_file() or previous.stat().st_size > 65_536:
            raise ValueError("previous profile has unexpected identity")
        history = config / "profile-history"
        history.mkdir(mode=0o700, exist_ok=True)
        with (history / (uuid.uuid4().hex + ".toml")).open("xb") as stream:
            stream.write(previous.read_bytes())
            stream.flush()
            os.fsync(stream.fileno())
    for path, content, permissions in (
        (config / "active.toml", source_profile.read_bytes(), 0o644),
        (config / "release.env", "".join(k + "=" + v + "\n" for k, v in sorted(image_environment(manifest).items())).encode("ascii"), 0o600),
    ):
        pending = path.with_suffix(path.suffix + ".new")
        with pending.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        pending.chmod(permissions)
        os.replace(pending, path)
    current = install / "current"
    pending = install / "current.new"
    pending.symlink_to(release, target_is_directory=True)
    os.replace(pending, current)
    preflight(release, config=config, run=run)
    unit = Path("/etc/systemd/system/mm2.service")
    shutil.copyfile(release / "infra/aws/mm2.service", unit)
    run(["systemctl", "daemon-reload"], check=True, timeout=30)
    print("Release selected and verified. No service started and no resume command sent.")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    verify = commands.add_parser("preflight")
    verify.add_argument("--release", required=True, type=Path)
    create = commands.add_parser("bootstrap")
    create.add_argument("--domain", required=True)
    load = commands.add_parser("import")
    load.add_argument("--source", required=True, type=Path)
    select = commands.add_parser("activate")
    select.add_argument("--release", required=True, type=Path)
    select.add_argument("--mode", choices=MODES, default="simulation")
    select.add_argument("--profile", type=Path, help="Reviewed public profile; mandatory owner setup for funded modes")
    args = parser.parse_args(argv)
    try:
        if not hasattr(os, "geteuid") or os.geteuid() != 0:
            raise ValueError("host deployment requires Linux root")
        if args.action == "bootstrap":
            bootstrap(args.domain)
        elif args.action == "preflight":
            preflight(args.release)
        elif args.action == "import":
            print("Imported verified release: " + str(import_release(args.source)))
        else:
            activate(args.release, args.mode, args.profile)
        return 0
    except (OSError, ValueError, subprocess.SubprocessError):
        print("Host operation failed. No resume command was sent; retained release/state require inspection.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
