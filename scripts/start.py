#!/usr/bin/env python3
"""Choose an execution profile for a paused mm2 startup; never resumes or submits orders."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parent.parent
PROFILES = {
    "simulation": "config/simulation.toml",
    "trading": "config/trading.toml",
    "both": "config/both.toml",
    "shadow": "config/public.toml",
    "demo": "config/demo.toml",
}


def preflight_trading(path, mode):
    """Reject inactive public templates without opening environment/credential values."""
    try:
        with path.open("rb") as source:
            raw = source.read(65_537)
        if len(raw) > 65_536:
            raise ValueError("profile exceeds 64 KiB")
        configuration = tomllib.loads(raw.decode("utf-8"))
    except (OSError, ValueError):
        raise ValueError("trading profile is unreadable, malformed or exceeds its byte bound") from None
    live = configuration.get("live")
    shared = configuration.get("live_state_dir")
    if (configuration.get("mode") != mode or not isinstance(live, dict)
            or live.get("owner_authorized") is not True
            or not isinstance(shared, str) or not shared.strip()
            or shared == configuration.get("state_dir")):
        raise ValueError("trading profile needs its selected mode, explicit owner_authorized and a separate shared live_state_dir")
    for field in ("owner_address", "signer_address", "funder_address", "exchange_address", "collateral_address"):
        address = live.get(field)
        if not isinstance(address, str) or not re.fullmatch(r"0x[0-9a-fA-F]{40}", address) or int(address[2:], 16) == 0:
            raise ValueError("trading profile public account/deployment addresses are not configured")
    if live.get("protocol") != 2 or live.get("chain_id") != 137 or live.get("clob_url") != "https://clob-v2.polymarket.com":
        raise ValueError("trading profile must explicitly select the supported Polygon CLOB V2 deployment")
    if not live.get("allowed_conditions") and not live.get("allowed_market_prefixes"):
        raise ValueError("trading profile needs an explicit approved market allowlist")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=PROFILES, default="simulation")
    parser.add_argument("--build", action="store_true", help="build images before starting")
    parser.add_argument("--foreground", action="store_true", help="attach to service output")
    parser.add_argument("--dry-run", action="store_true", help="show profile/commands without reading secrets or running Docker")
    parser.add_argument("--project-name", help="optional Compose project identifier")
    args = parser.parse_args(argv)
    if args.project_name and not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", args.project_name):
        parser.error("project name must contain 1-63 lowercase ASCII identifier characters")
    config = PROFILES[args.mode]
    if not (ROOT / config).is_file():
        parser.error("selected packaged profile is missing")
    prefix = ["docker", "compose"]
    if args.project_name:
        prefix += ["--project-name", args.project_name]
    launch = prefix + ["up"]
    if not args.foreground:
        launch.append("--detach")
    if args.build:
        launch.append("--build")
    plan = {"mode": args.mode, "config": config, "starts_paused": True,
            "commands": [prefix + ["config", "--quiet"], launch],
            "owner_resume_required": True}
    print(json.dumps(plan, sort_keys=True), flush=True)
    if args.dry_run:
        return 0
    if args.mode in {"trading", "both"}:
        try:
            preflight_trading(ROOT / config, args.mode)
        except ValueError as error:
            parser.error(str(error))
    if not (ROOT / ".env").is_file():
        parser.error("private .env is missing; run the platform bootstrap script first")
    environment = dict(os.environ)
    # Process-local selection preserves the owner's existing environment file and credentials.
    environment["MM2_ENGINE_CONFIG"] = "./" + config
    try:
        for command in plan["commands"]:
            subprocess.run(command, cwd=ROOT, env=environment, check=True)
        return 0
    except (OSError, subprocess.CalledProcessError):
        print("mm2 startup failed; inspect service status without printing environment values", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
