#!/usr/bin/env python3
"""Bounded public GET capture for the console; no engine/account credentials.

Publish verified immutable evidence in a dedicated secondary cache, then switch
current.json atomically. Keep two hot captures and immutable daily audit copies.
Errors retain the last pointer; quota stops collection without deleting audit
copies or primary data. Run separately from engine and console request handlers.
"""
import argparse
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import signal
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.research.archive import publish_bundle, verify_bundle
from tools.research.comparison_cache import (MARKER, MAX_STORAGE, commit, initialize, preflight, storage_usage)
from tools.research.console_comparison import publish_reference
from tools.research.observe_public_v2 import PublicHTTPV2, observe_account
from tools.research.registry import DEFAULT_ACCOUNTS, load_registry
from tools.research.schema import VERSION, ResearchError, canonical, digest, load_json, utc, window

def refresh(root, *, hours=24, registry_path=None, source_revision="unknown", now=None, client=None):
    if type(hours) is not int or not 1 <= hours <= 24:
        raise ResearchError("comparison lookback must be 1..24 hours")
    root = Path(root).absolute()
    if not (root / MARKER).exists():
        initialize(root)
    preflight(root)
    captured = now or datetime.now(timezone.utc)
    if captured.utcoffset() != timedelta(0):
        raise ResearchError("comparison capture requires UTC")
    end = captured.replace(microsecond=0)
    start = end - timedelta(hours=hours)
    interval = window(start.isoformat().replace("+00:00", "Z"), end.isoformat().replace("+00:00", "Z"))
    registry = load_registry(registry_path)
    account = next((row for row in registry["accounts"] if row["name"] == "mo-money"
                    and row["account_id"] == DEFAULT_ACCOUNTS["mo-money"]["account_id"]), None)
    if account is None:
        raise ResearchError("registered mo-money identity required")
    client = client or PublicHTTPV2(max_bytes=16_000_000, max_requests=120, max_seconds=90)
    observed = observe_account(client, account, interval["start"], interval["end"], max_rows=10_000)
    # A failed activity source must not replace a working view with a fresh zero.
    if not any(item["endpoint"] == "activity" for item in client.receipts):
        raise ResearchError("comparison activity unavailable; previous view retained")
    completed = datetime.now(timezone.utc) if now is None else max(captured, utc(observed["snapshot"]["at_utc"]))
    created = completed.isoformat().replace("+00:00", "Z")
    files = {"events.jsonl": observed["events"], "closing.json": observed["snapshot"],
             "coverage.json": observed["coverage"], "source-activity.jsonl": observed["activity_rows"],
             "source-positions.jsonl": observed["position_rows"], "requests.jsonl": client.receipts,
             "registry.json": {"schema_version": VERSION, "accounts": [account]}}
    observer = publish_bundle(root / "hot-observers", files,
        scope={"account_id": account["account_id"], "lane": "public-benchmark", "kind": "one-public-account", "chain_id": 137},
        utc_range=interval, quality=observed["quality"], source_revision=source_revision,
        config_hash=digest(registry), created_at_utc=created)
    verify_bundle(observer)
    view = publish_reference(observer, root, source_revision=source_revision, created_at_utc=created)
    cache = commit(root, observer, view)
    return {"schema_version": 1, "status": "PUBLISHED", "bundle_id": view.name, "observer_bundle_id": observer.name,
            "period": interval, "observed_events": len(observed["events"]), "coverage": observed["coverage"],
            "requests": client.request_count, "bytes": client.bytes_read, "cache": cache, "read_only_public_capture": True,
            "engine_commands": 0, "live_orders": 0, "messages_sent": 0}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="dedicated bounded secondary archive/view root")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--registry")
    parser.add_argument("--source-revision", default=os.environ.get("MM2_SOURCE_REVISION", "unknown"))
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval-seconds", type=int, default=300)
    args = parser.parse_args(argv)
    if not 60 <= args.interval_seconds <= 3600:
        parser.error("refresh interval must be 60..3600 seconds")
    stopped = False
    def stop(*_):
        nonlocal stopped
        stopped = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    previous_status = None
    while not stopped:
        try:
            result = refresh(args.out, hours=args.hours, registry_path=args.registry, source_revision=args.source_revision)
            code = 0
        except (ResearchError, ValueError, OSError):
            # Source URLs, response text and local environment never go to logs.
            result = {"schema_version": 1, "status": "REFRESH_FAILED_LAST_VIEW_RETAINED", "live_orders": 0}
            code = 1
        if result["status"] == "PUBLISHED" or previous_status != result["status"]:
            print(canonical(result).decode(), flush=True)
        previous_status = result["status"]
        if not args.loop:
            return code
        deadline = time.monotonic() + args.interval_seconds
        while not stopped and time.monotonic() < deadline:
            time.sleep(min(1, max(0, deadline - time.monotonic())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
