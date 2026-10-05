#!/usr/bin/env python3
"""Bounded, read-only loopback acceptance monitor. Never controls the bot or sends orders."""
import argparse
import ipaddress
import json
import math
import os
from pathlib import Path
import shutil
import socket
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import (HTTPRedirectHandler, ProxyHandler, Request, build_opener)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise HTTPError(req.full_url, code, "redirect refused", headers, fp)


def loopback_url(value):
    parsed = urlsplit(value)
    if (parsed.scheme not in {"http", "https"} or parsed.username or parsed.password
            or parsed.query or parsed.fragment or parsed.path not in {"", "/"}
            or parsed.hostname not in {"127.0.0.1", "::1", "localhost"}):
        raise ValueError("only a plain loopback HTTP(S) engine URL is allowed")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    if not 1 <= port <= 65535:
        raise ValueError("invalid engine port")
    addresses = socket.getaddrinfo(parsed.hostname, port, type=socket.SOCK_STREAM)
    if not addresses or any(not ipaddress.ip_address(address[4][0]).is_loopback for address in addresses):
        raise ValueError("engine hostname must resolve exclusively to loopback")
    return value.rstrip("/")


def request(opener, base, path, token, timeout):
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    started = time.monotonic_ns()
    with opener.open(Request(base + path, headers=headers), timeout=timeout) as response:
        raw = response.read(524_289)
        if len(raw) > 524_288 or response.status != 200:
            raise ValueError("engine response exceeds bound or is not successful")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("engine response must be an object")
    return value, (time.monotonic_ns() - started) // 1000


def safe_number(value):
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value >= 0)


def observations(state, free_bytes, free_percent, args, wall_ms):
    journal = state.get("journal") or state.get("journal_metrics") or {}
    metrics = state.get("metrics") or {}
    paper = state.get("parallel_simulation") or {}
    if not isinstance(paper, dict):
        paper = {}
    paper_journal = paper.get("journal") or {}
    alerts = []
    if not state.get("api_ready"):
        alerts.append("api_not_ready")
    if args.expect_mode and state.get("execution_mode") != args.expect_mode:
        alerts.append("execution_mode_mismatch")
    if args.require_feed_ready and not state.get("feed_ready"):
        alerts.append("feed_not_ready")
    if state.get("fault") or state.get("simulation_error"):
        alerts.append("engine_fault")
    if paper.get("fault") or paper.get("simulation_error"):
        alerts.append("paper_lane_fault")
    published = state.get("published_at_ms")
    if not safe_number(published) or wall_ms - published > args.max_snapshot_age_ms or published - wall_ms > 5000:
        alerts.append("state_snapshot_stale_or_clock_skew")
    if free_bytes < args.min_free_bytes or free_percent < args.min_free_percent:
        alerts.append("disk_capacity_low")
    for prefix, lane_journal in (("", journal), ("paper_", paper_journal)):
        for used_key, capacity_key, name in (
            ("total_bytes", "total_max_bytes", "journal_bytes"),
            ("archive_segments", "max_segments", "journal_segments"),
            ("owner_commands", "max_commands", "owner_command_history"),
            ("execution_operations", "max_execution_operations", "execution_history"),
        ):
            used, capacity = lane_journal.get(used_key), lane_journal.get(capacity_key)
            if safe_number(used) and safe_number(capacity) and capacity > 0:
                if used / capacity * 100 >= args.warn_capacity_percent:
                    alerts.append(prefix + name + "_capacity_threshold")
    safe_journal = {key: value for key, value in journal.items()
                    if key in {"total_bytes", "active_bytes", "manifest_bytes", "archive_segments",
                               "record_count", "owner_commands", "execution_operations",
                               "segment_max_bytes", "total_max_bytes", "max_segments", "max_commands",
                               "max_execution_operations"} and safe_number(value)}
    safe_metrics = {key: value for key, value in metrics.items()
                    if key in {"feed_events", "decision_count", "sample_count", "telemetry_coalesced",
                               "last_snapshot_copy_ns", "execution_noop_ticks", "decision_ns_p50",
                               "decision_ns_p99"} and safe_number(value)}
    live = state.get("live") or {}
    if not isinstance(live, dict):
        live = {}
    if live.get("phase") in {"fault", "faulted", "killed"}:
        alerts.append("live_lane_stopped")
    if (safe_number(live.get("journal_bytes")) and safe_number(live.get("journal_max_bytes"))
            and live["journal_max_bytes"] > 0
            and live["journal_bytes"] / live["journal_max_bytes"] * 100 >= args.warn_capacity_percent):
        alerts.append("live_journal_bytes_capacity_threshold")
    safe_paper_journal = {key: value for key, value in paper_journal.items()
                          if key in safe_journal and safe_number(value)}
    return {"at_ms": wall_ms, "execution_mode": state.get("execution_mode"),
            "mode": state.get("mode"), "api_ready": bool(state.get("api_ready")),
            "feed_ready": bool(state.get("feed_ready")), "live_eligible": bool(state.get("live_eligible")),
            "snapshot_age_ms": wall_ms - published if safe_number(published) else None,
            "disk_free_bytes": free_bytes, "disk_free_percent": round(free_percent, 3),
            "journal": safe_journal, "paper_journal": safe_paper_journal,
            "live_journal_bytes": live.get("journal_bytes") if safe_number(live.get("journal_bytes")) else None,
            "live_journal_max_bytes": live.get("journal_max_bytes") if safe_number(live.get("journal_max_bytes")) else None,
            "metrics": safe_metrics, "live_phase": live.get("phase"),
            "alerts": alerts}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8088")
    parser.add_argument("--token-env", default="MM2_ENGINE_API_TOKEN")
    parser.add_argument("--seconds", type=int, default=3600)
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--timeout", type=float, default=2)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-output-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--disk-path", type=Path, default=Path.cwd())
    parser.add_argument("--min-free-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--min-free-percent", type=float, default=5)
    parser.add_argument("--warn-capacity-percent", type=float, default=80)
    parser.add_argument("--max-snapshot-age-ms", type=int, default=5000)
    parser.add_argument("--require-feed-ready", action="store_true")
    parser.add_argument("--expect-mode", choices=["emulator", "simulation", "shadow", "trading", "both"])
    parser.add_argument("--allow-restarts", action="store_true")
    args = parser.parse_args(argv)
    if (not 1 <= args.seconds <= 7 * 24 * 3600 or not 1 <= args.interval <= 60
            or not 0.1 <= args.timeout <= 10 or not 65_536 <= args.max_output_bytes <= 256 * 1024 * 1024
            or args.min_free_bytes < 0 or not 0 <= args.min_free_percent <= 100
            or not 1 <= args.warn_capacity_percent <= 100 or not 1000 <= args.max_snapshot_age_ms <= 60_000):
        parser.error("monitor options exceed bounded resource limits")
    try:
        base = loopback_url(args.url)
        disk = args.disk_path.resolve(strict=True)
        token = os.environ.get(args.token_env)
        if not token or len(token) > 512 or "\n" in token or "\r" in token:
            parser.error("engine token environment variable is missing or invalid; never pass it in argv")
        opener = build_opener(ProxyHandler({}), NoRedirect())
        samples, failures, written, restarts = 0, 0, 0, 0
        previous_boot = None
        latencies = []
        started = time.monotonic()
        deadline = started + args.seconds
        # Exclusive creation preserves an earlier acceptance record.
        with args.output.open("x", encoding="utf-8") as output:
            while time.monotonic() < deadline:
                tick = time.monotonic()
                wall_ms = int(time.time() * 1000)
                try:
                    _, health_us = request(opener, base, "/healthz", None, args.timeout)
                    state, state_us = request(opener, base, "/v1/state", token, args.timeout)
                    usage = shutil.disk_usage(disk)
                    current = observations(state, usage.free, usage.free / usage.total * 100, args, wall_ms)
                    current["health_http_us"] = health_us
                    current["state_http_us"] = state_us
                    boot = state.get("boot_id")
                    if previous_boot is not None and boot != previous_boot:
                        restarts += 1
                        if not args.allow_restarts:
                            current["alerts"].append("unexpected_engine_restart")
                    previous_boot = boot
                    latencies.append(state_us)
                    # <=604800 samples; quantiles retain at most 10000 recent HTTP probes.
                    if len(latencies) > 10_000:
                        del latencies[0]
                except (OSError, HTTPError, URLError, ValueError):
                    current = {"at_ms": wall_ms, "alerts": ["loopback_health_or_state_unavailable"]}
                raw = json.dumps(current, sort_keys=True, allow_nan=False) + "\n"
                written += len(raw.encode("utf-8"))
                if written > args.max_output_bytes:
                    failures += 1
                    print("acceptance observation output capacity reached", file=sys.stderr)
                    break
                output.write(raw)
                output.flush()
                samples += 1
                failures += bool(current["alerts"])
                wait = min(args.interval - (time.monotonic() - tick), deadline - time.monotonic())
                if wait > 0:
                    time.sleep(wait)
        ordered = sorted(latencies)
        summary = {"samples": samples, "samples_with_alerts": failures, "engine_restarts": restarts,
                   "elapsed_seconds": round(time.monotonic() - started, 3),
                   "passed": samples > 0 and failures == 0,
                   "state_http_us_p95": ordered[(len(ordered) - 1) * 95 // 100] if ordered else None,
                   "state_http_us_p99": ordered[(len(ordered) - 1) * 99 // 100] if ordered else None,
                   "measurement": "loopback owner API probes; not exchange order latency",
                   "output_bytes": min(written, args.max_output_bytes)}
        print(json.dumps(summary, sort_keys=True))
        return 0 if summary["passed"] else 1
    except (OSError, ValueError) as error:
        print("acceptance monitor refused: " + str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
