#!/usr/bin/env python3
"""Bounded, unauthenticated HTTP/1.1 placement probe. Never submits orders."""

import argparse
import collections
import datetime
import http.client
import io
import json
import math
import pathlib
import queue
import socket
import ssl
import threading
import time


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
MAX_BODY_BYTES = 65536
MAX_REQUESTS = 2000
P99_MIN_SAMPLES = 1000
HEADER_ALLOWLIST = frozenset({
    "poly-ratelimit-remaining", "poly-ratelimit-reset", "poly-ratelimit-tier",
    "poly-ratelimit-warning", "retry-after", "cf-ray", "cf-cache-status",
    "server", "connection", "date",
})


def remaining(deadline):
    duration = deadline - time.perf_counter()
    if duration <= 0:
        raise TimeoutError("deadline exceeded")
    return duration


def bounded_connect(address, deadline):
    # OS DNS resolution has no socket timeout. A daemon resolver prevents it
    # from holding this bounded diagnostic open after its deadline.
    result = queue.Queue(maxsize=1)

    def resolve():
        try:
            result.put(socket.getaddrinfo(*address, type=socket.SOCK_STREAM))
        except OSError as error:
            result.put(error)

    threading.Thread(target=resolve, daemon=True).start()
    try:
        addresses = result.get(timeout=remaining(deadline))
    except queue.Empty:
        raise TimeoutError("DNS deadline exceeded") from None
    if isinstance(addresses, OSError):
        raise addresses
    last_error = None
    for family, kind, protocol, _, destination in addresses:
        sock = socket.socket(family, kind, protocol)
        try:
            sock.settimeout(remaining(deadline))
            sock.connect(destination)
            sock.settimeout(remaining(deadline))
            return sock
        except OSError as error:
            sock.close()
            last_error = error
    raise last_error or OSError("no address resolved")


class FirstByteReader(io.RawIOBase):
    def __init__(self, raw, sock, response, deadline):
        self.raw = raw
        self.sock = sock
        self.response = response
        self.deadline = deadline

    def readable(self):
        return True

    def readinto(self, buffer):
        self.sock.settimeout(remaining(self.deadline))
        count = self.raw.readinto(buffer)
        if count and self.response.first_byte_ns is None:
            self.response.first_byte_ns = time.perf_counter_ns()
        return count

    def close(self):
        self.raw.close()
        super().close()


class ResponseSocket:
    def __init__(self, sock, response, deadline):
        self.sock = sock
        self.response = response
        self.deadline = deadline

    def makefile(self, mode):
        raw = self.sock.makefile(mode, buffering=0)
        return io.BufferedReader(FirstByteReader(
            raw, self.sock, self.response, self.deadline,
        ))


class TimedHTTPResponse(http.client.HTTPResponse):
    def __init__(self, sock, *args, deadline, **kwargs):
        self.first_byte_ns = None
        super().__init__(ResponseSocket(sock, self, deadline), *args, **kwargs)


class ProbeConnection(http.client.HTTPSConnection):
    def __init__(self, host, context):
        super().__init__(host, context=context)
        self.deadline = 0
        self.last_response = None
        self._create_connection = self.connect_socket
        self.response_class = self.make_response

    def connect_socket(self, address, timeout=None, source_address=None):
        return bounded_connect(address, self.deadline)

    def make_response(self, sock, *args, **kwargs):
        self.last_response = TimedHTTPResponse(
            sock, *args, deadline=self.deadline, **kwargs,
        )
        return self.last_response


def safe_headers(headers):
    return {
        name.lower(): "".join(c for c in value[:256] if c.isprintable())
        for name, value in headers
        if name.lower() in HEADER_ALLOWLIST
    }


def sample(connection, path, phase, request_timeout, run_deadline):
    started = time.perf_counter_ns()
    connection.deadline = min(run_deadline, time.perf_counter() + request_timeout)
    connection.last_response = None
    previous_socket = connection.sock
    row = {
        "type": "sample", "phase": phase, "path": path,
        "utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "connection_reused": False, "http_status": None,
        "ok": False, "error": None, "headers": {}, "ttfb_ms": None,
    }
    body = b""
    try:
        connection.timeout = remaining(connection.deadline)
        if previous_socket is not None:
            previous_socket.settimeout(connection.timeout)
        connection.request("GET", path, headers={
            "Accept": "application/json", "User-Agent": "olympus-readonly-latency/1",
        })
        row["connection_reused"] = (
            previous_socket is not None and connection.sock is previous_socket
        )
        response = connection.getresponse()
        row["http_status"] = response.status
        row["headers"] = safe_headers(response.getheaders())
        body = response.read(MAX_BODY_BYTES + 1)
        if len(body) > MAX_BODY_BYTES:
            row["error"] = "body_limit"
            connection.close()
        elif response.status != 200:
            row["error"] = "http_status"
        else:
            row["ok"] = True
        response.close()
    except (OSError, http.client.HTTPException) as error:
        # Exception text may contain an IP or provider body. Persist classes only.
        row["error"] = type(error).__name__
        connection.close()
    ended = time.perf_counter_ns()
    response = connection.last_response
    if response is not None and response.first_byte_ns is not None:
        row["ttfb_ms"] = round((response.first_byte_ns - started) / 1e6, 3)
    row["total_ms"] = round((ended - started) / 1e6, 3)
    return row, body


def geoblock_result(row, body):
    output = {"country": None, "blocked": None}
    if not row["ok"]:
        return output
    try:
        parsed = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        row.update(ok=False, error="invalid_geoblock_json")
        return output
    country = parsed.get("country") if isinstance(parsed, dict) else None
    blocked = parsed.get("blocked") if isinstance(parsed, dict) else None
    if (not isinstance(country, str) or len(country) != 2
            or not country.isascii() or not country.isalpha()
            or not isinstance(blocked, bool)):
        row.update(ok=False, error="invalid_geoblock_schema")
        return output
    return {"country": country.upper(), "blocked": blocked}


def distribution(values):
    ordered = sorted(values)
    count = len(ordered)

    def percentile(fraction):
        return ordered[max(0, math.ceil(fraction * count) - 1)] if count else None

    return {
        "samples": count, "p50_ms": percentile(.50), "p95_ms": percentile(.95),
        "p99_ms": percentile(.99) if count >= P99_MIN_SAMPLES else None,
        "p99_status": "descriptive" if count >= P99_MIN_SAMPLES else "insufficient_samples",
        "p99_min_samples": P99_MIN_SAMPLES,
        "max_ms": ordered[-1] if count else None,
    }


def summarize(rows):
    output = {}
    for name, selected in (
        ("cold", [r for r in rows if r["phase"] == "cold"]),
        ("warm_reused", [r for r in rows if r["phase"] == "warm" and r["connection_reused"]]),
        ("warm_new_connection", [r for r in rows if r["phase"] == "warm" and not r["connection_reused"]]),
    ):
        valid = [r for r in selected if r["ok"]]
        output[name] = {
            "attempted": len(selected), "successful": len(valid),
            "errors": dict(collections.Counter(r["error"] for r in selected if not r["ok"])),
            "ttfb": distribution([r["ttfb_ms"] for r in valid if r["ttfb_ms"] is not None]),
            "total": distribution([r["total_ms"] for r in valid]),
        }
    return output


def output_path(value):
    path = pathlib.Path(value)
    if not path.is_absolute():
        path = REPO_ROOT / path
    path = path.resolve()
    output_root = (REPO_ROOT / "output").resolve()
    if (not output_root.is_relative_to(REPO_ROOT)
            or not path.is_relative_to(output_root) or path.suffix != ".jsonl"):
        raise ValueError("JSONL must be a .jsonl file inside this checkout's output/")
    return path


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", default="unspecified")
    parser.add_argument("--cold", type=int, default=5)
    parser.add_argument("--warm", type=int, default=50,
                        help="includes the initial new connection; reused samples are counted separately")
    parser.add_argument("--interval-ms", type=float, default=250)
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--deadline", type=float, default=180)
    parser.add_argument("--max-requests", type=int, default=100,
                        help="includes one geoblock request; hard maximum 2000")
    parser.add_argument("--jsonl", help="opt-in new artifact under output/; existing files are refused")
    args = parser.parse_args(argv)
    if (min(args.cold, args.warm) < 0 or args.cold + args.warm < 1
            or not 1 <= args.max_requests <= MAX_REQUESTS
            or args.cold + args.warm + 1 > args.max_requests):
        parser.error("sample counts must fit max-requests (including geoblock), maximum 2000")
    if (not all(math.isfinite(v) for v in (args.interval_ms, args.timeout, args.deadline))
            or not 0 <= args.interval_ms <= 10000
            or not 0 < args.timeout <= 30 or not 0 < args.deadline <= 3600):
        parser.error("interval must be 0..10000ms, timeout 0..30s, deadline 0..3600s; finite values only")
    if not args.label or len(args.label) > 64 or not all(c.isalnum() or c in "_-" for c in args.label):
        parser.error("label must contain 1..64 letters, numbers, underscores or hyphens")
    if args.jsonl:
        try:
            args.jsonl = output_path(args.jsonl)
        except ValueError as error:
            parser.error(str(error))
    return args


def run(args, artifact=None):
    started = time.perf_counter()
    run_deadline = started + args.deadline
    context = ssl.create_default_context()
    rows = []

    def record(row):
        if artifact is not None:
            artifact.write(json.dumps(row, sort_keys=True) + "\n")
            artifact.flush()

    record({"type": "config", "label": args.label, "cold": args.cold, "warm": args.warm,
            "interval_ms": args.interval_ms, "timeout_s": args.timeout,
            "deadline_s": args.deadline, "max_requests": args.max_requests,
            "endpoint": "https://clob.polymarket.com/time", "http_version": "1.1"})
    geo_connection = ProbeConnection("polymarket.com", context)
    try:
        geo_row, body = sample(geo_connection, "/api/geoblock", "geoblock", args.timeout, run_deadline)
        geo = geoblock_result(geo_row, body)
        geo_row["geoblock"] = geo
        record(geo_row)
    finally:
        geo_connection.close()
    stop_reason = "completed"
    attempted = 1
    warm_connection = None
    consecutive_errors = 0
    if not geo_row["ok"]:
        stop_reason = "geoblock_unavailable"
    elif geo["blocked"]:
        stop_reason = "geoblock_blocked"
    else:
        try:
            for phase, count in (("cold", args.cold), ("warm", args.warm)):
                for _ in range(count):
                    if time.perf_counter() >= run_deadline:
                        stop_reason = "deadline"
                        break
                    if phase == "cold":
                        connection = ProbeConnection("clob.polymarket.com", context)
                    else:
                        if warm_connection is None:
                            warm_connection = ProbeConnection("clob.polymarket.com", context)
                        connection = warm_connection
                    try:
                        row, _ = sample(connection, "/time", phase, args.timeout, run_deadline)
                    finally:
                        if phase == "cold":
                            connection.close()
                    attempted += 1
                    rows.append(row)
                    record(row)
                    consecutive_errors = 0 if row["ok"] else consecutive_errors + 1
                    if row["http_status"] == 429:
                        stop_reason = "rate_limited"
                        break
                    if consecutive_errors >= 3:
                        stop_reason = "consecutive_errors"
                        break
                    pause = min(args.interval_ms / 1000, max(0, run_deadline - time.perf_counter()))
                    if pause:
                        time.sleep(pause)
                if stop_reason != "completed":
                    break
        finally:
            if warm_connection is not None:
                warm_connection.close()
    summary = {
        "type": "summary", "label": args.label, "endpoint": "https://clob.polymarket.com/time",
        "geoblock": geo, "geoblock_error": geo_row["error"], "stop_reason": stop_reason,
        "requests_attempted": attempted, "elapsed_s": round(time.perf_counter() - started, 3),
        "latency": summarize(rows), "percentile_method": "nearest_rank_successful_only",
        "measurement": "public HTTP path, not order acceptance or cancel effectiveness",
    }
    record(summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if stop_reason == "completed" and all(r["ok"] for r in rows) else 1


def main(argv=None):
    args = arguments(argv)
    if args.jsonl:
        args.jsonl.parent.mkdir(parents=True, exist_ok=True)
        with args.jsonl.open("x", encoding="utf-8") as artifact:
            return run(args, artifact)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
