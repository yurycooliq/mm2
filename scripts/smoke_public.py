#!/usr/bin/env python3
"""Fail-closed bounded public-feed acceptance; paused shadow only, never an order benchmark."""
import argparse
from decimal import Decimal, InvalidOperation
import json
import re
import time
import uuid
from verify_stack import ROOT, docker, inspect, request


class SmokeFailure(Exception):
    """Only fixed public failure codes may enter the summary."""


def unsigned(value):
    return type(value) is int and value >= 0


def price(value, maximum=None):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,20}(?:\.[0-9]{1,12})?", value):
        return None
    try:
        number = Decimal(value)
        return number if number > 0 and (maximum is None or number <= maximum) else None
    except InvalidOperation:
        return None


def observe(current, wall_ms):
    """Verify the actual publication schema, including paused readiness without intents."""
    if not isinstance(current, dict):
        raise SmokeFailure("invalid_state")
    if (current.get("mode") != "paused" or current.get("execution_mode") != "shadow"
            or current.get("feed_source") != "public" or current.get("command_version") != 0
            or current.get("reset_epoch") != 0):
        raise SmokeFailure("mode_or_control_changed")
    # Shadow has no funded actor; the former actual_orders projection no longer exists.
    if (current.get("live_eligible") is not False or "live" not in current or current["live"] is not None
            or "simulation" not in current or current["simulation"] is not None
            or current.get("parallel_simulation") is not None or current.get("actual_orders", []) != []
            or current.get("shadow_intents", []) != []):
        raise SmokeFailure("unexpected_execution_activity")
    if any(current.get(field) is not None for field in ("fault", "last_input_error", "simulation_error")):
        raise SmokeFailure("engine_or_source_fault")
    if current.get("shutting_down") or current.get("committing"):
        raise SmokeFailure("engine_stopping_or_committing")
    published = current.get("published_at_ms")
    if not unsigned(published) or wall_ms - published > 5000 or published - wall_ms > 1000:
        raise SmokeFailure("stale_snapshot_or_clock_skew")
    publication_lag = max(0, wall_ms - published)
    phase = current.get("source_status") or {}
    phase = phase.get("phase") if isinstance(phase, dict) else None
    if phase in {"fault", "faulted", "failed", "stopped"}:
        raise SmokeFailure("source_failed")
    phase = phase if phase in {"starting", "discovering", "retrying", "streaming", "refreshing"} else "unknown"
    market = current.get("market")
    market_ready = False
    if isinstance(market, dict):
        slug = market.get("slug")
        match = re.fullmatch(r"btc-updown-5m-([0-9]{1,12})", slug) if isinstance(slug, str) else None
        start, end = market.get("window_start_ms"), market.get("window_end_ms")
        tokens = [market.get("up_token"), market.get("down_token")]
        market_ready = (match is not None and unsigned(start) and unsigned(end)
                        and int(match[1]) % 300 == 0 and start == int(match[1]) * 1000
                        and end - start == 300_000 and start <= published < end and start <= wall_ms < end
                        and isinstance(market.get("condition_id"), str)
                        and re.fullmatch(r"0x[0-9a-fA-F]{64}", market["condition_id"]) is not None
                        and all(isinstance(token, str) and re.fullmatch(r"[1-9][0-9]{0,77}", token)
                                and int(token) < 2**256 for token in tokens) and tokens[0] != tokens[1])
    limits = current.get("limits") or {}
    max_age = limits.get("max_feed_age_ms") if isinstance(limits, dict) else None
    books = current.get("books") or {}
    fresh = {}
    for outcome in ("up", "down"):
        book = books.get(outcome) if isinstance(books, dict) else None
        bid = price(book.get("best_bid"), 1) if isinstance(book, dict) else None
        ask = price(book.get("best_ask"), 1) if isinstance(book, dict) else None
        fresh[outcome] = (isinstance(book, dict) and book.get("initialized") is True
                          and price(book.get("tick"), 1) is not None and bid is not None and ask is not None and bid <= ask
                          and unsigned(max_age) and 1 <= max_age <= 60_000
                          and all(unsigned(book.get(key)) and book[key] + publication_lag <= max_age
                                  for key in ("exchange_age_ms", "receive_age_ms"))
                          and unsigned(book.get("exchange_ms")) and unsigned(book.get("observed_mono_ms"))
                          and 0 <= wall_ms - book["exchange_ms"] <= max_age)
    reference = current.get("reference")
    reference_ready = (current.get("binance_connected") is True and isinstance(reference, dict)
                       and reference.get("source") == "binance_btcusdt" and price(reference.get("price")) is not None
                       and unsigned(reference.get("exchange_ms")) and unsigned(reference.get("observed_mono_ms"))
                       and 0 <= wall_ms - reference["exchange_ms"] <= 5000 and reference.get("oracle") is False)
    feed_ready = (current.get("api_ready") is True and current.get("feed_ready") is True
                  and current.get("awaiting_source_refresh") is False and market_ready and all(fresh.values()))
    return {"api_ready": current.get("api_ready") is True, "feed_ready": bool(feed_ready),
            "market_ready": bool(market_ready), "up_book_fresh": bool(fresh["up"]), "down_book_fresh": bool(fresh["down"]),
            "binance_connected": current.get("binance_connected") is True, "reference_ready": bool(reference_ready),
            "shadow_ready": current.get("shadow_ready") is True, "source_phase": phase}


def ensure_running(info):
    status = info.get('State', {})
    if status.get('Running') is not True or status.get('Status') != 'running' or status.get('OOMKilled'):
        raise SmokeFailure('engine_exited_or_oom')
    if status.get('Health', {}).get('Status') == 'unhealthy':
        raise SmokeFailure('container_unhealthy')


def check_public(seconds, engine_image):
    owner = uuid.uuid4().hex
    label = 'mm2.public-check'
    volume = 'mm2-public-' + owner[:12]
    token = uuid.uuid4().hex * 2
    container, created, failure = None, False, None
    summary = {"host": "current Docker host", "seconds": seconds, "passed": False, "samples": 0,
               "api_ready_seen": False, "feed_ready_seen": False, "market_seen": False,
               "up_book_fresh_seen": False, "down_book_fresh_seen": False,
               "binance_connected_seen": False, "reference_seen": False, "complete_feed_seen": False,
               "shadow_ready_seen": False, "shadow_ready_required": False,
               "paused_verified": False, "live_eligible_false_verified": False, "live_orders": None,
               "authenticated_order_latency_measured": False, "source_phases": [], "cleanup_ok": True}
    try:
        docker('volume', 'create', '--label', label + '=' + owner, volume)
        created = True
        if inspect('volume', volume).get('Labels', {}).get(label) != owner:
            raise SmokeFailure('volume_owner_mismatch')
        container = docker('run', '--detach', '--label', label + '=' + owner,
                           '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
                           '--memory', '512m', '--cpus', '1', '--pids-limit', '64',
                           '--tmpfs', '/tmp:rw,noexec,nosuid,size=16m', '--publish', '127.0.0.1::8088',
                           '--env', 'MM2_ENGINE_API_TOKEN=' + token,
                           '--mount', 'type=volume,source=' + volume + ',target=/data',
                           '--mount', 'type=bind,source=' + str(ROOT / 'config/public.toml') +
                           ',target=/app/config.toml,readonly', engine_image)
        info = inspect('container', container)
        if info.get('Config', {}).get('Labels', {}).get(label) != owner:
            raise SmokeFailure('container_owner_mismatch')
        ensure_running(info)
        base = 'http://127.0.0.1:' + info['NetworkSettings']['Ports']['8088/tcp'][0]['HostPort']
        deadline = time.monotonic() + seconds
        boot = None
        while time.monotonic() < deadline:
            info = inspect('container', container)
            ensure_running(info)
            try:
                current = request(base, '/v1/state', token)[0]
            except OSError:
                if summary['samples']:
                    raise SmokeFailure('engine_api_became_unavailable') from None
                time.sleep(0.25)
                continue
            observation = observe(current, int(time.time() * 1000))
            identity = current.get('boot_id')
            if not isinstance(identity, str) or not re.fullmatch(r'[0-9a-f-]{36}', identity):
                raise SmokeFailure('invalid_boot_identity')
            if boot is not None and identity != boot:
                raise SmokeFailure('engine_restarted')
            boot = identity
            summary['samples'] += 1
            summary['paused_verified'] = summary['live_eligible_false_verified'] = True
            summary['live_orders'] = 0
            for field, result in (('api_ready_seen', 'api_ready'), ('feed_ready_seen', 'feed_ready'),
                                  ('market_seen', 'market_ready'), ('up_book_fresh_seen', 'up_book_fresh'),
                                  ('down_book_fresh_seen', 'down_book_fresh'), ('binance_connected_seen', 'binance_connected'),
                                  ('reference_seen', 'reference_ready'), ('shadow_ready_seen', 'shadow_ready')):
                summary[field] |= observation[result]
            summary['complete_feed_seen'] |= observation['feed_ready'] and observation['reference_ready']
            phase = observation['source_phase']
            if phase not in summary['source_phases']:
                summary['source_phases'].append(phase)
            time.sleep(0.25)
        if not summary['complete_feed_seen']:
            raise SmokeFailure('required_public_feeds_not_observed')
        ensure_running(inspect('container', container))
    except SmokeFailure as error:
        failure = str(error)
    except Exception:
        failure = 'probe_or_container_setup_failed'
    finally:
        for kind, name, exists in (('container', container, bool(container)), ('volume', volume, created)):
            if not exists:
                continue
            try:
                info = inspect(kind, name)
                labels = info.get('Config', {}).get('Labels', {}) if kind == 'container' else info.get('Labels', {})
                if labels.get(label) != owner:
                    raise SmokeFailure('cleanup_owner_mismatch')
                docker(kind, 'rm', *(('--force',) if kind == 'container' else ()), name)
            except Exception:
                summary['cleanup_ok'] = False
                failure = failure or 'owned_resource_cleanup_failed'
    summary['failure'] = failure
    summary['passed'] = failure is None
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=int, default=35)
    parser.add_argument('--engine-image', default='mm2-engine:local')
    args = parser.parse_args(argv)
    if not 5 <= args.seconds <= 60:
        parser.error('seconds must be between 5 and 60')
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}', args.engine_image):
        parser.error('engine-image must be one bounded public Docker image reference or immutable ID')
    result = check_public(args.seconds, args.engine_image)
    print(json.dumps(result, separators=(',', ':')), flush=True)
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
