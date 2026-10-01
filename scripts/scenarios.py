#!/usr/bin/env python3
"""Explicit synthetic latency sensitivity; never an economic score or observed venue latency."""
import argparse
import copy
import json
from pathlib import Path
import subprocess
import tempfile


MAX_TAPE_BYTES = 2 * 1024 * 1024
MAX_STEPS = 10_000
MAX_TIME = (1 << 64) - 1


def validate_source(source):
    if not isinstance(source, dict) or not isinstance(source.get('steps'), list):
        raise ValueError('tape must contain an ordered steps list')
    steps = source['steps']
    # Leave room for the two explicit cleanup operations appended to each run.
    if not steps or len(steps) > MAX_STEPS - 2:
        raise ValueError('tape requires 1-9998 steps before scenario cleanup')
    previous = 0
    for step in steps:
        at = step.get('at_ms') if isinstance(step, dict) else None
        if (not isinstance(at, int) or isinstance(at, bool)
                or not previous <= at <= MAX_TIME - 20_000):
            raise ValueError('step at_ms must be a forward u64 time with cleanup headroom')
        previous = at
    return previous


def summarize(snapshot):
    if not isinstance(snapshot, dict) or snapshot.get('simulated') is not True:
        raise ValueError('emulator did not return an explicitly simulated snapshot')
    account = snapshot['account']
    exact = [int(account[name]) for name in ('spent_pico', 'reserved_pico', 'available_pico')]
    if (any(amount < 0 for amount in exact)
            or sum(exact) != account['initial_cash_micros'] * 1_000_000):
        raise ValueError('emulator exact collateral conservation failed')
    pending = snapshot['pending_events']
    orders = snapshot['orders']
    fault = snapshot['fault']
    return {
        'account': account,
        'venue_stats': snapshot['venue_stats'],
        'execution_stats': snapshot['stats'],
        'final_time_ms': snapshot['now_ms'],
        'cleanup': {
            'settled': pending == 0 and not orders and exact[1] == 0 and fault is None,
            'pending_events': pending,
            'active_orders': len(orders),
            'fault': fault,
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tape', type=Path, required=True)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    if not args.tape.is_file():
        raise ValueError('tape must be a regular file')
    if args.tape.stat().st_size > MAX_TAPE_BYTES:
        raise ValueError('tape exceeds bound')
    source = json.loads(args.tape.read_text(encoding='utf-8'))
    last_time = validate_source(source)
    scenarios = []
    for post, cancel, ack, fill in ((0, 0, 0, 0), (25, 25, 15, 30), (100, 100, 50, 100), (250, 500, 100, 250)):
        tape = copy.deepcopy(source)
        tape['latency'] = dict(post_ms=post, cancel_ms=cancel, ack_ms=ack, fill_ms=fill, timeout_ms=1500)
        end = last_time + 10_000
        tape['steps'].extend([{'at_ms': end, 'op': 'cancel_all'}, {'at_ms': end + 10_000, 'op': 'advance'}])
        # Golden expectations describe the original latency, not this sensitivity run.
        tape.pop('expected', None)
        encoded = json.dumps(tape).encode('utf-8')
        if len(encoded) > MAX_TAPE_BYTES:
            raise ValueError('scenario tape exceeds emulator byte bound after cleanup')
        with tempfile.TemporaryDirectory(prefix='mm2-scenario-') as directory:
            path = Path(directory) / 'tape.json'
            path.write_bytes(encoded)
            result = subprocess.run([args.binary, str(path)], capture_output=True, text=True, timeout=30)
            if result.returncode: raise ValueError('emulator scenario failed: ' + result.stderr[:256])
            snapshot = json.loads(result.stdout)
            scenarios.append({'configured_latency_ms': tape['latency'], **summarize(snapshot)})
    with args.out.open('x', encoding='utf-8') as output:
        json.dump({'schema_version': 1,
                   'purpose': 'fixed synthetic event tape accounting and execution sensitivity',
                   'scoring_eligible': False, 'live_eligible': False,
                   'latencies_are_configured_not_measured': True,
                   'strategy_recomputed': False, 'observed_venue_data': False,
                   'economic_return_evaluated': False,
                   'limitations': [
                       'Synthetic fills and fixed intents do not validate attainable returns.',
                       'Configured delays are assumptions, not measurements of a host or venue.',
                       'Cleanup cancels orders; it does not settle or value remaining outcome tokens.',
                       'A long injected fault may remain unsettled and is exposed in cleanup.',
                   ],
                   'tape': str(args.tape), 'scenarios': scenarios}, output, indent=2)

if __name__ == '__main__': main()
