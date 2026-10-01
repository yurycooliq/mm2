#!/usr/bin/env python3
"""Bounded read-only source check on the current host; never a location benchmark."""
import argparse
import json
import time
import uuid
from verify_stack import ROOT, docker, inspect, request, wait_for


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=int, default=35)
    args = parser.parse_args()
    if not 5 <= args.seconds <= 60:
        parser.error('seconds must be between 5 and 60')
    owner = uuid.uuid4().hex
    label = 'mm2.public-check'
    volume = 'mm2-public-' + owner[:12]
    token = uuid.uuid4().hex * 2
    container = None
    created = False
    try:
        docker('volume', 'create', '--label', label + '=' + owner, volume)
        created = True
        container = docker('run', '--detach', '--label', label + '=' + owner,
                           '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
                           '--memory', '512m', '--cpus', '1', '--pids-limit', '64',
                           '--tmpfs', '/tmp:rw,noexec,nosuid,size=16m', '--publish', '127.0.0.1::8088',
                           '--env', 'MM2_ENGINE_API_TOKEN=' + token,
                           '--mount', 'type=volume,source=' + volume + ',target=/data',
                           '--mount', 'type=bind,source=' + str(ROOT / 'config/public.toml') +
                           ',target=/app/config.toml,readonly', 'mm2-engine:local')
        info = inspect('container', container)
        base = 'http://127.0.0.1:' + info['NetworkSettings']['Ports']['8088/tcp'][0]['HostPort']
        state = lambda: request(base, '/v1/state', token)[0]
        wait_for(lambda: state().get('api_ready'))
        deadline = time.monotonic() + args.seconds
        phases = []
        ready_seen = False
        while time.monotonic() < deadline:
            current = state()
            assert current['live_eligible'] is False and current['actual_orders'] == []
            assert current['execution_mode'] == 'shadow' and current['simulation'] is None
            phase = current.get('source_status', {})
            if not phases or phases[-1].get('phase') != phase.get('phase'):
                phases.append(phase)
            ready_seen |= bool(current.get('shadow_ready'))
            time.sleep(0.25)
        print(json.dumps({'host': 'current Docker host', 'seconds': args.seconds,
                          'api_ready': current['api_ready'], 'shadow_ready_seen': ready_seen,
                          'source_phases': phases, 'final_stop_reasons': current.get('stop_reasons'),
                          'live_eligible': False, 'live_orders': 0,
                          'authenticated_order_latency_measured': False}), flush=True)
    finally:
        if container:
            info = inspect('container', container)
            assert info['Config']['Labels'].get(label) == owner
            docker('container', 'rm', '--force', container)
        if created:
            assert inspect('volume', volume).get('Labels', {}).get(label) == owner
            docker('volume', 'rm', volume)


if __name__ == '__main__':
    main()
