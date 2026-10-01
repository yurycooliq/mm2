#!/usr/bin/env python3
"""Real image integration, with ownership-scoped disposable Docker resources only."""
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]

def docker(*args):
    result = subprocess.run(['docker', *args], capture_output=True, text=True, timeout=90)
    if result.returncode:
        # Fixture tokens are not printed; broad container stdout/environment dumps are avoided.
        raise RuntimeError('Docker operation failed: ' + ' '.join(args[:2]))
    return result.stdout.strip()

def inspect(kind, name):
    return json.loads(docker(kind, 'inspect', name))[0]

def request(base, path, token=None, payload=None, method=None, cookie=None, origin=None, expected=200):
    headers = {}
    if token: headers['Authorization'] = 'Bearer ' + token
    if cookie: headers['Cookie'] = cookie
    if origin: headers['Origin'] = origin
    data = None
    if payload is not None:
        headers['Content-Type'] = 'application/json'
        data = json.dumps(payload).encode()
    req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
    try:
        response = urllib.request.urlopen(req, timeout=8)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        body = response.read(2_097_153)
        assert len(body) <= 2_097_152, 'unbounded HTTP response'
        assert response.status == expected, f'{path}: expected {expected}, got {response.status}'
        try: value = json.loads(body)
        except (ValueError, UnicodeDecodeError): value = body.decode()
        return value, dict(response.headers)

def wait_for(predicate, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value: return value
        except (OSError, AssertionError): pass
        time.sleep(0.1)
    raise AssertionError('bounded wait expired')

def main():
    owner = uuid.uuid4().hex
    label = 'mm2.verification'
    prefix = 'mm2-verify-' + owner[:12]
    engine_token, console_token = uuid.uuid4().hex * 2, uuid.uuid4().hex * 2
    resources = []
    checks = []
    started = time.monotonic()
    def passed(name): checks.append(name); print('PASS ' + name, flush=True)
    try:
        network = docker('network', 'create', '--label', label + '=' + owner, prefix)
        resources.append(('network', network))
        for suffix in ('engine-data', 'console-data'):
            name = prefix + '-' + suffix
            docker('volume', 'create', '--label', label + '=' + owner, name)
            resources.append(('volume', name))
        common = ['--detach', '--label', label + '=' + owner, '--network', prefix,
                  '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
                  '--tmpfs', '/tmp:rw,noexec,nosuid,size=16m', '--pids-limit', '64']
        engine = docker('run', *common, '--name', prefix + '-engine', '--network-alias', 'engine',
                        '--publish', '127.0.0.1::8088', '--memory', '512m', '--cpus', '1',
                        '--env', 'MM2_ENGINE_API_TOKEN=' + engine_token,
                        '--mount', 'type=volume,source=' + prefix + '-engine-data,target=/data',
                        '--mount', 'type=bind,source=' + str(ROOT / 'config/demo.toml') + ',target=/app/config.toml,readonly',
                        'mm2-engine:local')
        resources.append(('container', engine))
        info = inspect('container', engine)
        engine_base = 'http://127.0.0.1:' + info['NetworkSettings']['Ports']['8088/tcp'][0]['HostPort']
        def state(): return request(engine_base, '/v1/state', engine_token)[0]
        boot = wait_for(lambda: state().get('api_ready') and state())
        assert boot['mode'] == 'paused' and boot['live_eligible'] is False
        assert boot['execution_mode'] == 'emulator' and boot['simulation']['simulated'] is True
        request(engine_base, '/v1/state', expected=401)
        passed('paused emulator boot and engine authentication')
        console = docker('run', *common, '--name', prefix + '-console', '--publish', '127.0.0.1::8080',
                         '--memory', '128m', '--cpus', '0.25',
                         '--env', 'MM2_ENGINE_URL=http://engine:8088',
                         '--env', 'MM2_ENGINE_API_TOKEN=' + engine_token,
                         '--env', 'MM2_CONSOLE_TOKEN=' + console_token,
                         '--env', 'MM2_ALLOWED_ORIGINS=http://127.0.0.1:8080',
                         '--mount', 'type=volume,source=' + prefix + '-console-data,target=/data',
                         'mm2-console:local')
        resources.append(('container', console))
        info = inspect('container', console)
        console_base = 'http://127.0.0.1:' + info['NetworkSettings']['Ports']['8080/tcp'][0]['HostPort']
        origin = console_base
        # Origin must match the published random port; recreate with that exact allowlist before auth.
        docker('stop', '--time', '5', console)
        checked = inspect('container', console)
        assert checked['Config']['Labels'].get(label) == owner
        docker('rm', console)
        resources.remove(('container', console))
        console = docker('run', *common, '--name', prefix + '-console',
                         '--publish', '127.0.0.1:' + origin.rsplit(':', 1)[1] + ':8080',
                         '--memory', '128m', '--cpus', '0.25',
                         '--env', 'MM2_ENGINE_URL=http://engine:8088', '--env', 'MM2_ENGINE_API_TOKEN=' + engine_token,
                         '--env', 'MM2_CONSOLE_TOKEN=' + console_token,
                         '--env', 'MM2_ALLOWED_ORIGINS=' + origin,
                         '--mount', 'type=volume,source=' + prefix + '-console-data,target=/data', 'mm2-console:local')
        resources.append(('container', console))
        wait_for(lambda: request(console_base, '/healthz')[0])
        request(console_base, '/api/state', expected=401)
        request(console_base, '/api/auth/local', payload={'token': 'wrong'}, origin=origin, expected=401)
        _, headers = request(console_base, '/api/auth/local', payload={'token': console_token}, origin=origin)
        cookie = headers['Set-Cookie'].split(';', 1)[0]
        assert 'HttpOnly' in headers['Set-Cookie'] and 'SameSite=Strict' in headers['Set-Cookie']
        request(console_base, '/api/control', payload={'id': 'csrf'}, cookie=cookie,
                origin='https://untrusted.invalid', expected=403)
        passed('console owner session and cross-origin mutation refusal')
        page = request(console_base, '/')[0]
        assert '<' in page and engine_token not in page and console_token not in page
        envelope = wait_for(lambda: (value := request(console_base, '/api/state', cookie=cookie)[0]).get('connected') and value)
        assert engine_token not in json.dumps(envelope)
        passed('built dashboard and private token separation')
        current = state()
        command = {'id': uuid.uuid4().hex, 'expected_version': current['command_version'], 'action': 'resume'}
        ack = request(console_base, '/api/control', payload=command, cookie=cookie, origin=origin)[0]
        retry = request(console_base, '/api/control', payload=command, cookie=cookie, origin=origin)[0]
        assert ack['receipt'] == retry['receipt']
        request(console_base, '/api/control', payload={**command, 'action': 'kill'}, cookie=cookie, origin=origin, expected=409)
        passed('durable owner command retry and changed-payload conflict')
        filled = wait_for(lambda: (value := state())['simulation']['venue_stats']['simulated_filled_micros'] > 0
                          and value['simulation']['account']['spent_pico'] != '0' and value)
        before_fills = filled['simulation']['venue_stats']['simulated_filled_micros']
        assert filled['simulation']['account']['spent_pico'] != '0'
        passed('emulated post, queue and delivered fill accounting')
        docker('stop', '--time', '5', console)
        independent = wait_for(lambda: (value := state())['simulation']['venue_stats']['simulated_filled_micros'] > before_fills and value, 20)
        assert independent['mode'] == 'shadow'
        passed('engine continues when console and statistics storage are stopped')
        kill_request = {'id': uuid.uuid4().hex, 'expected_version': state()['command_version'], 'action': 'kill'}
        kill = request(engine_base, '/v1/control', engine_token, kill_request)[0]
        assert kill['state']['mode'] == 'killed'
        settled = wait_for(lambda: (value := state())['simulation']['pending_events'] == 0 and value)
        inventory = (settled['simulation']['account']['up_inventory_micros'], settled['simulation']['account']['down_inventory_micros'])
        spent = settled['simulation']['account']['spent_pico']
        docker('kill', engine)
        docker('start', engine)
        # Docker may assign a new ephemeral host port when the container restarts.
        restarted_info = inspect('container', engine)
        engine_base = 'http://127.0.0.1:' + restarted_info['NetworkSettings']['Ports']['8088/tcp'][0]['HostPort']
        recovered = wait_for(lambda: (value := state()).get('api_ready') and value['boot_id'] != settled['boot_id'] and value)
        assert recovered['mode'] == 'killed'
        assert (recovered['simulation']['account']['up_inventory_micros'], recovered['simulation']['account']['down_inventory_micros']) == inventory
        assert recovered['simulation']['account']['spent_pico'] == spent
        assert request(engine_base, '/v1/control', engine_token, kill_request)[0]['receipt'] == kill['receipt']
        passed('abrupt restart recovers simulation ledger, kill latch and receipts')
        fence = docker('run', '--detach', '--label', label + '=' + owner, '--env', 'MM2_ENGINE_API_TOKEN=' + engine_token,
                       '--mount', 'type=volume,source=' + prefix + '-engine-data,target=/data',
                       '--mount', 'type=bind,source=' + str(ROOT / 'config/demo.toml') + ',target=/app/config.toml,readonly', 'mm2-engine:local')
        resources.append(('container', fence))
        wait_for(lambda: not inspect('container', fence)['State']['Running'])
        assert inspect('container', fence)['State']['ExitCode'] != 0
        passed('second engine refuses shared journal ownership')
        for name in (engine, console):
            value = inspect('container', name)
            assert value['HostConfig']['ReadonlyRootfs'] and value['Config']['User'] in ('10001:10001', '10002:10002')
        passed('non-root images and separate read-only runtime boundaries')
        docker('start', console)
        wait_for(lambda: request(console_base, '/healthz')[0])
        request(console_base, '/api/state', cookie=cookie, expected=401)
        passed('console restart invalidates ephemeral owner session')
        stop_started = time.monotonic()
        docker('stop', '--time', '10', engine)
        stop_ms = round((time.monotonic() - stop_started) * 1000)
        assert inspect('container', engine)['State']['ExitCode'] == 0 and stop_ms < 10_000
        passed('graceful engine stop within configured deadline')
        print(json.dumps({'checks': len(checks), 'elapsed_ms': round((time.monotonic()-started)*1000),
                          'engine_stop_ms': stop_ms, 'live_orders': 0}), flush=True)
    except Exception:
        for kind, name in resources:
            if kind == 'container':
                try:
                    info = inspect(kind, name)
                    assert info['Config']['Labels'].get(label) == owner
                    logged = subprocess.run(['docker', 'logs', '--tail', '12', name],
                                            capture_output=True, text=True, timeout=10)
                    tail = (logged.stdout + logged.stderr)[-16_384:]
                    for secret in (engine_token, console_token):
                        tail = tail.replace(secret, '[redacted fixture]')
                    print(json.dumps({'verification_container': info['Name'],
                                      'running': info['State']['Running'],
                                      'exit_code': info['State']['ExitCode'],
                                      'sanitized_log_tail': tail}), flush=True)
                except Exception:
                    pass
        try:
            failed_state = state()
            print(json.dumps({'verification_engine_state': {key: failed_state.get(key) for key in
                              ('mode', 'api_ready', 'fault', 'simulation_error', 'stop_reasons')}}), flush=True)
        except Exception:
            pass
        raise
    finally:
        errors = []
        for kind, name in reversed(resources):
            try:
                value = inspect(kind, name)
                labels = value['Config'].get('Labels', {}) if kind == 'container' else value.get('Labels', {})
                if labels.get(label) != owner: raise RuntimeError('refusing resource with another owner')
                docker(kind, 'rm', *(['--force'] if kind == 'container' else []), name)
            except Exception as error: errors.append(str(error))
        if errors: raise RuntimeError('verification cleanup errors: ' + '; '.join(errors))

if __name__ == '__main__': main()
