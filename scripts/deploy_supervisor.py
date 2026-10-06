#!/usr/bin/env python3
"""Own the AWS Compose lifecycle; recover the engine only through a fresh systemd start."""
import argparse
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import threading
import tempfile
import time

from deploy_host import CONFIG_ROOT, STATE_ROOT, check_state_mount, image_environment, preflight

PROJECT = "mm2-production"
SERVICES = ("engine", "console", "proxy")
POLL_SECONDS = 1
SECONDARY_BACKOFF_SECONDS = 5
START_TIMEOUT_SECONDS = 30
CONTAINER_ID = re.compile(r"[0-9a-f]{64}")
CACHE_FILE = Path("/run/mm2/owned-containers")


class EngineIntegrityError(ValueError):
    """A second engine can mutate the funded account; this is a whole-unit failure."""


class EngineMetadataTimeout(subprocess.SubprocessError):
    """The tracked engine process could not be verified within the fixed deadline."""


def failure_class(error):
    """Only fixed public categories enter the journal; never exception arguments/output."""
    if isinstance(error, EngineMetadataTimeout):
        return "engine_metadata_timeout"
    if isinstance(error, EngineIntegrityError):
        return "engine_integrity_error"
    if isinstance(error, subprocess.TimeoutExpired):
        return "command_timeout"
    if isinstance(error, subprocess.CalledProcessError):
        return "command_failed"
    if isinstance(error, OSError):
        return "os_error"
    if isinstance(error, ValueError):
        return "invalid_lifecycle_metadata"
    return "subprocess_error"


def containers(run=subprocess.run, include_running=False, allow_duplicates=False, timeout=0.4,
               isolate_secondaries=False):
    """Read only bounded process identities; never inspect Env, logs or HTTP health."""
    result = run(["docker", "ps", "--all", "--no-trunc", "--format",
                  '{{.ID}} {{.Label "com.docker.compose.service"}} {{.State}}', "--filter",
                  "label=com.docker.compose.project=" + PROJECT, "--filter", "label=com.docker.compose.oneoff=False"],
                 check=True, capture_output=True, text=True, timeout=timeout)
    if len(result.stdout) > 4096:
        raise ValueError("production container inventory exceeds its bound")
    found = {service: [] for service in SERVICES}
    flags = {service: False for service in SERVICES}
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) != 3 or not CONTAINER_ID.fullmatch(fields[0]) or fields[1] not in found or fields[2] not in {
                "created", "restarting", "running", "removing", "paused", "exited", "dead"}:
            raise ValueError("unexpected production container identity")
        found[fields[1]].append(fields[0])
        flags[fields[1]] = fields[2] in {"running", "paused"}
    if allow_duplicates:
        return [identity for ids in found.values() for identity in ids]
    if len(found["engine"]) > 1:
        raise EngineIntegrityError("duplicate production engine identity")
    if isolate_secondaries:
        for service in ("console", "proxy"):
            if len(found[service]) > 1:
                flags[service] = None  # Degraded: no recovery mutation, engine supervision continues.
                found[service] = []
    if any(len(ids) > 1 for ids in found.values()):
        raise ValueError("duplicate production service identity")
    identities = {service: ids[0] if ids else None for service, ids in found.items()}
    return (identities, flags) if include_running else identities


def running(container_ids, run=subprocess.run):
    identities = [identity for identity in container_ids.values() if identity]
    if not identities:
        return {service: False for service in SERVICES}
    result = run(["docker", "inspect", "--format", "{{.Id}} {{.State.Running}}", *identities],
                 check=True, capture_output=True, text=True, timeout=0.4)
    if len(result.stdout) > 4096:
        raise ValueError("production process metadata exceeds its bound")
    flags = {}
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) != 2 or fields[0] not in identities or fields[0] in flags or fields[1] not in {"true", "false"}:
            raise ValueError("unexpected production process metadata")
        flags[fields[0]] = fields[1] == "true"
    if set(flags) != set(identities):
        raise ValueError("production process metadata is incomplete")
    return {service: flags.get(identity, False) for service, identity in container_ids.items()}


def kill_helper(process):
    # Each helper owns its session so a timed-out Compose child cannot restart after stack cleanup.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=1)


def save_ids(path, identities):
    values = sorted({identity for identity in identities.values() if identity})
    if len(values) > 3 or any(not CONTAINER_ID.fullmatch(identity) for identity in values):
        raise ValueError("unexpected cached production identities")
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="ascii", dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            os.chmod(temporary, 0o600)
            stream.write("".join(identity + "\n" for identity in values))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def saved_ids(path):
    if not path.exists():
        return []
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > 256:
        raise ValueError("cached production identities must be private, bounded and root-owned")
    identities = path.read_text(encoding="ascii").splitlines()
    if len(identities) > 3 or len(set(identities)) != len(identities) or any(not CONTAINER_ID.fullmatch(identity) for identity in identities):
        raise ValueError("cached production identity is invalid")
    return identities


class Supervisor:
    def __init__(self, release, config=CONFIG_ROOT, state=STATE_ROOT, run=subprocess.run,
                 launch=subprocess.Popen, gate=preflight, clock=time.monotonic, kill=kill_helper, cache=CACHE_FILE):
        self.release = release
        self.config = config
        self.state = state
        self.run = run
        self.launch = launch
        self.gate = gate
        self.clock = clock
        self.kill = kill
        self.cache = cache
        self.cached = None
        self.environment = None
        self.engine_id = None
        self.pending = {}
        self.retry_at = {service: 0 for service in ("console", "proxy")}
        self.degraded = set()

    def compose(self, service):
        return ["docker", "compose", "--project-name", PROJECT, "--project-directory", str(self.release),
                "--env-file", str(self.config / "runtime.env"), "-f", "infra/aws/compose.yaml", "up",
                "--detach", "--no-deps", "--no-build", "--pull", "never", service]

    def start(self):
        # This method runs again in every new unit process, including after host/Docker/engine failure.
        manifest = self.gate(self.release, config=self.config, state=self.state, run=self.run)
        self.environment = {key: value for key, value in os.environ.items() if not key.startswith("MM2_")}
        self.environment.update(image_environment(manifest))
        _, flags = containers(self.run, include_running=True, isolate_secondaries=True)
        if flags["engine"]:
            raise ValueError("an existing engine must be stopped through the unit before startup")
        self.run(self.compose("engine"), cwd=self.release, env=self.environment,
                 check=True, capture_output=True, timeout=START_TIMEOUT_SECONDS)
        identities = containers(self.run, isolate_secondaries=True)
        flags = running({"engine": identities["engine"]}, self.run)
        if not identities["engine"] or not flags["engine"]:
            raise ValueError("engine did not start")
        self.engine_id = identities["engine"]
        self.remember(identities)
        print("Verified engine process started; owner Resume remains required.", flush=True)

    def step(self):
        try:
            engine_flags = running({"engine": self.engine_id}, self.run)
        except subprocess.TimeoutExpired:
            # Keep the same fail-closed behavior/deadline; distinguish it from a helper timeout.
            raise EngineMetadataTimeout() from None
        if not engine_flags["engine"]:
            # No in-process engine restart: systemd cleanup and a new full preflight own recovery.
            raise ValueError("engine exited or changed identity")
        now = self.clock()
        for service, (process, deadline) in list(self.pending.items()):
            completed = process.poll()
            if completed is None and now < deadline:
                continue
            if completed is None:
                self.kill(process)
            del self.pending[service]
            self.retry_at[service] = now + SECONDARY_BACKOFF_SECONDS
        try:
            identities, flags = containers(self.run, include_running=True, isolate_secondaries=True)
        except EngineIntegrityError:
            raise
        except (OSError, ValueError, subprocess.SubprocessError):
            if "inventory" not in self.degraded:
                print("Secondary inventory unavailable; engine process unchanged.", file=sys.stderr, flush=True)
                self.degraded.add("inventory")
            return
        self.degraded.discard("inventory")
        if identities["engine"] != self.engine_id:
            raise EngineIntegrityError("engine changed identity")
        self.remember(identities)
        for service in ("console", "proxy"):
            if flags[service] is None:
                if service in self.pending:
                    self.kill(self.pending.pop(service)[0])
                if service not in self.degraded:
                    print("Duplicate " + service + " is degraded; engine process unchanged.", file=sys.stderr, flush=True)
                    self.degraded.add(service)
                continue
            self.degraded.discard(service)
            if flags[service] or service in self.pending or now < self.retry_at[service]:
                continue
            # The helper checks the mount before Compose. Slow storage/secondary operations never
            # block the engine metadata loop; only these two bounded helper processes can exist.
            self.retry_at[service] = now + SECONDARY_BACKOFF_SECONDS
            try:
                process = self.launch([sys.executable, str(Path(__file__).resolve()), "recover", "--release",
                                       str(self.release), "--service", service], cwd=self.release, env=self.environment,
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
                self.pending[service] = (process, now + START_TIMEOUT_SECONDS)
                print("Recovering " + service + " independently; engine process unchanged.", flush=True)
            except (OSError, ValueError, subprocess.SubprocessError):
                print(service + " recovery deferred; inspect its process and persistent mount.", file=sys.stderr, flush=True)

    def remember(self, identities):
        if self.cache is not None and identities != self.cached:
            try:
                save_ids(self.cache, identities)
            except (OSError, ValueError):
                if self.cached is None or self.cached.get("engine") != self.engine_id:
                    raise
                if "cache" not in self.degraded:
                    print("Container cache update deferred; previous verified engine identity retained.", file=sys.stderr, flush=True)
                    self.degraded.add("cache")
                return
            self.degraded.discard("cache")
            self.cached = identities.copy()

    def close(self):
        # Cancel only our Compose client processes. Unit ExecStopPost stops the owned stack.
        for process, _ in self.pending.values():
            if process.poll() is None:
                self.kill(process)
        self.pending.clear()

    def serve(self, stop):
        try:
            self.start()
            while not stop.is_set():
                tick = self.clock()
                self.step()
                stop.wait(max(0, POLL_SECONDS - (self.clock() - tick)))
        finally:
            self.close()


def stop_stack(run=subprocess.run, cache=CACHE_FILE):
    # Cleanup works even when preflight/config/private env is invalid. No volume removal or creation.
    try:
        identities = set(saved_ids(cache)) if cache is not None else set()
    except (OSError, ValueError):
        identities = set()  # An untrusted cache cannot prevent independent label-validated cleanup.
    discovered = False
    for _ in range(2):
        try:
            identities.update(containers(run, allow_duplicates=True, timeout=10))
            discovered = True
            break
        except (OSError, ValueError, subprocess.SubprocessError):
            continue
    if not discovered and not identities:
        raise ValueError("no verified production identities available for cleanup")
    if identities:
        stopped = run(["docker", "stop", "--time", "30", *sorted(identities)], check=False, capture_output=True, timeout=60)
        if stopped.returncode != 0:
            # Missing old cached IDs may yield exit 1 after stopping all remaining owned containers.
            _, flags = containers(run, include_running=True, timeout=10)
            if any(flags.values()):
                raise ValueError("production cleanup did not stop every owned process")


def recover_secondary(release, service, config=CONFIG_ROOT, state=STATE_ROOT, run=subprocess.run,
                      mount_check=check_state_mount):
    if service not in {"console", "proxy"}:
        raise ValueError("only a secondary service may recover independently")
    mount_check(state)
    # Exact image IDs arrive from the parent that completed full preflight, never a private env file.
    if any(not re.fullmatch(r"sha256:[0-9a-f]{64}", os.environ.get("MM2_" + role.upper() + "_IMAGE", ""))
           for role in SERVICES):
        raise ValueError("secondary recovery requires the verified immutable image identities")
    supervisor = Supervisor(release, config=config, state=state)
    run(supervisor.compose(service), cwd=release, check=True, capture_output=True,
        timeout=START_TIMEOUT_SECONDS)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    command = commands.add_parser("run")
    command.add_argument("--release", required=True, type=Path)
    commands.add_parser("stop")
    recover = commands.add_parser("recover")
    recover.add_argument("--release", required=True, type=Path)
    recover.add_argument("--service", required=True, choices=("console", "proxy"))
    args = parser.parse_args(argv)
    try:
        if not hasattr(os, "geteuid") or os.geteuid() != 0:
            raise ValueError("supervisor requires Linux root")
        if args.action == "stop":
            stop_stack()
        elif args.action == "recover":
            recover_secondary(args.release, args.service)
        else:
            stop = threading.Event()
            for signum in (signal.SIGTERM, signal.SIGINT):
                signal.signal(signum, lambda *_: stop.set())
            Supervisor(args.release).serve(stop)
        return 0
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print("Production lifecycle failed (" + failure_class(error)
              + "); systemd must stop the stack and repeat full preflight before recovery.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
