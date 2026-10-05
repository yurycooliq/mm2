"""Release and host boundaries: no dirty/secret bundle, wrong platform or implicit live activation."""
from contextlib import redirect_stdout
from io import StringIO
import json
import os
from pathlib import Path
import shlex
import stat
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import deploy_host
import deploy_package
import deploy_supervisor


class DeployTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.release = self.root / "release"
        self.release.mkdir()
        self.images = {role: "sha256:" + character * 64 for role, character in (("engine", "a"), ("console", "b"), ("proxy", "c"))}
        for name in (*deploy_package.PUBLIC_FILES, "images.tar"):
            path = self.release / name
            path.parent.mkdir(exist_ok=True, parents=True)
            path.write_bytes(b"public fixture\n")
        self.manifest = {"schema": 1, "source_commit": "d" * 40, "platform": "linux/amd64", "images": self.images,
                         "files": {name: deploy_package.digest(self.release / name) for name in (*deploy_package.PUBLIC_FILES, "images.tar")}}
        self.write_manifest()

    def write_manifest(self):
        (self.release / "release-manifest.json").write_text(json.dumps(self.manifest))

    def tearDown(self):
        self.temp.cleanup()

    def test_complete_allowlisted_release_verified(self):
        self.assertEqual(deploy_package.verify_release(self.release), self.manifest)

    def test_secret_or_incomplete_extra_file_rejects_release(self):
        for name in (".env", "INCOMPLETE"):
            with self.subTest(name=name):
                extra = self.release / name
                extra.write_text("private-fixture")
                with self.assertRaises(ValueError):
                    deploy_package.verify_release(self.release)
                extra.unlink()

    def test_checksum_or_file_set_tampering_rejects(self):
        (self.release / "images.tar").write_bytes(b"changed image")
        with self.assertRaises(ValueError):
            deploy_package.verify_release(self.release)
        self.manifest["files"].pop("config/both.toml")
        self.write_manifest()
        with self.assertRaises(ValueError):
            deploy_package.verify_release(self.release)

    def test_manifest_identity_cannot_use_mutable_tag_or_wrong_platform(self):
        for field, value in (("platform", "linux/arm64"), ("source_commit", "main")):
            with self.subTest(field=field):
                original = self.manifest[field]
                self.manifest[field] = value
                self.write_manifest()
                with self.assertRaises(ValueError):
                    deploy_package.verify_release(self.release)
                self.manifest[field] = original
        self.manifest["images"]["engine"] = "mm2-engine:latest"
        self.write_manifest()
        with self.assertRaises(ValueError):
            deploy_package.verify_release(self.release)

    def test_export_refuses_existing_path_and_dirty_source_before_docker(self):
        with patch.object(deploy_package.subprocess, "run") as runner:
            with self.assertRaises(ValueError):
                deploy_package.export_release(self.release, self.images)
            runner.assert_not_called()
        runner = lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, " M private-change\n", "")
        with self.assertRaises(ValueError):
            deploy_package.export_release(self.root / "new-release", self.images, run=runner)
        self.assertFalse((self.root / "new-release").exists())

    def test_image_platform_check_uses_no_environment_output(self):
        calls = []
        def runner(command, **kwargs):
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, self.images["engine"] + " linux arm64\n", "")
        with self.assertRaises(ValueError):
            deploy_package.image_identity("reviewed-local", runner)
        self.assertEqual(calls[0][4], "{{.Id}} {{.Os}} {{.Architecture}}")
        self.assertNotIn("Env", " ".join(calls[0]))

    def test_application_images_require_exact_reviewed_revision_label(self):
        for label in ("", "unknown", "e" * 40):
            def runner(command, **kwargs):
                return subprocess.CompletedProcess(command, 0, self.images["engine"] + " linux amd64 " + label + "\n", "")
            with self.subTest(label=label), self.assertRaises(ValueError):
                deploy_package.image_identity("application", runner, "d" * 40)
        def runner(command, **kwargs):
            return subprocess.CompletedProcess(command, 0, self.images["engine"] + " linux amd64 " + "d" * 40 + "\n", "")
        self.assertEqual(deploy_package.image_identity("application", runner, "d" * 40), self.images["engine"])

    def test_export_rejects_old_image_before_creating_release(self):
        def runner(command, **kwargs):
            if command[:3] == ["git", "status", "--porcelain"]:
                output = ""
            elif command[:2] == ["git", "rev-parse"]:
                output = "d" * 40
            elif command[:2] == ["git", "ls-files"]:
                output = "\n".join(deploy_package.PUBLIC_FILES)
            else:
                output = self.images["engine"] + " linux amd64 " + "e" * 40
            return subprocess.CompletedProcess(command, 0, output, "")
        destination = self.root / "old-image-export"
        with self.assertRaises(ValueError):
            deploy_package.export_release(destination, self.images, root=self.release, run=runner)
        self.assertFalse(destination.exists())

    def test_private_environment_rejects_interpolation_and_image_overrides(self):
        path = self.root / "runtime.env"
        base = "MM2_ENGINE_API_TOKEN=" + "a" * 64 + "\nMM2_CONSOLE_TOKEN=" + "b" * 64 + "\nMM2_DOMAIN=owner.example.com\n"
        metadata = SimpleNamespace(st_mode=stat.S_IFREG | 0o600, st_uid=0, st_size=512)
        with patch.object(Path, "lstat", return_value=metadata):
            path.write_text(base)
            self.assertEqual(deploy_host.private_environment(path)["MM2_DOMAIN"], "owner.example.com")
            path.write_text(base + "MM2_POLYGON_RPC_URL=https://rpc.example.com/path?api-key=fixture-secret\n")
            self.assertEqual(deploy_host.private_environment(path)["MM2_POLYGON_RPC_URL"], "https://rpc.example.com/path?api-key=fixture-secret")
            for extra in ("MM2_POLY_API_SECRET=${OTHER}\n", "MM2_ENGINE_IMAGE=sha256:other\n", "MM2_DOMAIN=another.example.com\n"):
                with self.subTest(extra=extra):
                    path.write_text(base + extra)
                    with self.assertRaises(ValueError):
                        deploy_host.private_environment(path)

    def test_private_environment_checks_root_ownership_and_exact_mode(self):
        for mode, uid in ((0o644, 0), (0o600, 1000)):
            metadata = SimpleNamespace(st_mode=stat.S_IFREG | mode, st_uid=uid, st_size=1)
            with patch.object(Path, "lstat", return_value=metadata), self.assertRaises(ValueError):
                deploy_host.private_environment(self.root / "unused")

    def test_private_relayer_credentials_are_paired_literal_and_address_bounded(self):
        path = self.root / "runtime.env"
        base = "MM2_ENGINE_API_TOKEN=" + "a" * 64 + "\nMM2_CONSOLE_TOKEN=" + "b" * 64 + "\nMM2_DOMAIN=owner.example.com\n"
        owner = "0x" + "b" * 40
        metadata = SimpleNamespace(st_mode=stat.S_IFREG | 0o600, st_uid=0, st_size=512)
        with patch.object(Path, "lstat", return_value=metadata):
            path.write_text(base + "MM2_POLY_RELAYER_API_KEY=fixture-relayer\nMM2_POLY_RELAYER_API_KEY_ADDRESS=" + owner + "\n")
            self.assertEqual(deploy_host.private_environment(path)["MM2_POLY_RELAYER_API_KEY_ADDRESS"], owner)
            for extra in ("MM2_POLY_RELAYER_API_KEY=fixture-relayer\n", "MM2_POLY_RELAYER_API_KEY_ADDRESS=" + owner + "\n",
                          "MM2_POLY_RELAYER_API_KEY=fixture-relayer\nMM2_POLY_RELAYER_API_KEY_ADDRESS=invalid\n",
                          "MM2_POLY_RELAYER_API_KEY=" + "x" * 1025 + "\nMM2_POLY_RELAYER_API_KEY_ADDRESS=" + owner + "\n",
                          "MM2_POLY_RELAYER_API_KEY=fixture-relayer\nMM2_POLY_RELAYER_API_KEY_ADDRESS=0x" + "0" * 40 + "\n"):
                path.write_text(base + extra)
                with self.assertRaises(ValueError):
                    deploy_host.private_environment(path)

    def test_deposit_host_preflight_requires_relayer_key_bound_to_controller_before_docker(self):
        config = self.root / "private"
        config.mkdir()
        profile = (deploy_package.ROOT / "config/trading.toml").read_text().replace("owner_authorized = false", "owner_authorized = true")
        (config / "active.toml").write_text(profile)
        expected = deploy_host.image_environment(self.manifest)
        (config / "release.env").write_text("".join(k + "=" + v + "\n" for k, v in sorted(expected.items())))
        controller = "0xa2300f310b59df6d0dec94576776ce5435bd0b3f"
        credentials = {key: "private-fixture-never-display" for key in (
            "MM2_POLY_PRIVATE_KEY", "MM2_POLY_API_KEY", "MM2_POLY_API_SECRET", "MM2_POLY_API_PASSPHRASE",
            "MM2_POLYGON_RPC_URL", "MM2_POLY_RELAYER_API_KEY")}
        credentials["MM2_POLY_RELAYER_API_KEY_ADDRESS"] = controller
        for fault in ("MM2_POLY_RELAYER_API_KEY", "MM2_POLY_RELAYER_API_KEY_ADDRESS", "foreign-owner"):
            invalid = dict(credentials)
            if fault == "foreign-owner":
                invalid["MM2_POLY_RELAYER_API_KEY_ADDRESS"] = "0x" + "a" * 40
            else:
                invalid.pop(fault)
            with self.subTest(fault=fault), patch.object(deploy_host, "check_state_mount"), \
                    patch.object(deploy_host, "private_environment", return_value=invalid):
                runner = unittest.mock.Mock()
                with self.assertRaises(ValueError):
                    deploy_host.preflight(self.release, config=config, run=runner)
                runner.assert_not_called()
        calls = []
        def runner(command, **kwargs):
            calls.append(command)
            output = command[-1] + " linux amd64" + (" " + "d" * 40 if "image.revision" in command[4] else "") + "\n" if command[1:3] == ["image", "inspect"] else ""
            return subprocess.CompletedProcess(command, 0, output, "")
        with patch.object(deploy_host, "check_state_mount"), patch.object(deploy_host, "private_environment", return_value=credentials):
            self.assertEqual(deploy_host.preflight(self.release, config=config, run=runner), self.manifest)
        self.assertTrue(calls)
        self.assertNotIn("private-fixture-never-display", json.dumps(calls))

    def test_compose_exposes_relayer_credentials_only_to_execution_container(self):
        for name in ("compose.yaml", "infra/aws/compose.yaml"):
            compose = (deploy_package.ROOT / name).read_text()
            engine, secondary = compose.split("  console:", 1)
            for key in ("MM2_POLY_RELAYER_API_KEY", "MM2_POLY_RELAYER_API_KEY_ADDRESS"):
                self.assertIn(key + ": ${" + key + ":-}", engine)
                self.assertNotIn(key, secondary)

    def test_activate_refuses_running_service_before_profile_or_state_mutation(self):
        install = self.root / "install"
        (install / "releases").mkdir(parents=True)
        selected = install / "releases" / ("d" * 40)
        self.release.rename(selected)
        runner = lambda command, **kwargs: subprocess.CompletedProcess(command, 0, "", "")
        with self.assertRaises(ValueError):
            deploy_host.activate(selected, "simulation", config=self.root / "private-config", install=install, run=runner)
        self.assertFalse((self.root / "private-config").exists())
        self.assertFalse((install / "current").exists())

    def test_preflight_config_check_has_no_network_or_credentials_and_compose_is_quiet(self):
        config = self.root / "private"
        config.mkdir()
        (config / "active.toml").write_text('mode="simulation"\nstate_dir="/data/simulation"\n')
        expected = deploy_host.image_environment(self.manifest)
        (config / "release.env").write_text("".join(k + "=" + v + "\n" for k, v in sorted(expected.items())))
        calls = []
        def runner(command, **kwargs):
            calls.append((command, kwargs))
            output = command[-1] + " linux amd64" + (" " + "d" * 40 if "image.revision" in command[4] else "") + "\n" if command[1:3] == ["image", "inspect"] else ""
            return subprocess.CompletedProcess(command, 0, output, "")
        with patch.object(deploy_host, "check_state_mount"), \
                patch.object(deploy_host, "private_environment", return_value={"MM2_DOMAIN": "owner.example.com"}), \
                patch.dict(os.environ, {"MM2_POLY_PRIVATE_KEY": "fixture-secret", "MM2_ENGINE_IMAGE": "unapproved:moving"}):
            self.assertEqual(deploy_host.preflight(self.release, config=config, state=self.root / "unused", run=runner), self.manifest)
        isolated = next(command for command, _ in calls if command[1] == "run")
        self.assertIn("none", isolated)
        self.assertEqual(isolated[-1], "--check")
        self.assertNotIn("fixture-secret", " ".join(isolated))
        command, arguments = calls[-1]
        self.assertEqual(command[-2:], ["config", "--quiet"])
        self.assertNotIn("MM2_POLY_PRIVATE_KEY", arguments["env"])
        self.assertEqual(arguments["env"]["MM2_ENGINE_IMAGE"], self.images["engine"])

    def test_unmounted_state_refuses_before_private_credentials_or_docker(self):
        with patch.object(deploy_host, "check_state_mount", side_effect=ValueError("unmounted")), \
                patch.object(deploy_host, "private_environment") as secrets, \
                patch.object(deploy_host.subprocess, "run") as runner:
            with self.assertRaises(ValueError):
                deploy_host.preflight(self.release)
        secrets.assert_not_called()
        runner.assert_not_called()

    def test_offline_template_retains_encrypted_state_and_exposes_only_https_foundation(self):
        template = json.loads((deploy_package.ROOT / "infra/aws/host.cloudformation.json").read_text())
        resources = template["Resources"]
        for name, resource in resources.items():
            if name == "SshIngress":
                self.assertNotIn("DeletionPolicy", resource)
                self.assertNotIn("UpdateReplacePolicy", resource)
                continue
            self.assertEqual(resource["DeletionPolicy"], "Retain")
            self.assertEqual(resource["UpdateReplacePolicy"], "Retain")
        self.assertTrue(resources["StateVolume"]["Properties"]["Encrypted"])
        self.assertNotIn("UserData", resources["Host"]["Properties"])
        self.assertEqual(resources["Host"]["Properties"]["MetadataOptions"]["HttpTokens"], "required")
        self.assertEqual({rule["FromPort"] for rule in resources["HostSecurityGroup"]["Properties"]["SecurityGroupIngress"]}, {80, 443})

    @unittest.skipUnless(os.name == "posix", "Linux installer shell execution")
    def test_runtime_installer_rejects_classic_identity_backend_without_installing_or_migrating(self):
        source = (deploy_package.ROOT / "infra/aws/install_runtime.sh").read_text()
        # Execute the complete older-version path; privileged commands cannot perform effects.
        isolated = 'id() { printf 0; }; docker() { return 96; };\n' + source
        for version in ("5:28.5.2-1~ubuntu.24.04~noble", "28.5.2-1~ubuntu.24.04~noble"):
            result = subprocess.run(["sh", "-s", "--", version, "29.1.0", "2.1.0", "0.30.0", "2.40.3"],
                                    input=isolated, capture_output=True, text=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("requires Docker Engine 29 or later", result.stderr)
            self.assertNotIn("Pinned Docker components", result.stdout)
        # Run the actual post-install body with a synthetic Docker result; no OS/apt/service calls.
        body = source.split("systemctl enable --now docker chrony\n", 1)[1]
        trace = self.root / "installer-actions"
        for backend in ("", "overlay2", "io.containerd.snapshotter.v1junk", "io.containerd.snapshotter.v1"):
            if trace.exists():
                trace.unlink()
            prelude = ("set -eu\n"
                       "docker() { printf '%s\\n' " + shlex.quote(backend) + "; }\n"
                       "install() { printf '%s\\n' \"$*\" >> " + shlex.quote(str(trace)) + "; }\n")
            result = subprocess.run(["sh", "-s"], input=prelude + body,
                                    capture_output=True, text=True, timeout=5)
            with self.subTest(backend=backend):
                if backend == "io.containerd.snapshotter.v1":
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn("No mm2 service started", result.stdout)
                    self.assertIn("/etc/mm2", trace.read_text())
                else:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("image store is unsupported", result.stderr)
                    self.assertIn("no daemon configuration was changed", result.stderr)
                    self.assertFalse(trace.exists())

    def test_production_unit_is_only_engine_restart_owner(self):
        unit = (deploy_package.ROOT / "infra/aws/mm2.service").read_text()
        compose = (deploy_package.ROOT / "infra/aws/compose.yaml").read_text()
        self.assertIn("Type=simple", unit)
        self.assertIn("Restart=on-failure", unit)
        self.assertIn("Environment=PYTHONDONTWRITEBYTECODE=1", unit)
        self.assertIn("ExecStart=/usr/bin/python3 scripts/deploy_supervisor.py run", unit)
        self.assertIn("ExecStopPost=/usr/bin/python3 scripts/deploy_supervisor.py stop", unit)
        self.assertIn("WantedBy=multi-user.target docker.service", unit)
        self.assertEqual(compose.count('restart: "no"'), 3)
        self.assertNotIn("service_healthy", compose)
        self.assertIn("scripts/deploy_supervisor.py", deploy_package.PUBLIC_FILES)


class SupervisorTests(unittest.TestCase):
    def setUp(self):
        self.ids = {service: character * 64 for service, character in (("engine", "a"), ("console", "b"), ("proxy", "c"))}
        self.images = {service: "sha256:" + identity for service, identity in self.ids.items()}
        self.flags = {service: False for service in deploy_supervisor.SERVICES}
        self.calls = []
        self.starts = []
        self.gates = []
        self.helpers = []
        self.now = 0
        self.kill_calls = []
        self.manifest = {"images": self.images}

    def runner(self, command, **arguments):
        self.calls.append((command, arguments))
        if command[:2] == ["docker", "ps"]:
            output = "\n".join(identity + " " + service + (" running" if self.flags[service] else " exited")
                               for service, identity in self.ids.items())
        elif command[:2] == ["docker", "inspect"]:
            self.assertEqual(command[3], "{{.Id}} {{.State.Running}}")
            self.assertEqual(command[4:], [self.ids["engine"]])
            output = self.ids["engine"] + (" true" if self.flags["engine"] else " false")
        else:
            self.assertEqual(command[:2], ["docker", "compose"])
            self.assertIn("--no-deps", command)
            self.starts.append(command[-1])
            self.flags[command[-1]] = True
            output = "private fixture output that must never be printed"
        return subprocess.CompletedProcess(command, 0, output, "")

    def gate(self, release, **arguments):
        self.gates.append((release, arguments))
        return self.manifest

    def launch(self, command, **arguments):
        self.assertEqual(arguments["stdout"], subprocess.DEVNULL)
        self.assertEqual(arguments["stderr"], subprocess.DEVNULL)
        self.assertTrue(arguments["start_new_session"])
        process = SimpleNamespace(returncode=None)
        process.poll = lambda: process.returncode
        self.helpers.append((command, arguments, process))
        return process

    def supervisor(self, **arguments):
        return deploy_supervisor.Supervisor(Path("/reviewed-release"), run=self.runner, gate=self.gate,
                                           launch=self.launch, clock=lambda: self.now,
                                           kill=lambda process: self.kill_calls.append(process),
                                           cache=arguments.pop("cache", None), **arguments)

    def test_engine_exit_cannot_reexecute_without_fresh_preflight(self):
        supervisor = self.supervisor()
        supervisor.start()
        self.flags["engine"] = False
        with self.assertRaisesRegex(ValueError, "engine exited"):
            supervisor.step()
        self.assertEqual(self.starts, ["engine"])
        blocked = deploy_supervisor.Supervisor(Path("/reviewed-release"), run=self.runner,
                                               gate=lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("wrong mount")))
        with self.assertRaises(ValueError):
            blocked.start()
        self.assertEqual(self.starts, ["engine"])
        self.supervisor().start()
        self.assertEqual(len(self.gates), 2)
        self.assertEqual(self.starts, ["engine", "engine"])

    def test_secondary_failure_launches_only_secondary_helpers_without_engine_restart(self):
        supervisor = self.supervisor()
        supervisor.start()
        supervisor.step()
        self.assertEqual(self.starts, ["engine"])
        self.assertEqual([command[-1] for command, _, _ in self.helpers], ["console", "proxy"])
        self.assertTrue(all("recover" in command for command, _, _ in self.helpers))
        self.assertTrue(self.flags["engine"])
        self.flags["console"] = self.flags["proxy"] = True
        supervisor.step()
        self.assertEqual(len(self.helpers), 2)
        self.assertEqual(self.starts, ["engine"])

    def test_secondary_duplicates_and_inventory_timeout_leave_engine_running(self):
        supervisor = self.supervisor()
        supervisor.start()
        base_run = self.runner
        def duplicate(command, **arguments):
            result = base_run(command, **arguments)
            if command[1] == "ps":
                result.stdout += "\n" + "d" * 64 + " console exited"
            return result
        supervisor.run = duplicate
        supervisor.step()
        self.assertEqual([command[-1] for command, _, _ in self.helpers], ["proxy"])
        self.assertIn("console", supervisor.degraded)
        def slow(command, **arguments):
            if command[1] == "ps":
                raise subprocess.TimeoutExpired(command, arguments["timeout"])
            return base_run(command, **arguments)
        supervisor.run = slow
        supervisor.step()
        self.assertIn("inventory", supervisor.degraded)
        self.assertTrue(self.flags["engine"])
        self.assertEqual(self.starts, ["engine"])
        self.assertEqual(len(self.helpers), 1)

    def test_duplicate_engine_still_fails_closed_and_oneoffs_are_filtered(self):
        supervisor = self.supervisor()
        supervisor.start()
        base_run = self.runner
        def duplicate(command, **arguments):
            result = base_run(command, **arguments)
            if command[1] == "ps":
                self.assertIn("label=com.docker.compose.oneoff=False", command)
                result.stdout += "\n" + "d" * 64 + " engine running"
            return result
        supervisor.run = duplicate
        with self.assertRaises(deploy_supervisor.EngineIntegrityError):
            supervisor.step()
        self.assertEqual(self.helpers, [])

    def test_secondary_retry_backoff_and_timeout_kill_only_owned_helpers(self):
        supervisor = self.supervisor()
        supervisor.start()
        supervisor.step()
        self.helpers[0][2].returncode = 1
        self.now = 1
        supervisor.step()
        self.now = 5
        supervisor.step()
        self.assertEqual(len(self.helpers), 2)
        self.now = 6
        supervisor.step()
        self.assertEqual(len(self.helpers), 3)
        self.now = 31
        supervisor.step()
        self.assertEqual(self.kill_calls, [self.helpers[1][2]])
        self.assertEqual(self.starts, ["engine"])
        supervisor.close()
        self.assertIn(self.helpers[2][2], self.kill_calls)

    def test_secondary_recovery_checks_mount_before_any_docker_mutation(self):
        for reason in ("unmounted", "wrong EBS volume serial"):
            with self.subTest(reason=reason), patch.dict(os.environ, deploy_host.image_environment(self.manifest)):
                with self.assertRaises(ValueError):
                    deploy_supervisor.recover_secondary(Path("/reviewed-release"), "console", run=self.runner,
                        mount_check=lambda *_: (_ for _ in ()).throw(ValueError(reason)))
        self.assertEqual(self.calls, [])

    def test_wrong_ebs_serial_rejects_actual_mount_check(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            (state / ".mm2-state-volume").write_text("vol-0123456789abcdef0\n")
            values = ("/dev/nvme1n1 rw,nodev,nosuid,noexec", "vol99999999999999999 disk")
            def run(command, **arguments):
                return subprocess.CompletedProcess(command, 0, values[0] if command[0] == "findmnt" else values[1], "")
            with patch.object(deploy_host.os.path, "ismount", return_value=True), patch.object(deploy_host.subprocess, "run", side_effect=run):
                with self.assertRaisesRegex(ValueError, "selected EBS volume serial"):
                    deploy_host.check_state_mount(state)

    def test_valid_secondary_recovery_never_starts_its_dependencies(self):
        checked = []
        with patch.dict(os.environ, deploy_host.image_environment(self.manifest)):
            deploy_supervisor.recover_secondary(Path("/reviewed-release"), "proxy", run=self.runner,
                                               mount_check=lambda state: checked.append(state))
        self.assertEqual(checked, [deploy_host.STATE_ROOT])
        self.assertEqual(self.starts, ["proxy"])
        self.assertNotIn("private fixture output", self.calls[0][0])

    def test_duplicate_project_identity_refuses_start_but_cleanup_stops_both(self):
        output = self.ids["engine"] + " engine running\n" + "d" * 64 + " engine exited\n"
        def run(command, **arguments):
            self.calls.append((command, arguments))
            return subprocess.CompletedProcess(command, 0, output if command[1] == "ps" else "", "")
        with self.assertRaisesRegex(ValueError, "duplicate"):
            deploy_supervisor.containers(run)
        deploy_supervisor.stop_stack(run, cache=None)
        stop, arguments = self.calls[-1]
        self.assertEqual(stop[:4], ["docker", "stop", "--time", "30"])
        self.assertEqual(set(stop[4:]), {self.ids["engine"], "d" * 64})
        self.assertTrue(arguments["capture_output"])

    def test_slow_docker_inventory_cleanup_still_stops_cached_engine(self):
        calls = []
        def run(command, **arguments):
            calls.append((command, arguments))
            if command[1] == "ps":
                raise subprocess.TimeoutExpired(command, arguments["timeout"])
            return subprocess.CompletedProcess(command, 0, "private fixture", "")
        with patch.object(deploy_supervisor, "saved_ids", return_value=[self.ids["engine"]]):
            deploy_supervisor.stop_stack(run, cache=Path("/root-owned-cache"))
        self.assertEqual([arguments["timeout"] for command, arguments in calls if command[1] == "ps"], [10, 10])
        self.assertEqual(calls[-1][0], ["docker", "stop", "--time", "30", self.ids["engine"]])

    def test_cleanup_unions_cached_and_newly_discovered_container_ids(self):
        calls = []
        def run(command, **arguments):
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, self.ids["proxy"] + " proxy running" if command[1] == "ps" else "", "")
        with patch.object(deploy_supervisor, "saved_ids", return_value=[self.ids["engine"]]):
            deploy_supervisor.stop_stack(run, cache=Path("/root-owned-cache"))
        self.assertEqual(set(calls[-1][4:]), {self.ids["engine"], self.ids["proxy"]})

    def test_corrupt_cache_cannot_block_label_validated_cleanup(self):
        def run(command, **arguments):
            self.calls.append((command, arguments))
            return subprocess.CompletedProcess(command, 0, self.ids["engine"] + " engine running" if command[1] == "ps" else "", "")
        with patch.object(deploy_supervisor, "saved_ids", side_effect=ValueError("wrong owner")):
            deploy_supervisor.stop_stack(run, cache=Path("/untrusted-cache"))
        self.assertEqual(self.calls[-1][0], ["docker", "stop", "--time", "30", self.ids["engine"]])

    def test_cleanup_cache_refuses_non_private_or_untrusted_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory) / "owned-containers"
            metadata = SimpleNamespace(st_mode=stat.S_IFREG | 0o600, st_uid=0, st_size=65)
            cache.write_text(self.ids["engine"] + "\n")
            with patch.object(Path, "lstat", return_value=metadata):
                self.assertEqual(deploy_supervisor.saved_ids(cache), [self.ids["engine"]])
                cache.write_text("foreign-container\n")
                with self.assertRaises(ValueError):
                    deploy_supervisor.saved_ids(cache)
                metadata.st_uid = 1000
                with self.assertRaises(ValueError):
                    deploy_supervisor.saved_ids(cache)

    def test_initial_cache_failure_is_fatal_but_secondary_update_retains_engine(self):
        failed = self.supervisor(cache=Path("/private-cache"))
        with patch.object(deploy_supervisor, "save_ids", side_effect=OSError("write failed")):
            with self.assertRaises(OSError):
                failed.start()
        self.assertIsNone(failed.cached)
        self.flags["engine"] = False  # Unit cleanup before the fresh fully gated start.
        supervisor = self.supervisor(cache=Path("/private-cache"))
        with patch.object(deploy_supervisor, "save_ids", side_effect=[None, OSError("write failed")]):
            supervisor.start()
            original = supervisor.cached.copy()
            self.ids["console"] = "d" * 64
            supervisor.step()
        self.assertEqual(supervisor.cached, original)
        self.assertEqual(supervisor.cached["engine"], supervisor.engine_id)
        self.assertIn("cache", supervisor.degraded)
        self.assertTrue(self.flags["engine"])
        self.assertEqual(self.starts, ["engine", "engine"])

    @unittest.skipUnless(hasattr(os, "O_DIRECTORY"), "Linux directory fsync")
    def test_cache_save_is_atomic_canonical_private_and_preserves_previous_on_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory) / "owned-containers"
            deploy_supervisor.save_ids(cache, self.ids)
            self.assertEqual(cache.read_text(), "".join(identity + "\n" for identity in sorted(self.ids.values())))
            self.assertEqual(stat.S_IMODE(cache.stat().st_mode), 0o600)
            previous = cache.read_bytes()
            with patch.object(deploy_supervisor.os, "replace", side_effect=OSError("rename failed")):
                with self.assertRaises(OSError):
                    deploy_supervisor.save_ids(cache, {"engine": "d" * 64})
            self.assertEqual(cache.read_bytes(), previous)
            self.assertEqual(list(Path(directory).iterdir()), [cache])

    def test_existing_running_engine_refuses_start_after_preflight(self):
        self.flags["engine"] = True
        with self.assertRaisesRegex(ValueError, "existing engine"):
            self.supervisor().start()
        self.assertEqual(len(self.gates), 1)
        self.assertEqual(self.starts, [])

    def test_polling_wait_is_bounded_and_normal_stop_closes_helpers(self):
        supervisor = self.supervisor()
        stop = SimpleNamespace(done=False, waits=[])
        stop.is_set = lambda: stop.done
        def wait(seconds):
            stop.waits.append(seconds)
            stop.done = True
        stop.wait = wait
        supervisor.serve(stop)
        self.assertEqual(stop.waits, [1])
        self.assertEqual(len(self.kill_calls), 2)
        self.assertEqual(supervisor.pending, {})


if __name__ == "__main__":
    unittest.main()
