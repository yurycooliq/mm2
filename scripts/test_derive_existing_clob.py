"""Credential responses stay in a private pipe; failed/foreign workers cannot publish or persist them."""
import base64
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import derive_existing_clob as derive

OWNER = "b" * 32
IMAGE = "sha256:" + "c" * 64
PRIVATE = "1" * 64
PAYLOAD = {"private_key": PRIVATE, "signer_address": "0x" + "2" * 40, "nonce": 0}
CREDENTIALS = {"schema": 1, "kind": "clob", "api_key": "00000000-0000-0000-0000-000000000001",
               "api_secret": base64.urlsafe_b64encode(b"fixture-key-only-32bytes-length!!").decode(),
               "api_passphrase": "private-fixture-passphrase"}


class ExistingClobTests(unittest.TestCase):
    def worker(self, *, code=0, live=False, foreign=False, timeout=False, output=None, calls=None, stderr=None):
        calls = [] if calls is None else calls

        def run(arguments, **kwargs):
            calls.append((arguments, kwargs))
            if arguments[:2] == ["docker", "run"]:
                if timeout:
                    raise subprocess.TimeoutExpired(arguments, 35, output=PRIVATE.encode())
                raw = json.dumps(CREDENTIALS).encode() if output is None else output
                return SimpleNamespace(returncode=code, stdout=raw, stderr=PRIVATE.encode() if stderr is None else stderr)
            if arguments[:3] == ["docker", "container", "inspect"]:
                return SimpleNamespace(returncode=0 if live else 1, stdout=("foreign" if foreign else OWNER).encode())
            return SimpleNamespace(returncode=0)

        with patch.object(derive.subprocess, "run", side_effect=run), \
                patch.object(derive.uuid, "uuid4", return_value=SimpleNamespace(hex=OWNER)), \
                patch.dict(derive.os.environ, {"MM2_UNRELATED_SECRET": "must-not-enter-child"}):
            result = derive.run_auth_worker(IMAGE, PAYLOAD)
        return result, calls

    def test_private_pipe_exact_image_and_no_persistent_docker_logs(self):
        result, calls = self.worker()
        self.assertEqual(result, CREDENTIALS)
        command, options = calls[0]
        self.assertEqual(command[-1], IMAGE)
        self.assertEqual(command[command.index("--entrypoint") + 1], "mm2-credential-auth")
        self.assertEqual(command[command.index("--log-driver") + 1], "none")
        self.assertIn("--read-only", command)
        self.assertNotIn(PRIVATE, " ".join(command))
        self.assertEqual(json.loads(options["input"]), PAYLOAD)
        self.assertNotIn("MM2_UNRELATED_SECRET", options["env"])
        self.assertEqual(options["env"], {"PATH": derive.os.defpath, "DOCKER_HOST": "unix:///var/run/docker.sock"})
        self.assertTrue(all(call[1]["env"] == options["env"] for call in calls))
        self.assertFalse(any(call[0][:3] == ["docker", "container", "rm"] for call in calls))

    def test_failure_does_not_forward_sensitive_stdout_stderr_or_auth_headers(self):
        with self.assertRaises(derive.DeriveError) as error:
            self.worker(code=1, output=PRIVATE.encode())
        self.assertEqual(str(error.exception), "existing_clob_derivation_failed_no_creation")
        self.assertNotIn(PRIVATE, str(error.exception))

    def test_owned_worker_cleaned_and_foreign_worker_never_removed(self):
        _, calls = self.worker(live=True)
        removed = [call[0] for call in calls if call[0][:3] == ["docker", "container", "rm"]]
        self.assertEqual(len(removed), 1)
        self.assertEqual(removed[0][-1], "mm2-clob-derive-" + OWNER[:16])
        foreign_calls = []
        with self.assertRaisesRegex(derive.DeriveError, "temporary_worker_owner_mismatch"):
            self.worker(live=True, foreign=True, calls=foreign_calls)
        self.assertFalse(any(call[0][:3] == ["docker", "container", "rm"] for call in foreign_calls))
        timeout_calls = []
        with self.assertRaises(subprocess.TimeoutExpired):
            self.worker(live=True, timeout=True, calls=timeout_calls)
        self.assertEqual(sum(call[0][:3] == ["docker", "container", "rm"] for call in timeout_calls), 1)

    def test_timeout_and_unexpected_credentials_are_masked_by_cli(self):
        for failure in (subprocess.TimeoutExpired(["docker", "run"], 35, output=PRIVATE.encode()),
                        ValueError(PRIVATE), RuntimeError(CREDENTIALS["api_secret"])):
            output, errors = StringIO(), StringIO()
            with patch.object(derive, "derive", side_effect=failure), redirect_stdout(output), redirect_stderr(errors):
                self.assertEqual(derive.main([]), 1)
            self.assertEqual(output.getvalue(), "")
            self.assertEqual(errors.getvalue(), "CLOB derivation refused: safe_setup_or_transport_failed\n")
        with self.assertRaises(Exception):
            self.worker(output=json.dumps({**CREDENTIALS, "activation": "running"}).encode())

    def test_only_exact_known_error_classes_may_enter_public_result(self):
        for code in (b"clob_existing_credentials_absent", b"clob_auth_rejected", b"clob_existing_derivation_rejected"):
            with self.subTest(code=code), self.assertRaises(derive.DeriveError) as failure:
                self.worker(code=1, stderr=code + b"\n")
            self.assertEqual(str(failure.exception), code.decode())
        with self.assertRaises(derive.DeriveError) as failure:
            self.worker(code=1, stderr=b"clob_auth_rejected " + PRIVATE.encode())
        self.assertEqual(str(failure.exception), "existing_clob_derivation_failed_no_creation")

    def test_nonzero_nonce_and_existing_partial_credentials_have_no_effect(self):
        with patch.object(derive, "derive") as execute, redirect_stderr(StringIO()):
            self.assertEqual(derive.main(["--nonce", "1"]), 1)
            execute.assert_not_called()
        current = Path("/opt/mm2/releases/" + "a" * 40)
        for field in ("MM2_POLY_API_KEY", "MM2_POLY_API_SECRET", "MM2_POLY_API_PASSPHRASE"):
            with patch.object(derive.sys, "platform", "linux"), patch.object(derive.os, "geteuid", return_value=0, create=True), \
                    patch.object(derive.Path, "resolve", return_value=current), \
                    patch.object(derive, "verify_release", return_value={"images": {"engine": IMAGE}, "source_commit": "a" * 40}), \
                    patch.object(derive, "image_identity"), \
                    patch.object(derive, "private_environment", return_value={"MM2_POLY_PRIVATE_KEY": PRIVATE, field: "existing"}), \
                    patch.object(derive, "run_auth_worker") as worker:
                with self.assertRaisesRegex(derive.DeriveError, "existing_local_clob_settings_preserved"):
                    derive.derive(0)
                worker.assert_not_called()

    def test_accidental_secret_arguments_are_not_echoed(self):
        for arguments in (["--private-key", PRIVATE], ["--nonce", PRIVATE + "not-an-int"]):
            output, errors = StringIO(), StringIO()
            with patch.object(derive, "derive") as execute, redirect_stdout(output), redirect_stderr(errors):
                with self.assertRaises(SystemExit) as failure:
                    derive.main(arguments)
                self.assertEqual(failure.exception.code, 2)
            execute.assert_not_called()
            self.assertEqual(output.getvalue(), "")
            self.assertNotIn(PRIVATE, errors.getvalue())

    def test_owner_state_requires_valid_control_identity(self):
        good = {"mode": "paused", "live": None, "live_eligible": False, "api_ready": True,
                "boot_id": "00000000-0000-0000-0000-000000000001", "command_version": 0, "reset_epoch": 0}
        class Response:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *_): return False
            def read(self, _): return json.dumps(value).encode()
        for changes in ({}, {"boot_id": None}, {"boot_id": "00000000-0000-0000-0000-000000000000"},
                        {"command_version": True}, {"command_version": -1}, {"reset_epoch": None}):
            value = {**good, **changes}
            with patch.object(derive.urllib.request, "build_opener") as opener:
                opener.return_value.open.return_value = Response()
                if changes:
                    with self.assertRaisesRegex(derive.DeriveError, "paused_owner_state_required"):
                        derive.owner_state("private-token")
                else:
                    self.assertEqual(derive.owner_state("private-token"), (good["boot_id"], 0, 0))

    def test_derived_settings_use_locked_original_environment_snapshot(self):
        current = Path("/opt/mm2/releases/" + "a" * 40)
        values = {"MM2_POLY_PRIVATE_KEY": PRIVATE, "MM2_ENGINE_API_TOKEN": "private-token"}
        with patch.object(derive.sys, "platform", "linux"), patch.object(derive.os, "geteuid", return_value=0, create=True), \
                patch.object(derive.Path, "resolve", return_value=current), \
                patch.object(derive, "verify_release", return_value={"images": {"engine": IMAGE}, "source_commit": "a" * 40}), \
                patch.object(derive, "image_identity"), patch.object(derive, "private_environment", return_value=values), \
                patch.object(derive, "expected_signer", return_value=PAYLOAD["signer_address"]), \
                patch.object(derive, "owner_state", return_value=("boot", 0, 0)), \
                patch.object(derive, "run_auth_worker", return_value=CREDENTIALS), \
                patch.object(derive, "apply_update", side_effect=RuntimeError("snapshot changed")) as store:
            with self.assertRaises(RuntimeError):
                derive.derive(0)
            store.assert_called_once_with(CREDENTIALS, expected_environment=values)


if __name__ == "__main__":
    unittest.main()
