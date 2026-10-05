"""Credential handoff boundaries: hidden input, no secret argv/logs, identity and atomic storage."""
from contextlib import redirect_stdout, redirect_stderr
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import provision_private_runtime as provision


BOT_TOKEN = "123456789:" + "x" * 35  # Offline test fixture; never a real credential.
KEY_ONE = "0" * 63 + "1"  # Published secp256k1 vector, never a funded/private account.
ADDRESS_ONE = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
OWNER = 366612345


class CredentialHandoffTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = self.root / "ssh-config"
        self.config.write_text("Host mm2-aws\n StrictHostKeyChecking yes\n")
        self.args = SimpleNamespace(kind="telegram", ssh_config=self.config, host="mm2-aws", ssh_executable="ssh")

    def tearDown(self):
        self.temp.cleanup()

    def test_owner_id_and_exact_fields_reject_ellipsis_bool_unknowns(self):
        valid = {"schema": 1, "kind": "telegram", "token": BOT_TOKEN, "owner_id": OWNER}
        self.assertEqual(provision.validate_payload(valid), valid)
        for owner in ("3666...", True, 0, -1, 2**52):
            with self.subTest(owner=owner), self.assertRaises(provision.ProvisionError):
                provision.validate_payload({**valid, "owner_id": owner})
        for fault in ({**valid, "extra": "field"}, {**valid, "token": "bad\n"}, {**valid, "schema": 2}, {**valid, "schema": True}):
            with self.assertRaises(provision.ProvisionError):
                provision.validate_payload(fault)

    def test_wallet_only_hex_not_seed_zero_or_out_of_range(self):
        self.assertEqual(provision.validate_payload({"schema": 1, "kind": "wallet", "private_key": KEY_ONE})["private_key"], KEY_ONE)
        for key in ("word " * 12, "0" * 64, f"{provision.CURVE_ORDER:064x}", "0x" + "f" * 64,
                    KEY_ONE + "\n", "$OTHER", "0X" + KEY_ONE):
            with self.subTest(key=key), self.assertRaises(provision.ProvisionError):
                provision.validate_payload({"schema": 1, "kind": "wallet", "private_key": key})

    def test_credential_json_refuses_duplicates_oversized_non_ascii_and_unknown_kinds(self):
        good = json.dumps({"schema": 1, "kind": "wallet", "private_key": KEY_ONE}).encode()
        self.assertEqual(provision.decode_payload(good)["kind"], "wallet")
        for bad in (b'{"schema":1,"schema":1,"kind":"wallet","private_key":"' + KEY_ONE.encode() + b'"}',
                    b"x" * (provision.MAX_INPUT + 1), b"\xff", b"{", b'[]',
                    b'{"schema":1,"kind":[]}', b'{"schema":1,"kind":"unknown"}'):
            with self.subTest(raw=bad[:30]), self.assertRaises(provision.ProvisionError):
                provision.decode_payload(bad)

    def test_hidden_input_refuses_pipes_and_getpass_echo_fallback(self):
        with patch.object(sys, "stdin", StringIO("fixture")), self.assertRaises(provision.ProvisionError):
            provision.hidden_input("hidden: ")
        with patch.object(sys, "stdin", SimpleNamespace(isatty=lambda: True)), \
                patch.object(provision.getpass, "getpass", side_effect=provision.getpass.GetPassWarning("unsafe")), \
                self.assertRaises(provision.ProvisionError):
            provision.hidden_input("hidden: ")

    def test_mistaken_credential_arguments_are_not_echoed_by_parser(self):
        for arguments in ([BOT_TOKEN], ["telegram", "--token", BOT_TOKEN], ["wallet", "--key", KEY_ONE]):
            errors = StringIO()
            with redirect_stderr(errors), self.assertRaises(SystemExit) as caught:
                provision.main(arguments)
            self.assertEqual(caught.exception.code, 2)
            self.assertNotIn(BOT_TOKEN, errors.getvalue())
            self.assertNotIn(KEY_ONE, errors.getvalue())

    def test_transport_secret_is_only_stdin_and_only_public_result_printed(self):
        calls = []

        def run(command, **kwargs):
            calls.append((command, kwargs))
            result = {"ok": True, "kind": "telegram", "username": "mm2b_bot", "bot_id": 123456789,
                      "owner_id": OWNER, "restart_required": "console"}
            return SimpleNamespace(returncode=0, stdout=("public SSM session\n" + json.dumps(result)).encode())

        output = StringIO()
        with patch("builtins.input", return_value=str(OWNER)), patch.object(provision, "hidden_input", return_value=BOT_TOKEN), \
                patch.dict(os.environ, {"MM2_TELEGRAM_BOT_TOKEN": BOT_TOKEN, "MM2_POLY_PRIVATE_KEY": KEY_ONE}), redirect_stdout(output):
            provision.local_main(self.args, run=run)
        command, options = calls[0]
        self.assertNotIn(BOT_TOKEN, repr(command))
        self.assertNotIn(BOT_TOKEN, repr(options["env"]))
        self.assertEqual(json.loads(options["input"])["token"], BOT_TOKEN)
        self.assertEqual(options["stderr"], subprocess.DEVNULL)
        self.assertIn("StrictHostKeyChecking=yes", command)
        self.assertIn("-T", command)
        self.assertEqual(command[-1], "sudo -n python3 -B /opt/mm2/tools/provision_private_runtime.py --remote")
        self.assertNotIn(BOT_TOKEN, output.getvalue())
        self.assertNotIn("systemctl", repr(command))
        self.assertNotIn("docker", repr(command))

    def test_transport_exceptions_and_invalid_reply_do_not_leak_credentials(self):
        with patch("builtins.input", return_value=str(OWNER)), patch.object(provision, "hidden_input", return_value=BOT_TOKEN):
            with self.assertRaises(provision.ProvisionError) as caught:
                provision.local_main(self.args, run=lambda *a, **kw: (_ for _ in ()).throw(RuntimeError(BOT_TOKEN)))
        self.assertNotIn(BOT_TOKEN, str(caught.exception))
        malicious = {"ok": True, "kind": "telegram", "username": "mm2b_bot", "bot_id": 123456789,
                     "owner_id": OWNER, "token": BOT_TOKEN}
        with patch("builtins.input", return_value=str(OWNER)), patch.object(provision, "hidden_input", return_value=BOT_TOKEN), self.assertRaises(provision.ProvisionError):
            provision.local_main(self.args, run=lambda *a, **kw: SimpleNamespace(returncode=0, stdout=json.dumps(malicious).encode()))

    def test_wrong_owner_reply_and_address_field_cannot_be_printed(self):
        malicious = {"ok": True, "kind": "wallet", "signer_address": KEY_ONE}
        self.args.kind = "wallet"
        output = StringIO()
        with patch.object(provision, "hidden_input", return_value=KEY_ONE), redirect_stdout(output), self.assertRaises(provision.ProvisionError):
            provision.local_main(self.args, run=lambda *a, **kw: SimpleNamespace(returncode=0, stdout=json.dumps(malicious).encode()))
        self.assertNotIn(KEY_ONE, output.getvalue())

    def test_telegram_identity_wrong_bot_or_secret_error_is_sanitized(self):
        class Response(BytesIO):
            def __enter__(self): return self
            def __exit__(self, *args): self.close()

        class Opener:
            def __init__(self, reply): self.reply = reply
            def open(self, *args, **kwargs): return Response(json.dumps(self.reply).encode())

        valid = {"ok": True, "result": {"id": 123456789, "is_bot": True, "username": "mm2b_bot"}}
        self.assertEqual(provision.telegram_identity(BOT_TOKEN, Opener(valid))["username"], "mm2b_bot")
        for result in ({**valid["result"], "username": "someone_else"}, {**valid["result"], "id": 999}, {**valid["result"], "is_bot": False}):
            with self.assertRaises(provision.ProvisionError):
                provision.telegram_identity(BOT_TOKEN, Opener({"ok": True, "result": result}))
        bad = SimpleNamespace(open=lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("https://api.telegram.org/bot" + BOT_TOKEN)))
        with self.assertRaises(provision.ProvisionError) as caught:
            provision.telegram_identity(BOT_TOKEN, bad)
        self.assertNotIn(BOT_TOKEN, str(caught.exception))
        with self.assertRaises(provision.ProvisionError):
            provision.NoRedirect().redirect_request(None, None, 302, "", {}, "https://other.example")

    def test_keccak_ethereum_vectors_and_sha3_distinction(self):
        self.assertEqual(provision.keccak_public(b"").hex(), "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")
        self.assertEqual(provision.keccak_public(b"abc").hex(), "4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45")
        with self.assertRaises(provision.ProvisionError): provision.keccak_public(b"x" * 4097)

    def test_openssl_public_identity_fixture_and_no_key_argument(self):
        executable = shutil.which("openssl")
        if not executable and sys.platform == "win32":
            candidate = Path("C:/Program Files/Git/usr/bin/openssl.exe")
            executable = str(candidate) if candidate.exists() else None
        if not executable:
            self.skipTest("OpenSSL is unavailable on this test host")
        calls = []

        def run(command, **kwargs):
            calls.append((command, kwargs))
            return subprocess.run([executable, *command[1:]], **kwargs)

        self.assertEqual(provision.signer_address(KEY_ONE, run=run), ADDRESS_ONE)
        self.assertNotIn(KEY_ONE, repr(calls[0][0]))
        self.assertEqual(calls[0][1]["stderr"], subprocess.DEVNULL)
        self.assertIn(bytes.fromhex(KEY_ONE), calls[0][1]["input"])


@unittest.skipUnless(sys.platform == "linux" and hasattr(os, "geteuid") and os.geteuid() == 0,
                     "Atomic root runtime checks run in the isolated Linux container")
class AtomicRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        os.chmod(self.root, 0o700)
        self.runtime = self.root / "runtime.env"
        self.original = ("MM2_ENGINE_API_TOKEN=" + "a" * 48 + "\nMM2_CONSOLE_TOKEN=" + "b" * 48 +
                         "\nMM2_DOMAIN=mm2.vstage.site\nMM2_TELEGRAM_BOT_TOKEN=\nMM2_TELEGRAM_OWNER_ID=\nMM2_POLY_PRIVATE_KEY=\n")
        self.runtime.write_text(self.original)
        os.chmod(self.runtime, 0o600)
        self.profile = self.root / "trading.toml"
        self.profile.write_text('[live]\nsigner_address = "' + ADDRESS_ONE + '"\n')
        os.chmod(self.profile, 0o644)
        self.telegram = {"schema": 1, "kind": "telegram", "token": BOT_TOKEN, "owner_id": OWNER}

    def tearDown(self):
        self.temp.cleanup()

    def apply(self, payload):
        return provision.apply_update(payload, self.root, self.profile,
                                      get_identity=lambda token: {"username": "mm2b_bot", "bot_id": 123456789},
                                      derive_address=lambda key: ADDRESS_ONE)

    def test_atomically_preserves_tokens_and_mode_no_activation(self):
        inode = self.runtime.stat().st_ino
        result = self.apply(self.telegram)
        actual = self.runtime.read_text()
        self.assertIn("MM2_ENGINE_API_TOKEN=" + "a" * 48, actual)
        self.assertIn("MM2_CONSOLE_TOKEN=" + "b" * 48, actual)
        self.assertIn("MM2_TELEGRAM_OWNER_ID=" + str(OWNER), actual)
        self.assertNotEqual(inode, self.runtime.stat().st_ino)
        self.assertEqual(stat.S_IMODE(self.runtime.stat().st_mode), 0o600)
        self.assertEqual(self.runtime.stat().st_uid, 0)
        self.assertNotIn(BOT_TOKEN, json.dumps(result))
        self.assertEqual(list(self.root.glob(".runtime-*")), [])
        self.apply({"schema": 1, "kind": "wallet", "private_key": KEY_ONE})
        self.assertIn("MM2_TELEGRAM_BOT_TOKEN=" + BOT_TOKEN, self.runtime.read_text())

    def test_existing_other_owner_and_signer_mismatch_leave_config_unchanged(self):
        self.apply(self.telegram)
        before = self.runtime.read_bytes()
        with self.assertRaises(provision.ProvisionError): self.apply({**self.telegram, "owner_id": OWNER + 1})
        self.assertEqual(self.runtime.read_bytes(), before)
        self.profile.write_text('[live]\nsigner_address = "0x' + "f" * 40 + '"\n')
        with self.assertRaises(provision.ProvisionError): self.apply({"schema": 1, "kind": "wallet", "private_key": KEY_ONE})
        self.assertEqual(self.runtime.read_bytes(), before)

    def test_unsafe_directory_file_or_lock_refuses_without_replacement(self):
        os.chmod(self.root, 0o755)
        with self.assertRaises(provision.ProvisionError): self.apply(self.telegram)
        os.chmod(self.root, 0o700)
        os.chmod(self.runtime, 0o644)
        with self.assertRaises(ValueError): self.apply(self.telegram)
        os.chmod(self.runtime, 0o600)
        unsafe = self.root / "linked.env"
        self.runtime.rename(unsafe)
        self.runtime.symlink_to(unsafe)
        with self.assertRaises(ValueError): self.apply(self.telegram)
        self.runtime.unlink(); unsafe.rename(self.runtime)
        lock = self.root / ".provision.lock"
        lock.unlink(missing_ok=True); lock.symlink_to(self.runtime)
        with self.assertRaises(OSError): self.apply(self.telegram)
        self.assertEqual(self.runtime.read_text(), self.original)

    def test_failed_replace_retains_original_and_cleans_private_temporary(self):
        with patch.object(provision.os, "replace", side_effect=OSError("offline fixture")), self.assertRaises(OSError):
            self.apply(self.telegram)
        self.assertEqual(self.runtime.read_text(), self.original)
        self.assertEqual(list(self.root.glob(".runtime-*")), [])

    def test_concurrent_change_is_retained_and_staged_secret_removed(self):
        changed = self.original + "# concurrent trusted operator update\n"

        def identity(token):
            self.runtime.write_text(changed)
            return {"username": "mm2b_bot", "bot_id": 123456789}

        with self.assertRaises(provision.ProvisionError):
            provision.apply_update(self.telegram, self.root, self.profile, get_identity=identity)
        self.assertEqual(self.runtime.read_text(), changed)
        self.assertNotIn(BOT_TOKEN, self.runtime.read_text())
        self.assertEqual(list(self.root.glob(".runtime-*")), [])

    def test_hardlink_and_concurrent_lock_refuse_before_identity_check(self):
        import fcntl
        linked = self.root / "hardlink"
        os.link(self.runtime, linked)
        with self.assertRaises(provision.ProvisionError): self.apply(self.telegram)
        linked.unlink()
        lock = os.open(self.root / ".provision.lock", os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError): self.apply(self.telegram)
        finally:
            os.close(lock)
        self.assertEqual(self.runtime.read_text(), self.original)


if __name__ == "__main__":
    unittest.main()
