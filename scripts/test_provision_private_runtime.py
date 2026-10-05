"""Credential handoff boundaries: hidden input, no secret argv/logs, identity and atomic storage."""
from contextlib import redirect_stdout, redirect_stderr
from io import BytesIO, StringIO
import base64
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
CLOB_KEY = "7b1e2d60-6f9a-4dd7-8f3e-21b8f94c77a2"  # Public documentation UUID, offline fixture.
CLOB_SECRET = base64.urlsafe_b64encode(b"offline-hmac-fixture-only-32byte").decode()
CLOB_PASSPHRASE = "offline-passphrase-fixture"
RELAYER_KEY = "offline-relayer-fixture"
RPC_URL = "https://rpc.example.com/v2/offline-provider-key?network=polygon"
EXTRA_PAYLOADS = (
    {"schema": 1, "kind": "clob", "api_key": CLOB_KEY, "api_secret": CLOB_SECRET,
     "api_passphrase": CLOB_PASSPHRASE},
    {"schema": 1, "kind": "relayer", "api_key": RELAYER_KEY, "key_address": ADDRESS_ONE},
    {"schema": 1, "kind": "rpc", "url": RPC_URL},
)


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

    def test_existing_clob_credentials_validate_uuid_base64_and_literal_passphrase(self):
        valid = EXTRA_PAYLOADS[0]
        self.assertEqual(provision.validate_payload(valid), valid)
        self.assertEqual(provision.validate_payload({**valid, "api_secret": CLOB_SECRET.rstrip("=")})["kind"], "clob")
        for field, bad in (("api_key", "0" * 36), ("api_key", "00000000-0000-0000-0000-000000000000"),
                           ("api_key", "{" + CLOB_KEY + "}"), ("api_key", True),
                           ("api_secret", "A" * 17), ("api_secret", "A" * 16),
                           ("api_secret", CLOB_SECRET + "==="), ("api_secret", CLOB_SECRET + "\n"),
                           ("api_secret", "A" * 513), ("api_secret", None),
                           ("api_passphrase", ""), ("api_passphrase", "x" * 257),
                           ("api_passphrase", "$KEY"), ("api_passphrase", "space separated"),
                           ("api_passphrase", "quotes'"), ("api_passphrase", "`command`")):
            with self.subTest(field=field, bad=bad), self.assertRaises(provision.ProvisionError):
                provision.validate_payload({**valid, field: bad})

    def test_clob_padding_normalization_preserves_31_and_32_byte_hmac_keys(self):
        for size, padding in ((31, "=="), (32, "=")):
            raw = b"x" * size  # Public HMAC test fixture.
            canonical = base64.urlsafe_b64encode(raw).decode()
            self.assertTrue(canonical.endswith(padding))
            unpadded = canonical.rstrip("=")
            self.assertNotEqual(unpadded, canonical)
            for supplied in (canonical, unpadded):
                payload = {**EXTRA_PAYLOADS[0], "api_secret": supplied}
                self.assertEqual(provision.validate_payload(payload), payload)
                stored = provision.canonical_clob_secret(supplied)
                self.assertEqual(stored, canonical)
                self.assertEqual(base64.b64decode(stored, altchars=b"-_", validate=True), raw)

    def test_relayer_key_and_public_owner_are_bounded_literal_values(self):
        valid = EXTRA_PAYLOADS[1]
        self.assertEqual(provision.validate_payload(valid), valid)
        for field, bad in (("api_key", ""), ("api_key", "x" * 1025), ("api_key", "header\r\ninjection"),
                           ("api_key", "$KEY"), ("api_key", "key\\escaped"), ("api_key", True),
                           ("key_address", "0x" + "0" * 40), ("key_address", ADDRESS_ONE + "\n"),
                           ("key_address", KEY_ONE), ("key_address", None)):
            with self.subTest(field=field, bad=bad), self.assertRaises(provision.ProvisionError):
                provision.validate_payload({**valid, field: bad})

    def test_rpc_url_is_bounded_https_without_ambiguous_authority_or_encoded_controls(self):
        valid = EXTRA_PAYLOADS[2]
        for url in (RPC_URL, "https://rpc.example.com:443/path", "https://[2001:db8::1]/rpc"):
            self.assertEqual(provision.validate_payload({**valid, "url": url})["url"], url)
        for url in ("http://rpc.example.com", "HTTPS://rpc.example.com", "https://", "https://rpc.example.com:",
                    "https://rpc.example.com:0", "https://rpc.example.com:65536", "https://rpc.example.com:invalid",
                    "https://user:password@rpc.example.com", "https://rpc.example.com/#", "https://rpc.example.com/#key",
                    "https://rpc.example.com/space path", "https://rpc.example.com/\n", "https://rpc.example.com/%0dheader",
                    "https://rpc.example.com/%20key", "https://rpc.example.com/%GG", "https://rpc.example.com/$KEY",
                    "https://rpc.example.com/`key`", "https://rpc.example.com/back\\slash", "https://[invalid]/",
                    "https://999.999.999.999", "https://-invalid.example", "https://rрc.example.com",
                    "https://rpc.example.com/" + "x" * 2048, None, True):
            with self.subTest(url=url), self.assertRaises(provision.ProvisionError) as caught:
                provision.validate_payload({**valid, "url": url})
            if isinstance(url, str) and len(url) > 15:
                self.assertNotIn(url, str(caught.exception))

    def test_all_added_modes_reject_missing_extra_duplicate_and_wrong_schema_fields(self):
        for payload in EXTRA_PAYLOADS:
            self.assertEqual(provision.decode_payload(json.dumps(payload).encode()), payload)
            field = next(iter(provision.FIELDS[payload["kind"]]))
            missing = {key: value for key, value in payload.items() if key != field}
            for bad in (missing, {**payload, "extra": "secret"}, {**payload, "schema": True}):
                with self.subTest(kind=payload["kind"]), self.assertRaises(provision.ProvisionError):
                    provision.validate_payload(bad)
            duplicate = json.dumps(payload)[:-1] + ',"kind":"' + payload["kind"] + '"}'
            with self.assertRaises(provision.ProvisionError):
                provision.decode_payload(duplicate.encode())

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
        for arguments in ([BOT_TOKEN], ["telegram", "--token", BOT_TOKEN], ["wallet", "--key", KEY_ONE],
                          ["clob", "--secret", CLOB_SECRET], ["relayer", RELAYER_KEY], ["rpc", RPC_URL]):
            errors = StringIO()
            with redirect_stderr(errors), self.assertRaises(SystemExit) as caught:
                provision.main(arguments)
            self.assertEqual(caught.exception.code, 2)
            self.assertNotIn(BOT_TOKEN, errors.getvalue())
            self.assertNotIn(KEY_ONE, errors.getvalue())
            for secret in (CLOB_SECRET, RELAYER_KEY, RPC_URL):
                self.assertNotIn(secret, errors.getvalue())

    def test_added_modes_send_all_credentials_only_as_stdin_and_never_activate(self):
        for payload in EXTRA_PAYLOADS:
            self.args.kind = payload["kind"]
            inputs = [payload[field] for field in (("api_key", "api_secret", "api_passphrase") if self.args.kind == "clob"
                                                 else ("api_key", "key_address") if self.args.kind == "relayer" else ("url",))]
            calls = []
            result = {"ok": True, "kind": self.args.kind, "activation": "unchanged", "restart_required": provision.INACTIVE}
            if self.args.kind == "relayer":
                result["signer_address"] = ADDRESS_ONE

            def run(command, **kwargs):
                calls.append((command, kwargs))
                return SimpleNamespace(returncode=0, stdout=json.dumps(result).encode())

            output = StringIO()
            with patch.object(provision, "hidden_input", side_effect=inputs), \
                    patch.dict(os.environ, {"MM2_POLY_API_SECRET": CLOB_SECRET, "MM2_POLYGON_RPC_URL": RPC_URL}), redirect_stdout(output):
                provision.local_main(self.args, run=run)
            self.assertEqual(len(calls), 1)
            command, options = calls[0]
            self.assertEqual(json.loads(options["input"]), payload)
            self.assertFalse(any(name.startswith("MM2_") for name in options["env"]))
            self.assertEqual(command[-1], "sudo -n python3 -B /opt/mm2/tools/provision_private_runtime.py --remote")
            self.assertEqual(options["stderr"], subprocess.DEVNULL)
            for secret in (CLOB_KEY, CLOB_SECRET, CLOB_PASSPHRASE, RELAYER_KEY, RPC_URL):
                self.assertNotIn(secret, repr(command))
                self.assertNotIn(secret, output.getvalue())
            self.assertIn("paused and gated", output.getvalue())

    def test_added_mode_transport_failure_malformed_or_secret_reply_is_redacted(self):
        for payload in EXTRA_PAYLOADS:
            self.args.kind = payload["kind"]
            inputs = [payload[field] for field in (("api_key", "api_secret", "api_passphrase") if self.args.kind == "clob"
                                                 else ("api_key", "key_address") if self.args.kind == "relayer" else ("url",))]
            valid = {"ok": True, "kind": self.args.kind, "activation": "unchanged", "restart_required": provision.INACTIVE}
            if self.args.kind == "relayer":
                valid["signer_address"] = ADDRESS_ONE
            replies = [b"{", json.dumps({**valid, "credential": CLOB_SECRET}).encode(),
                       (json.dumps(valid)[:-1] + ',"ok":true}').encode(),
                       json.dumps({**valid, "activation": "running"}).encode()]
            if self.args.kind == "relayer":
                replies.append(json.dumps({**valid, "signer_address": "0x" + "f" * 40}).encode())
            for reply in replies:
                output = StringIO()
                with patch.object(provision, "hidden_input", side_effect=inputs), redirect_stdout(output), \
                        self.assertRaises(provision.ProvisionError) as caught:
                    provision.local_main(self.args, run=lambda *a, **kw: SimpleNamespace(returncode=0, stdout=reply))
                self.assertNotIn(CLOB_SECRET, str(caught.exception))
                self.assertEqual(output.getvalue(), "")
            with patch.object(provision, "hidden_input", side_effect=inputs), self.assertRaises(provision.ProvisionError) as caught:
                provision.local_main(self.args, run=lambda *a, **kw: (_ for _ in ()).throw(RuntimeError(CLOB_SECRET + RPC_URL)))
            self.assertNotIn(CLOB_SECRET, str(caught.exception))
            self.assertNotIn(RPC_URL, str(caught.exception))

    def test_cli_errors_for_every_mode_never_echo_secret_exception_details(self):
        for kind in provision.FIELDS:
            output, errors = StringIO(), StringIO()
            with patch.object(provision, "local_main", side_effect=provision.ProvisionError(CLOB_SECRET + RPC_URL)), \
                    redirect_stdout(output), redirect_stderr(errors):
                result = provision.main([kind, "--ssh-config", str(self.config)])
            self.assertEqual(result, 1)
            self.assertEqual(output.getvalue(), "")
            self.assertNotIn(CLOB_SECRET, errors.getvalue())
            self.assertNotIn(RPC_URL, errors.getvalue())

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

    def test_all_added_modes_atomically_preserve_old_environment_and_profile(self):
        from deploy_host import private_environment

        self.apply(self.telegram)
        self.apply({"schema": 1, "kind": "wallet", "private_key": KEY_ONE})
        profile = self.profile.read_bytes()
        for payload in EXTRA_PAYLOADS:
            before = private_environment(self.runtime)
            inode = self.runtime.stat().st_ino
            with patch.object(provision, "telegram_identity", side_effect=AssertionError("no network")), \
                    patch.object(provision, "signer_address", side_effect=AssertionError("no signing")):
                result = self.apply(payload)
            actual = private_environment(self.runtime)
            for key, value in before.items():
                self.assertEqual(actual[key], value)
            self.assertNotEqual(self.runtime.stat().st_ino, inode)
            self.assertEqual(stat.S_IMODE(self.runtime.stat().st_mode), 0o600)
            self.assertEqual(self.runtime.stat().st_uid, 0)
            self.assertEqual(self.runtime.stat().st_nlink, 1)
            self.assertEqual(result["activation"], "unchanged")
            self.assertEqual(result["restart_required"], provision.INACTIVE)
            self.assertEqual(self.profile.read_bytes(), profile)
            self.assertEqual(list(self.root.glob(".runtime-*")), [])
            for secret in (CLOB_KEY, CLOB_SECRET, CLOB_PASSPHRASE, RELAYER_KEY, RPC_URL):
                self.assertNotIn(secret, json.dumps(result))
        final = private_environment(self.runtime)
        self.assertEqual(final["MM2_POLY_API_KEY"], CLOB_KEY)
        self.assertEqual(final["MM2_POLY_API_SECRET"], CLOB_SECRET)
        self.assertEqual(final["MM2_POLY_API_PASSPHRASE"], CLOB_PASSPHRASE)
        self.assertEqual(final["MM2_POLY_RELAYER_API_KEY"], RELAYER_KEY)
        self.assertEqual(final["MM2_POLY_RELAYER_API_KEY_ADDRESS"], ADDRESS_ONE)
        self.assertEqual(final["MM2_POLYGON_RPC_URL"], RPC_URL)

    def test_relayer_mismatch_and_malformed_added_modes_leave_previous_config_unchanged(self):
        self.apply(self.telegram)
        before = self.runtime.read_bytes()
        for payload in ({**EXTRA_PAYLOADS[1], "key_address": "0x" + "f" * 40},
                        {**EXTRA_PAYLOADS[0], "api_key": "bad"},
                        {**EXTRA_PAYLOADS[1], "api_key": "bad\n"},
                        {**EXTRA_PAYLOADS[2], "url": "http://rpc.example.com"}):
            with self.subTest(kind=payload["kind"]), self.assertRaises(provision.ProvisionError):
                self.apply(payload)
            self.assertEqual(self.runtime.read_bytes(), before)
            self.assertEqual(list(self.root.glob(".runtime-*")), [])

    def test_unpadded_clob_secret_is_stored_with_sdk_padding_without_printing_it(self):
        from deploy_host import private_environment

        for size in (31, 32):
            raw = b"x" * size
            canonical = base64.urlsafe_b64encode(raw).decode()
            unpadded = canonical.rstrip("=")
            output = StringIO()
            with redirect_stdout(output):
                result = self.apply({**EXTRA_PAYLOADS[0], "api_secret": unpadded})
            self.assertEqual(private_environment(self.runtime)["MM2_POLY_API_SECRET"], canonical)
            self.assertEqual(output.getvalue(), "")
            self.assertNotIn(unpadded, json.dumps(result))
            self.assertNotIn(canonical, json.dumps(result))
            self.assertEqual(result["activation"], "unchanged")

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
        for payload in (self.telegram, {"schema": 1, "kind": "wallet", "private_key": KEY_ONE}, *EXTRA_PAYLOADS):
            with patch.object(provision.os, "replace", side_effect=OSError("offline fixture")), self.assertRaises(OSError):
                self.apply(payload)
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
