"""Profile dispatch preserves secrets, preflight ordering and paused-only launch."""
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import start


class StartProfileTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        (self.root / "config").mkdir()
        for config in start.PROFILES.values():
            (self.root / config).write_text("mode = 'paused'\n")

    def configured_profile(self, mode):
        address = "0x" + "a" * 40
        (self.root / start.PROFILES[mode]).write_text(
            f'mode = "{mode}"\nstate_dir = "/data/{mode}"\nlive_state_dir = "/data/account-live"\n'
            '[live]\nowner_authorized = true\nprotocol = 2\nchain_id = 137\nsignature_type = 0\n'
            'clob_url = "https://clob.polymarket.com"\nallowed_market_prefixes = ["btc-updown-5m-"]\n'
            + "\n".join(f'{field} = "{address}"' for field in (
                "owner_address", "signer_address", "funder_address", "exchange_address", "collateral_address")) + '\n[live.chain]\nrpc_env = "MM2_POLYGON_RPC_URL"\n')

    def configured_deposit_profile(self, mode):
        self.configured_profile(mode)
        path = self.root / start.PROFILES[mode]
        content = path.read_text().replace("signature_type = 0", "signature_type = 3")
        content = content.replace('signer_address = "0x' + "a" * 40 + '"', 'signer_address = "0x' + "b" * 40 + '"')
        path.write_text(content + '\n[live.chain.relayer]\nurl = "https://relayer-v2.polymarket.com"\n'
                        'key_env = "MM2_POLY_RELAYER_API_KEY"\nkey_address_env = "MM2_POLY_RELAYER_API_KEY_ADDRESS"\n'
                        'deadline_seconds = 600\nlog_chunk_blocks = 50\nmax_scan_pages = 8\n')
        return path

    def tearDown(self):
        self.directory.cleanup()

    def test_dry_run_every_profile_does_not_read_environment_file_or_launch_docker(self):
        for mode, config in start.PROFILES.items():
            captured = StringIO()
            with self.subTest(mode=mode), patch.object(start, "ROOT", self.root), \
                    patch.object(start.subprocess, "run") as runner, redirect_stdout(captured):
                self.assertEqual(start.main(["--mode", mode, "--dry-run"]), 0)
            runner.assert_not_called()
            plan = json.loads(captured.getvalue())
            self.assertEqual(plan["config"], config)
            self.assertTrue(plan["starts_paused"])
            self.assertEqual(plan["commands"][0][-2:], ["config", "--quiet"])

    def test_launch_overrides_only_process_config_and_preserves_existing_secret_file(self):
        self.configured_profile("both")
        secret = "private-test-secret-never-display"
        original = "MM2_ENGINE_API_TOKEN=" + secret + "\nMM2_ENGINE_CONFIG=./config/demo.toml\n"
        env_file = self.root / ".env"
        env_file.write_text(original)
        captured = StringIO()
        with patch.object(start, "ROOT", self.root), patch.dict(os.environ, {"MM2_ENGINE_API_TOKEN": secret}), \
                patch.object(start.subprocess, "run") as runner, redirect_stdout(captured):
            self.assertEqual(start.main(["--mode", "both", "--build"]), 0)
            self.assertEqual(os.environ["MM2_ENGINE_API_TOKEN"], secret)
        self.assertEqual(env_file.read_text(), original)
        self.assertEqual(runner.call_count, 2)
        for call in runner.call_args_list:
            self.assertEqual(call.kwargs["env"]["MM2_ENGINE_CONFIG"], "./config/both.toml")
            self.assertEqual(call.kwargs["env"]["MM2_ENGINE_API_TOKEN"], secret)
            self.assertTrue(call.kwargs["check"])
            self.assertNotIn("resume", " ".join(call.args[0]))
        self.assertEqual(runner.call_args_list[-1].args[0], ["docker", "compose", "up", "--detach", "--build"])
        self.assertNotIn(secret, captured.getvalue())

    def test_failed_preflight_never_runs_up(self):
        self.configured_profile("trading")
        (self.root / ".env").write_text("private-runtime-settings\n")
        captured = StringIO()
        with patch.object(start, "ROOT", self.root), \
                patch.object(start.subprocess, "run", side_effect=subprocess.CalledProcessError(1, ["docker", "compose", "config", "--quiet"])) as runner, \
                redirect_stdout(captured), redirect_stderr(captured):
            self.assertEqual(start.main(["--mode", "trading"]), 1)
        self.assertEqual(runner.call_count, 1)
        self.assertEqual(runner.call_args.args[0], ["docker", "compose", "config", "--quiet"])

    def test_inactive_template_missing_account_or_shared_ledger_refuses_before_docker(self):
        (self.root / ".env").write_text("private-runtime-settings\n")
        for fault in ("inactive", "account", "shared", "protocol", "markets", "chain", "oversized"):
            with self.subTest(fault=fault):
                self.configured_profile("both")
                path = self.root / start.PROFILES["both"]
                content = path.read_text()
                if fault == "inactive":
                    content = content.replace("owner_authorized = true", "owner_authorized = false")
                elif fault == "account":
                    content = content.replace('owner_address = "0x' + "a" * 40 + '"', 'owner_address = ""')
                elif fault == "shared":
                    content = content.replace('live_state_dir = "/data/account-live"', 'live_state_dir = "/data/both"')
                elif fault == "protocol":
                    content = content.replace("protocol = 2", "protocol = 3")
                elif fault == "markets":
                    content = content.replace('["btc-updown-5m-"]', '[]')
                elif fault == "chain":
                    content = content.replace('rpc_env = "MM2_POLYGON_RPC_URL"', 'rpc_env = ""')
                else:
                    content = "#" + "x" * 65_536
                path.write_text(content)
                captured = StringIO()
                with patch.object(start, "ROOT", self.root), patch.object(start.subprocess, "run") as runner, \
                        redirect_stdout(captured), redirect_stderr(captured):
                    with self.assertRaises(SystemExit):
                        start.main(["--mode", "both"])
                runner.assert_not_called()
                self.assertNotIn("private-runtime-settings", captured.getvalue())

    def test_missing_private_environment_and_invalid_project_names_refuse_startup(self):
        for arguments in (["--mode", "simulation"], ["--project-name", "--dangerous", "--dry-run"],
                          ["--project-name", "../escape", "--dry-run"]):
            with self.subTest(arguments=arguments), patch.object(start, "ROOT", self.root), \
                    patch.object(start.subprocess, "run") as runner, redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                with self.assertRaises(SystemExit):
                    start.main(arguments)
            runner.assert_not_called()

    def test_deposit_profile_binds_controller_relayer_and_finite_limits_without_reading_keys(self):
        for mode in ("trading", "both"):
            path = self.configured_deposit_profile(mode)
            start.preflight_trading(path, mode)
            valid = path.read_text()
            for fault in ("missing", "same", "url", "key", "key_owner", "builder_key", "builder_secret", "builder_passphrase",
                          "deadline", "scan", "owner", "type"):
                content = valid
                if fault == "missing":
                    content = content.split("[live.chain.relayer]")[0]
                elif fault == "same":
                    content = content.replace('signer_address = "0x' + "b" * 40, 'signer_address = "0x' + "a" * 40)
                elif fault == "url":
                    content = content.replace("https://relayer-v2.polymarket.com", "https://other.example.com")
                elif fault == "key":
                    content = content.replace('key_env = "MM2_POLY_RELAYER_API_KEY"', 'key_env = "OTHER_KEY"')
                elif fault == "key_owner":
                    content = content.replace('key_address_env = "MM2_POLY_RELAYER_API_KEY_ADDRESS"', 'key_address_env = "OTHER_ADDRESS"')
                elif fault.startswith("builder_"):
                    content += fault + '_env = "OTHER_BUILDER_FIELD"\n'
                elif fault == "deadline":
                    content = content.replace("deadline_seconds = 600", "deadline_seconds = 0")
                elif fault == "scan":
                    content = content.replace("max_scan_pages = 8", "max_scan_pages = 33")
                elif fault == "owner":
                    content = content.replace('owner_address = "0x' + "a" * 40, 'owner_address = "0x' + "b" * 40)
                else:
                    content = content.replace("signature_type = 3", "signature_type = 4")
                path.write_text(content)
                with self.subTest(mode=mode, fault=fault), self.assertRaises(ValueError):
                    start.preflight_trading(path, mode)

    def test_packaged_public_wallet_profiles_remain_inactive(self):
        for mode in ("trading", "both"):
            path = Path(__file__).resolve().parent.parent / start.PROFILES[mode]
            profile = start.tomllib.loads(path.read_text(encoding="utf-8"))
            self.assertIs(profile["live"]["owner_authorized"], False)
            self.assertEqual(profile["live"]["signature_type"], 3)
            self.assertEqual(profile["live"]["owner_address"], profile["live"]["funder_address"])
            self.assertNotEqual(profile["live"]["signer_address"].lower(), profile["live"]["funder_address"].lower())
            with self.assertRaises(ValueError):
                start.preflight_trading(path, mode)


if __name__ == "__main__":
    unittest.main()
