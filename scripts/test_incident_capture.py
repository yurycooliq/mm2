import json
import subprocess
import sys
import unittest

from incident_capture import MAX_BYTES, bounded_command, collect, diagnostic_message, fingerprint, sanitize


class IncidentCaptureTests(unittest.TestCase):
    def test_fixed_diagnostic_projection_is_idempotent_without_arbitrary_suffixes(self):
        safe = "diagnostic_code=feed_queue_unavailable source=binance fatal=true"
        self.assertEqual(diagnostic_message(safe), safe)
        hostile = safe + " token=SECRET_CANARY"
        self.assertEqual(diagnostic_message(hostile), "diagnostic_code=feed_queue_unavailable")
        self.assertEqual(diagnostic_message("diagnostic_code=unknown_private_code source=binance fatal=true"), "")

    def test_private_values_and_url_paths_are_removed(self):
        private = "token=secret-value https://polygon-mainnet.g.alchemy.com/v2/private-url 123456789:" + "a" * 35
        private += " 0x" + "b" * 64 + " MM2_POLY_PRIVATE_KEY=private-wallet"
        cleaned = sanitize(private)
        for secret in ("secret-value", "private-url", "a" * 35, "b" * 64, "private-wallet"):
            self.assertNotIn(secret, cleaned)
        self.assertIn("polygon-mainnet.g.alchemy.com", cleaned)

    def test_fingerprint_normalizes_numeric_values(self):
        self.assertEqual(fingerprint("durability", "capacity reached 100"), fingerprint("durability", "capacity reached 200"))

    def test_structured_proxy_logs_exclude_requests_and_info_noise(self):
        raw = '2026-10-06T09:00:00Z ' + json.dumps({"level": "error", "msg": "connection refused", "request": {"headers": {"Cookie": ["private-cookie"]}}})
        self.assertEqual(diagnostic_message(raw), "diagnostic_code=transport")
        self.assertEqual(diagnostic_message(json.dumps({"level": "info", "msg": "TLS connection policies"})), "")
        self.assertNotIn("private-json", sanitize('{"token":"private-json"}'))

    def test_rust_nested_codes_and_unknown_text_never_export_private_values(self):
        raw = json.dumps({"level": "WARN", "fields": {"message": "secret=short-private",
            "code": "native_snapshot_bootstrap_timeout", "source": "clob", "fatal": False,
            "private_key": "do-not-export"}})
        self.assertEqual(diagnostic_message(raw), "diagnostic_code=native_snapshot_bootstrap_timeout source=clob fatal=false")
        self.assertEqual(diagnostic_message("thread panicked secret=short-private"), "diagnostic_code=panic")
        self.assertEqual(diagnostic_message("secret=short-private"), "")

    def test_collection_is_bounded_and_never_reads_environment_or_ledger(self):
        commands = []
        identity = "a" * 64
        def run(args, **kwargs):
            commands.append(args)
            self.assertLessEqual(kwargs["timeout"], 12)
            text = ""
            if args[0] == "systemctl":
                text = "NRestarts=7\nActiveState=active\nUnexpectedSecret=do-not-export\n"
            elif args[0] == "journalctl":
                text = json.dumps({"MESSAGE": "Production lifecycle failed; token=secret", "__REALTIME_TIMESTAMP": "123"})
            elif args[1] == "ps":
                text = identity + " engine\n"
            elif args[1] == "inspect":
                text = "running 0 false 0 2026-10-06T09:00:00Z 0001-01-01T00:00:00Z\n"
            elif args[1] == "logs":
                text = "thread panicked at https://example.com/private\n"
            return subprocess.CompletedProcess(args, 0, text, "")
        result = collect(run=run)
        self.assertEqual(result["systemd"]["NRestarts"], "7")
        self.assertEqual(len(result["incidents"]), 2)
        self.assertEqual(result["quality"]["status"], "PARTIAL")
        self.assertNotIn("do-not-export", json.dumps(result))
        self.assertNotIn("token=secret", json.dumps(result))
        self.assertTrue(all(".Config.Env" not in " ".join(args) for args in commands))
        self.assertTrue(all("/etc/mm2" not in " ".join(args) for args in commands))

    def test_command_failures_do_not_export_stderr(self):
        def run(args, **kwargs):
            return subprocess.CompletedProcess(args, 1, "", "private stderr token")
        result = collect(run=run)
        self.assertEqual(result["quality"]["status"], "PARTIAL")
        self.assertNotIn("private stderr", json.dumps(result))

    def test_scope_refuses_arbitrary_units_and_time_ranges(self):
        for unit, hours in (("other.service", 6), ("mm2.service", 100), ("mm2.service", True)):
            with self.assertRaises(ValueError):
                collect(unit, hours)

    def test_real_child_output_has_hard_byte_limit(self):
        text, error = bounded_command([sys.executable, "-c", f"import sys; sys.stdout.write('x'*{MAX_BYTES + 100})"])
        self.assertIsNone(text)
        self.assertEqual(error, "output_bound")


if __name__ == "__main__":
    unittest.main()
