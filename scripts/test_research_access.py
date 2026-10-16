import subprocess
import unittest
from types import SimpleNamespace

from scripts.research_access import check_access


class ResearchAccessTests(unittest.TestCase):
    def test_fixed_failure_classes_never_export_raw_credential_or_response_text(self):
        for message, expected in (("Your session has expired token=SECRET_CANARY", "EXPIRED"),
                                  ("The config profile could not be found", "UNCONFIGURED"),
                                  ("AccessDeniedException: SECRET_CANARY", "DENIED"),
                                  ("Could not connect to endpoint URL SECRET_CANARY", "UNREACHABLE"),
                                  ("unknown response SECRET_CANARY", "AUTH_ERROR")):
            with self.subTest(expected=expected):
                result = check_access("mm2", "eu-west-1", run=lambda *a, **k: SimpleNamespace(returncode=1, stderr=message))
                self.assertEqual(result["status"], expected)
                self.assertNotIn("SECRET_CANARY", str(result))
                self.assertEqual(result["scope"], "authentication_only_not_engine_health")

    def test_authenticated_root_is_reported_without_claiming_renewal_or_engine_health(self):
        reply = SimpleNamespace(returncode=0, stdout='{"Arn":"arn:aws:iam::123456789012:root"}')
        first = check_access("mm2", "eu-west-1", run=lambda *a, **k: reply, now="2026-10-07T00:00:00Z")
        later = check_access("mm2", "eu-west-1", run=lambda *a, **k: reply, now="2026-10-07T01:00:00Z")
        self.assertTrue(first["root_identity"])
        self.assertEqual(first["fingerprint"], later["fingerprint"])
        self.assertIsNone(first["credential_expiry_utc"])
        self.assertFalse(first["primary_mutations"])

    def test_missing_cli_and_timeout_are_separate_from_service_failure(self):
        def absent(*args, **kwargs):
            raise FileNotFoundError()
        def timeout(*args, **kwargs):
            raise subprocess.TimeoutExpired("fixture", 20)
        self.assertEqual(check_access("mm2", "eu-west-1", run=absent)["status"], "CLI_UNAVAILABLE")
        self.assertEqual(check_access("mm2", "eu-west-1", run=timeout)["status"], "UNREACHABLE")

    def test_profile_and_region_reject_command_or_option_injection(self):
        for profile, region in (("--profile", "eu-west-1"), ("mm2;evil", "eu-west-1"), ("mm2", "--region")):
            with self.assertRaises(ValueError):
                check_access(profile, region)


if __name__ == "__main__":
    unittest.main()
