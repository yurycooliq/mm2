import importlib.util
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("mm2_iam_access", Path(__file__).with_name("mm2_iam_access.py"))
access = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(access)

class ProfileMigrationTests(unittest.TestCase):
    def temporary_directory(self):
        root = Path(__file__).resolve().parents[1] / "output" / "iam-access-tests"
        root.mkdir(parents=True, exist_ok=True)
        directory = tempfile.TemporaryDirectory(dir=root)
        self.assertIn(root.resolve(), Path(directory.name).resolve().parents)
        return directory

    def test_target_replacement_preserves_other_sections_exactly(self):
        prefix = "# unrelated settings\r\n[default]\r\nregion = eu-central-1\r\n\r\n"
        suffix = "[profile colleague]\r\ncredential_process = unchanged command\r\n"
        original = prefix + "[profile mm2]\r\nlogin_session = old\r\n\r\n" + suffix
        replacement = "[profile mm2]\nregion = eu-west-1\ncredential_process = protected-helper\n\n"
        result = access.section_edit(original, "profile mm2", replacement)
        self.assertEqual(result, prefix + replacement + suffix)
        self.assertNotIn("login_session", result)

    def test_shared_credential_removal_preserves_other_credentials(self):
        original = "[default]\naws_access_key_id = unrelated\n\n[mm2]\naws_session_token = expired\n\n[other]\nvalue = retained\n"
        result = access.section_edit(original, "mm2", "")
        self.assertEqual(result, "[default]\naws_access_key_id = unrelated\n\n[other]\nvalue = retained\n")

    def test_absent_profile_appends_without_overwriting_other_settings(self):
        self.assertEqual(access.section_edit("[default]\nregion=x", "profile mm2", "[profile mm2]\nregion=y\n"),
                         "[default]\nregion=x\n[profile mm2]\nregion=y\n")

    def test_duplicate_target_is_rejected_before_modification(self):
        with self.assertRaises(RuntimeError):
            access.section_edit("[profile mm2]\nx=1\n[profile mm2]\nx=2\n", "profile mm2", "replacement")

    def test_credential_paths_in_repository_are_rejected(self):
        repo = Path(__file__).resolve().parents[1]
        with self.assertRaises(RuntimeError):
            access.outside_repo(repo / "output" / "credentials", repo)

    def test_existing_unknown_key_blocks_another_creation(self):
        policy = json.loads((Path(__file__).resolve().parents[1] / "infra" / "aws" / "mm2-automation-policy.json").read_text())
        calls = []
        def fake_aws(executable, arguments, env=None):
            calls.append(arguments[:2])
            operation = arguments[1]
            responses = {
                "get-caller-identity": {"Account": access.ACCOUNT, "Arn": "arn:aws:iam::" + access.ACCOUNT + ":root"},
                "get-user": {"User": {"Arn": access.USER_ARN, "Tags": [{"Key": "Project", "Value": "mm2"}]}},
                "list-groups-for-user": {"Groups": []},
                "list-attached-user-policies": {"AttachedPolicies": []},
                "list-user-policies": {"PolicyNames": []},
                "put-user-policy": {},
                "get-user-policy": {"PolicyDocument": policy},
                "list-access-keys": {"AccessKeyMetadata": [{"AccessKeyId": "TEST-UNKNOWN", "Status": "Active"}]},
            }
            if operation == "get-login-profile":
                raise access.AwsError("NoSuchEntity")
            return responses[operation]
        with self.temporary_directory() as temp, mock.patch.object(access, "PRIVATE", Path(temp) / "private"), \
                mock.patch.object(access, "windows_acl"), mock.patch.object(access, "aws", side_effect=fake_aws):
            with self.assertRaisesRegex(RuntimeError, "Unreconciled access key"):
                access.provision(SimpleNamespace(aws="unused", receipt=Path(temp) / "receipt.json"))
        self.assertNotIn(["iam", "create-access-key"], calls)

    def test_wrong_account_is_rejected_before_local_or_remote_mutation(self):
        with self.temporary_directory() as temp, mock.patch.object(access, "PRIVATE", Path(temp) / "must-not-exist"), \
                mock.patch.object(access, "aws", return_value={"Account": "000000000000", "Arn": "wrong"}) as invoke:
            with self.assertRaisesRegex(RuntimeError, "intended account"):
                access.provision(SimpleNamespace(aws="unused", receipt=Path(temp) / "receipt.json"))
            self.assertFalse((Path(temp) / "must-not-exist").exists())
            self.assertEqual(invoke.call_count, 1)

    def test_new_key_visibility_retries_only_the_read(self):
        identity = {"Arn": access.USER_ARN}
        with mock.patch.object(access, "aws", side_effect=[access.AwsError("InvalidClientTokenId"), identity]) as invoke, \
                mock.patch.object(access.time, "sleep") as pause:
            self.assertEqual(access.verify_new_credentials("unused", ["sts", "get-caller-identity"], {}), identity)
            self.assertEqual(invoke.call_count, 2)
            self.assertTrue(all(call.args[1] == ["sts", "get-caller-identity"] for call in invoke.call_args_list))
            pause.assert_called_once_with(2)

    def test_propagation_retries_are_bounded(self):
        with mock.patch.object(access, "aws", side_effect=access.AwsError("InvalidClientTokenId")) as invoke, \
                mock.patch.object(access.time, "sleep"):
            with self.assertRaises(access.AwsError):
                access.verify_new_credentials("unused", ["sts", "get-caller-identity"], {})
            self.assertEqual(invoke.call_count, 6)

    @unittest.skipUnless(os.name == "nt", "Windows DPAPI integration")
    def test_dpapi_roundtrip_keeps_plaintext_out_of_blob(self):
        original = json.dumps({"test": "non-secret disposable roundtrip marker"}).encode()
        encrypted = access.dpapi(original)
        self.assertNotIn(original, encrypted)
        self.assertEqual(access.dpapi(encrypted, decrypt=True), original)

if __name__ == "__main__":
    unittest.main()
