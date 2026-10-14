"""Explicit MM2 IAM-key provisioning; credential material stays outside the repo.

Use --provision only after the owner has authorized the IAM user/access key.
credential-process is for AWS CLI consumption, never an interactive diagnostic.
"""
import argparse
import configparser
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from datetime import datetime, timezone

ACCOUNT = "276571117378"
REGION = "eu-west-1"
INSTANCE = "i-053db5508efc00b4a"
USER = "mm2-codex-automation"
USER_PATH = "/mm2/"
USER_ARN = f"arn:aws:iam::{ACCOUNT}:user{USER_PATH}{USER}"
POLICY_NAME = "mm2-single-instance-operator"
PROFILE = "mm2"
AUTH_KEYS = {"login_session", "credential_process", "credential_source", "source_profile",
             "role_arn", "role_session_name", "external_id", "mfa_serial", "web_identity_token_file",
             "aws_access_key_id", "aws_secret_access_key", "aws_session_token", "aws_security_token"}
PRIVATE = Path.home() / ".aws" / "mm2-automation"

def section_edit(text, name, replacement):
    """Replace one INI section while preserving all other bytes and comments."""
    headers = list(re.finditer(r"(?m)^\[[^\r\n]+\][ \t]*(?:\r?\n|$)", text))
    matches = [i for i, header in enumerate(headers) if header.group().strip() == f"[{name}]"]
    if len(matches) > 1:
        raise RuntimeError("Duplicate target AWS profile sections")
    if not matches:
        return text if not replacement else text + ("\n" if text and not text.endswith("\n") else "") + replacement
    i = matches[0]
    start = headers[i].start()
    end = headers[i + 1].start() if i + 1 < len(headers) else len(text)
    return text[:start] + replacement + text[end:]

def profile_sections(text):
    parser = configparser.RawConfigParser()
    parser.read_string(text)
    return {name: dict(parser.items(name)) for name in parser.sections()}

def windows_acl(path):
    if os.name != "nt":
        raise RuntimeError("Provisioning requires the intended Windows user")
    who = subprocess.run(["whoami", "/user", "/fo", "csv", "/nh"], capture_output=True, text=True, check=True)
    sid = re.search(r"S-1-5-[0-9-]+", who.stdout)
    if not sid:
        raise RuntimeError("Cannot identify current Windows user")
    command = ["icacls", str(path), "/inheritance:r", "/grant:r",
               "*" + sid.group() + ":(F)", "*S-1-5-18:(F)"]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError("Cannot restrict credential file permissions")

class Blob(ctypes.Structure):
    _fields_ = [("size", ctypes.c_ulong), ("data", ctypes.POINTER(ctypes.c_ubyte))]

def dpapi(value, decrypt=False):
    if os.name != "nt":
        raise RuntimeError("DPAPI requires Windows")
    buffer = ctypes.create_string_buffer(value)
    source = Blob(len(value), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte)))
    target = Blob()
    crypt = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.LocalFree.argtypes = [ctypes.c_void_p]
    kernel.LocalFree.restype = ctypes.c_void_p
    if decrypt:
        ok = crypt.CryptUnprotectData(ctypes.byref(source), None, None, None, None, 1, ctypes.byref(target))
    else:
        ok = crypt.CryptProtectData(ctypes.byref(source), "MM2 AWS automation", None, None, None, 1, ctypes.byref(target))
    if not ok:
        raise RuntimeError("Windows credential protection failed")
    try:
        return ctypes.string_at(target.data, target.size)
    finally:
        kernel.LocalFree(ctypes.cast(target.data, ctypes.c_void_p))

def atomic_write(path, contents, private=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".pending")
    if temporary.exists():
        raise RuntimeError("Pending file exists; inspect the previous outcome")
    with temporary.open("xb") as stream:
        stream.write(contents)
        stream.flush()
        os.fsync(stream.fileno())
    if private:
        windows_acl(temporary)
    os.replace(temporary, path)

class AwsError(RuntimeError):
    pass

def aws(executable, arguments, env=None):
    child_env = dict(os.environ if env is None else env)
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN"):
        child_env.pop(key, None)
    child_env["AWS_PAGER"] = ""
    command = [executable] + arguments + ["--profile", PROFILE, "--region", REGION,
                                        "--output", "json", "--no-cli-pager",
                                        "--cli-connect-timeout", "10", "--cli-read-timeout", "30"]
    result = subprocess.run(command, capture_output=True, text=True, env=child_env, timeout=60)
    if result.returncode:
        match = re.search(r"\(([A-Za-z0-9_.-]+)\) when calling", result.stderr)
        code = match.group(1) if match else "AWS_CLI_FAILED"
        raise AwsError(code)
    return json.loads(result.stdout) if result.stdout.strip() else {}

def outside_repo(path, repo):
    path = path.resolve()
    if path == repo or repo in path.parents:
        raise RuntimeError("AWS credential/config material must stay outside the repository")
    return path

def credential_output(secret_path):
    secret = json.loads(dpapi(secret_path.read_bytes(), decrypt=True))
    if secret["user_arn"] != USER_ARN:
        raise RuntimeError("Credential identity does not match MM2 automation")
    return {"Version": 1, "AccessKeyId": secret["AccessKeyId"], "SecretAccessKey": secret["SecretAccessKey"]}

def provision(args):
    repo = Path(__file__).resolve().parents[1]
    policy_path = repo / "infra" / "aws" / "mm2-automation-policy.json"
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    policy_hash = hashlib.sha256(json.dumps(policy, sort_keys=True).encode()).hexdigest()
    identity = aws(args.aws, ["sts", "get-caller-identity"])
    if identity["Account"] != ACCOUNT or identity["Arn"] == USER_ARN:
        raise RuntimeError("Expected an administrative session in the intended account")
    PRIVATE.mkdir(parents=True, exist_ok=True)
    windows_acl(PRIVATE)
    secret_path = PRIVATE / "key.dpapi"
    marker = PRIVATE / "key-creation-intent.json"
    try:
        user = aws(args.aws, ["iam", "get-user", "--user-name", USER])["User"]
    except AwsError as error:
        if str(error) != "NoSuchEntity":
            raise
        user = aws(args.aws, ["iam", "create-user", "--user-name", USER, "--path", USER_PATH,
                              "--tags", "Key=Project,Value=mm2", "Key=Purpose,Value=local-automation"])["User"]
    if user["Arn"] != USER_ARN or {tag["Key"]: tag["Value"] for tag in user.get("Tags", [])}.get("Project") != "mm2":
        raise RuntimeError("Existing IAM user is not the intended dedicated MM2 user")
    for operation, field in [("list-groups-for-user", "Groups"), ("list-attached-user-policies", "AttachedPolicies")]:
        if aws(args.aws, ["iam", operation, "--user-name", USER])[field]:
            raise RuntimeError("Existing IAM user has unexpected inherited/managed permissions")
    inline_names = aws(args.aws, ["iam", "list-user-policies", "--user-name", USER])["PolicyNames"]
    if set(inline_names) - {POLICY_NAME}:
        raise RuntimeError("Existing IAM user has unexpected inline permissions")
    try:
        aws(args.aws, ["iam", "get-login-profile", "--user-name", USER])
    except AwsError as error:
        if str(error) != "NoSuchEntity":
            raise
    else:
        raise RuntimeError("Dedicated automation user unexpectedly has console sign-in")
    aws(args.aws, ["iam", "put-user-policy", "--user-name", USER, "--policy-name", POLICY_NAME,
                   "--policy-document", "file://" + policy_path.as_posix()])
    readback = aws(args.aws, ["iam", "get-user-policy", "--user-name", USER, "--policy-name", POLICY_NAME])
    if readback["PolicyDocument"] != policy:
        raise RuntimeError("Installed IAM policy differs from the reviewed policy")
    keys = aws(args.aws, ["iam", "list-access-keys", "--user-name", USER])["AccessKeyMetadata"]
    if not secret_path.exists():
        if keys or marker.exists():
            raise RuntimeError("Unreconciled access key/creation intent; do not create another key blindly")
        atomic_write(marker, json.dumps({"user_arn": USER_ARN, "at": datetime.now(timezone.utc).isoformat()}).encode(), True)
        response = aws(args.aws, ["iam", "create-access-key", "--user-name", USER])["AccessKey"]
        secret = {"user_arn": USER_ARN, "AccessKeyId": response["AccessKeyId"], "SecretAccessKey": response["SecretAccessKey"]}
        atomic_write(secret_path, dpapi(json.dumps(secret).encode()), True)
    credential = credential_output(secret_path)
    keys = aws(args.aws, ["iam", "list-access-keys", "--user-name", USER])["AccessKeyMetadata"]
    if len(keys) != 1 or keys[0]["AccessKeyId"] != credential["AccessKeyId"] or keys[0]["Status"] != "Active":
        raise RuntimeError("IAM access-key metadata does not match the protected local credential")
    installed_helper = PRIVATE / "credential_helper.py"
    atomic_write(installed_helper, Path(__file__).read_bytes(), True)
    command = subprocess.list2cmdline([sys.executable, "-B", str(installed_helper), "--credential-process", str(secret_path)])
    config_path = outside_repo(Path(os.environ.get("AWS_CONFIG_FILE", Path.home() / ".aws" / "config")), repo)
    credentials_path = outside_repo(Path(os.environ.get("AWS_SHARED_CREDENTIALS_FILE", Path.home() / ".aws" / "credentials")), repo)
    config_before = config_path.read_bytes() if config_path.exists() else b""
    credentials_before = credentials_path.read_bytes() if credentials_path.exists() else b""
    config_text = config_before.decode("utf-8-sig")
    credentials_text = credentials_before.decode("utf-8-sig")
    old_profiles = profile_sections(config_text)
    settings = {key: value for key, value in old_profiles.get("profile " + PROFILE, {}).items()
                if key not in AUTH_KEYS and not key.startswith("sso_")}
    settings.update(region=REGION, output="json", credential_process=command)
    section = "[profile " + PROFILE + "]\n" + "".join(key + " = " + value + "\n" for key, value in settings.items()) + "\n"
    config_after = section_edit(config_text, "profile " + PROFILE, section).encode("utf-8")
    credentials_after = section_edit(credentials_text, PROFILE, "").encode("utf-8")
    backup = PRIVATE / ("backup-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    backup.mkdir()
    windows_acl(backup)
    atomic_write(backup / "config", config_before, True)
    atomic_write(backup / "credentials", credentials_before, True)
    validation_config = PRIVATE / "validation.config"
    validation_credentials = PRIVATE / "validation.credentials"
    atomic_write(validation_config, section.encode(), True)
    atomic_write(validation_credentials, b"", True)
    validation_env = dict(os.environ, AWS_CONFIG_FILE=str(validation_config), AWS_SHARED_CREDENTIALS_FILE=str(validation_credentials))
    verified = aws(args.aws, ["sts", "get-caller-identity"], env=validation_env)
    if verified["Arn"] != USER_ARN:
        raise RuntimeError("Protected credential process did not authenticate as the intended IAM user")
    connection = aws(args.aws, ["ssm", "get-connection-status", "--target", INSTANCE], env=validation_env)
    atomic_write(config_path, config_after, True)
    if credentials_path.exists():
        atomic_write(credentials_path, credentials_after, True)
    final_identity = aws(args.aws, ["sts", "get-caller-identity"])
    if final_identity["Arn"] != USER_ARN:
        raise RuntimeError("Profile migration identity mismatch; inspect the protected backup")
    receipt = {"status": "CONFIGURED", "at": datetime.now(timezone.utc).isoformat(), "user_arn": USER_ARN,
               "profile": PROFILE, "region": REGION, "instance": INSTANCE, "policy_sha256": policy_hash,
               "key_storage": "Windows DPAPI current user", "key_count": len(keys), "console_login": False,
               "connection_status": connection.get("Status"), "admin_source_arn": identity["Arn"],
               "protected_backup": str(backup), "helper_sha256": hashlib.sha256(installed_helper.read_bytes()).hexdigest(),
               "owner_commands_sent": False}
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    atomic_write(args.receipt, json.dumps(receipt, indent=2).encode())
    print(json.dumps(receipt))

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--provision", action="store_true")
    action.add_argument("--credential-process", type=Path)
    parser.add_argument("--aws", default="aws")
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()
    if args.credential_process:
        print(json.dumps(credential_output(args.credential_process)))
    else:
        if args.receipt is None:
            parser.error("--provision requires --receipt (public metadata only)")
        provision(args)

if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # No subprocess output, response bodies, credentials or locals in diagnostics.
        print("MM2 credential setup failed: " + type(error).__name__ + ": " + str(error), file=sys.stderr)
        raise SystemExit(1)
