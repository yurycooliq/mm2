"""Private immutable S3 transport. Credentials come from the host role, never payloads."""
import base64
import hashlib
from pathlib import Path
import re
from .archive import verify_bundle
from .journal_export import verify_bundle as verify_projection
from .schema import ResearchError, canonical

MAX_OBJECT = 64 * 1024 * 1024


def client(region):
    import boto3
    from botocore.config import Config
    return boto3.Session(region_name=region).client("s3", config=Config(
        connect_timeout=3, read_timeout=15, retries={"mode": "standard", "total_max_attempts": 2}))


def safe_key(prefix, identity, name):
    if not re.fullmatch(r"(?:exports/mm2|research)/", prefix) or not re.fullmatch(r"[0-9a-f]{64}", identity):
        raise ResearchError("unsupported transport namespace")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}", name):
        raise ResearchError("unsafe object name")
    return prefix + identity + "/" + name


def remote_bytes(s3, bucket, key, version=None, maximum=MAX_OBJECT):
    arguments = {"Bucket": bucket, "Key": key}
    if version:
        arguments["VersionId"] = version
    response = s3.get_object(**arguments)
    actual_version = response.get("VersionId")
    if not isinstance(actual_version, str) or not actual_version or actual_version == "null" or (version and actual_version != version):
        response["Body"].close()
        raise ResearchError("versioned transport receipt required")
    if response["ContentLength"] > maximum:
        response["Body"].close()
        raise ResearchError("remote object quota exceeded")
    try:
        data = response["Body"].read(maximum + 1)
    finally:
        response["Body"].close()
    if len(data) > maximum or len(data) != response["ContentLength"]:
        raise ResearchError("remote read quota exceeded")
    return data, response.get("VersionId")


def put_immutable(s3, bucket, key, data):
    if len(data) > MAX_OBJECT:
        raise ResearchError("transport object quota exceeded")
    sha = hashlib.sha256(data).digest()
    # A conditional failure or ambiguous reply is reconciled by an exact bounded GET.
    # No overwrite or delete is issued. Other service errors are never printed.
    try:
        result = s3.put_object(Bucket=bucket, Key=key, Body=data, IfNoneMatch="*",
            ServerSideEncryption="AES256", ChecksumSHA256=base64.b64encode(sha).decode(),
            ContentType="application/json", Metadata={"sha256": sha.hex()})
        version = result.get("VersionId")
    except Exception as error:
        # Only SDK errors can represent an uncertain network/service send.
        from botocore.exceptions import BotoCoreError, ClientError
        if not isinstance(error, (BotoCoreError, ClientError)):
            raise
        version = None
    found, confirmed_version = remote_bytes(s3, bucket, key, version)
    if found != data or not confirmed_version or confirmed_version == "null":
        raise ResearchError("immutable upload reconciliation failed")
    return {"key": key, "sha256": sha.hex(), "bytes": len(data), "version_id": confirmed_version}


def upload_bundle(s3, bucket, directory, prefix):
    directory = Path(directory)
    from .schema import load_json
    projection = "publication_id" in load_json(directory / "manifest.json")
    manifest = (verify_projection if projection else verify_bundle)(directory)
    if prefix == "exports/mm2/" and not projection:
        if manifest["scope"].get("kind") != "sanitized-host-diagnostics" or {item["path"] for item in manifest["files"]} != {"diagnostics.json", "health.json", "coverage.json"}:
            raise ResearchError("producer export scope/member allowlist mismatch")
    identity = manifest.get("publication_id", manifest.get("bundle_id"))
    names = [item["name"] for item in manifest["objects"]] if "objects" in manifest else [item["path"] for item in manifest["files"]]
    # Manifest is the final commit marker; consumers never ingest a partial prefix.
    receipts = [put_immutable(s3, bucket, safe_key(prefix, identity, name), (directory / name).read_bytes())
                for name in names + ["manifest.json"]]
    return {"schema_version": 1, "bundle_id": identity, "bucket": bucket, "objects": receipts,
            "verified_remote": True, "primary_cleanup_allowed": False}


def download_bundle(s3, bucket, prefix, identity, root):
    from .schema import parse_json
    manifest_raw, version = remote_bytes(s3, bucket, safe_key(prefix, identity, "manifest.json"), maximum=512 * 1024)
    manifest = parse_json(manifest_raw)
    if manifest.get("publication_id", manifest.get("bundle_id")) != identity:
        raise ResearchError("transport manifest identity mismatch")
    projection = "publication_id" in manifest
    members = manifest.get("objects") if projection else manifest.get("files")
    if not isinstance(members, list) or not 1 <= len(members) <= 32:
        raise ResearchError("transport member quota exceeded")
    declared_total, declared_names = 0, set()
    for member in members:
        if not isinstance(member, dict):
            raise ResearchError("transport member contract invalid")
        name = member.get("name", member.get("path"))
        safe_key(prefix, identity, name)
        if name in declared_names or name == "manifest.json":
            raise ResearchError("duplicate or reserved transport member")
        declared_names.add(name)
        if type(member.get("bytes")) is not int or member["bytes"] < 0:
            raise ResearchError("transport declaration invalid")
        declared_total += member["bytes"]
        if declared_total > MAX_OBJECT:
            raise ResearchError("transport bundle byte quota exceeded")
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    target = root / identity
    if target.exists():
        (verify_projection if projection else verify_bundle)(target)
        if (target / "manifest.json").read_bytes() != manifest_raw:
            raise ResearchError("remote and retained manifest differ")
        return target
    import uuid, os
    pending = root / (".download-" + uuid.uuid4().hex)
    pending.mkdir(mode=0o700)
    total = 0
    for member in members:
        name = member.get("name", member.get("path"))
        expected = member.get("bytes")
        if type(expected) is not int or not 0 <= expected <= MAX_OBJECT - total:
            raise ResearchError("transport declared object quota exceeded")
        data, _ = remote_bytes(s3, bucket, safe_key(prefix, identity, name), maximum=expected)
        total += len(data)
        if total > MAX_OBJECT or len(data) != member.get("bytes") or hashlib.sha256(data).hexdigest() != member.get("sha256"):
            raise ResearchError("transport member hash/quota mismatch")
        with (pending / name).open("xb") as stream:
            stream.write(data); stream.flush(); os.fsync(stream.fileno())
    with (pending / "manifest.json").open("xb") as stream:
        stream.write(manifest_raw); stream.flush(); os.fsync(stream.fileno())
    (verify_projection if projection else verify_bundle)(pending)
    os.rename(pending, target)
    return target
