#!/usr/bin/env python3
"""Export reviewed local Linux/amd64 images and a public AWS runtime bundle, without secrets."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")
PUBLIC_FILES = (
    "infra/aws/host.cloudformation.json", "infra/aws/compose.yaml", "infra/aws/Caddyfile",
    "infra/aws/mm2.service", "infra/aws/install_runtime.sh", "infra/aws/mount_state.sh",
    "scripts/deploy_package.py", "scripts/deploy_host.py", "scripts/deploy_supervisor.py", "scripts/deploy_verify_proxy.py", "scripts/start.py",
    "scripts/ledger_ops.py", "scripts/soak.py", "scripts/bootstrap.sh", "scripts/provision_private_runtime.py",
    "scripts/smoke_public.py", "scripts/verify_stack.py",
    "config/demo.toml", "config/simulation.toml", "config/public.toml",
    "config/trading.toml", "config/both.toml", "docs/AWS_DEPLOY.md", "docs/TELEGRAM_SETUP.md",
    "docs/POLYMARKET_ACCESS.md",
)
MAX_IMAGE_ARCHIVE = 8 * 1024**3


def digest(path):
    result = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            size += len(chunk)
            if size > MAX_IMAGE_ARCHIVE:
                raise ValueError("release object exceeds the 8 GiB bound")
            result.update(chunk)
    return {"sha256": result.hexdigest(), "bytes": size}


def image_identity(reference, run=subprocess.run, expected_revision=None):
    projection = "{{.Id}} {{.Os}} {{.Architecture}}"
    if expected_revision is not None:
        if not re.fullmatch(r"[0-9a-f]{40}", expected_revision):
            raise ValueError("reviewed source revision must be an exact Git commit")
        projection += ' {{index .Config.Labels "org.opencontainers.image.revision"}}'
    completed = run(["docker", "image", "inspect", "--format", projection, reference],
                    check=True, capture_output=True, text=True, timeout=30)
    fields = completed.stdout.strip().split()
    expected_fields = 4 if expected_revision is not None else 3
    if len(fields) != expected_fields or not IMAGE_ID.fullmatch(fields[0]) or fields[1:3] != ["linux", "amd64"]:
        raise ValueError("release requires locally available immutable Linux/amd64 images")
    if expected_revision is not None and fields[3] != expected_revision:
        raise ValueError("local application image does not bind the reviewed source commit")
    return fields[0]


def verify_release(directory):
    directory = directory.resolve(strict=True)
    manifest_path = directory / "release-manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file() or manifest_path.stat().st_size > 64 * 1024:
        raise ValueError("release manifest is missing, indirect or oversized")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if set(manifest) != {"schema", "source_commit", "platform", "images", "files"}:
        raise ValueError("unsupported release manifest fields")
    if manifest["schema"] != 1 or manifest["platform"] != "linux/amd64" or not re.fullmatch(r"[0-9a-f]{40}", manifest["source_commit"]):
        raise ValueError("unsupported release identity")
    if not isinstance(manifest["images"], dict) or set(manifest["images"]) != {"engine", "console", "proxy"}:
        raise ValueError("release requires exactly three reviewed images")
    if any(not isinstance(value, str) or not IMAGE_ID.fullmatch(value) for value in manifest["images"].values()):
        raise ValueError("images must use immutable local image IDs")
    expected = set(PUBLIC_FILES) | {"images.tar"}
    if not isinstance(manifest["files"], dict) or set(manifest["files"]) != expected:
        raise ValueError("release has missing or unexpected public objects")
    found = set()
    for path in directory.rglob("*"):
        if path.is_symlink():
            raise ValueError("release symlinks are forbidden")
        if path.is_file():
            found.add(path.relative_to(directory).as_posix())
    if found != expected | {"release-manifest.json"}:
        raise ValueError("release contains unexpected files, including possible private material")
    for name, evidence in manifest["files"].items():
        if not isinstance(evidence, dict) or set(evidence) != {"sha256", "bytes"} or digest(directory / name) != evidence:
            raise ValueError("release checksum/size mismatch")
    return manifest


def export_release(destination, references, root=ROOT, run=subprocess.run):
    destination = destination.absolute()
    if set(references) != {"engine", "console", "proxy"}:
        raise ValueError("export requires exactly engine, console and proxy images")
    if destination.exists() or destination.is_symlink():
        raise ValueError("release destination must be new; existing paths are preserved")
    if not destination.parent.is_dir():
        raise ValueError("release parent must already exist")
    status = run(["git", "status", "--porcelain", "--untracked-files=normal"], cwd=root,
                 check=True, capture_output=True, text=True, timeout=30)
    if status.stdout.strip():
        raise ValueError("commit the reviewed source before exporting; dirty releases are refused")
    revision = run(["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True,
                   text=True, timeout=30).stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("source revision is not a Git commit")
    tracked = set(run(["git", "ls-files"], cwd=root, check=True, capture_output=True,
                      text=True, timeout=30).stdout.splitlines())
    for name in PUBLIC_FILES:
        if name not in tracked or (root / name).is_symlink() or not (root / name).is_file():
            raise ValueError("a required public release file is absent or indirect")
    identities = {role: image_identity(reference, run, revision if role in {"engine", "console"} else None)
                  for role, reference in references.items()}
    destination.mkdir(mode=0o700)
    marker = destination / "INCOMPLETE"
    marker.write_text("Never activate an incomplete release. Preserve for diagnosis.\n", encoding="ascii")
    for name in PUBLIC_FILES:
        output = destination / name
        output.parent.mkdir(parents=True, exist_ok=True)
        # Normalize Windows checkout line endings for Linux scripts and configuration.
        output.write_bytes((root / name).read_bytes().replace(b"\r\n", b"\n"))
    run(["docker", "image", "save", "--output", str(destination / "images.tar"), *identities.values()],
        check=True, timeout=600)
    manifest = {"schema": 1, "source_commit": revision, "platform": "linux/amd64", "images": identities,
                "files": {name: digest(destination / name) for name in (*PUBLIC_FILES, "images.tar")}}
    (destination / "release-manifest.json").write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    marker.unlink()
    verify_release(destination)
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    export = commands.add_parser("export")
    export.add_argument("--destination", type=Path, required=True)
    export.add_argument("--engine-image", default="mm2-engine:local")
    export.add_argument("--console-image", default="mm2-console:local")
    export.add_argument("--proxy-image", required=True, help="Explicitly reviewed, already local Caddy image reference")
    verify = commands.add_parser("verify")
    verify.add_argument("--source", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        manifest = (verify_release(args.source) if args.action == "verify" else export_release(
            args.destination, {"engine": args.engine_image, "console": args.console_image, "proxy": args.proxy_image}))
        print(json.dumps({"verified": True, "source_commit": manifest["source_commit"], "platform": manifest["platform"],
                          "images": manifest["images"], "contains_runtime_secrets": False}, sort_keys=True))
        return 0
    except (OSError, ValueError, subprocess.SubprocessError):
        print("Release preparation/verification failed; existing state is unchanged and incomplete output is preserved.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
