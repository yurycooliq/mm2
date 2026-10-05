#!/usr/bin/env python3
"""Owner-entered credentials over verified SSH stdin; no secret files on the client."""
import argparse
import base64
import getpass
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import stat
import subprocess
import sys
import tempfile
import tomllib
import uuid
import warnings
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


ROOT = Path(__file__).resolve().parent.parent
EXPECTED_BOT = "mm2b_bot"
REMOTE_TOOL = "/opt/mm2/tools/provision_private_runtime.py"
MAX_INPUT = 4096
MAX_REPLY = 16384
CURVE_ORDER = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
TOKEN = re.compile(r"([1-9][0-9]{4,19}):[A-Za-z0-9_-]{30,128}")
ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}")
FIELDS = {"telegram": {"token", "owner_id"}, "wallet": {"private_key"},
          "clob": {"api_key", "api_secret", "api_passphrase"},
          "relayer": {"api_key", "key_address"}, "rpc": {"url"}}
INACTIVE = "none; funded configuration remains a separate gate"


class ProvisionError(Exception):
    """Public error text contains no supplied credentials or transport exception."""


class SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        # A mistakenly pasted credential in argv must not be echoed by argparse.
        self.print_usage(sys.stderr)
        self.exit(2, "Invalid arguments. Choose telegram, wallet, clob, relayer or rpc; credentials belong only in the hidden prompt.\n")


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        raise ProvisionError("Telegram redirects are refused.")


def literal_credential(value, maximum):
    """Accept bounded ASCII runtime literals, never shell/Compose interpolation."""
    return (isinstance(value, str) and 1 <= len(value) <= maximum
            and all(33 <= ord(char) <= 126 and char not in "$'\"\\`" for char in value))


def rpc_url(value):
    """Validate a secret-bearing endpoint locally; do not fetch or print it."""
    if not literal_credential(value, 2048):
        raise ProvisionError("Enter one bounded HTTPS Polygon RPC URL without whitespace or interpolation.")
    try:
        parsed = urlsplit(value)
        host = parsed.hostname
        if (not value.startswith("https://") or parsed.scheme != "https" or not host or parsed.username is not None or parsed.password is not None
                or "#" in value or parsed.netloc.endswith(":") or (parsed.port is not None and not 1 <= parsed.port <= 65535)
                or re.search(r"%(?![0-9a-fA-F]{2})|%(?:0[0-9a-fA-F]|1[0-9a-fA-F]|20|7[fF])", value)):
            raise ValueError("invalid endpoint")
        if ":" in host or re.fullmatch(r"[0-9.]+", host):
            ipaddress.ip_address(host)
        elif not re.fullmatch(r"(?=.{1,253}$)(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)*[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?", host):
            raise ValueError("invalid hostname")
    except (ValueError, TypeError):
        raise ProvisionError("Polygon RPC must be a valid HTTPS endpoint without userinfo, fragment or encoded whitespace.") from None
    return value


def canonical_clob_secret(secret):
    """Retain the HMAC key bytes and supply canonical padding required by the pinned SDK."""
    if not isinstance(secret, str) or not re.fullmatch(r"[A-Za-z0-9_-]{16,512}={0,2}", secret):
        raise ValueError("invalid base64")
    decoded = base64.b64decode(secret + "=" * (-len(secret) % 4), altchars=b"-_", validate=True)
    canonical = base64.urlsafe_b64encode(decoded).decode("ascii")
    if not 16 <= len(decoded) <= 256 or canonical.rstrip("=") != secret.rstrip("="):
        raise ValueError("invalid base64 size or encoding")
    return canonical


def validate_payload(payload):
    if not isinstance(payload, dict) or type(payload.get("schema")) is not int or payload.get("schema") != 1:
        raise ProvisionError("Unsupported credential request.")
    kind = payload.get("kind")
    if not isinstance(kind, str) or kind not in FIELDS or set(payload) != {"schema", "kind"} | FIELDS[kind]:
        raise ProvisionError("Unexpected credential fields.")
    if kind == "telegram":
        token, owner = payload["token"], payload["owner_id"]
        if not isinstance(token, str) or not TOKEN.fullmatch(token) or int(token.split(":", 1)[0]) >= 2**52:
            raise ProvisionError("Invalid Telegram bot-token format.")
        if type(owner) is not int or not 0 < owner < 2**52:
            raise ProvisionError("Enter the complete numeric Telegram user ID.")
    elif kind == "wallet":
        key = payload["private_key"]
        if not isinstance(key, str) or not re.fullmatch(r"(?:0x)?[0-9a-fA-F]{64}", key):
            raise ProvisionError("Enter one 32-byte hex signing key; seed phrases are refused.")
        scalar = int(key.removeprefix("0x"), 16)
        if not 0 < scalar < CURVE_ORDER:
            raise ProvisionError("Signing key is outside the secp256k1 range.")
    elif kind == "clob":
        key, secret, passphrase = payload["api_key"], payload["api_secret"], payload["api_passphrase"]
        try:
            if not isinstance(key, str) or len(key) != 36 or str(uuid.UUID(key)) != key.lower() or uuid.UUID(key).int == 0:
                raise ValueError("invalid UUID")
            canonical_clob_secret(secret)
            if not literal_credential(passphrase, 256):
                raise ValueError("invalid passphrase")
        except (ValueError, TypeError):
            raise ProvisionError("Enter an existing CLOB UUID API key, URL-safe base64 secret and bounded literal passphrase.") from None
    elif kind == "relayer":
        if not literal_credential(payload["api_key"], 1024):
            raise ProvisionError("Relayer API key must be a bounded literal header value.")
        owner = payload["key_address"]
        if not isinstance(owner, str) or not ADDRESS.fullmatch(owner) or int(owner[2:], 16) == 0:
            raise ProvisionError("Enter the complete nonzero Relayer key-owner address.")
    elif kind == "rpc":
        rpc_url(payload["url"])
    return payload


def unique_fields(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ProvisionError("Duplicate credential fields are refused.")
        result[key] = value
    return result


def decode_payload(raw):
    """Reject ambiguous JSON and non-ASCII/oversized stdin before any effect."""
    if not isinstance(raw, bytes) or len(raw) > MAX_INPUT:
        raise ProvisionError("Credential request exceeds the supported bound.")

    try:
        return validate_payload(json.loads(raw.decode("ascii"), object_pairs_hook=unique_fields))
    except ProvisionError:
        raise
    except Exception:
        raise ProvisionError("Malformed credential request.") from None


def telegram_identity(token, opener=None):
    """Only public identity returns; Bot API URLs/exceptions never reach stdout."""
    opener = opener or build_opener(ProxyHandler({}), NoRedirect())
    try:
        request = Request("https://api.telegram.org/bot" + token + "/getMe", data=b"{}",
                          headers={"Content-Type": "application/json"}, method="POST")
        with opener.open(request, timeout=15) as response:
            raw = response.read(MAX_REPLY + 1)
        if len(raw) > MAX_REPLY:
            raise ValueError("oversized")
        result = json.loads(raw)
        identity = result.get("result") if isinstance(result, dict) and result.get("ok") is True else None
        if not isinstance(identity, dict) or identity.get("is_bot") is not True:
            raise ValueError("not a bot")
        if identity.get("username") != EXPECTED_BOT or type(identity.get("id")) is not int:
            raise ValueError("wrong bot")
        if identity["id"] != int(token.split(":", 1)[0]):
            raise ValueError("identity mismatch")
        return {"username": EXPECTED_BOT, "bot_id": identity["id"]}
    except Exception:
        raise ProvisionError("Telegram getMe failed or the credential does not belong to @mm2b_bot.") from None


def keccak_public(data):
    """Legacy Keccak-256 for PUBLIC uncompressed EC points, never secret arithmetic."""
    if not isinstance(data, bytes) or len(data) > 4096:
        raise ProvisionError("Invalid public hash input.")
    rotations = ((0, 36, 3, 41, 18), (1, 44, 10, 45, 2), (62, 6, 43, 15, 61),
                 (28, 55, 25, 21, 56), (27, 20, 39, 8, 14))
    constants = (0x0000000000000001, 0x0000000000008082, 0x800000000000808A, 0x8000000080008000,
                 0x000000000000808B, 0x0000000080000001, 0x8000000080008081, 0x8000000000008009,
                 0x000000000000008A, 0x0000000000000088, 0x0000000080008009, 0x000000008000000A,
                 0x000000008000808B, 0x800000000000008B, 0x8000000000008089, 0x8000000000008003,
                 0x8000000000008002, 0x8000000000000080, 0x000000000000800A, 0x800000008000000A,
                 0x8000000080008081, 0x8000000000008080, 0x0000000080000001, 0x8000000080008008)
    mask = (1 << 64) - 1

    def rotate(value, amount):
        return ((value << amount) | (value >> ((64 - amount) % 64))) & mask

    padded = bytearray(data)
    padded.append(1)  # Ethereum uses Keccak domain 0x01, not SHA3 domain 0x06.
    padded.extend(b"\0" * ((-len(padded)) % 136))
    padded[-1] |= 0x80
    state = [0] * 25
    for offset in range(0, len(padded), 136):
        for lane in range(17):
            state[lane] ^= int.from_bytes(padded[offset + lane * 8:offset + lane * 8 + 8], "little")
        for constant in constants:
            columns = [state[x] ^ state[x + 5] ^ state[x + 10] ^ state[x + 15] ^ state[x + 20] for x in range(5)]
            delta = [columns[(x - 1) % 5] ^ rotate(columns[(x + 1) % 5], 1) for x in range(5)]
            for y in range(5):
                for x in range(5):
                    state[x + 5 * y] ^= delta[x]
            moved = [0] * 25
            for y in range(5):
                for x in range(5):
                    moved[y + 5 * ((2 * x + 3 * y) % 5)] = rotate(state[x + 5 * y], rotations[x][y])
            for y in range(5):
                for x in range(5):
                    state[x + 5 * y] = moved[x + 5 * y] ^ ((~moved[(x + 1) % 5 + 5 * y]) & moved[(x + 2) % 5 + 5 * y])
            state[0] ^= constant
    return b"".join(value.to_bytes(8, "little") for value in state)[:32]


def signer_address(private_key, run=subprocess.run):
    """OpenSSL derives the public key. No signing, network or private-key argv/files."""
    validate_payload({"schema": 1, "kind": "wallet", "private_key": private_key})
    secret = bytes.fromhex(private_key.removeprefix("0x"))
    # RFC 5915 ECPrivateKey: version 1, 32-byte scalar, named curve secp256k1.
    encoded = bytes.fromhex("302e0201010420") + secret + bytes.fromhex("a00706052b8104000a")
    try:
        completed = run(["openssl", "ec", "-inform", "DER", "-pubout", "-outform", "DER"],
                        input=encoded, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=10, check=False)
        prefix = bytes.fromhex("3056301006072a8648ce3d020106052b8104000a03420004")
        if completed.returncode != 0 or len(completed.stdout) != len(prefix) + 64 or not completed.stdout.startswith(prefix):
            raise ValueError("invalid public key")
        return "0x" + keccak_public(completed.stdout[len(prefix):])[-20:].hex()
    except Exception:
        raise ProvisionError("OpenSSL could not verify the signing-key public address.") from None


def private_directory(path):
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o700:
        raise ProvisionError("Runtime directory must be root-owned, mode 0700 and not a symlink.")


def expected_signer(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022 or info.st_size > 65536:
        raise ProvisionError("Reviewed trading profile is absent, indirect, writable or oversized.")
    try:
        value = tomllib.loads(path.read_text(encoding="utf-8"))["live"]["signer_address"]
    except Exception:
        raise ProvisionError("Reviewed trading profile has no signer identity.") from None
    if not isinstance(value, str) or not ADDRESS.fullmatch(value) or int(value[2:], 16) == 0:
        raise ProvisionError("Reviewed trading signer is invalid.")
    return value.lower()


def apply_update(payload, config=Path("/etc/mm2"), profile=Path("/opt/mm2/current/config/trading.toml"),
                 get_identity=telegram_identity, derive_address=signer_address):
    """Linux root helper: locked, fsynced atomic replacement; no automatic activation."""
    import fcntl
    from deploy_host import private_environment

    validate_payload(payload)
    private_directory(config)
    lock = os.open(config / ".provision.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        lock_info = os.fstat(lock)
        if not stat.S_ISREG(lock_info.st_mode) or lock_info.st_uid != 0 or stat.S_IMODE(lock_info.st_mode) != 0o600:
            raise ProvisionError("Credential-update lock has an unsafe identity.")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        target = config / "runtime.env"
        original = target.lstat()
        if original.st_nlink != 1:
            raise ProvisionError("Runtime environment must have exactly one filesystem link.")
        values = private_environment(target)
        if payload["kind"] == "telegram":
            existing_owner = values.get("MM2_TELEGRAM_OWNER_ID", "")
            if existing_owner and existing_owner != str(payload["owner_id"]):
                raise ProvisionError("Existing Telegram owner differs; owner changes require a separate review.")
            identity = get_identity(payload["token"])
            values["MM2_TELEGRAM_BOT_TOKEN"] = payload["token"]
            values["MM2_TELEGRAM_OWNER_ID"] = str(payload["owner_id"])
            result = {"kind": "telegram", "username": identity["username"], "bot_id": identity["bot_id"],
                      "owner_id": payload["owner_id"], "restart_required": "console"}
        elif payload["kind"] == "wallet":
            address = derive_address(payload["private_key"])
            if address.lower() != expected_signer(profile):
                raise ProvisionError("Signing key does not match the reviewed controlling EOA; nothing was changed.")
            values["MM2_POLY_PRIVATE_KEY"] = "0x" + payload["private_key"].removeprefix("0x").lower()
            result = {"kind": "wallet", "signer_address": address, "activation": "unchanged",
                      "restart_required": INACTIVE}
        elif payload["kind"] == "clob":
            values["MM2_POLY_API_KEY"] = payload["api_key"]
            values["MM2_POLY_API_SECRET"] = canonical_clob_secret(payload["api_secret"])
            values["MM2_POLY_API_PASSPHRASE"] = payload["api_passphrase"]
            result = {"kind": "clob", "activation": "unchanged", "restart_required": INACTIVE}
        elif payload["kind"] == "relayer":
            owner = payload["key_address"].lower()
            if owner != expected_signer(profile):
                raise ProvisionError("Relayer key owner does not match the reviewed controlling EOA; nothing was changed.")
            values["MM2_POLY_RELAYER_API_KEY"] = payload["api_key"]
            values["MM2_POLY_RELAYER_API_KEY_ADDRESS"] = owner
            result = {"kind": "relayer", "signer_address": owner, "activation": "unchanged",
                      "restart_required": INACTIVE}
        else:
            values["MM2_POLYGON_RPC_URL"] = payload["url"]
            result = {"kind": "rpc", "activation": "unchanged", "restart_required": INACTIVE}
        text = "".join(key + "=" + value + "\n" for key, value in values.items())
        if len(text.encode("ascii")) > 16384:
            raise ProvisionError("Runtime credentials exceed the supported bound.")
        descriptor, temporary_name = tempfile.mkstemp(prefix=".runtime-", dir=config)
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            os.fchown(descriptor, 0, 0)
            with os.fdopen(descriptor, "w", encoding="ascii", newline="\n") as stream:
                stream.write(text)
                stream.flush()
                os.fsync(stream.fileno())
            private_environment(temporary)
            now = target.lstat()
            if (now.st_dev, now.st_ino, now.st_mtime_ns, now.st_size, now.st_mode, now.st_uid) != (
                    original.st_dev, original.st_ino, original.st_mtime_ns, original.st_size, original.st_mode, original.st_uid):
                raise ProvisionError("Runtime environment changed concurrently; retry after reviewing it.")
            os.replace(temporary, target)
            directory = os.open(config, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            temporary.unlink(missing_ok=True)
        return result
    finally:
        os.close(lock)


def remote_main():
    if sys.platform != "linux" or os.geteuid() != 0:
        raise ProvisionError("Remote credential helper requires Linux root.")
    source = Path(__file__)
    source_info, directory_info = source.lstat(), source.parent.lstat()
    if (not stat.S_ISREG(source_info.st_mode) or source_info.st_uid != 0 or source_info.st_mode & 0o022
            or not stat.S_ISDIR(directory_info.st_mode) or directory_info.st_uid != 0 or directory_info.st_mode & 0o022):
        raise ProvisionError("Install the reviewed helper in a root-owned directory without group/world write access.")
    sys.path.insert(0, "/opt/mm2/current/scripts")
    try:
        raw = sys.stdin.buffer.read(MAX_INPUT + 1)
        if len(raw) > MAX_INPUT:
            raise ProvisionError("Credential request exceeds the supported bound.")
        payload = decode_payload(raw)
        result = apply_update(payload)
    except ProvisionError:
        raise
    except Exception:
        raise ProvisionError("Credential update failed; transport/configuration details were withheld.") from None
    print(json.dumps({"ok": True, **result}, separators=(",", ":")))


def hidden_input(prompt):
    if not sys.stdin.isatty():
        raise ProvisionError("Run this command yourself in an interactive terminal; piped secret entry is refused.")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            return getpass.getpass(prompt)
    except (getpass.GetPassWarning, EOFError):
        raise ProvisionError("Hidden terminal input is unavailable; no secret was accepted.") from None


def ssh_command(config, host, executable):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", host):
        raise ProvisionError("SSH host alias is invalid.")
    # No token, wallet key, owner input or other variable content enters the command.
    remote = "sudo -n python3 -B " + shlex.quote(REMOTE_TOOL) + " --remote"
    return [executable, "-F", str(config), "-o", "StrictHostKeyChecking=yes", "-o", "BatchMode=yes",
            "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none", "-o", "PasswordAuthentication=no",
            "-o", "KbdInteractiveAuthentication=no", "-o", "LogLevel=ERROR", "-T", host, remote]


def local_main(args, run=subprocess.run):
    if args.ssh_config.is_symlink():
        raise ProvisionError("Use a regular dedicated SSH configuration, not a symlink.")
    config = args.ssh_config.resolve(strict=True)
    if not config.is_file() or config.is_symlink() or config.stat().st_size > 16384:
        raise ProvisionError("Use the dedicated reviewed SSH config with its pinned known_hosts file.")
    if args.kind == "telegram":
        raw_owner = input("Complete Telegram owner user ID (no ellipsis): ").strip()
        if not re.fullmatch(r"[1-9][0-9]{0,15}", raw_owner):
            raise ProvisionError("Enter the complete numeric Telegram user ID.")
        payload = {"schema": 1, "kind": "telegram", "owner_id": int(raw_owner),
                   "token": hidden_input("@mm2b_bot token (hidden): ")}
    elif args.kind == "wallet":
        payload = {"schema": 1, "kind": "wallet", "private_key": hidden_input("Controlling EOA signing key, hex (hidden): ")}
    elif args.kind == "clob":
        payload = {"schema": 1, "kind": "clob", "api_key": hidden_input("Existing CLOB API key, UUID (hidden): "),
                   "api_secret": hidden_input("Existing CLOB API secret, URL-safe base64 (hidden): "),
                   "api_passphrase": hidden_input("Existing CLOB API passphrase (hidden): ")}
    elif args.kind == "relayer":
        payload = {"schema": 1, "kind": "relayer", "api_key": hidden_input("Owner Relayer API key (hidden): "),
                   "key_address": hidden_input("Relayer key Signer Address (hidden): ")}
    elif args.kind == "rpc":
        payload = {"schema": 1, "kind": "rpc", "url": hidden_input("Polygon HTTPS RPC URL, including provider API key (hidden): ")}
    else:
        raise ProvisionError("Unsupported credential operation.")
    validate_payload(payload)
    # Unrelated mm2 credentials from an operator's shell never become child env.
    environment = {key: value for key, value in os.environ.items() if not key.startswith("MM2_")}
    if sys.platform == "win32":
        cli = Path(os.environ.get("LOCALAPPDATA", "")) / "Programs/Amazon/AWSCLIV2"
        plugin = Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "Amazon/SessionManagerPlugin/bin"
        environment["PATH"] = os.pathsep.join((str(cli), str(plugin), environment.get("PATH", "")))
    try:
        reply = run(ssh_command(config, args.host, args.ssh_executable), input=json.dumps(payload).encode("ascii"),
                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=environment, timeout=90, check=False)
        if reply.returncode != 0 or len(reply.stdout) > MAX_REPLY:
            raise ValueError("ssh failed")
        # SSM may print public session preamble before the last JSON result.
        result = json.loads(reply.stdout.decode("ascii").strip().splitlines()[-1], object_pairs_hook=unique_fields)
        expected = {"ok", "kind", "activation", "restart_required"}
        if args.kind == "telegram":
            expected = {"ok", "kind", "username", "bot_id", "owner_id", "restart_required"}
        elif args.kind in {"wallet", "relayer"}:
            expected.add("signer_address")
        if not isinstance(result, dict) or result.get("ok") is not True or set(result) != expected or result.get("kind") != args.kind:
            raise ValueError("invalid result")
        if args.kind == "telegram":
            if result.get("username") != EXPECTED_BOT or result.get("bot_id") != int(payload["token"].split(":", 1)[0]) or result.get("owner_id") != payload["owner_id"]:
                raise ValueError("wrong public identity")
        elif args.kind in {"wallet", "relayer"}:
            if not isinstance(result.get("signer_address"), str) or not ADDRESS.fullmatch(result["signer_address"]):
                raise ValueError("invalid public address")
            if args.kind == "relayer" and result["signer_address"].lower() != payload["key_address"].lower():
                raise ValueError("wrong Relayer owner")
        if args.kind != "telegram" and (result.get("activation") != "unchanged" or result.get("restart_required") != INACTIVE):
            raise ValueError("unexpected activation")
    except Exception:
        raise ProvisionError("Verified SSH provisioning failed. Check AWS session, pinned host key and installed helper; no secret was printed.") from None
    print("Credentials stored in root-owned /etc/mm2/runtime.env. Existing console/engine tokens were preserved.")
    if args.kind == "telegram":
        print("Verified bot: @mm2b_bot; owner: " + str(payload["owner_id"]) + ". Recreate only the console to apply it.")
    elif args.kind in {"wallet", "relayer"}:
        print("Verified controlling EOA: " + result["signer_address"] + ". Trading remains paused and gated.")
    else:
        print("Credential format checked; authenticated venue/RPC acceptance remains unverified. Trading remains paused and gated.")


def main(argv=None):
    parser = SafeArgumentParser(description=__doc__)
    parser.add_argument("kind", nargs="?", choices=tuple(FIELDS))
    parser.add_argument("--ssh-config", type=Path, default=ROOT / "output/deployment-access/ssh-config")
    parser.add_argument("--host", default="mm2-aws")
    parser.add_argument("--ssh-executable", default="ssh")
    parser.add_argument("--remote", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        if args.remote:
            if args.kind:
                raise ProvisionError("Remote operation is specified only through stdin.")
            remote_main()
        else:
            if args.kind is None:
                parser.error("choose a credential operation")
            local_main(args)
        return 0
    except (ProvisionError, OSError):
        # Deliberately generic even for OS errors containing an input-bearing path.
        print("Credential provisioning was refused or failed. No supplied secret was printed or automatically activated.", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Credential provisioning cancelled.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
