#!/usr/bin/env python3
"""Local-only Caddy TLS/console acceptance with fixture credentials and an isolated Docker network."""
import argparse
import base64
import http.client
import inspect
import json
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import uuid

from deploy_package import ROOT, image_identity


def docker(*arguments, input=None, check=True):
    return subprocess.run(["docker", *arguments], input=input, capture_output=True, text=True, check=check, timeout=30)


def https_request(port, context, path, payload=None, origin=None, cookie=None, host="127.0.0.1"):
    body = json.dumps(payload).encode("ascii") if payload is not None else b""
    method = "POST" if payload is not None else "GET"
    headers = [f"{method} {path} HTTP/1.1", "Host: owner.example.test", "Connection: close"]
    if payload is not None:
        headers += ["Content-Type: application/json", f"Content-Length: {len(body)}"]
    if origin:
        headers.append("Origin: " + origin)
    if cookie:
        headers.append("Cookie: " + cookie)
    with socket.create_connection((host, port), timeout=3) as raw:
        with context.wrap_socket(raw, server_hostname="owner.example.test") as secure:
            secure.sendall(("\r\n".join(headers) + "\r\n\r\n").encode("ascii") + body)
            response = http.client.HTTPResponse(secure)
            response.begin()
            result = response.read(131_073)
            if len(result) > 131_072:
                raise ValueError("fixture response exceeded its bound")
            return response.status, dict(response.getheaders()), result


def verify(proxy_image, console_image):
    proxy_image = image_identity(proxy_image)
    console_image = image_identity(console_image)
    client_image = image_identity("python:3.13-slim-bookworm@sha256:2325bb286ec344af3e5898cc224b5844e2707ac6e26b1632516fd3edc84a5e26")
    identity = uuid.uuid4().hex[:12]
    label = "mm2.aws-proxy-smoke=" + identity
    network = "mm2-aws-proxy-" + identity
    containers = []
    output = ROOT / "output"
    output.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="aws-proxy-", dir=output) as temporary:
        fixture = Path(temporary) / "Caddyfile"
        content = (ROOT / "infra/aws/Caddyfile").read_text()
        content = content.replace("https://{$MM2_DOMAIN}:8443 {", "https://{$MM2_DOMAIN}:8443 {\n    tls internal")
        fixture.write_text(content, encoding="ascii", newline="\n")
        try:
            docker("network", "create", "--internal", "--label", label, network)
            name = "mm2-aws-console-" + identity
            containers.append(name)
            docker("run", "--detach", "--name", name, "--label", label, "--network", network, "--network-alias", "console",
                   "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true", "--user", "10002:10002",
                   "--tmpfs", "/data:rw,noexec,nosuid,size=16m,mode=1777", "--tmpfs", "/tmp:rw,noexec,nosuid,size=16m,mode=1777",
                   "-e", "MM2_ENGINE_URL=http://engine:8088", "-e", "MM2_ENGINE_API_TOKEN=" + "e" * 64,
                   "-e", "MM2_CONSOLE_TOKEN=" + "c" * 64, "-e", "MM2_ALLOWED_ORIGINS=https://owner.example.test",
                   "-e", "MM2_MINIAPP_URL=https://owner.example.test/", "-e", "MM2_CONSOLE_STATIC_DIR=/app/dashboard", console_image)
            name = "mm2-aws-caddy-" + identity
            containers.append(name)
            docker("run", "--detach", "--name", name, "--label", label, "--network", network, "--network-alias", "proxy", "--read-only", "--cap-drop", "ALL",
                   "--cap-add", "NET_BIND_SERVICE", "--security-opt", "no-new-privileges:true",
                   "--tmpfs", "/data:rw,noexec,nosuid,size=16m", "--tmpfs", "/config:rw,noexec,nosuid,size=16m",
                   "--tmpfs", "/tmp:rw,noexec,nosuid,size=16m",
                   "-e", "MM2_DOMAIN=owner.example.test", "--mount", "type=bind,source=" + str(fixture) + ",target=/etc/caddy/Caddyfile,readonly",
                   proxy_image)
            deadline = time.monotonic() + 20
            while True:
                ca = docker("exec", name, "cat", "/data/caddy/pki/authorities/local/root.crt", check=False)
                if ca.returncode == 0:
                    break
                if time.monotonic() >= deadline:
                    raise ValueError("fixture TLS authority did not become ready")
                time.sleep(0.25)
            client = "import base64, http.client, json, os, socket, ssl, time\n" + inspect.getsource(https_request) + '''
context = ssl.create_default_context(cadata=base64.b64decode(os.environ["MM2_FIXTURE_CA"]).decode("ascii"))
def request(path, *args, **kwargs):
    return https_request(8443, context, path, *args, host="proxy", **kwargs)
deadline = time.monotonic() + 15
while True:
    try:
        if request("/healthz")[0] == 200:
            break
    except (OSError, ValueError):
        pass
    if time.monotonic() >= deadline:
        raise ValueError("proxied fixture console did not become ready")
    time.sleep(0.25)
assert request("/")[0] == 200
assert request("/api/state")[0] == 401
assert request("/api/auth/local", {"token": "c" * 64}, "https://wrong.example.test")[0] == 403
status, headers, _ = request("/api/auth/local", {"token": "c" * 64}, "https://owner.example.test")
assert status == 200
cookie = headers.get("Set-Cookie", "")
assert all(flag in cookie for flag in ("Secure", "HttpOnly", "SameSite=Strict"))
assert request("/api/state", cookie=cookie.split(";", 1)[0])[0] == 200
plain = http.client.HTTPConnection("proxy", 8080, timeout=3)
try:
    plain.request("GET", "/dashboard?fixture=1", headers={"Host": "owner.example.test"})
    response = plain.getresponse()
    assert response.status == 308 and response.getheader("Location") == "https://owner.example.test/dashboard?fixture=1"
    response.read(1024)
finally:
    plain.close()
'''
            name = "mm2-aws-client-" + identity
            containers.append(name)
            docker("run", "--rm", "--interactive", "--name", name, "--label", label, "--network", network,
                   "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                   "-e", "MM2_FIXTURE_CA=" + base64.b64encode(ca.stdout.encode("ascii")).decode("ascii"),
                   client_image, "python", "-B", "-", input=client)
            print(json.dumps({"checks": 7, "tls_chain_verified": True, "owner_auth_and_secure_cookie": True,
                              "origin_enforced": True, "https_redirect": True, "external_network": False,
                              "fixtures_only": True}, sort_keys=True))
        finally:
            for name in reversed(containers):
                evidence = docker("inspect", "--format", '{{index .Config.Labels "mm2.aws-proxy-smoke"}}', name, check=False)
                if evidence.returncode == 0 and evidence.stdout.strip() == identity:
                    docker("rm", "--force", name, check=False)
            evidence = docker("network", "inspect", "--format", '{{index .Labels "mm2.aws-proxy-smoke"}}', network, check=False)
            if evidence.returncode == 0 and evidence.stdout.strip() == identity:
                docker("network", "rm", network, check=False)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proxy-image", required=True, help="Already local reviewed Linux/amd64 Caddy image")
    parser.add_argument("--console-image", default="mm2-console:local")
    args = parser.parse_args(argv)
    try:
        verify(args.proxy_image, args.console_image)
        return 0
    except (OSError, ValueError, AssertionError, subprocess.SubprocessError):
        print("Local fixture proxy acceptance failed; owner-labelled disposable resources were cleaned up.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
