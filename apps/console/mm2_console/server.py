from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hmac
import ipaddress
import json
import mimetypes
from pathlib import Path
import threading
from urllib.parse import parse_qs, unquote, urlsplit

from .auth import COOKIE, telegram_session

MAX_BODY = 16_384


class ConsoleServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, app):
        self.app = app
        self._workers = threading.BoundedSemaphore(16)
        super().__init__(address, Handler)

    def process_request(self, request, client_address):
        if not self._workers.acquire(blocking=False):
            try:
                request.sendall(b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            finally:
                self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self._workers.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._workers.release()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "mm2-console"

    def setup(self):
        super().setup()
        self.connection.settimeout(3)

    def log_message(self, format, *args):
        # URLs, cookies, initData and tokens never enter access logs.
        pass

    def _send(self, status, value=None, cookie=None, raw=None, content_type="application/json"):
        body = raw if raw is not None else json.dumps(value, separators=(",", ":"), allow_nan=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self' https://telegram.org; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'self' https://web.telegram.org; base-uri 'self'; form-action 'self'")
        self.send_header("Connection", "close")
        if cookie:
            self.send_header("Set-Cookie", cookie)
        self.end_headers()
        self.close_connection = True
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _session(self):
        value = self.headers.get("Cookie", "")
        if len(value) > 2048:
            return ""
        cookie = SimpleCookie()
        try:
            cookie.load(value)
        except Exception:
            return ""
        return cookie[COOKIE].value if COOKIE in cookie else ""

    def _client_key(self):
        peer = self.client_address[0]
        if not self.server.app.config.trusted_proxy_header:
            return peer
        try:
            address = ipaddress.ip_address(peer)
            if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
                address = address.ipv4_mapped
            if not address.is_private or address.is_loopback:
                return peer
            values = self.headers.get_all("X-MM2-Client-IP", [])
            if len(values) != 1 or not 1 <= len(values[0]) <= 45 or "%" in values[0]:
                return peer
            client = ipaddress.ip_address(values[0])
            if isinstance(client, ipaddress.IPv6Address) and client.ipv4_mapped:
                client = client.ipv4_mapped
            if client.is_unspecified or client.is_multicast:
                return peer
            # AWS alone enables this on an unpublished console behind its isolated
            # Caddy network. Caddy replaces this header with its socket client IP.
            # This key separates rate buckets; it never authenticates a user.
            return str(client)
        except ValueError:
            return peer

    def _body(self):
        if self.headers.get("Transfer-Encoding") or len(self.headers.get_all("Content-Length", [])) != 1:
            raise ValueError("one bounded Content-Length is required")
        if self.headers.get("Content-Type", "").split(";", 1)[0].strip() != "application/json":
            raise ValueError("application/json is required")
        length = int(self.headers.get("Content-Length", "0"))
        if not 1 <= length <= MAX_BODY:
            raise ValueError("request body exceeds limit")
        raw = self.rfile.read(length)
        if len(raw) != length:
            raise ValueError("incomplete request body")
        def unique_object(pairs):
            if len({key for key, _ in pairs}) != len(pairs):
                raise ValueError("duplicate JSON fields")
            return dict(pairs)
        return json.loads(raw, object_pairs_hook=unique_object,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))

    def _cookie(self, token, telegram=False):
        secure = self.headers.get("Origin", "").startswith("https://")
        same_site = "None" if telegram and secure else "Strict"
        return f"{COOKIE}={token}; Path=/; HttpOnly; SameSite={same_site}; Max-Age={3600 if token else 0}" + ("; Secure" if secure else "")

    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()

    def do_PUT(self):
        self._handle()

    def _handle(self):
        app = self.server.app
        if len(self.path) > 2048 or sum(len(key) + len(value) for key, value in self.headers.items()) > 16_384:
            self._send(413, {"error": "request_too_large"})
            return
        client_key = self._client_key()
        if not app.ingress.allow(client_key):
            self._send(429, {"error": "rate_limited"})
            return
        parsed = urlsplit(self.path)
        path = parsed.path
        unsafe = self.command in {"POST", "PUT"}
        request_origin = self.headers.get("Origin", "")
        if unsafe and (request_origin not in app.config.allowed_origins
                       or urlsplit(request_origin).netloc.lower() != self.headers.get("Host", "").lower()):
            self._send(403, {"error": "origin_rejected", "message": "an explicitly allowed same-origin request is required"})
            return
        try:
            if path == "/healthz" and self.command == "GET":
                self._send(200, {"ok": not app.stop.is_set(), "component": "console", "sampling_interval_ms": 1000})
                return
            if path in {"/api/auth/local", "/api/auth/telegram"} and self.command == "POST":
                if not app.auth_attempts.allow(client_key):
                    self._send(429, {"error": "authentication_rate_limited"})
                    return
                body = self._body()
                if path.endswith("local"):
                    if app.config.telegram_only:
                        self._send(404, {"error": "not_found"})
                        return
                    if not isinstance(body, dict) or set(body) != {"token"} or not isinstance(body["token"], str) or len(body["token"]) > 256:
                        raise ValueError("invalid owner authentication")
                    if not hmac.compare_digest(body["token"].encode(), app.config.owner_token.encode()):
                        self._send(401, {"error": "unauthorized", "message": "owner token rejected"})
                        return
                else:
                    existing = self._session()
                    try:
                        if not isinstance(body, dict) or set(body) != {"init_data"}:
                            raise ValueError("invalid Telegram authentication")
                        token = telegram_session(app.sessions, existing, body["init_data"],
                                                 app.config.telegram_token, app.config.telegram_owner)
                    except ValueError:
                        # A switched Telegram account must not keep an earlier
                        # owner's cookie usable in a shared browser/WebView.
                        app.sessions.revoke(existing)
                        self._send(404, {"error": "not_found"})
                        return
                if path.endswith("local"):
                    token = app.sessions.create()
                self._send(200, {"authenticated": True}, cookie=self._cookie(token, path.endswith("telegram")))
                return
            if path.startswith("/api/"):
                session = self._session()
                if not app.sessions.valid(session):
                    if app.config.telegram_only:
                        self._send(404, {"error": "not_found"})
                    else:
                        self._send(401, {"error": "unauthorized", "message": "owner session required"})
                    return
                query = parse_qs(parsed.query, max_num_fields=4)
                limit = int(query.get("limit", ["120"])[0])
                if path == "/api/state" and self.command == "GET":
                    self._send(200, app.state())
                elif path == "/api/comparison" and self.command == "GET":
                    if set(query) - {"period"} or len(query.get("period", [])) > 1:
                        raise ValueError("unsupported comparison query")
                    self._send(200, app.comparison(query.get("period", ["1h"])[0]))
                elif path == "/api/history" and self.command == "GET":
                    self._send(200, {"samples": app.store.history(limit)})
                elif path == "/api/logs" and self.command == "GET":
                    self._send(200, {"logs": app.store.logs(limit)})
                elif path == "/api/logout" and self.command == "POST":
                    app.sessions.revoke(session)
                    self._send(200, {"authenticated": False}, cookie=self._cookie(""))
                elif (path, self.command) in {("/api/control", "POST"), ("/api/limits", "PUT")}:
                    if not app.command_rate.allow(session):
                        self._send(429, {"error": "command_rate_limited", "message": "command was not sent"})
                        return
                    reply = app.proxy(self._body(), path == "/api/limits")
                    self._send(reply.status, reply.payload)
                else:
                    self._send(404, {"error": "not_found"})
                return
            if self.command != "GET":
                self._send(405, {"error": "method_not_allowed"})
                return
            relative = unquote(path).lstrip("/") or "index.html"
            target = (app.config.static_dir / relative).resolve()
            if not target.is_relative_to(app.config.static_dir.resolve()) or not target.is_file() or target.stat().st_size > 2_097_152:
                self._send(404, {"error": "not_found"})
                return
            self._send(200, raw=target.read_bytes(), content_type=mimetypes.guess_type(target.name)[0] or "application/octet-stream")
        except (ValueError, UnicodeError, RecursionError):
            self._send(400, {"error": "invalid_request", "message": "request format or authentication is unsupported"})
        except (OSError, TimeoutError):
            self._send(503, {"error": "console_unavailable", "message": "secondary console request unavailable"})
