from dataclasses import dataclass
import json
import ssl
import threading
from urllib.error import HTTPError, URLError
from urllib.request import build_opener, HTTPRedirectHandler, HTTPSHandler, Request

MAX_ENGINE_BYTES = 262_144
SECRET_KEYS = {"token", "api_token", "engine_api_token", "bot_token", "console_token",
               "authorization", "init_data", "signing_key", "secret"}


def redact(value, secrets=(), depth=0):
    if depth > 24:
        raise ValueError("engine state nesting exceeds limit")
    if isinstance(value, dict):
        return {key: "[redacted]" if key.lower() in SECRET_KEYS else redact(item, secrets, depth + 1)
                for key, item in value.items()}
    if isinstance(value, list):
        return [redact(item, secrets, depth + 1) for item in value]
    if isinstance(value, str):
        for secret in secrets:
            if secret:
                value = value.replace(secret, "[redacted]")
    return value


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


@dataclass
class Reply:
    status: int
    payload: dict


class EngineClient:
    def __init__(self, base_url, token, secret_values=(), opener=None):
        self.base_url, self.token = base_url.rstrip("/"), token
        self.secrets = tuple(secret_values) + (token,)
        self.opener = opener or build_opener(NoRedirect(), HTTPSHandler(context=ssl.create_default_context()))
        self._capacity = threading.BoundedSemaphore(4)

    def request(self, method, path, payload=None):
        if (method, path) not in {("GET", "/v1/state"), ("POST", "/v1/control"), ("PUT", "/v1/limits")}:
            raise ValueError("unsupported internal engine endpoint")
        if not self._capacity.acquire(blocking=False):
            return Reply(503, {"error": "engine_busy", "message": "private engine request capacity reached; request was not sent"})
        try:
            data = None if payload is None else json.dumps(payload, separators=(",", ":"), allow_nan=False).encode()
            request = Request(self.base_url + path, data=data, method=method,
                              headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json", "Accept": "application/json"})
            try:
                response = self.opener.open(request, timeout=1.5)
            except HTTPError as response_error:
                response = response_error
            with response:
                status = response.status
                raw = response.read(MAX_ENGINE_BYTES + 1)
            if len(raw) > MAX_ENGINE_BYTES:
                raise ValueError("engine response exceeds limit")
            value = json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))
            if not isinstance(value, dict):
                raise ValueError("engine response is not an object")
            return Reply(status, redact(value, self.secrets))
        except (OSError, URLError, ValueError, UnicodeError, RecursionError):
            return Reply(503, {"error": "outcome_unknown" if method != "GET" else "engine_offline",
                               "message": "engine unavailable; command outcome is unknown, retry the same ID and payload" if method != "GET" else "engine state unavailable"})
        finally:
            self._capacity.release()
