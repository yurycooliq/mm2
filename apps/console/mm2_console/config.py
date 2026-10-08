from dataclasses import dataclass, field
import os
from pathlib import Path
import secrets
from urllib.parse import urlsplit


def origin(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("unsupported origin")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ValueError("origin must not contain a path")
    try:
        parsed.port
    except ValueError as exc:
        raise ValueError("invalid origin port") from exc
    return f"{parsed.scheme}://{parsed.netloc.lower()}"


@dataclass(frozen=True)
class Config:
    engine_url: str
    engine_token: str = field(repr=False)
    owner_token: str = field(repr=False)
    state_dir: Path
    static_dir: Path
    allowed_origins: frozenset[str]
    host: str = "0.0.0.0"
    port: int = 8080
    telegram_token: str = field(default="", repr=False)
    telegram_owner: int | None = None
    miniapp_url: str = ""
    telegram_only: bool = False
    trusted_proxy_header: bool = False
    research_view_dir: Path | None = None

    @classmethod
    def from_env(cls):
        state = Path(os.environ.get("MM2_CONSOLE_STATE_DIR", "/data"))
        state.mkdir(parents=True, exist_ok=True)
        owner = os.environ.get("MM2_CONSOLE_TOKEN", "")
        if not owner:
            path = state / "console-owner-token"
            try:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                owner = path.read_text(encoding="ascii").strip()
            else:
                owner = secrets.token_urlsafe(32)
                with os.fdopen(fd, "w", encoding="ascii") as stream:
                    stream.write(owner + "\n")
        engine_token = os.environ.get("MM2_ENGINE_API_TOKEN", "")
        if not 32 <= len(owner) <= 256 or not 32 <= len(engine_token) <= 256:
            raise ValueError("owner and engine tokens must each contain 32 to 256 characters")
        if owner == engine_token or any(not 33 <= ord(char) <= 126 for value in (owner, engine_token) for char in value):
            raise ValueError("owner and engine tokens must be distinct printable ASCII values")
        engine = origin(os.environ.get("MM2_ENGINE_URL", "http://engine:8088"))
        allowed = frozenset(origin(item.strip()) for item in os.environ.get(
            "MM2_ALLOWED_ORIGINS", "http://127.0.0.1:8080,http://localhost:8080").split(",") if item.strip())
        if not allowed:
            raise ValueError("an explicit allowed origin is required")
        token = os.environ.get("MM2_TELEGRAM_BOT_TOKEN", "")
        owner_id = os.environ.get("MM2_TELEGRAM_OWNER_ID", "")
        if bool(token) != bool(owner_id):
            raise ValueError("Telegram requires both bot token and owner ID")
        telegram_owner = int(owner_id) if owner_id else None
        if telegram_owner is not None and not 0 < telegram_owner < 2**52:
            raise ValueError("invalid Telegram owner ID")
        telegram_policy = os.environ.get("MM2_TELEGRAM_ONLY", "false")
        if telegram_policy not in {"true", "false"}:
            raise ValueError("MM2_TELEGRAM_ONLY must be true or false")
        proxy_policy = os.environ.get("MM2_TRUSTED_PROXY_HEADER", "false")
        if proxy_policy not in {"true", "false"}:
            raise ValueError("MM2_TRUSTED_PROXY_HEADER must be true or false")
        miniapp = os.environ.get("MM2_MINIAPP_URL", "")
        if miniapp:
            parsed = urlsplit(miniapp)
            if parsed.scheme != "https" or parsed.username or parsed.password or parsed.fragment:
                raise ValueError("Mini App requires an HTTPS URL without credentials or fragment")
            if origin(f"https://{parsed.netloc}") not in allowed:
                raise ValueError("Mini App origin must be explicitly allowed")
        return cls(engine, engine_token, owner, state,
                   Path(os.environ.get("MM2_CONSOLE_STATIC_DIR", "/app/dashboard")), allowed,
                   os.environ.get("MM2_CONSOLE_HOST", "0.0.0.0"), int(os.environ.get("MM2_CONSOLE_PORT", "8080")),
                   token, telegram_owner, miniapp, bool(token and telegram_owner) or telegram_policy == "true",
                   proxy_policy == "true",
                   Path(os.environ["MM2_CONSOLE_RESEARCH_VIEW_DIR"]) if os.environ.get("MM2_CONSOLE_RESEARCH_VIEW_DIR") else None)

    @property
    def secret_values(self):
        return tuple(value for value in (self.engine_token, self.owner_token, self.telegram_token) if value)
