"""Runtime configuration from ``MCP_``-prefixed env vars and mounted secret files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal
from urllib.parse import quote

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MCP_", env_file=".env", extra="ignore")

    # Server: HTTP listener and log rendering.
    host: str = "0.0.0.0"
    port: int = 8080
    log_level: str = "INFO"
    log_format: Literal["json", "console"] = "json"

    # Database: TimescaleDB connection and pool sizing. In-cluster the credentials
    # arrive as mounted Secret files via the *_file paths; literal env vars still
    # work for tests/local dev.
    db_host: str
    db_port: int = 5432
    db_name: str
    db_username: str = ""
    db_password: str = Field(default="", repr=False)
    db_username_file: str | None = None
    db_password_file: str | None = None
    db_pool_min: int = 2
    db_pool_max: int = 10

    # The one write path: the verdict a person attaches to an episode. Left
    # unset the server stays read-only and the verdict tool refuses; every
    # other tool is unaffected.
    db_write_username: str = ""
    db_write_password: str = Field(default="", repr=False)
    db_write_username_file: str | None = None
    db_write_password_file: str | None = None

    # Query limits: cap on rows returned by the time-series tools.
    query_row_limit: int = 5000

    # Metrics: port of the Prometheus /metrics HTTP server.
    metrics_port: int = 9090

    # Tracing: the OTLP/HTTP collector base URL (http://host:4318), unset keeps
    # tracing off; the share of new traces kept, the platform's rendered ratio.
    tracing_endpoint: str | None = None
    tracing_sampling_ratio: float = Field(default=0.1, ge=0.0, le=1.0)

    # Auth: OIDC bearer validation via JWKS; disabled by default for local runs.
    auth_enabled: bool = False
    auth_jwks_url: str | None = None
    auth_issuer: str | None = None
    auth_audience: str | None = None
    auth_jwks_ttl_seconds: int = 3600
    # Cooldown between JWKS fetch attempts — bounds how often unauthenticated
    # callers with unknown-kid tokens can force a request to the issuer.
    auth_jwks_min_refresh_seconds: float = 30.0
    auth_resource_url: str | None = None

    # Machine clients: an agent that cannot run an OAuth flow authenticates
    # with a static API key instead of a JWT. The keys arrive as a mounted
    # Secret file (a JSON object of client name to key, keys at least 32
    # characters); the tool allowlist per client name is plain configuration
    # (a JSON object of client name to tool names or fnmatch globs). A
    # machine client without an allowlist entry sees no tools; a user client
    # without one keeps every tool.
    auth_clients_file: str | None = None
    auth_clients: dict[str, str] = Field(default_factory=dict, repr=False)
    auth_client_tools: dict[str, list[str]] = Field(default_factory=dict)
    # Persons: the handles of the declared persons (a JSON list). A machine
    # client of that name, or a token whose `preferred_username` is that
    # name, is that person and reads that person's runs; every other caller
    # has no person. Each handle needs a key and an allowlist entry above.
    auth_persons: list[str] = Field(default_factory=list)

    # NATS: live current-state reads from JetStream (last message per subject).
    # The nkey seed arrives as a mounted Secret file, same as the DB credentials;
    # connects anonymously when unset. Disabled in tests/dev where the layer is
    # mocked or absent.
    nats_enabled: bool = True
    nats_servers: str = "nats://nats.nats.svc:4222"
    nats_nkey_seed_file: str | None = None
    live_stale_seconds: int = 600
    subscribe_max_seconds: int = 30

    # Wiki.js: read-only page access for the wiki tools. Both must be set (one
    # alone is a config error); the API key arrives as a mounted Secret file.
    wikijs_url: str | None = None
    wikijs_token_file: str | None = None
    # The one wiki write, under a key of its own whose group may write pages;
    # it needs the URL above. A page it creates lives in this locale, and an
    # existing page is looked up there.
    wikijs_write_token_file: str | None = None
    wikijs_locale: str = "en"

    # Proxmox Backup Server: the backup tools read through an API token with
    # the Audit role. All three must be set (a part alone is a config error);
    # the token id is `<user>@<realm>!<token name>`, its secret a mounted file.
    pbs_url: str | None = None
    pbs_token_id: str | None = None
    pbs_token_file: str | None = None

    # S3 object store: the storage tools list buckets and objects with a key
    # whose policy allows listing and nothing else. All three must be set (a
    # part alone is a config error); both key halves arrive as mounted files.
    s3_endpoint_url: str | None = None
    s3_access_key_file: str | None = None
    s3_secret_key_file: str | None = None

    # lares-agent-trigger: where start_run carries its request. Both must be
    # set (one alone is a config error); the key both pods share arrives as a
    # mounted Secret file.
    trigger_url: str | None = None
    trigger_key_file: str | None = None

    @model_validator(mode="after")
    def _resolve_db_secret_files(self) -> Settings:
        if self.db_username_file:
            self.db_username = Path(self.db_username_file).read_text(encoding="utf-8").strip()
        if self.db_password_file:
            self.db_password = Path(self.db_password_file).read_text(encoding="utf-8").strip()
        if not self.db_username:
            raise ValueError("MCP_DB_USERNAME or MCP_DB_USERNAME_FILE is required")
        if not self.db_password:
            raise ValueError("MCP_DB_PASSWORD or MCP_DB_PASSWORD_FILE is required")
        if self.db_write_username_file:
            self.db_write_username = (
                Path(self.db_write_username_file).read_text(encoding="utf-8").strip()
            )
        if self.db_write_password_file:
            self.db_write_password = (
                Path(self.db_write_password_file).read_text(encoding="utf-8").strip()
            )
        return self

    @model_validator(mode="after")
    def _check_auth_config(self) -> Settings:
        if self.auth_enabled:
            missing = [
                name
                for name, value in (
                    ("MCP_AUTH_JWKS_URL", self.auth_jwks_url),
                    ("MCP_AUTH_ISSUER", self.auth_issuer),
                    ("MCP_AUTH_AUDIENCE", self.auth_audience),
                )
                if not value
            ]
            if missing:
                raise ValueError(f"MCP_AUTH_ENABLED=true requires {', '.join(missing)}")
        return self

    @model_validator(mode="after")
    def _resolve_auth_clients_file(self) -> Settings:
        if self.auth_clients_file:
            raw = json.loads(Path(self.auth_clients_file).read_text(encoding="utf-8"))
            if not isinstance(raw, dict) or not all(
                isinstance(k, str) and isinstance(v, str) for k, v in raw.items()
            ):
                raise ValueError("MCP_AUTH_CLIENTS_FILE must hold a JSON object of name to key")
            self.auth_clients = raw
        short = sorted(name for name, key in self.auth_clients.items() if len(key) < 32)
        if short:
            names = ", ".join(short)
            raise ValueError(f"machine client keys must be at least 32 characters: {names}")
        return self

    @model_validator(mode="after")
    def _check_auth_persons(self) -> Settings:
        keyless = sorted(set(self.auth_persons) - set(self.auth_clients))
        if keyless:
            names = ", ".join(keyless)
            raise ValueError(
                f"MCP_AUTH_PERSONS lists persons with no key in MCP_AUTH_CLIENTS_FILE: {names}"
            )
        unlisted = sorted(set(self.auth_persons) - set(self.auth_client_tools))
        if unlisted:
            names = ", ".join(unlisted)
            raise ValueError(
                f"MCP_AUTH_PERSONS lists persons with no entry in MCP_AUTH_CLIENT_TOOLS: {names}"
            )
        return self

    @model_validator(mode="after")
    def _check_wikijs_config(self) -> Settings:
        if bool(self.wikijs_url) != bool(self.wikijs_token_file):
            raise ValueError("MCP_WIKIJS_URL and MCP_WIKIJS_TOKEN_FILE must be set together")
        return self

    @model_validator(mode="after")
    def _check_wikijs_write_config(self) -> Settings:
        if self.wikijs_write_token_file and not self.wikijs_url:
            raise ValueError("MCP_WIKIJS_WRITE_TOKEN_FILE needs MCP_WIKIJS_URL")
        return self

    @model_validator(mode="after")
    def _check_pbs_config(self) -> Settings:
        parts = (self.pbs_url, self.pbs_token_id, self.pbs_token_file)
        if any(parts) and not all(parts):
            raise ValueError(
                "MCP_PBS_URL, MCP_PBS_TOKEN_ID and MCP_PBS_TOKEN_FILE must be set together"
            )
        return self

    @model_validator(mode="after")
    def _check_s3_config(self) -> Settings:
        parts = (self.s3_endpoint_url, self.s3_access_key_file, self.s3_secret_key_file)
        if any(parts) and not all(parts):
            raise ValueError(
                "MCP_S3_ENDPOINT_URL, MCP_S3_ACCESS_KEY_FILE and MCP_S3_SECRET_KEY_FILE"
                " must be set together"
            )
        return self

    @model_validator(mode="after")
    def _check_trigger_config(self) -> Settings:
        if bool(self.trigger_url) != bool(self.trigger_key_file):
            raise ValueError("MCP_TRIGGER_URL and MCP_TRIGGER_KEY_FILE must be set together")
        return self

    @property
    def nats_servers_list(self) -> list[str]:
        return [s.strip() for s in self.nats_servers.split(",") if s.strip()]

    def _dsn(self, username: str, password: str) -> str:
        # URL-encode user + password — random-generated passwords routinely
        # contain `/`, `@`, `:`, `+` that break psycopg's URI parser.
        user = quote(username, safe="")
        secret = quote(password, safe="")
        return f"postgresql://{user}:{secret}@{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def db_dsn(self) -> str:
        return self._dsn(self.db_username, self.db_password)

    @property
    def db_write_enabled(self) -> bool:
        return bool(self.db_write_username and self.db_write_password)

    @property
    def wikijs_enabled(self) -> bool:
        return bool(self.wikijs_url and self.wikijs_token_file)

    @property
    def wikijs_write_enabled(self) -> bool:
        return bool(self.wikijs_url and self.wikijs_write_token_file)

    @property
    def pbs_enabled(self) -> bool:
        return bool(self.pbs_url and self.pbs_token_id and self.pbs_token_file)

    @property
    def s3_enabled(self) -> bool:
        return bool(self.s3_endpoint_url and self.s3_access_key_file and self.s3_secret_key_file)

    @property
    def trigger_enabled(self) -> bool:
        return bool(self.trigger_url and self.trigger_key_file)

    @property
    def db_write_dsn(self) -> str:
        if not self.db_write_enabled:
            raise ValueError(
                "MCP_DB_WRITE_USERNAME / MCP_DB_WRITE_PASSWORD (or *_FILE variants) are required"
            )
        return self._dsn(self.db_write_username, self.db_write_password)


def load_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
