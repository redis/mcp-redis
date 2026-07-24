
# src/common/config.py
import os
import urllib.parse
from typing import Optional, Literal, Dict, Any, Union
from dotenv import load_dotenv
from pydantic import BaseModel, Field, model_validator

load_dotenv()

ENTRAID_CFG: Dict[str, Any] = {}
REDIS_CFG: Dict[str, Any] = {}

DEFAULT_TIMEOUT_MS = 10000
DEFAULT_RETRY_MAX_ATTEMPTS = 3

MCP_DOCS_SEARCH_URL = os.getenv(
    "MCP_DOCS_SEARCH_URL", "https://redis.io/convai/api/docs/search"
)



class SSLConfig(BaseModel):
    enabled: bool = False
    ssl_ca_path: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_cert_reqs: Literal["none", "optional", "required"] = "required"
    ssl_ca_certs: Optional[str] = None


class RedisMCPConfig(BaseModel):
    uri: Optional[str] = None
    url: Optional[str] = None

    host: str = Field(default="127.0.0.1")
    port: int = Field(default=6379, ge=1, le=65535)
    db: int = Field(default=0, ge=0)
    username: Optional[str] = None
    password: Optional[str] = None
    cluster_mode: bool = False

    # SSL Sub-configuration
    ssl: SSLConfig = Field(default_factory=SSLConfig)

    @model_validator(mode="before")
    @classmethod
    def parse_redis_uri_if_present(cls, data: Any) -> Any:
        """
        If the input is a plain URL string (e.g., 'redis://...' or 'rediss://...'),
        convert it into a dict before validation.
        """
        if isinstance(data, str):
            data = {"url": data}

        if not isinstance(data, dict):
            return data

        # Accept either 'url' or 'uri'
        raw_url = data.get("url") or data.get("uri")
        if not raw_url:
            return data

        parsed = urllib.parse.urlparse(raw_url)
        if parsed.scheme not in ("redis", "rediss"):
            raise ValueError(
                f"Unsupported scheme: '{parsed.scheme}'. Must be 'redis' or 'rediss'"
            )

        # 1. Host & Port
        data.setdefault("host", parsed.hostname or "127.0.0.1")
        data.setdefault("port", parsed.port or 6379)

        # 2. SSL
        ssl_dict = data.get("ssl", {})
        if isinstance(ssl_dict, bool):
            ssl_dict = {"enabled": ssl_dict}

        if parsed.scheme == "rediss":
            ssl_dict["enabled"] = True

        data["ssl"] = ssl_dict

        # 3. Database routing
        if parsed.path and parsed.path != "/":
            try:
                data.setdefault("db", int(parsed.path.lstrip("/")))
            except ValueError:
                data.setdefault("db", 0)

        # 4. User Credentials
        if parsed.username:
            data.setdefault("username", parsed.username)
        if parsed.password:
            data.setdefault("password", parsed.password)

        # 5. Query parameters (e.g. ?ssl_cert_reqs=none&db=2)
        if parsed.query:
            query_params = urllib.parse.parse_qs(parsed.query)
            for param in ["ssl_cert_reqs", "ssl_ca_certs", "ssl_ca_path", "ssl_keyfile", "ssl_certfile"]:
                if param in query_params:
                    ssl_dict[param] = query_params[param][0]
            if "db" in query_params:
                try:
                    data["db"] = int(query_params["db"][0])
                except ValueError:
                    pass

        return data

    @classmethod
    def from_any(cls, raw_config: Union[str, dict]) -> "RedisMCPConfig":
        """
        Universal loader: Handles raw string URLs, JSON string inputs, or dict objects.
        """
        import json

        if isinstance(raw_config, str):
            raw_config = raw_config.strip()
            # If customer passed a JSON string like '{"host": "localhost", ...}'
            if raw_config.startswith("{"):
                try:
                    raw_config = json.loads(raw_config)
                except json.JSONDecodeError as err:
                    raise ValueError(f"Invalid JSON configuration string: {err}") from err
            else:
                # Customer passed a direct URL string like "redis://localhost:6379"
                raw_config = {"url": raw_config}

        return cls.model_validate(raw_config)