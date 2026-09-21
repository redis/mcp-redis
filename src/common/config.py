import os
import urllib.parse

from dotenv import load_dotenv

load_dotenv()

# Default values for Entra ID authentication
DEFAULT_TOKEN_EXPIRATION_REFRESH_RATIO = 0.9
DEFAULT_LOWER_REFRESH_BOUND_MILLIS = 30000  # 30 seconds
DEFAULT_TOKEN_REQUEST_EXECUTION_TIMEOUT_MS = 10000  # 10 seconds
DEFAULT_RETRY_MAX_ATTEMPTS = 3
DEFAULT_RETRY_DELAY_MS = 100
DEFAULT_SENTINEL_PORT = 26379


def parse_bool(value) -> bool:
    """Parse a value into a boolean using common truthy strings."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ("true", "1", "t", "yes", "y", "on")


def parse_sentinel_nodes(value) -> list[tuple[str, int]]:
    """Parse sentinel nodes from a comma-separated string or iterable."""
    if value in (None, ""):
        return []

    if isinstance(value, list):
        nodes = value
    else:
        nodes = str(value).split(",")

    parsed_nodes = []
    for node in nodes:
        if isinstance(node, tuple):
            host, port = node
            parsed_nodes.append((str(host), int(port)))
            continue

        entry = str(node).strip()
        if not entry:
            continue

        if ":" in entry:
            host, port = entry.rsplit(":", 1)
            parsed_nodes.append((host.strip(), int(port)))
        else:
            parsed_nodes.append((entry, DEFAULT_SENTINEL_PORT))

    return parsed_nodes


def normalize_topology(topology=None, cluster_mode=False) -> str:
    """Resolve the effective Redis topology."""
    if topology:
        topology = str(topology).strip().lower()
        if topology not in {"standalone", "sentinel", "cluster"}:
            raise ValueError(f"Unsupported topology: {topology}")
        if cluster_mode and topology != "cluster":
            raise ValueError(
                "REDIS_CLUSTER_MODE conflicts with REDIS_TOPOLOGY unless topology=cluster"
            )
        return topology

    return "cluster" if parse_bool(cluster_mode) else "standalone"


def build_redis_config_from_env() -> dict:
    """Build Redis configuration from environment variables."""
    cluster_mode = parse_bool(os.getenv("REDIS_CLUSTER_MODE", False))
    topology = normalize_topology(os.getenv("REDIS_TOPOLOGY"), cluster_mode)

    sentinel_host = os.getenv("REDIS_SENTINEL_HOST", None)
    sentinel_port = os.getenv("REDIS_SENTINEL_PORT", None)
    sentinel_nodes = parse_sentinel_nodes(os.getenv("REDIS_SENTINEL_NODES", None))
    if sentinel_host:
        sentinel_nodes.append(
            (
                sentinel_host,
                int(sentinel_port or DEFAULT_SENTINEL_PORT),
            )
        )

    return {
        "host": os.getenv("REDIS_HOST", "127.0.0.1"),
        "port": int(os.getenv("REDIS_PORT", 6379)),
        "username": os.getenv("REDIS_USERNAME", None),
        "password": os.getenv("REDIS_PWD", ""),
        "ssl": parse_bool(os.getenv("REDIS_SSL", False)),
        "ssl_ca_path": os.getenv("REDIS_SSL_CA_PATH", None),
        "ssl_keyfile": os.getenv("REDIS_SSL_KEYFILE", None),
        "ssl_certfile": os.getenv("REDIS_SSL_CERTFILE", None),
        "ssl_cert_reqs": os.getenv("REDIS_SSL_CERT_REQS", "required"),
        "ssl_ca_certs": os.getenv("REDIS_SSL_CA_CERTS", None),
        "cluster_mode": topology == "cluster",
        "topology": topology,
        "db": int(os.getenv("REDIS_DB", 0)),
        "sentinel_master_name": os.getenv("REDIS_SENTINEL_MASTER_NAME", None),
        "sentinel_nodes": sentinel_nodes,
        "sentinel_username": os.getenv("REDIS_SENTINEL_USERNAME", None),
        "sentinel_password": os.getenv("REDIS_SENTINEL_PWD", None),
    }

REDIS_CFG = build_redis_config_from_env()

# Entra ID Authentication Configuration
ENTRAID_CFG = {
    # Authentication flow selection
    "auth_flow": os.getenv(
        "REDIS_ENTRAID_AUTH_FLOW", None
    ),  # service_principal, managed_identity, default_credential
    # Service Principal Authentication
    "client_id": os.getenv("REDIS_ENTRAID_CLIENT_ID", None),
    "client_secret": os.getenv("REDIS_ENTRAID_CLIENT_SECRET", None),
    "tenant_id": os.getenv("REDIS_ENTRAID_TENANT_ID", None),
    # Managed Identity Authentication
    "identity_type": os.getenv(
        "REDIS_ENTRAID_IDENTITY_TYPE", "system_assigned"
    ),  # system_assigned, user_assigned
    "user_assigned_identity_client_id": os.getenv(
        "REDIS_ENTRAID_USER_ASSIGNED_CLIENT_ID", None
    ),
    # Default Azure Credential Authentication
    "scopes": os.getenv("REDIS_ENTRAID_SCOPES", "https://redis.azure.com/.default"),
    # Token lifecycle configuration
    "token_expiration_refresh_ratio": float(
        os.getenv(
            "REDIS_ENTRAID_TOKEN_EXPIRATION_REFRESH_RATIO",
            DEFAULT_TOKEN_EXPIRATION_REFRESH_RATIO,
        )
    ),
    "lower_refresh_bound_millis": int(
        os.getenv(
            "REDIS_ENTRAID_LOWER_REFRESH_BOUND_MILLIS",
            DEFAULT_LOWER_REFRESH_BOUND_MILLIS,
        )
    ),
    "token_request_execution_timeout_ms": int(
        os.getenv(
            "REDIS_ENTRAID_TOKEN_REQUEST_EXECUTION_TIMEOUT_MS",
            DEFAULT_TOKEN_REQUEST_EXECUTION_TIMEOUT_MS,
        )
    ),
    # Retry configuration
    "retry_max_attempts": int(
        os.getenv("REDIS_ENTRAID_RETRY_MAX_ATTEMPTS", DEFAULT_RETRY_MAX_ATTEMPTS)
    ),
    "retry_delay_ms": int(
        os.getenv("REDIS_ENTRAID_RETRY_DELAY_MS", DEFAULT_RETRY_DELAY_MS)
    ),
    # Resource configuration
    "resource": os.getenv("REDIS_ENTRAID_RESOURCE", "https://redis.azure.com/"),
}

# ConvAI API configuration
MCP_DOCS_SEARCH_URL = os.getenv(
    "MCP_DOCS_SEARCH_URL", "https://redis.io/convai/api/docs/search"
)


def parse_redis_uri(uri: str) -> dict:
    """Parse a Redis URI and return connection parameters."""
    parsed = urllib.parse.urlparse(uri)

    config = {"topology": "standalone", "cluster_mode": False}

    # Scheme determines SSL
    if parsed.scheme == "rediss":
        config["ssl"] = True
    elif parsed.scheme == "redis":
        config["ssl"] = False
    else:
        raise ValueError(f"Unsupported scheme: {parsed.scheme}")

    # Host and port
    config["host"] = parsed.hostname or "127.0.0.1"
    config["port"] = parsed.port or 6379

    # Database
    if parsed.path and parsed.path != "/":
        try:
            config["db"] = int(parsed.path.lstrip("/"))
        except ValueError:
            config["db"] = 0
    else:
        config["db"] = 0

    # Authentication
    if parsed.username:
        config["username"] = parsed.username
    if parsed.password:
        config["password"] = parsed.password

    # Parse query parameters for SSL and other options
    if parsed.query:
        query_params = urllib.parse.parse_qs(parsed.query)

        # Handle SSL parameters
        if "ssl_cert_reqs" in query_params:
            config["ssl_cert_reqs"] = query_params["ssl_cert_reqs"][0]
        if "ssl_ca_certs" in query_params:
            config["ssl_ca_certs"] = query_params["ssl_ca_certs"][0]
        if "ssl_ca_path" in query_params:
            config["ssl_ca_path"] = query_params["ssl_ca_path"][0]
        if "ssl_keyfile" in query_params:
            config["ssl_keyfile"] = query_params["ssl_keyfile"][0]
        if "ssl_certfile" in query_params:
            config["ssl_certfile"] = query_params["ssl_certfile"][0]

        # Handle other parameters. According to https://www.iana.org/assignments/uri-schemes/prov/redis,
        # The database number to use for the Redis SELECT command comes from
        #   either the "db-number" portion of the URI (described in the previous
        #   section) or the value from the key-value pair from the "query" URI
        #   field with the key "db".  If neither of these are present, the
        #   default database number is 0.
        if "db" in query_params:
            try:
                config["db"] = int(query_params["db"][0])
            except ValueError:
                pass

        if "topology" in query_params:
            config["topology"] = normalize_topology(query_params["topology"][0])
            config["cluster_mode"] = config["topology"] == "cluster"

        if "cluster_mode" in query_params and parse_bool(query_params["cluster_mode"][0]):
            config["topology"] = "cluster"
            config["cluster_mode"] = True

        if "sentinel_master_name" in query_params:
            config["sentinel_master_name"] = query_params["sentinel_master_name"][0]

        if "sentinel_nodes" in query_params:
            config["sentinel_nodes"] = parse_sentinel_nodes(
                query_params["sentinel_nodes"][0]
            )

    return config


def set_redis_config_from_cli(config: dict):
    for key, value in config.items():
        if key in ["port", "db"]:
            # Keep port and db as integers
            REDIS_CFG[key] = int(value)
        elif key in ["ssl", "cluster_mode"]:
            # Keep ssl and cluster_mode as booleans
            REDIS_CFG[key] = parse_bool(value)
        elif key == "topology":
            REDIS_CFG[key] = normalize_topology(value, REDIS_CFG.get("cluster_mode"))
            REDIS_CFG["cluster_mode"] = REDIS_CFG[key] == "cluster"
        elif key == "sentinel_nodes":
            REDIS_CFG[key] = parse_sentinel_nodes(value)
        elif isinstance(value, bool):
            # Convert other booleans to strings for environment compatibility
            REDIS_CFG[key] = "true" if value else "false"
        else:
            # Convert other values to strings
            REDIS_CFG[key] = str(value) if value is not None else None

    if "cluster_mode" in config and "topology" not in config and REDIS_CFG["cluster_mode"]:
        REDIS_CFG["topology"] = "cluster"
    elif "cluster_mode" in config and not REDIS_CFG["cluster_mode"] and REDIS_CFG.get(
        "topology"
    ) == "cluster":
        REDIS_CFG["topology"] = "standalone"

    if REDIS_CFG.get("topology") == "cluster":
        REDIS_CFG["cluster_mode"] = True
    elif REDIS_CFG.get("topology") in {"standalone", "sentinel"}:
        REDIS_CFG["cluster_mode"] = False


def validate_redis_config(config: dict | None = None) -> tuple[bool, str]:
    """Validate Redis topology-specific configuration."""
    config = config or REDIS_CFG

    try:
        topology = normalize_topology(
            config.get("topology"),
            config.get("cluster_mode", False),
        )
    except ValueError as exc:
        return False, str(exc)

    if topology == "sentinel":
        if not config.get("sentinel_master_name"):
            return False, "Sentinel topology requires sentinel_master_name"
        if not parse_sentinel_nodes(config.get("sentinel_nodes")):
            return False, "Sentinel topology requires at least one sentinel node"

    if topology == "cluster" and config.get("sentinel_master_name"):
        return False, "Cluster topology cannot be combined with sentinel_master_name"

    return True, ""


def set_entraid_config_from_cli(config: dict):
    """Update Entra ID configuration from CLI parameters."""
    for key, value in config.items():
        if value is not None:
            if key in ["token_expiration_refresh_ratio"]:
                # Keep float values as floats
                ENTRAID_CFG[key] = float(value)
            elif key in [
                "lower_refresh_bound_millis",
                "token_request_execution_timeout_ms",
                "retry_max_attempts",
                "retry_delay_ms",
            ]:
                # Keep integer values as integers
                ENTRAID_CFG[key] = int(value)
            else:
                # Convert other values to strings
                ENTRAID_CFG[key] = str(value)


def is_entraid_auth_enabled() -> bool:
    """Check if Entra ID authentication is enabled."""
    return ENTRAID_CFG["auth_flow"] is not None


def get_entraid_auth_flow() -> str:
    """Get the configured Entra ID authentication flow."""
    return ENTRAID_CFG["auth_flow"]


def validate_entraid_config() -> tuple[bool, str]:
    """Validate Entra ID configuration based on the selected auth flow.

    Returns:
        tuple: (is_valid, error_message)
    """
    auth_flow = ENTRAID_CFG["auth_flow"]

    if not auth_flow:
        return True, ""  # No Entra ID auth configured, which is valid

    if auth_flow == "service_principal":
        required_fields = ["client_id", "client_secret", "tenant_id"]
        missing_fields = [field for field in required_fields if not ENTRAID_CFG[field]]
        if missing_fields:
            return (
                False,
                f"Service principal authentication requires: {', '.join(missing_fields)}",
            )

    elif auth_flow == "managed_identity":
        identity_type = ENTRAID_CFG["identity_type"]
        if (
            identity_type == "user_assigned"
            and not ENTRAID_CFG["user_assigned_identity_client_id"]
        ):
            return (
                False,
                "User-assigned managed identity requires user_assigned_identity_client_id",
            )

    elif auth_flow == "default_credential":
        # Default credential doesn't require specific configuration
        pass

    else:
        return (
            False,
            f"Invalid auth_flow: {auth_flow}. Must be one of: service_principal, managed_identity, default_credential",
        )

    return True, ""
