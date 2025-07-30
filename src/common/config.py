import os
import urllib.parse

from dotenv import load_dotenv

load_dotenv()

REDIS_CFG = {
    "host": os.getenv("REDIS_HOST", "127.0.0.1"),
    "port": int(os.getenv("REDIS_PORT", 6379)),
    "username": os.getenv("REDIS_USERNAME", None),
    "password": os.getenv("REDIS_PWD", ""),
    "ssl": os.getenv("REDIS_SSL", False) in ("true", "1", "t"),
    "ssl_ca_path": os.getenv("REDIS_SSL_CA_PATH", None),
    "ssl_keyfile": os.getenv("REDIS_SSL_KEYFILE", None),
    "ssl_certfile": os.getenv("REDIS_SSL_CERTFILE", None),
    "ssl_cert_reqs": os.getenv("REDIS_SSL_CERT_REQS", "required"),
    "ssl_ca_certs": os.getenv("REDIS_SSL_CA_CERTS", None),
    "cluster_mode": os.getenv("REDIS_CLUSTER_MODE", False) in ("true", "1", "t"),
    "db": int(os.getenv("REDIS_DB", 0)),
}


def parse_redis_uri(uri: str) -> dict:
    """Parse a Redis URI and return connection parameters."""
    parsed = urllib.parse.urlparse(uri)

    config = {}

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

    return config


def set_redis_config_from_cli(config: dict):
    for key, value in config.items():
        if key in ["port", "db"]:
            # Keep port and db as integers
            REDIS_CFG[key] = int(value)
        elif key == "ssl" or key == "cluster_mode":
            # Keep ssl and cluster_mode as booleans
            REDIS_CFG[key] = bool(value)
        elif isinstance(value, bool):
            # Convert other booleans to strings for environment compatibility
            REDIS_CFG[key] = "true" if value else "false"
        else:
            # Convert other values to strings
            REDIS_CFG[key] = str(value) if value is not None else None
