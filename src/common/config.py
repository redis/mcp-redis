from dotenv import load_dotenv
import os
import urllib.parse

load_dotenv()


class RedisConfig:
    """Redis configuration management class."""
    
    def __init__(self):
        self._config = {
            "host": os.getenv('REDIS_HOST', '127.0.0.1'),
            "port": int(os.getenv('REDIS_PORT', 6379)),
            "username": os.getenv('REDIS_USERNAME', None),
            "password": os.getenv('REDIS_PWD', ''),
            "ssl": os.getenv('REDIS_SSL', False) in ('true', '1', 't'),
            "ssl_ca_path": os.getenv('REDIS_SSL_CA_PATH', None),
            "ssl_keyfile": os.getenv('REDIS_SSL_KEYFILE', None),
            "ssl_certfile": os.getenv('REDIS_SSL_CERTFILE', None),
            "ssl_cert_reqs": os.getenv('REDIS_SSL_CERT_REQS', 'required'),
            "ssl_ca_certs": os.getenv('REDIS_SSL_CA_CERTS', None),
            "cluster_mode": os.getenv('REDIS_CLUSTER_MODE', False) in ('true', '1', 't'),
            "db": int(os.getenv('REDIS_DB', 0))
        }
    
    @property
    def config(self) -> dict:
        """Get the current configuration."""
        return self._config.copy()
    
    def get(self, key: str, default=None):
        """Get a configuration value."""
        return self._config.get(key, default)
    
    def __getitem__(self, key: str):
        """Get a configuration value using dictionary syntax."""
        return self._config[key]
    
    def update(self, config: dict):
        """Update configuration from dictionary."""
        for key, value in config.items():
            if key in ['port', 'db']:
                # Keep port and db as integers
                self._config[key] = int(value)
            elif key in ['ssl', 'cluster_mode']:
                # Keep ssl and cluster_mode as booleans
                self._config[key] = bool(value)
            else:
                # Store other values as-is
                self._config[key] = value if value is not None else None


def parse_redis_uri(uri: str) -> dict:
    """Parse a Redis URI and return connection parameters."""
    parsed = urllib.parse.urlparse(uri)

    config = {}

    # Scheme determines SSL
    if parsed.scheme == 'rediss':
        config['ssl'] = True
    elif parsed.scheme == 'redis':
        config['ssl'] = False
    else:
        raise ValueError(f"Unsupported scheme: {parsed.scheme}")

    # Host and port
    config['host'] = parsed.hostname or '127.0.0.1'
    config['port'] = parsed.port or 6379

    # Database
    if parsed.path and parsed.path != '/':
        try:
            config['db'] = int(parsed.path.lstrip('/'))
        except ValueError:
            config['db'] = 0
    else:
        config['db'] = 0

    # Authentication
    if parsed.username:
        config['username'] = parsed.username
    if parsed.password:
        config['password'] = parsed.password

    # Parse query parameters for SSL and other options
    if parsed.query:
        query_params = urllib.parse.parse_qs(parsed.query)

        # Handle SSL parameters
        if 'ssl_cert_reqs' in query_params:
            config['ssl_cert_reqs'] = query_params['ssl_cert_reqs'][0]
        if 'ssl_ca_certs' in query_params:
            config['ssl_ca_certs'] = query_params['ssl_ca_certs'][0]
        if 'ssl_ca_path' in query_params:
            config['ssl_ca_path'] = query_params['ssl_ca_path'][0]
        if 'ssl_keyfile' in query_params:
            config['ssl_keyfile'] = query_params['ssl_keyfile'][0]
        if 'ssl_certfile' in query_params:
            config['ssl_certfile'] = query_params['ssl_certfile'][0]

        # Handle other parameters. According to https://www.iana.org/assignments/uri-schemes/prov/redis,
        # The database number to use for the Redis SELECT command comes from
        #   either the "db-number" portion of the URI (described in the previous
        #   section) or the value from the key-value pair from the "query" URI
        #   field with the key "db".  If neither of these are present, the
        #   default database number is 0.
        if 'db' in query_params:
            try:
                config['db'] = int(query_params['db'][0])
            except ValueError:
                pass

    return config


def build_redis_config(url=None, host=None, port=None, db=None, username=None, 
                      password=None, ssl=None, ssl_ca_path=None, ssl_keyfile=None,
                      ssl_certfile=None, ssl_cert_reqs=None, ssl_ca_certs=None,
                      cluster_mode=None, host_id=None):
    """
    Build Redis configuration from URL or individual parameters.
    Handles cluster mode conflicts and parameter validation.
    
    Returns:
        dict: Redis configuration dictionary
        str: Generated host_id if not provided
    """
    # Parse configuration from URL or individual parameters
    if url:
        config = parse_redis_uri(url)
        parsed_url = urllib.parse.urlparse(url)
        # Generate host_id from URL if not provided
        if host_id is None:
            host_id = f"{parsed_url.hostname}:{parsed_url.port or 6379}"
    else:
        # Build config from individual parameters
        config = {
            "host": host or "127.0.0.1",
            "port": port or 6379,
            "db": db or 0,
            "username": username,
            "password": password or "",
            "ssl": ssl or False,
            "ssl_ca_path": ssl_ca_path,
            "ssl_keyfile": ssl_keyfile,
            "ssl_certfile": ssl_certfile,
            "ssl_cert_reqs": ssl_cert_reqs or "required",
            "ssl_ca_certs": ssl_ca_certs,
            "cluster_mode": cluster_mode  # Allow None for auto-detection
        }
        # Generate host_id from host:port if not provided
        if host_id is None:
            host_id = f"{config['host']}:{config['port']}"
    
    # Override individual parameters if provided (useful when using URL + specific overrides)
    # Only override URL values if the parameter was explicitly specified
    if url is None or (host is not None and host != "127.0.0.1"):
        if host is not None:
            config["host"] = host
    if url is None or (port is not None and port != 6379):
        if port is not None:
            config["port"] = port
    if url is None or (db is not None and db != 0):
        if db is not None:
            config["db"] = db
    if username is not None:
        config["username"] = username
    if password is not None:
        config["password"] = password
    if ssl is not None:
        config["ssl"] = ssl
    if ssl_ca_path is not None:
        config["ssl_ca_path"] = ssl_ca_path
    if ssl_keyfile is not None:
        config["ssl_keyfile"] = ssl_keyfile
    if ssl_certfile is not None:
        config["ssl_certfile"] = ssl_certfile
    if ssl_cert_reqs is not None:
        config["ssl_cert_reqs"] = ssl_cert_reqs
    if ssl_ca_certs is not None:
        config["ssl_ca_certs"] = ssl_ca_certs
    if cluster_mode is not None:
        config["cluster_mode"] = cluster_mode
    
    # Handle cluster mode conflicts
    if config.get("cluster_mode", False):
        # Remove db parameter in cluster mode as it's not supported
        config.pop('db', None)
    
    return config, host_id
