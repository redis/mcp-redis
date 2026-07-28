# src/common/constants.py
import os

# --- Connection Pool Defaults ---
DEFAULT_MAX_POOLS: int = int(os.getenv("MAX_POOLS", "20"))
DEFAULT_IDLE_TIMEOUT: int = int(os.getenv("IDLE_TIMEOUT_SECONDS", "100"))

# --- Redis Socket Settings ---
DEFAULT_REDIS_MAX_CONNECTIONS: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "10"))
DEFAULT_REDIS_SOCKET_TIMEOUT: float = float(os.getenv("REDIS_SOCKET_TIMEOUT", "5.0"))
DEFAULT_REDIS_CONNECT_TIMEOUT: float = float(os.getenv("REDIS_CONNECT_TIMEOUT", "3.0"))
DEFAULT_REDIS_HEALTH_CHECK_INTERVAL: int = int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "30"))

# --- PostgreSQL Connection Pool Settings ---
DEFAULT_POSTGRES_MIN_SIZE: int = int(os.getenv("POSTGRES_MIN_POOL_SIZE", "1"))
DEFAULT_POSTGRES_MAX_SIZE: int = int(os.getenv("POSTGRES_MAX_POOL_SIZE", "10"))
DEFAULT_POSTGRES_TIMEOUT: float = float(os.getenv("POSTGRES_COMMAND_TIMEOUT", "10.0"))

# --- Application Metadata ---
DEFAULT_REDIS_DB_INDEX: int = 0