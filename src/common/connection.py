import sys
from version import __version__
import redis
from redis import Redis
from redis.cluster import RedisCluster
from redis.exceptions import RedisError
from typing import Optional, Type, Union, Dict, Any
from common.config import REDIS_CFG


class RedisConnectionManager:
    _instance: Optional[Redis] = None
    _DEFAULT_MAX_CONNECTIONS = 10

    @classmethod
    def _build_connection_params(cls, decode_responses: bool = True, db: Optional[int] = None) -> Dict[str, Any]:
        """Build connection parameters from configuration."""
        params = {
            "host": REDIS_CFG["host"],
            "port": REDIS_CFG["port"],
            "username": REDIS_CFG["username"],
            "password": REDIS_CFG["password"],
            "ssl": REDIS_CFG["ssl"],
            "ssl_ca_path": REDIS_CFG["ssl_ca_path"],
            "ssl_keyfile": REDIS_CFG["ssl_keyfile"],
            "ssl_certfile": REDIS_CFG["ssl_certfile"],
            "ssl_cert_reqs": REDIS_CFG["ssl_cert_reqs"],
            "ssl_ca_certs": REDIS_CFG["ssl_ca_certs"],
            "decode_responses": decode_responses,
            "lib_name": f"redis-py(mcp-server_v{__version__})",
        }
        
        # Handle database parameter
        if REDIS_CFG["cluster_mode"]:
            if db is not None:
                raise RedisError("Database switching not supported in cluster mode")
            params["max_connections_per_node"] = cls._DEFAULT_MAX_CONNECTIONS
        else:
            params["db"] = db if db is not None else REDIS_CFG["db"]
            params["max_connections"] = cls._DEFAULT_MAX_CONNECTIONS
            
        return params

    @classmethod
    def _create_connection(cls, decode_responses: bool = True, db: Optional[int] = None) -> Redis:
        """Create a new Redis connection with the given parameters."""
        try:
            connection_params = cls._build_connection_params(decode_responses, db)
            
            if REDIS_CFG["cluster_mode"]:
                redis_class: Type[Union[Redis, RedisCluster]] = redis.cluster.RedisCluster
            else:
                redis_class: Type[Union[Redis, RedisCluster]] = redis.Redis
            
            return redis_class(**connection_params)
            
        except redis.exceptions.ConnectionError:
            print("Failed to connect to Redis server", file=sys.stderr)
            raise
        except redis.exceptions.AuthenticationError:
            print("Authentication failed", file=sys.stderr)
            raise
        except redis.exceptions.TimeoutError:
            print("Connection timed out", file=sys.stderr)
            raise
        except redis.exceptions.ResponseError as e:
            print(f"Response error: {e}", file=sys.stderr)
            raise
        except redis.exceptions.RedisError as e:
            print(f"Redis error: {e}", file=sys.stderr)
            raise
        except redis.exceptions.ClusterError as e:
            print(f"Redis Cluster error: {e}", file=sys.stderr)
            raise
        except Exception as e:
            if db is not None:
                raise RedisError(f"Error connecting to database {db}: {str(e)}")
            else:
                print(f"Unexpected error: {e}", file=sys.stderr)
                raise

    @classmethod
    def get_connection(cls, decode_responses: bool = True, db: Optional[int] = None, use_singleton: bool = True) -> Redis:
        """
        Get a Redis connection.
        
        Args:
            decode_responses (bool): Whether to decode responses
            db (Optional[int]): Database number to connect to (None uses config default)
            use_singleton (bool): Whether to use singleton pattern (True) or create new connection (False)
            
        Returns:
            Redis: Redis connection instance
            
        Raises:
            RedisError: If cluster mode is enabled and db is specified, or connection fails
        """
        if use_singleton and db is None:
            # Singleton behavior for default database
            if cls._instance is None:
                cls._instance = cls._create_connection(decode_responses)
            return cls._instance
        else:
            # Create new connection for specific database or when singleton is disabled
            return cls._create_connection(decode_responses, db)

    @classmethod
    def get_connection_for_db(cls, db: int, decode_responses: bool = True) -> Redis:
        """
        Get a Redis connection for a specific database.
        This creates a new connection rather than using the singleton.
        
        Args:
            db (int): Database number to connect to
            decode_responses (bool): Whether to decode responses
            
        Returns:
            Redis: Redis connection instance for the specified database
            
        Raises:
            RedisError: If cluster mode is enabled or connection fails
        """
        return cls.get_connection(decode_responses=decode_responses, db=db, use_singleton=False)
