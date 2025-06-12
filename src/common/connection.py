import sys
from version import __version__
import redis
from redis import Redis
from redis.cluster import RedisCluster
from redis.exceptions import RedisError
from typing import Optional, Type, Union
from common.config import REDIS_CFG

from common.config import generate_redis_uri


class RedisConnectionManager:
    _instance: Optional[Redis] = None

    @classmethod
    def get_connection(cls, decode_responses=True) -> Redis:
        if cls._instance is None:
            try:
                if REDIS_CFG["cluster_mode"]:
                    redis_class: Type[Union[Redis, RedisCluster]] = redis.cluster.RedisCluster
                    connection_params = {
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
                        "max_connections_per_node": 10 
                    }
                else:
                    redis_class: Type[Union[Redis, RedisCluster]] = redis.Redis
                    connection_params = {
                        "host": REDIS_CFG["host"],
                        "port": REDIS_CFG["port"],
                        "db": REDIS_CFG["db"],
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
                        "max_connections": 10
                    }
                
                cls._instance = redis_class(**connection_params)

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
                print(f"Unexpected error: {e}", file=sys.stderr)
                raise

        return cls._instance

    @classmethod
    def get_connection_for_db(cls, db: int, decode_responses=True) -> Redis:
        """
        Get a Redis connection for a specific database.
        This creates a new connection rather than using the singleton.
        
        Args:
            db (int): Database number to connect to
            decode_responses (bool): Whether to decode responses
            
        Returns:
            Redis: Redis connection instance for the specified database
        """
        try:
            if REDIS_CFG["cluster_mode"]:
                # Cluster mode doesn't support SELECT/database switching
                raise RedisError("Database switching not supported in cluster mode")
            
            connection_params = {
                "host": REDIS_CFG["host"],
                "port": REDIS_CFG["port"],
                "db": db,  # Use the specified database
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
                "max_connections": 10
            }
            
            return redis.Redis(**connection_params)
            
        except Exception as e:
            raise RedisError(f"Error connecting to database {db}: {str(e)}")
