import logging
from typing import Optional, Type, Union

import redis
from redis import Redis
from redis.cluster import RedisCluster
from redis.sentinel import Sentinel

from src.common.config import REDIS_CFG, is_entraid_auth_enabled
from src.common.entraid_auth import (
    create_credential_provider,
    EntraIDAuthenticationError,
)
from src.version import __version__

_logger = logging.getLogger(__name__)


class RedisConnectionManager:
    _instance: Optional[Redis] = None

    @staticmethod
    def _build_common_connection_params(decode_responses=True) -> dict:
        connection_params = {
            "username": REDIS_CFG["username"],
            "password": REDIS_CFG["password"],
            "ssl": REDIS_CFG["ssl"],
            "decode_responses": decode_responses,
            "lib_name": f"redis-py(mcp-server_v{__version__})",
        }

        if REDIS_CFG["ssl"]:
            for key in [
                "ssl_ca_path",
                "ssl_keyfile",
                "ssl_certfile",
                "ssl_cert_reqs",
                "ssl_ca_certs",
            ]:
                value = REDIS_CFG[key]
                if value is not None:
                    connection_params[key] = value

        # Sentinel connections are more strict about unexpected kwargs.
        return {
            key: value
            for key, value in connection_params.items()
            if value is not None and value != ""
            or key in {"password", "username", "ssl", "decode_responses", "lib_name"}
        }

    @staticmethod
    def _build_sentinel_kwargs() -> dict:
        sentinel_kwargs = {}
        if REDIS_CFG["sentinel_username"]:
            sentinel_kwargs["username"] = REDIS_CFG["sentinel_username"]
        if REDIS_CFG["sentinel_password"] is not None:
            sentinel_kwargs["password"] = REDIS_CFG["sentinel_password"]

        return sentinel_kwargs

    @classmethod
    def get_connection(cls, decode_responses=True) -> Redis:
        if cls._instance is None:
            try:
                # Create Entra ID credential provider if configured
                credential_provider = None
                if is_entraid_auth_enabled():
                    try:
                        credential_provider = create_credential_provider()
                    except EntraIDAuthenticationError as e:
                        _logger.error(
                            "Failed to create Entra ID credential provider: %s", e
                        )
                        raise

                connection_params = cls._build_common_connection_params(
                    decode_responses=decode_responses
                )

                if credential_provider:
                    connection_params["credential_provider"] = credential_provider
                    # Note: Azure Redis Enterprise with EntraID uses plain text connections
                    # SSL setting is controlled by REDIS_SSL environment variable

                topology = REDIS_CFG["topology"]
                if topology == "cluster":
                    redis_class: Type[Union[Redis, RedisCluster]] = (
                        redis.cluster.RedisCluster
                    )
                    cls._instance = redis_class(
                        host=REDIS_CFG["host"],
                        port=REDIS_CFG["port"],
                        max_connections_per_node=10,
                        **connection_params,
                    )
                elif topology == "sentinel":
                    sentinel_client = Sentinel(
                        REDIS_CFG["sentinel_nodes"],
                        sentinel_kwargs=cls._build_sentinel_kwargs(),
                        db=REDIS_CFG["db"],
                        max_connections=10,
                        **connection_params,
                    )
                    cls._instance = sentinel_client.master_for(
                        REDIS_CFG["sentinel_master_name"],
                        redis_class=redis.Redis,
                    )
                else:
                    redis_class = redis.Redis
                    cls._instance = redis_class(
                        host=REDIS_CFG["host"],
                        port=REDIS_CFG["port"],
                        db=REDIS_CFG["db"],
                        max_connections=10,
                        **connection_params,
                    )

            except redis.exceptions.ConnectionError:
                _logger.error("Failed to connect to Redis server")
                raise
            except redis.exceptions.AuthenticationError:
                _logger.error("Authentication failed")
                raise
            except redis.exceptions.TimeoutError:
                _logger.error("Connection timed out")
                raise
            except redis.exceptions.ResponseError as e:
                _logger.error("Response error: %s", e)
                raise
            except redis.exceptions.RedisError as e:
                _logger.error("Redis error: %s", e)
                raise
            except redis.exceptions.ClusterError as e:
                _logger.error("Redis Cluster error: %s", e)
                raise
            except Exception as e:
                _logger.error("Unexpected error: %s", e)
                raise

        return cls._instance
