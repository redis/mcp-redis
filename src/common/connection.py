

import asyncio
import logging
from contextvars import ContextVar
from typing import Optional, Dict, Union, Type
import redis.asyncio as aioredis
from redis.asyncio.cluster import RedisCluster

from src.common.tenant_db import tenant_db_manager, DEFAULT_IDLE_TIMEOUT
from src.common.lru_cache import AsyncLRUCache
from src.version import __version__


from src.common.constants import (
    DEFAULT_MAX_POOLS,
    DEFAULT_IDLE_TIMEOUT,
    DEFAULT_REDIS_MAX_CONNECTIONS,
    DEFAULT_REDIS_SOCKET_TIMEOUT,
    DEFAULT_REDIS_CONNECT_TIMEOUT,
    DEFAULT_REDIS_HEALTH_CHECK_INTERVAL,
)

logger = logging.getLogger("mcp.redis_pool")

current_tenant_id: ContextVar[Optional[str]] = ContextVar("current_tenant_id", default=None)
current_mcp_id: ContextVar[Optional[str]] = ContextVar("current_mcp_id", default=None)


async def _close_redis_client_callback(cache_key: str, client: Union[aioredis.Redis, RedisCluster]):
    """Callback function executed when a Redis client/pool is evicted from LRU Cache."""
    try:
        await client.aclose()
        logger.info("   ↳ [REDIS DISCONNECT] Closed active Redis pool for cache key: '%s'", cache_key)
    except Exception as e:
        logger.error("Error closing Redis pool during eviction for cache key '%s': %s", cache_key, e)


class TenantRedisPoolManager:
    """Manages isolated Redis connection pools per tenant and MCP ID using AsyncLRUCache."""

    def __init__(self, max_pools: int = 20, idle_timeout: int = DEFAULT_IDLE_TIMEOUT):
        self.pools = AsyncLRUCache[Union[aioredis.Redis, RedisCluster]](
            max_size=max_pools,
            ttl_seconds=idle_timeout,
            on_evict=_close_redis_client_callback
        )
        self._creation_locks: Dict[str, asyncio.Lock] = {}
        self._lock = asyncio.Lock()

    async def ensure_tenant_connection(self, tenant_id: str, mcp_id: Optional[str] = None) -> Union[aioredis.Redis, RedisCluster]:
        """Resolves or establishes an isolated Redis connection pool."""
        # Compose a composite cache key so different MCP instances within the same tenant don't collide
        cache_key = f"{tenant_id}:{mcp_id or 'default'}"

        # Sync LRU access timestamp for the tenant's Postgres DB pool to prevent unsynchronized eviction
        await tenant_db_manager.pools.get(tenant_id)

        client = await self.pools.get(cache_key)
        if client:
            logger.info("[REDIS POOL MANAGER] Reusing active Redis pool for key: '%s'", cache_key)
            return client

        async with self._lock:
            if cache_key not in self._creation_locks:
                self._creation_locks[cache_key] = asyncio.Lock()

        async with self._creation_locks[cache_key]:
            client = await self.pools.get(cache_key)
            if client:
                logger.info("[REDIS POOL MANAGER] Reusing active Redis pool for key: '%s'", cache_key)
                return client

            logger.info("[REDIS POOL MANAGER] Cold starting isolated Redis client for Tenant: '%s' | MCP ID: '%s'", tenant_id, mcp_id)

            # --- STEP 1: Fetch Redis configuration from PostgreSQL by mcp_id ---
            try:
                raw_config = await tenant_db_manager.get_mcp_config_by_id(tenant_id, mcp_id)
            except Exception as e:
                err_msg = f"PostgreSQL failure while loading Redis config for tenant '{tenant_id}' (MCP ID: '{mcp_id}'): {str(e)}"
                logger.error("[POSTGRES METADATA ERROR] %s", err_msg)
                raise RuntimeError(err_msg) from e

            # --- STEP 2: Instantiate Redis Client & Verify Connection ---
            connection_params = {}
            try:
                config_working = raw_config.copy()
                cluster_mode = config_working.pop("REDIS_CLUSTER_MODE", False)

                if cluster_mode:
                    redis_class: Type[Union[aioredis.Redis, RedisCluster]] = RedisCluster
                    connection_params = {
                        "host": config_working["REDIS_HOST"],
                        "port": config_working["REDIS_PORT"],
                        "username": config_working.get("REDIS_USERNAME"),
                        "password": config_working.get("REDIS_PWD"),
                        "ssl": config_working.get("REDIS_SSL", False),
                        "decode_responses": True,
                        "lib_name": f"redis-py(mcp-server_v{__version__})",
                        "max_connections_per_node": DEFAULT_REDIS_MAX_CONNECTIONS,
                        "socket_timeout": DEFAULT_REDIS_SOCKET_TIMEOUT,
                        "socket_connect_timeout": DEFAULT_REDIS_CONNECT_TIMEOUT,
                    }
                    driver_type = "REDIS CLUSTER"
                else:
                    redis_class = aioredis.Redis
                    redis_db_index = config_working.get("REDIS_DB", 0)
                    connection_params = {
                        "host": config_working["REDIS_HOST"],
                        "port": config_working["REDIS_PORT"],
                        "db": redis_db_index,
                        "username": config_working.get("REDIS_USERNAME"),
                        "password": config_working.get("REDIS_PWD"),
                        "ssl": config_working.get("REDIS_SSL", False),
                        "decode_responses": True,
                        "lib_name": f"redis-py(mcp-server_v{__version__})",
                        "max_connections": DEFAULT_REDIS_MAX_CONNECTIONS,
                        "socket_timeout": DEFAULT_REDIS_SOCKET_TIMEOUT,
                        "socket_connect_timeout": DEFAULT_REDIS_CONNECT_TIMEOUT,
                        "health_check_interval": DEFAULT_REDIS_HEALTH_CHECK_INTERVAL,
                    }
                    driver_type = f"REDIS STANDALONE (Redis DB Index: {redis_db_index})"

                target_addr = f"{connection_params['host']}:{connection_params['port']}"
                logger.info("[REDIS POOL MANAGER] Instantiating %s client targeting: %s", driver_type, target_addr)

                new_client = redis_class(**connection_params)

                # Verify network reachability before caching in LRU
                await new_client.ping()

                logger.info("[REDIS POOL MANAGER] Ping handshake verified! Cached Redis client for key '%s'", cache_key)

                await self.pools.put(cache_key, new_client)
                return new_client

            except Exception as e:
                target = f"{connection_params.get('host')}:{connection_params.get('port')}" if connection_params else "unknown"
                err_msg = f"Redis network handshake failed targeting '{target}' for key '{cache_key}': {str(e)}"
                logger.error("[REDIS CONNECTION ERROR] %s", err_msg)
                raise ConnectionError(err_msg) from e

    async def get_client(self) -> Union[aioredis.Redis, RedisCluster]:
        """Helper method called inside @mcp.tool functions to resolve the active Redis client."""
        tenant_id = current_tenant_id.get()
        if not tenant_id:
            raise RuntimeError("Context Violation: Cannot resolve a connection outside an active tenant request trace.")

        mcp_id = current_mcp_id.get()
        return await self.ensure_tenant_connection(tenant_id, mcp_id)

    async def close_idle_pools(self):
        """Evicts expired connection pools."""
        await self.pools.evict_idle()

    async def shutdown_all_pools(self):
        """Gracefully disconnects all active Redis connection pools on server shutdown."""
        logger.info("[POOL MANAGER] Terminating dynamic tenant Redis pools...")
        await self.pools.clear_all()
        self._creation_locks.clear()


tenant_redis_manager = TenantRedisPoolManager(max_pools=DEFAULT_MAX_POOLS, idle_timeout=DEFAULT_IDLE_TIMEOUT)