

# # # # src/common/connection.py
# # # import asyncio
# # # import logging
# # # from contextvars import ContextVar
# # # from typing import Optional, Dict, Union, Type
# # # import redis.asyncio as aioredis
# # # from redis.asyncio.cluster import RedisCluster

# # # from src.common.tenant_db import tenant_db_manager
# # # # from src.common.entraid_auth import (
# # # #     create_credential_provider,
# # # #     EntraIDAuthenticationError,
# # # # )
# # # from src.version import __version__

# # # logger = logging.getLogger("mcp.connection")

# # # # The magic variable isolating tenant context safely across streaming HTTP request tasks
# # # current_tenant_id: ContextVar[Optional[str]] = ContextVar("current_tenant_id", default=None)

# # # class TenantRedisPoolManager:
# # #     """
# # #     Manages isolated, high-throughput asynchronous Redis/Cluster connection pools 
# # #     per tenant dynamically, mapping configurations from a centralized registry.
# # #     """
# # #     def __init__(self):
# # #         # Cache memory trace mapping: { "tenant_abc": Redis / RedisCluster Instance }
# # #         self._pools: Dict[str, Union[aioredis.Redis, RedisCluster]] = {}
# # #         self._lock = asyncio.Lock()

# # #     async def ensure_tenant_connection(self, tenant_id: str) -> Union[aioredis.Redis, RedisCluster]:
# # #         """Thread-safe, lazy initialization of connection routing structures."""
# # #         if tenant_id in self._pools:
# # #             print(f"♻️  [POOL MANAGER] Reusing active ConnectionPool for Tenant: '{tenant_id}'", flush=True)
# # #             return self._pools[tenant_id]

# # #         async with self._lock:
# # #             # Double-check pattern prevents parallel async requests from overlapping initialization
# # #             if tenant_id in self._pools:
# # #                 return self._pools[tenant_id]

# # #             print(f"❄️  [POOL MANAGER] Cold starting isolated Redis infrastructure for tenant: '{tenant_id}'", flush=True)
# # #             try:
# # #                 # 1. Fetch parameters out of the centralized metadata registry database pipeline
# # #                 raw_config = await tenant_db_manager.get_redis_config_for_tenant(tenant_id)
                
# # #                 # Copy config to mutate safely without altering structural cache lookups
# # #                 config_working = raw_config.copy()
                
# # #                 # Extract routing flags before initializing parameters
# # #                 cluster_mode = config_working.pop("REDIS_CLUSTER_MODE", False)
# # #                 use_entraid = config_working.pop("REDIS_ENTRAID_AUTH", False)
                
# # #                 # 2. Extract and handle Entra ID Credentials dynamically per tenant
# # #                 credential_provider = None
# # #                 # if use_entraid:
# # #                 #     try:
# # #                 #         print(f"🔑 [POOL MANAGER] Resolving EntraID Token Provider Context for Tenant: '{tenant_id}'", flush=True)
# # #                 #         credential_provider = create_credential_provider()
# # #                 #     except EntraIDAuthenticationError as e:
# # #                 #         logger.error(f"Entra ID Token provider creation failed for tenant '{tenant_id}': {e}")
# # #                 #         raise

# # #                 # 3. Dynamic Driver Core Routing Branch
# # #                 if cluster_mode:
# # #                     redis_class: Type[Union[aioredis.Redis, RedisCluster]] = RedisCluster
# # #                     connection_params = {
# # #                         "host": config_working["REDIS_HOST"],
# # #                         "port": config_working["REDIS_PORT"],
# # #                         "username": config_working.get("REDIS_USERNAME"),
# # #                         "password": config_working.get("REDIS_PWD"),
# # #                         "ssl": config_working.get("REDIS_SSL", False),
# # #                         "ssl_ca_path": config_working.get("ssl_ca_path"),
# # #                         "ssl_keyfile": config_working.get("ssl_keyfile"),
# # #                         "ssl_certfile": config_working.get("ssl_certfile"),
# # #                         "ssl_cert_reqs": config_working.get("ssl_cert_reqs"),
# # #                         "ssl_ca_certs": config_working.get("ssl_ca_certs"),
# # #                         "decode_responses": True,
# # #                         "lib_name": f"redis-py(mcp-server_v{__version__})",
# # #                         "max_connections_per_node": 10,
# # #                     }
# # #                 else:
# # #                     redis_class = aioredis.Redis
# # #                     connection_params = {
# # #                         "host": config_working["REDIS_HOST"],
# # #                         "port": config_working["REDIS_PORT"],
# # #                         "db": config_working.get("REDIS_DB", 0),
# # #                         "username": config_working.get("REDIS_USERNAME"),
# # #                         "password": config_working.get("REDIS_PWD"),
# # #                         "ssl": config_working.get("REDIS_SSL", False),
# # #                         "ssl_keyfile": config_working.get("ssl_keyfile"),
# # #                         "ssl_certfile": config_working.get("ssl_certfile"),
# # #                         "ssl_cert_reqs": config_working.get("ssl_cert_reqs"),
# # #                         "ssl_ca_certs": config_working.get("ssl_ca_certs"),
# # #                         "decode_responses": True,
# # #                         "lib_name": f"redis-py(mcp-server_v{__version__})",
# # #                         "max_connections": 10,
# # #                     }
                    
# # #                     if config_working.get("ssl_ca_path") and not connection_params.get("ssl_ca_certs"):
# # #                         connection_params["ssl_ca_certs"] = config_working.get("ssl_ca_path")

# # #                 # Attach token engine if active for this specific tenant instance
# # #                 if credential_provider:
# # #                     connection_params["credential_provider"] = credential_provider

# # #                 # 4. Instantiate and store the dynamic driver container
# # #                 driver_type = "CLUSTER" if cluster_mode else f"STANDALONE (DB: {connection_params.get('db')})"
# # #                 print(f"🚀 [POOL MANAGER] Instantiating {driver_type} async driver targeting: {connection_params['host']}:{connection_params['port']}", flush=True)
                
# # #                 client = redis_class(**connection_params)
# # #                 self._pools[tenant_id] = client
# # #                 return client

# # #             except Exception as e:
# # #                 print(f"❌ [POOL MANAGER ERROR] Failed building multi-tenant driver setup for '{tenant_id}': {str(e)}", flush=True)
# # #                 logger.error(f"Failed configuring multi-tenant driver setup for '{tenant_id}': {str(e)}")
# # #                 raise

# # #     async def get_client(self) -> Union[aioredis.Redis, RedisCluster]:
# # #         """
# # #         Retrieves the scoped active tenant client pool linked to the context trace thread.
# # #         Call this directly inside your FastMCP tools.
# # #         """
# # #         tenant_id = current_tenant_id.get()
# # #         if not tenant_id:
# # #             raise RuntimeError("Context Violation: Cannot resolve a connection outside an active tenant request trace.")

# # #         if tenant_id in self._pools:
# # #             return self._pools[tenant_id]

# # #         return await self.ensure_tenant_connection(tenant_id)

# # #     async def shutdown_all_pools(self):
# # #         """Drains and destroys all open tenant execution tunnels gracefully at server shutdown."""
# # #         async with self._lock:
# # #             print(f"🛑 [POOL MANAGER] Terminating {len(self._pools)} dynamic tenant connection pools...", flush=True)
# # #             for tenant_id, client in self._pools.items():
# # #                 try:
# # #                     await client.aclose()
# # #                     print(f"   ↳ Disconnected active Redis pool for tenant: '{tenant_id}'", flush=True)
# # #                 except Exception as e:
# # #                     logger.error(f"Error wrapping up connection sequence for tenant '{tenant_id}': {str(e)}")
# # #             self._pools.clear()
            
# # #             # Deep teardown: Signal the PostgreSQL multi-hop infrastructure to drop pools cleanly too
# # #             await tenant_db_manager.shutdown_all_pools()

# # # # Global context execution hub instance
# # # tenant_redis_manager = TenantRedisPoolManager()





# # # src/common/connection.py
# # import asyncio
# # import logging
# # from contextvars import ContextVar
# # from typing import Optional, Dict, Union, Type
# # import redis.asyncio as aioredis
# # from redis.asyncio.cluster import RedisCluster

# # from src.common.tenant_db import tenant_db_manager
# # from src.common.lru_cache import AsyncLRUCache
# # from src.version import __version__

# # logger = logging.getLogger("mcp.connection")

# # current_tenant_id: ContextVar[Optional[str]] = ContextVar("current_tenant_id", default=None)

# # async def _close_redis_client_callback(tenant_id: str, client: Union[aioredis.Redis, RedisCluster]):
# #     """Callback function executed when a Redis client/pool is evicted."""
# #     try:
# #         await client.aclose()
# #         print(f"   ↳ [REDIS DISCONNECT] Closed active Redis pool for tenant: '{tenant_id}'", flush=True)
# #     except Exception as e:
# #         logger.error(f"Error closing Redis pool during eviction for tenant '{tenant_id}': {e}")


# # class TenantRedisPoolManager:
# #     """Manages isolated Redis connection pools per tenant using AsyncLRUCache."""
# #     def __init__(self, max_pools: int = 20, idle_timeout: int = 600):
# #         self.pools = AsyncLRUCache[Union[aioredis.Redis, RedisCluster]](
# #             max_size=max_pools,
# #             ttl_seconds=idle_timeout,
# #             on_evict=_close_redis_client_callback
# #         )
# #         self._creation_locks: Dict[str, asyncio.Lock] = {}
# #         self._lock = asyncio.Lock()

# #     async def ensure_tenant_connection(self, tenant_id: str) -> Union[aioredis.Redis, RedisCluster]:
# #         client = await self.pools.get(tenant_id)
# #         if client:
# #             print(f"♻️  [POOL MANAGER] Reusing active ConnectionPool for Tenant: '{tenant_id}'", flush=True)
# #             return client

# #         async with self._lock:
# #             if tenant_id not in self._creation_locks:
# #                 self._creation_locks[tenant_id] = asyncio.Lock()

# #         async with self._creation_locks[tenant_id]:
# #             client = await self.pools.get(tenant_id)
# #             if client:
# #                 return client

# #             print(f"❄️  [POOL MANAGER] Cold starting isolated Redis infrastructure for tenant: '{tenant_id}'", flush=True)
# #             try:
# #                 raw_config = await tenant_db_manager.get_redis_config_for_tenant(tenant_id)
# #                 config_working = raw_config.copy()

# #                 cluster_mode = config_working.pop("REDIS_CLUSTER_MODE", False)

# #                 if cluster_mode:
# #                     redis_class: Type[Union[aioredis.Redis, RedisCluster]] = RedisCluster
# #                     connection_params = {
# #                         "host": config_working["REDIS_HOST"],
# #                         "port": config_working["REDIS_PORT"],
# #                         "username": config_working.get("REDIS_USERNAME"),
# #                         "password": config_working.get("REDIS_PWD"),
# #                         "ssl": config_working.get("REDIS_SSL", False),
# #                         "decode_responses": True,
# #                         "lib_name": f"redis-py(mcp-server_v{__version__})",
# #                         "max_connections_per_node": 10,
# #                         "socket_timeout": 5.0,
# #                         "socket_connect_timeout": 3.0,
# #                     }
# #                 else:
# #                     redis_class = aioredis.Redis
# #                     connection_params = {
# #                         "host": config_working["REDIS_HOST"],
# #                         "port": config_working["REDIS_PORT"],
# #                         "db": config_working.get("REDIS_DB", 0),
# #                         "username": config_working.get("REDIS_USERNAME"),
# #                         "password": config_working.get("REDIS_PWD"),
# #                         "ssl": config_working.get("REDIS_SSL", False),
# #                         "decode_responses": True,
# #                         "lib_name": f"redis-py(mcp-server_v{__version__})",
# #                         "max_connections": 10,
# #                         "socket_timeout": 5.0,
# #                         "socket_connect_timeout": 3.0,
# #                         "health_check_interval": 30,
# #                     }

# #                 driver_type = "CLUSTER" if cluster_mode else f"STANDALONE (DB: {connection_params.get('db')})"
# #                 print(f"🚀 [POOL MANAGER] Instantiating {driver_type} async driver targeting: {connection_params['host']}:{connection_params['port']}", flush=True)

# #                 new_client = redis_class(**connection_params)
# #                 await self.pools.put(tenant_id, new_client)
# #                 return new_client

# #             except Exception as e:
# #                 print(f"❌ [POOL MANAGER ERROR] Failed building multi-tenant driver setup for '{tenant_id}': {str(e)}", flush=True)
# #                 logger.error(f"Failed configuring multi-tenant driver setup for '{tenant_id}': {str(e)}")
# #                 raise

# #     async def get_client(self) -> Union[aioredis.Redis, RedisCluster]:
# #         tenant_id = current_tenant_id.get()
# #         if not tenant_id:
# #             raise RuntimeError("Context Violation: Cannot resolve a connection outside an active tenant request trace.")

# #         return await self.ensure_tenant_connection(tenant_id)

# #     async def close_idle_pools(self):
# #         await self.pools.evict_idle()

# #     async def shutdown_all_pools(self):
# #         print(f"🛑 [POOL MANAGER] Terminating dynamic tenant Redis pools...", flush=True)
# #         await self.pools.clear_all()
# #         await tenant_db_manager.shutdown_all_pools()


# # tenant_redis_manager = TenantRedisPoolManager()





import asyncio
import logging
from contextvars import ContextVar
from typing import Optional, Dict, Union, Type
import redis.asyncio as aioredis
from redis.asyncio.cluster import RedisCluster

from src.common.tenant_db import tenant_db_manager, DEFAULT_IDLE_TIMEOUT
from src.common.lru_cache import AsyncLRUCache
from src.version import __version__

logger = logging.getLogger("mcp.connection")

current_tenant_id: ContextVar[Optional[str]] = ContextVar("current_tenant_id", default=None)

async def _close_redis_client_callback(tenant_id: str, client: Union[aioredis.Redis, RedisCluster]):
    """Callback function executed when a Redis client/pool is evicted."""
    try:
        await client.aclose()
        print(f"   ↳ [REDIS DISCONNECT] Closed active Redis pool for tenant: '{tenant_id}'", flush=True)
    except Exception as e:
        logger.error(f"Error closing Redis pool during eviction for tenant '{tenant_id}': {e}")


class TenantRedisPoolManager:
    """Manages isolated Redis connection pools per tenant using AsyncLRUCache."""
    def __init__(self, max_pools: int = 20, idle_timeout: int = DEFAULT_IDLE_TIMEOUT):
        self.pools = AsyncLRUCache[Union[aioredis.Redis, RedisCluster]](
            max_size=max_pools,
            ttl_seconds=idle_timeout,
            on_evict=_close_redis_client_callback
        )
        self._creation_locks: Dict[str, asyncio.Lock] = {}
        self._lock = asyncio.Lock()

    async def ensure_tenant_connection(self, tenant_id: str) -> Union[aioredis.Redis, RedisCluster]:
        # Sync LRU access timestamp for DB pool as well to prevent unsynchronized eviction
        await tenant_db_manager.pools.get(tenant_id)

        client = await self.pools.get(tenant_id)
        if client:
            print(f"♻️  [POOL MANAGER] Reusing active ConnectionPool for Tenant: '{tenant_id}'", flush=True)
            return client

        async with self._lock:
            if tenant_id not in self._creation_locks:
                self._creation_locks[tenant_id] = asyncio.Lock()

        async with self._creation_locks[tenant_id]:
            client = await self.pools.get(tenant_id)
            if client:
                return client

            print(f"❄️  [POOL MANAGER] Cold starting isolated Redis infrastructure for tenant: '{tenant_id}'", flush=True)
            try:
                raw_config = await tenant_db_manager.get_redis_config_for_tenant(tenant_id)
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
                        "max_connections_per_node": 10,
                        "socket_timeout": 5.0,
                        "socket_connect_timeout": 3.0,
                    }
                else:
                    redis_class = aioredis.Redis
                    connection_params = {
                        "host": config_working["REDIS_HOST"],
                        "port": config_working["REDIS_PORT"],
                        "db": config_working.get("REDIS_DB", 0),
                        "username": config_working.get("REDIS_USERNAME"),
                        "password": config_working.get("REDIS_PWD"),
                        "ssl": config_working.get("REDIS_SSL", False),
                        "decode_responses": True,
                        "lib_name": f"redis-py(mcp-server_v{__version__})",
                        "max_connections": 10,
                        "socket_timeout": 5.0,
                        "socket_connect_timeout": 3.0,
                        "health_check_interval": 30,
                    }

                driver_type = "CLUSTER" if cluster_mode else f"STANDALONE (DB: {connection_params.get('db')})"
                print(f"🚀 [POOL MANAGER] Instantiating {driver_type} async driver targeting: {connection_params['host']}:{connection_params['port']}", flush=True)

                new_client = redis_class(**connection_params)
                # Verify network reachability before caching
                await new_client.ping()

                await self.pools.put(tenant_id, new_client)
                return new_client

            except Exception as e:
                print(f"❌ [POOL MANAGER ERROR] Failed building multi-tenant driver setup for '{tenant_id}': {str(e)}", flush=True)
                logger.error(f"Failed configuring multi-tenant driver setup for '{tenant_id}': {str(e)}")
                raise

    async def get_client(self) -> Union[aioredis.Redis, RedisCluster]:
        tenant_id = current_tenant_id.get()
        if not tenant_id:
            raise RuntimeError("Context Violation: Cannot resolve a connection outside an active tenant request trace.")

        return await self.ensure_tenant_connection(tenant_id)

    async def close_idle_pools(self):
        await self.pools.evict_idle()

    async def shutdown_all_pools(self):
        print(f"🛑 [POOL MANAGER] Terminating dynamic tenant Redis pools...", flush=True)
        await self.pools.clear_all()
        self._creation_locks.clear()
        await tenant_db_manager.shutdown_all_pools()


tenant_redis_manager = TenantRedisPoolManager(idle_timeout=DEFAULT_IDLE_TIMEOUT)




# import asyncio
# import logging
# from contextvars import ContextVar
# from typing import Optional, Dict, Union, Type
# import redis.asyncio as aioredis
# from redis.asyncio.cluster import RedisCluster

# from src.common.tenant_db import tenant_db_manager, DEFAULT_IDLE_TIMEOUT
# from src.common.lru_cache import AsyncLRUCache
# from src.version import __version__

# logger = logging.getLogger("mcp.connection")

# current_tenant_id: ContextVar[Optional[str]] = ContextVar("current_tenant_id", default=None)


# async def _close_redis_client_callback(tenant_id: str, client: Union[aioredis.Redis, RedisCluster]):
#     """Callback function executed when a Redis client/pool is evicted."""
#     try:
#         await client.aclose()
#         logger.info(f"Closed active Redis connection pool for evicted tenant: '{tenant_id}'")
#     except Exception as e:
#         logger.error(f"Error closing Redis connection pool during eviction for tenant '{tenant_id}': {e}")


# class TenantRedisPoolManager:
#     """Manages isolated Redis connection pools per tenant using AsyncLRUCache."""
#     def __init__(self, max_pools: int = 20, idle_timeout: int = DEFAULT_IDLE_TIMEOUT):
#         self.pools = AsyncLRUCache[Union[aioredis.Redis, RedisCluster]](
#             max_size=max_pools,
#             ttl_seconds=idle_timeout,
#             on_evict=_close_redis_client_callback
#         )
#         self._creation_locks: Dict[str, asyncio.Lock] = {}
#         self._lock = asyncio.Lock()

#     async def ensure_tenant_connection(self, tenant_id: str) -> Union[aioredis.Redis, RedisCluster]:
#         # Sync LRU access timestamp for DB pool as well to prevent unsynchronized eviction
#         await tenant_db_manager.pools.get(tenant_id)

#         client = await self.pools.get(tenant_id)
#         if client:
#             logger.debug(f"Reusing existing Redis connection pool for tenant: '{tenant_id}'")
#             return client

#         async with self._lock:
#             if tenant_id not in self._creation_locks:
#                 self._creation_locks[tenant_id] = asyncio.Lock()

#         async with self._creation_locks[tenant_id]:
#             client = await self.pools.get(tenant_id)
#             if client:
#                 return client

#             logger.info(f"Initializing Redis connection pool for tenant: '{tenant_id}'")
#             try:
#                 raw_config = await tenant_db_manager.get_redis_config_for_tenant(tenant_id)
#                 config_working = raw_config.copy()

#                 cluster_mode = config_working.pop("REDIS_CLUSTER_MODE", False)

#                 if cluster_mode:
#                     redis_class: Type[Union[aioredis.Redis, RedisCluster]] = RedisCluster
#                     connection_params = {
#                         "host": config_working["REDIS_HOST"],
#                         "port": config_working["REDIS_PORT"],
#                         "username": config_working.get("REDIS_USERNAME"),
#                         "password": config_working.get("REDIS_PWD"),
#                         "ssl": config_working.get("REDIS_SSL", False),
#                         "decode_responses": True,
#                         "lib_name": f"redis-py(mcp-server_v{__version__})",
#                         "max_connections_per_node": 10,
#                         "socket_timeout": 5.0,
#                         "socket_connect_timeout": 3.0,
#                     }
#                 else:
#                     redis_class = aioredis.Redis
#                     connection_params = {
#                         "host": config_working["REDIS_HOST"],
#                         "port": config_working["REDIS_PORT"],
#                         "db": config_working.get("REDIS_DB", 0),
#                         "username": config_working.get("REDIS_USERNAME"),
#                         "password": config_working.get("REDIS_PWD"),
#                         "ssl": config_working.get("REDIS_SSL", False),
#                         "decode_responses": True,
#                         "lib_name": f"redis-py(mcp-server_v{__version__})",
#                         "max_connections": 10,
#                         "socket_timeout": 5.0,
#                         "socket_connect_timeout": 3.0,
#                         "health_check_interval": 30,
#                     }

#                 driver_type = "CLUSTER" if cluster_mode else f"STANDALONE (DB: {connection_params.get('db')})"
#                 logger.info(
#                     f"Instantiating {driver_type} Redis client targeting "
#                     f"{connection_params['host']}:{connection_params['port']} for tenant: '{tenant_id}'"
#                 )

#                 new_client = redis_class(**connection_params)
#                 # Verify network reachability before caching
#                 await new_client.ping()

#                 await self.pools.put(tenant_id, new_client)
#                 return new_client

#             except Exception as e:
#                 logger.error(f"Failed to configure Redis connection pool for tenant '{tenant_id}': {e}", exc_info=True)
#                 raise

#     async def get_client(self) -> Union[aioredis.Redis, RedisCluster]:
#         tenant_id = current_tenant_id.get()
#         if not tenant_id:
#             raise RuntimeError("Context Violation: Cannot resolve a connection outside an active tenant request trace.")

#         return await self.ensure_tenant_connection(tenant_id)

#     async def close_idle_pools(self):
#         """Triggered by background task to reclaim idle pools."""
#         await self.pools.evict_idle()

#     async def shutdown_all_pools(self):
#         """Triggered on server shutdown to clear all connection pools."""
#         logger.info("Terminating all active tenant Redis connection pools")
#         await self.pools.clear_all()
#         self._creation_locks.clear()
#         await tenant_db_manager.shutdown_all_pools()


# tenant_redis_manager = TenantRedisPoolManager(idle_timeout=DEFAULT_IDLE_TIMEOUT)