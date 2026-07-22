# # # import json
# # # from typing import Union, Optional

# # # from redis.exceptions import RedisError
# # # from redis import Redis

# # # from src.common.connection import RedisConnectionManager
# # # from src.common.server import mcp


# # # @mcp.tool()
# # # async def set(
# # #     key: str,
# # #     value: Union[str, bytes, int, float, dict],
# # #     expiration: Optional[int] = None,
# # # ) -> str:
# # #     """Set a Redis string value with an optional expiration time.

# # #     Args:
# # #         key (str): The key to set.
# # #         value (str, bytes, int, float, dict): The value to store.
# # #         expiration (int, optional): Expiration time in seconds.

# # #     Returns:
# # #         str: Confirmation message or an error message.
# # #     """
# # #     if isinstance(value, bytes):
# # #         encoded_value = value
# # #     elif isinstance(value, dict):
# # #         encoded_value = json.dumps(value)
# # #     else:
# # #         encoded_value = str(value)

# # #     if isinstance(encoded_value, str):
# # #         encoded_value = encoded_value.encode("utf-8")

# # #     try:
# # #         r: Redis = RedisConnectionManager.get_connection()
# # #         if expiration:
# # #             r.setex(key, expiration, encoded_value)
# # #         else:
# # #             r.set(key, encoded_value)

# # #         return f"Successfully set {key}" + (
# # #             f" with expiration {expiration} seconds" if expiration else ""
# # #         )
# # #     except RedisError as e:
# # #         return f"Error setting key {key}: {str(e)}"


# # # @mcp.tool()
# # # async def get(key: str) -> Union[str, bytes]:
# # #     """Get a Redis string value.

# # #     Args:
# # #         key (str): The key to retrieve.

# # #     Returns:
# # #         str, bytes: The stored value or an error message.
# # #     """
# # #     try:
# # #         r: Redis = RedisConnectionManager.get_connection()
# # #         value = r.get(key)

# # #         if value is None:
# # #             return f"Key {key} does not exist"

# # #         if isinstance(value, bytes):
# # #             try:
# # #                 text = value.decode("utf-8")
# # #                 return text
# # #             except UnicodeDecodeError:
# # #                 return value

# # #         return value
# # #     except RedisError as e:
# # #         return f"Error retrieving key {key}: {str(e)}"




# # # src/tools/redis_tools.py
# # import json
# # from typing import Union, Optional
# # from redis.exceptions import RedisError
# # import redis.asyncio as aioredis

# # from src.common.connection import current_tenant_id, redis_manager
# # from src.common.server import mcp


# # @mcp.tool()
# # async def set(
# #     key: str,
# #     value: Union[str, bytes, int, float, dict],
# #     expiration: Optional[int] = None,
# # ) -> str:
# #     """Set a Redis string value with an optional expiration time.

# #     Args:
# #         key (str): The key to set.
# #         value (str, bytes, int, float, dict): The value to store.
# #         expiration (int, optional): Expiration time in seconds.

# #     Returns:
# #         str: Confirmation message or an error message.
# #     """
# #     if isinstance(value, bytes):
# #         encoded_value = value
# #     elif isinstance(value, dict):
# #         encoded_value = json.dumps(value)
# #     else:
# #         encoded_value = str(value)

# #     if isinstance(encoded_value, str):
# #         encoded_value = encoded_value.encode("utf-8")

# #     try:
# #         # 1. Fetch the active tenant ID from context
# #         tenant_id = current_tenant_id.get()
        
# #         # 2. Get the isolated async Redis pool instance allocated for this tenant
# #         r: aioredis.Redis = await redis_manager.get_pool_for_tenant(tenant_id)
        
# #         # 3. Target the client pool asynchronously
# #         if expiration:
# #             await r.setex(key, expiration, encoded_value)
# #         else:
# #             await r.set(key, encoded_value)

# #         db_index = r.connection_pool.connection_kwargs.get('db', 0)
# #         return f"Successfully set {key} for tenant {tenant_id} (DB {db_index})" + (
# #             f" with expiration {expiration} seconds" if expiration else ""
# #         )
# #     except RedisError as e:
# #         return f"Error setting key {key}: {str(e)}"


# # @mcp.tool()
# # async def get(key: str) -> str:
# #     """Get a Redis string value.

# #     Args:
# #         key (str): The key to retrieve.

# #     Returns:
# #         str: The stored value or an error message.
# #     """
# #     try:
# #         # 1. Fetch the active tenant ID from context
# #         tenant_id = current_tenant_id.get()
        
# #         # 2. Get the isolated async Redis pool instance allocated for this tenant
# #         r: aioredis.Redis = await redis_manager.get_pool_for_tenant(tenant_id)
        
# #         # 3. Await the async read operation
# #         value = await r.get(key)

# #         if value is None:
# #             return f"Key '{key}' does not exist for tenant {tenant_id}"

# #         # If decode_responses=True is used in the manager, value will already be a string,
# #         # but handling bytes here guarantees compatibility.
# #         if isinstance(value, bytes):
# #             try:
# #                 return value.decode("utf-8")
# #             except UnicodeDecodeError:
# #                 return str(value)

# #         return str(value)
# #     except RedisError as e:
# #         return f"Error retrieving key {key}: {str(e)}"





# # src/tools/redis_tools.py
# import json
# from typing import Union, Optional
# from redis.exceptions import RedisError
# import redis.asyncio as aioredis

# from src.common.connection import current_tenant_id, redis_manager
# from src.common.server import mcp


# @mcp.tool()
# async def set(
#     key: str,
#     value: Union[str, bytes, int, float, dict],
#     expiration: Optional[int] = None,
# ) -> str:
#     """Set a Redis string value with an optional expiration time.

#     Args:
#         key (str): The key to set.
#         value (str, bytes, int, float, dict): The value to store.
#         expiration (int, optional): Expiration time in seconds.

#     Returns:
#         str: Confirmation message or an error message.
#     """
#     if isinstance(value, bytes):
#         encoded_value = value
#     elif isinstance(value, dict):
#         encoded_value = json.dumps(value)
#     else:
#         encoded_value = str(value)

#     if isinstance(encoded_value, str):
#         encoded_value = encoded_value.encode("utf-8")

#     try:
#         # 1. Safely fetch active tenant ID from context
#         try:
#             tenant_id = current_tenant_id.get()
#         except LookupError:
#             return "Error: No active tenant context detected for this tool execution."
        
#         # 2. Get the isolated async Redis pool instance allocated for this tenant
#         r: aioredis.Redis = await redis_manager.get_pool_for_tenant(tenant_id)
        
#         # 3. Target the client pool asynchronously
#         if expiration:
#             await r.setex(key, expiration, encoded_value)
#         else:
#             await r.set(key, encoded_value)

#         # Architectural check: safely read connection pool kwargs fallback
#         pool = getattr(r, "connection_pool", None)
#         kwargs = getattr(pool, "connection_kwargs", {}) if pool else {}
#         db_index = kwargs.get('db', 0)
        
#         return f"Successfully set {key} for tenant {tenant_id} (DB {db_index})" + (
#             f" with expiration {expiration} seconds" if expiration else ""
#         )
#     except RedisError as e:
#         return f"Error setting key {key}: {str(e)}"


# @mcp.tool()
# async def get(key: str) -> str:
#     """Get a Redis string value.

#     Args:
#         key (str): The key to retrieve.

#     Returns:
#         str: The stored value or an error message.
#     """
#     try:
#         # 1. Safely fetch active tenant ID from context
#         try:
#             tenant_id = current_tenant_id.get()
#         except LookupError:
#             return "Error: No active tenant context detected for this tool execution."
        
#         # 2. Get the isolated async Redis pool instance allocated for this tenant
#         r: aioredis.Redis = await redis_manager.get_pool_for_tenant(tenant_id)
        
#         # 3. Await the async read operation
#         value = await r.get(key)

#         if value is None:
#             return f"Key '{key}' does not exist for tenant {tenant_id}"

#         if isinstance(value, bytes):
#             try:
#                 return value.decode("utf-8")
#             except UnicodeDecodeError:
#                 return str(value)

#         return str(value)
#     except RedisError as e:
#         return f"Error retrieving key {key}: {str(e)}"






# src/tools/redis_tools.py
import json
from typing import Union, Optional
from redis.exceptions import RedisError
import redis.asyncio as aioredis

# CHANGED: Import the actual new variable 'tenant_redis_manager'
from src.common.connection import current_tenant_id, tenant_redis_manager
from src.common.server import mcp


@mcp.tool()
async def set(
    key: str,
    value: Union[str, bytes, int, float, dict],
    expiration: Optional[int] = None,
) -> str:
    """Set a Redis string value with an optional expiration time.

    Args:
        key (str): The key to set.
        value (str, bytes, int, float, dict): The value to store.
        expiration (int, optional): Expiration time in seconds.

    Returns:
        str: Confirmation message or an error message.
    """
    if isinstance(value, bytes):
        encoded_value = value
    elif isinstance(value, dict):
        encoded_value = json.dumps(value)
    else:
        encoded_value = str(value)

    if isinstance(encoded_value, str):
        encoded_value = encoded_value.encode("utf-8")

    try:
        # 1. Safely fetch active tenant ID from context (kept for logging string below)
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."
        
        # 2. CHANGED: Use native get_client() mapped to tenant_redis_manager
        r: aioredis.Redis = await tenant_redis_manager.get_client()
        
        # 3. Target the client pool asynchronously
        if expiration:
            await r.setex(key, expiration, encoded_value)
        else:
            await r.set(key, encoded_value)

        # Architectural check: safely read connection pool kwargs fallback
        pool = getattr(r, "connection_pool", None)
        kwargs = getattr(pool, "connection_kwargs", {}) if pool else {}
        db_index = kwargs.get('db', 0)
        
        return f"Successfully set {key} for tenant {tenant_id} (DB {db_index})" + (
            f" with expiration {expiration} seconds" if expiration else ""
        )
    except RedisError as e:
        return f"Error setting key {key}: {str(e)}"


@mcp.tool()
async def get(key: str) -> str:
    """Get a Redis string value.

    Args:
        key (str): The key to retrieve.

    Returns:
        str: The stored value or an error message.
    """
    try:
        # 1. Safely fetch active tenant ID from context (kept for logging string below)
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."
        
        # 2. CHANGED: Use native get_client() mapped to tenant_redis_manager
        r: aioredis.Redis = await tenant_redis_manager.get_client()
        
        # 3. Await the async read operation
        value = await r.get(key)

        if value is None:
            return f"Key '{key}' does not exist for tenant {tenant_id}"

        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError:
                return str(value)

        return str(value)
    except RedisError as e:
        return f"Error retrieving key {key}: {str(e)}"