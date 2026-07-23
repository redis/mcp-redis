

from typing import List, Union, Optional
import numpy as np
from redis.exceptions import RedisError
import redis.asyncio as aioredis

from src.common.connection import current_tenant_id, tenant_redis_manager
from src.common.server import mcp


@mcp.tool()
async def hset(
    name: str, key: str, value: str | int | float, expire_seconds: Optional[int] = None
) -> str:
    """Set a field in a hash stored at key with an optional expiration time."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        await r.hset(name, key, str(value))

        if expire_seconds is not None:
            await r.expire(name, expire_seconds)

        return f"Field '{key}' set successfully in hash '{name}' for tenant {tenant_id}." + (
            f" Expires in {expire_seconds} seconds." if expire_seconds else ""
        )
    except RedisError as e:
        return f"Error setting field '{key}' in hash '{name}': {str(e)}"


@mcp.tool()
async def hget(name: str, key: str) -> str:
    """Get the value of a field in a Redis hash."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        value = await r.hget(name, key)
        if value is None:
            return f"Field '{key}' not found in hash '{name}' for tenant {tenant_id}."

        return value.decode("utf-8") if isinstance(value, bytes) else str(value)
    except RedisError as e:
        return f"Error getting field '{key}' from hash '{name}': {str(e)}"


@mcp.tool()
async def hdel(name: str, key: str) -> str:
    """Delete a field from a Redis hash."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        deleted = await r.hdel(name, key)
        return (
            f"Field '{key}' deleted from hash '{name}' for tenant {tenant_id}."
            if deleted
            else f"Field '{key}' not found in hash '{name}' for tenant {tenant_id}."
        )
    except RedisError as e:
        return f"Error deleting field '{key}' from hash '{name}': {str(e)}"


@mcp.tool()
async def hgetall(name: str) -> Union[dict, str]:
    """Get all fields and values from a Redis hash."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        hash_data = await r.hgetall(name)
        if not hash_data:
            return f"Hash '{name}' is empty or does not exist for tenant {tenant_id}."

        # Ensure keys and values are decoded strings
        return {
            (k.decode("utf-8") if isinstance(k, bytes) else k): (
                v.decode("utf-8") if isinstance(v, bytes) else v
            )
            for k, v in hash_data.items()
        }
    except RedisError as e:
        return f"Error getting all fields from hash '{name}': {str(e)}"


@mcp.tool()
async def hexists(name: str, key: str) -> Union[bool, str]:
    """Check if a field exists in a Redis hash."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        return bool(await r.hexists(name, key))
    except RedisError as e:
        return f"Error checking existence of field '{key}' in hash '{name}': {str(e)}"


@mcp.tool()
async def set_vector_in_hash(
    name: str, vector: List[float], vector_field: str = "vector"
) -> Union[bool, str]:
    """Store a vector as a field in a Redis hash."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        vector_array = np.array(vector, dtype=np.float32)
        binary_blob = vector_array.tobytes()

        await r.hset(name, vector_field, binary_blob)
        return True
    except RedisError as e:
        return f"Error storing vector in hash '{name}' with field '{vector_field}': {str(e)}"


@mcp.tool()
async def get_vector_from_hash(name: str, vector_field: str = "vector") -> Union[List[float], str]:
    """Retrieve a vector from a Redis hash and convert it back from binary blob."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        binary_blob = await r.hget(name, vector_field)

        if binary_blob:
            vector_array = np.frombuffer(binary_blob, dtype=np.float32)
            return vector_array.tolist()
        else:
            return f"Field '{vector_field}' not found in hash '{name}' for tenant {tenant_id}."

    except RedisError as e:
        return f"Error retrieving vector field '{vector_field}' from hash '{name}': {str(e)}"