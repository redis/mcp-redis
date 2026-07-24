


import json
from typing import Optional
from redis.exceptions import RedisError
import redis.asyncio as aioredis

from src.common.connection import current_tenant_id, tenant_redis_manager
from src.common.server import mcp


@mcp.tool()
async def json_set(
    name: str,
    path: str,
    value: str,
    expire_seconds: Optional[int] = None,
) -> str:
    """Set a JSON value in Redis at a given path with an optional expiration time."""
    try:
        parsed_value = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        parsed_value = value

    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        await r.json().set(name, path, parsed_value)

        if expire_seconds is not None:
            await r.expire(name, expire_seconds)

        return f"JSON value set at path '{path}' in '{name}' for tenant {tenant_id}." + (
            f" Expires in {expire_seconds} seconds." if expire_seconds else ""
        )
    except RedisError as e:
        return f"Error setting JSON value at path '{path}' in '{name}': {str(e)}"


@mcp.tool()
async def json_get(name: str, path: str = "$") -> str:
    """Retrieve a JSON value from Redis at a given path."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        value = await r.json().get(name, path)
        if value is not None:
            return json.dumps(value, ensure_ascii=False, indent=2)
        else:
            return f"No data found at path '{path}' in '{name}' for tenant {tenant_id}."
    except RedisError as e:
        return f"Error retrieving JSON value at path '{path}' in '{name}': {str(e)}"


@mcp.tool()
async def json_del(name: str, path: str = "$") -> str:
    """Delete a JSON value from Redis at a given path."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        deleted = await r.json().delete(name, path)
        return (
            f"Deleted JSON value at path '{path}' in '{name}' for tenant {tenant_id}."
            if deleted
            else f"No JSON value found at path '{path}' in '{name}' for tenant {tenant_id}."
        )
    except RedisError as e:
        return f"Error deleting JSON value at path '{path}' in '{name}': {str(e)}"