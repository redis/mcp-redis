

import json
from typing import Union, List, Optional

from redis.exceptions import RedisError
from redis.typing import FieldT
import redis.asyncio as aioredis

from src.common.connection import current_tenant_id, tenant_redis_manager
from src.common.server import mcp


@mcp.tool()
async def lpush(name: str, value: FieldT, expire: Optional[int] = None) -> str:
    """Push a value onto the left of a Redis list and optionally set an expiration time."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        await r.lpush(name, value)
        if expire:
            await r.expire(name, expire)
        return f"Value '{value}' pushed to the left of list '{name}' for tenant {tenant_id}."
    except RedisError as e:
        return f"Error pushing value to list '{name}': {str(e)}"


@mcp.tool()
async def rpush(name: str, value: FieldT, expire: Optional[int] = None) -> str:
    """Push a value onto the right of a Redis list and optionally set an expiration time."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        await r.rpush(name, value)
        if expire:
            await r.expire(name, expire)
        return f"Value '{value}' pushed to the right of list '{name}' for tenant {tenant_id}."
    except RedisError as e:
        return f"Error pushing value to list '{name}': {str(e)}"


@mcp.tool()
async def lpop(name: str) -> str:
    """Remove and return the first element from a Redis list."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        value = await r.lpop(name)
        if value is None:
            return f"List '{name}' is empty or does not exist for tenant {tenant_id}."

        return value.decode("utf-8") if isinstance(value, bytes) else str(value)
    except RedisError as e:
        return f"Error popping value from list '{name}': {str(e)}"


@mcp.tool()
async def rpop(name: str) -> str:
    """Remove and return the last element from a Redis list."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        value = await r.rpop(name)
        if value is None:
            return f"List '{name}' is empty or does not exist for tenant {tenant_id}."

        return value.decode("utf-8") if isinstance(value, bytes) else str(value)
    except RedisError as e:
        return f"Error popping value from list '{name}': {str(e)}"


@mcp.tool()
async def lrange(name: str, start: int, stop: int) -> Union[str, List[str]]:
    """Get elements from a Redis list within a specific range."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        values = await r.lrange(name, start, stop)
        if not values:
            return f"List '{name}' is empty or does not exist for tenant {tenant_id}."

        decoded_values = [
            v.decode("utf-8") if isinstance(v, bytes) else str(v) for v in values
        ]
        return json.dumps(decoded_values)
    except RedisError as e:
        return f"Error retrieving values from list '{name}': {str(e)}"


@mcp.tool()
async def llen(name: str) -> Union[int, str]:
    """Get the length of a Redis list."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        return await r.llen(name)
    except RedisError as e:
        return f"Error retrieving length of list '{name}': {str(e)}"


@mcp.tool()
async def lrem(name: str, count: int, element: FieldT) -> str:
    """Remove elements from a Redis list."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        removed_count = await r.lrem(name, count, element)

        if removed_count == 0:
            return f"Element '{element}' not found in list '{name}' or list does not exist for tenant {tenant_id}."
        else:
            return f"Removed {removed_count} occurrence(s) of '{element}' from list '{name}' for tenant {tenant_id}."

    except RedisError as e:
        return f"Error removing element from list '{name}': {str(e)}"