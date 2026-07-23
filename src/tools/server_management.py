

from typing import Union
from redis.exceptions import RedisError
import redis.asyncio as aioredis

from src.common.connection import current_tenant_id, tenant_redis_manager
from src.common.server import mcp


@mcp.tool()
async def dbsize() -> Union[int, str]:
    """Get the number of keys stored in the Redis database for the active tenant."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        size = await r.dbsize()
        return size
    except RedisError as e:
        return f"Error getting database size: {str(e)}"


@mcp.tool()
async def info(section: str = "default") -> Union[dict, str]:
    """Get Redis server information and statistics.

    Args:
        section: The section of the info command (default, memory, cpu, etc.).

    Returns:
        A dictionary of server information or an error message.
    """
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        info_data = await r.info(section)
        return info_data
    except RedisError as e:
        return f"Error retrieving Redis info: {str(e)}"


@mcp.tool()
async def client_list() -> Union[list, str]:
    """Get a list of connected clients to the Redis server for the current context."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        clients = await r.client_list()
        return clients
    except RedisError as e:
        return f"Error retrieving client list: {str(e)}"