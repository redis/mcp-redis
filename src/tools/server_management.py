from redis.exceptions import RedisError

from src.common.annotations import READ_ONLY
from src.common.connection import RedisConnectionManager
from src.common.server import mcp


@mcp.tool(annotations=READ_ONLY)
async def dbsize() -> int:
    """Get the number of keys stored in the Redis database"""
    try:
        r = RedisConnectionManager.get_connection()
        return r.dbsize()
    except RedisError as e:
        return f"Error getting database size: {str(e)}"


@mcp.tool(annotations=READ_ONLY)
async def info(section: str = "default") -> dict:
    """Get Redis server information and statistics.

    Args:
        section: The section of the info command (default, memory, cpu, etc.).

    Returns:
        A dictionary of server information or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        info = r.info(section)
        return info
    except RedisError as e:
        return f"Error retrieving Redis info: {str(e)}"


@mcp.tool(annotations=READ_ONLY)
async def client_list() -> list:
    """Get a list of connected clients to the Redis server."""
    try:
        r = RedisConnectionManager.get_connection()
        clients = r.client_list()
        return clients
    except RedisError as e:
        return f"Error retrieving client list: {str(e)}"
