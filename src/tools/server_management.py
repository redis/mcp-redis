from typing import Optional
from src.common.connection import RedisConnectionManager
from redis.exceptions import RedisError
from src.common.server import mcp

@mcp.tool()
async def dbsize(host_id: Optional[str] = None) -> str | int:
    """Get the number of keys stored in the Redis database
    
    Args:
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.
    
    Returns:
        The number of keys or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)
        return r.dbsize()
    except RedisError as e:
        return f"Error getting database size: {str(e)}"


@mcp.tool()
async def info(section: str = "default", host_id: Optional[str] = None) -> dict:
    """Get Redis server information and statistics.

    Args:
        section: The section of the info command (default, memory, cpu, etc.).
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.

    Returns:
        A dictionary of server information or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)
        info = r.info(section)
        return info
    except RedisError as e:
        return f"Error retrieving Redis info: {str(e)}"


@mcp.tool()
async def client_list(host_id: Optional[str] = None) -> list:
    """Get a list of connected clients to the Redis server.
    
    Args:
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.
    
    Returns:
        A list of connected clients or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)
        clients = r.client_list()
        return clients
    except RedisError as e:
        return f"Error retrieving client list: {str(e)}"