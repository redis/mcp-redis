import json
from typing import Optional
from src.common.connection import RedisConnectionManager
from redis.exceptions import RedisError
from src.common.server import mcp
from redis.typing import FieldT

@mcp.tool()
async def lpush(name: str, value: FieldT, expire: int = None, host_id: Optional[str] = None) -> str:
    """Push a value onto the left of a Redis list and optionally set an expiration time.
    
    Args:
        name: The Redis list key.
        value: The value to push.
        expire: Optional expiration time in seconds.
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.
    
    Returns:
        A success message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)
        r.lpush(name, value)
        if expire:
            r.expire(name, expire)
        return f"Value '{value}' pushed to the left of list '{name}'."
    except RedisError as e:
        return f"Error pushing value to list '{name}': {str(e)}"

@mcp.tool()
async def rpush(name: str, value: FieldT, expire: int = None, host_id: Optional[str] = None) -> str:
    """Push a value onto the right of a Redis list and optionally set an expiration time.
    
    Args:
        name: The Redis list key.
        value: The value to push.
        expire: Optional expiration time in seconds.
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.
    
    Returns:
        A success message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)
        r.rpush(name, value)
        if expire:
            r.expire(name, expire)
        return f"Value '{value}' pushed to the right of list '{name}'."
    except RedisError as e:
        return f"Error pushing value to list '{name}': {str(e)}"

@mcp.tool()
async def lpop(name: str, host_id: Optional[str] = None) -> str:
    """Remove and return the first element from a Redis list.
    
    Args:
        name: The Redis list key.
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.
    
    Returns:
        The popped value or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)
        value = r.lpop(name)
        return value if value else f"List '{name}' is empty or does not exist."
    except RedisError as e:
        return f"Error popping value from list '{name}': {str(e)}"

@mcp.tool()
async def rpop(name: str, host_id: Optional[str] = None) -> str:
    """Remove and return the last element from a Redis list.
    
    Args:
        name: The Redis list key.
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.
    
    Returns:
        The popped value or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)
        value = r.rpop(name)
        return value if value else f"List '{name}' is empty or does not exist."
    except RedisError as e:
        return f"Error popping value from list '{name}': {str(e)}"

@mcp.tool()
async def lrange(name: str, start: int, stop: int, host_id: Optional[str] = None) -> list:
    """Get elements from a Redis list within a specific range.

    Args:
        name: The Redis list key.
        start: Starting index.
        stop: Ending index.
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.

    Returns:
        A JSON string containing the list of elements or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)
        values = r.lrange(name, start, stop)
        if not values:
            return f"List '{name}' is empty or does not exist."
        else:
            return json.dumps(values)
    except RedisError as e:
        return f"Error retrieving values from list '{name}': {str(e)}"

@mcp.tool()
async def llen(name: str, host_id: Optional[str] = None) -> int:
    """Get the length of a Redis list.
    
    Args:
        name: The Redis list key.
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.
    
    Returns:
        The length of the list or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)
        return r.llen(name)
    except RedisError as e:
        return f"Error retrieving length of list '{name}': {str(e)}"
