from typing import Union, List, Optional

from redis.exceptions import RedisError

from src.common.connection import RedisConnectionManager
from src.common.server import mcp


@mcp.tool()
async def sadd(name: str, value: str, expire_seconds: Optional[int] = None) -> str:
    """Add a value to a Redis set with an optional expiration time.

    Args:
        name: The Redis set key.
        value: The value to add to the set.
        expire_seconds: Optional; time in seconds after which the set should expire.

    Returns:
        A success message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        r.sadd(name, value)

        if expire_seconds is not None:
            r.expire(name, expire_seconds)

        return f"Value '{value}' added successfully to set '{name}'." + (
            f" Expires in {expire_seconds} seconds." if expire_seconds else ""
        )
    except RedisError as e:
        return f"Error adding value '{value}' to set '{name}': {str(e)}"


@mcp.tool()
async def srem(name: str, value: str) -> str:
    """Remove a value from a Redis set.

    Args:
        name: The Redis set key.
        value: The value to remove from the set.

    Returns:
        A success message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        removed = r.srem(name, value)
        return (
            f"Value '{value}' removed from set '{name}'."
            if removed
            else f"Value '{value}' not found in set '{name}'."
        )
    except RedisError as e:
        return f"Error removing value '{value}' from set '{name}': {str(e)}"


@mcp.tool()
async def smembers(name: str) -> Union[str, List[str]]:
    """Get all members of a Redis set.

    Args:
        name: The Redis set key.

    Returns:
        A list of values in the set or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        members = r.smembers(name)
        return list(members) if members else f"Set '{name}' is empty or does not exist."
    except RedisError as e:
        return f"Error retrieving members of set '{name}': {str(e)}"
