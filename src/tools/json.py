from redis.commands.json._util import JsonType
from redis.exceptions import RedisError

from src.common.connection import RedisConnectionManager
from src.common.server import mcp


@mcp.tool()
async def json_set(
    name: str, path: str, value: JsonType, expire_seconds: int = None
) -> str:
    """Set a JSON value in Redis at a given path with an optional expiration time.

    Args:
        name: The Redis key where the JSON document is stored.
        path: The JSON path where the value should be set.
        value: The JSON value to store.
        expire_seconds: Optional; time in seconds after which the key should expire.

    Returns:
        A success message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        r.json().set(name, path, value)

        if expire_seconds is not None:
            r.expire(name, expire_seconds)

        return f"JSON value set at path '{path}' in '{name}'." + (
            f" Expires in {expire_seconds} seconds." if expire_seconds else ""
        )
    except RedisError as e:
        return f"Error setting JSON value at path '{path}' in '{name}': {str(e)}"


@mcp.tool()
async def json_get(name: str, path: str = "$") -> str:
    """Retrieve a JSON value from Redis at a given path.

    Args:
        name: The Redis key where the JSON document is stored.
        path: The JSON path to retrieve (default: root '$').

    Returns:
        The retrieved JSON value or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        value = r.json().get(name, path)
        return (
            value
            if value is not None
            else f"No data found at path '{path}' in '{name}'."
        )
    except RedisError as e:
        return f"Error retrieving JSON value at path '{path}' in '{name}': {str(e)}"


@mcp.tool()
async def json_del(name: str, path: str = "$") -> str:
    """Delete a JSON value from Redis at a given path.

    Args:
        name: The Redis key where the JSON document is stored.
        path: The JSON path to delete (default: root '$').

    Returns:
        A success message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        deleted = r.json().delete(name, path)
        return (
            f"Deleted JSON value at path '{path}' in '{name}'."
            if deleted
            else f"No JSON value found at path '{path}' in '{name}'."
        )
    except RedisError as e:
        return f"Error deleting JSON value at path '{path}' in '{name}': {str(e)}"
