import json
from typing import Optional
from redis.exceptions import RedisError
from pydantic_core import core_schema

from src.common.connection import RedisConnectionManager
from src.common.server import mcp


# Custom type that accepts any JSON value but generates a proper schema
class JsonValue:
    """Accepts any JSON-serializable value."""

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type, _handler):
        """Define how Pydantic should validate this type."""
        # Accept any value
        return core_schema.any_schema()

    @classmethod
    def __get_pydantic_json_schema__(cls, _core_schema, _handler):
        """Define the JSON schema for this type."""
        # Return a schema that accepts string, number, boolean, object, array, or null
        return {
            "anyOf": [
                {"type": "string"},
                {"type": "number"},
                {"type": "boolean"},
                {"type": "object"},
                {"type": "array", "items": {"type": "string"}},
                {"type": "null"},
            ]
        }


@mcp.tool()
async def json_set(
    name: str,
    path: str,
    value: JsonValue,
    expire_seconds: Optional[int] = None,
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
        if value is not None:
            # Convert the value to JSON string for consistent return type
            return json.dumps(value, ensure_ascii=False, indent=2)
        else:
            return f"No data found at path '{path}' in '{name}'."
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
