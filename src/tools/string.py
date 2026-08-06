import json
from typing import Union, Optional

from redis.exceptions import RedisError
from redis import Redis

from src.common.connection import RedisConnectionManager
from src.common.server import mcp


@mcp.tool()
async def mset(mappings: dict) -> str:
    """Set multiple Redis string values in a single atomic operation.

    Args:
        mappings (dict): A dictionary of key-value pairs to set.

    Returns:
        str: Confirmation message or an error message.
    """
    if not mappings:
        return "Error: mappings cannot be empty"

    try:
        r: Redis = RedisConnectionManager.get_connection()
        normalized = {}
        for k, v in mappings.items():
            if isinstance(v, dict):
                normalized[k] = json.dumps(v)
            elif isinstance(v, bytes):
                try:
                    normalized[k] = v.decode("utf-8")
                except UnicodeDecodeError:
                    return f"Error setting keys: value for key '{k}' is not valid UTF-8"
            else:
                normalized[k] = str(v)
        r.mset(normalized)
        return f"Successfully set {len(mappings)} keys: {', '.join(mappings.keys())}"
    except RedisError as e:
        return f"Error setting keys: {str(e)}"


@mcp.tool()
async def mget(keys: list) -> dict:
    """Get multiple Redis string values in a single operation.

    Args:
        keys (list): List of keys to retrieve.

    Returns:
        dict: A dictionary mapping each key to its value (or None if the key does not exist).
    """
    if not keys:
        return {"error": "keys cannot be empty"}

    try:
        r: Redis = RedisConnectionManager.get_connection()
        values = r.mget(keys)
        result = {}
        for key, value in zip(keys, values):
            if value is None:
                result[key] = None
            elif isinstance(value, bytes):
                try:
                    result[key] = value.decode("utf-8")
                except UnicodeDecodeError:
                    result[key] = repr(value)
            else:
                result[key] = value
        return result
    except RedisError as e:
        return {"error": str(e)}


@mcp.tool()
async def set(
    key: str,
    value: Union[str, bytes, int, float, dict],
    expiration: Optional[int] = None,
) -> str:
    """Set a Redis string value with an optional expiration time.

    Args:
        key (str): The key to set.
        value (str, bytes, int, float, dict): The value to store.
        expiration (int, optional): Expiration time in seconds.

    Returns:
        str: Confirmation message or an error message.
    """
    if isinstance(value, bytes):
        encoded_value = value
    elif isinstance(value, dict):
        encoded_value = json.dumps(value)
    else:
        encoded_value = str(value)

    if isinstance(encoded_value, str):
        encoded_value = encoded_value.encode("utf-8")

    try:
        r: Redis = RedisConnectionManager.get_connection()
        if expiration:
            r.setex(key, expiration, encoded_value)
        else:
            r.set(key, encoded_value)

        return f"Successfully set {key}" + (
            f" with expiration {expiration} seconds" if expiration else ""
        )
    except RedisError as e:
        return f"Error setting key {key}: {str(e)}"


@mcp.tool()
async def get(key: str) -> Union[str, bytes]:
    """Get a Redis string value.

    Args:
        key (str): The key to retrieve.

    Returns:
        str, bytes: The stored value or an error message.
    """
    try:
        r: Redis = RedisConnectionManager.get_connection()
        value = r.get(key)

        if value is None:
            return f"Key {key} does not exist"

        if isinstance(value, bytes):
            try:
                text = value.decode("utf-8")
                return text
            except UnicodeDecodeError:
                return value

        return value
    except RedisError as e:
        return f"Error retrieving key {key}: {str(e)}"
