import json
from typing import Union, Optional

from redis.exceptions import RedisError
from redis import Redis

from src.common.connection import RedisConnectionManager
from src.common.server import mcp


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
