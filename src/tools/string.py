from typing import Optional
from src.common.connection import RedisConnectionManager
from redis.exceptions import RedisError
from src.common.server import mcp
from redis.typing import EncodableT


@mcp.tool()
async def set(key: str, value: EncodableT, expiration: int = None, host_id: Optional[str] = None) -> str:
    """Set a Redis string value with an optional expiration time.

    Args:
        key (str): The key to set.
        value (str): The value to store.
        expiration (int, optional): Expiration time in seconds.
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.

    Returns:
        str: Confirmation message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)
        if expiration:
            r.setex(key, expiration, value)
        else:
            r.set(key, value)
        return f"Successfully set {key}" + (f" with expiration {expiration} seconds" if expiration else "")
    except RedisError as e:
        return f"Error setting key {key}: {str(e)}"


@mcp.tool()
async def get(key: str, host_id: Optional[str] = None) -> str:
    """Get a Redis string value.

    Args:
        key (str): The key to retrieve.
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.

    Returns:
        str: The stored value or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)
        value = r.get(key)
        return value if value else f"Key {key} does not exist"
    except RedisError as e:
        return f"Error retrieving key {key}: {str(e)}"

