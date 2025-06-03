from typing import Dict, Any
from common.connection import RedisConnectionManager
from redis.exceptions import RedisError
from common.server import mcp


@mcp.tool()
async def delete(key: str) -> str:
    """Delete a Redis key.

    Args:
        key (str): The key to delete.

    Returns:
        str: Confirmation message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        result = r.delete(key)
        return f"Successfully deleted {key}" if result else f"Key {key} not found"
    except RedisError as e:
        return f"Error deleting key {key}: {str(e)}"


@mcp.tool()  
async def type(key: str) -> Dict[str, Any]:
    """Returns the string representation of the type of the value stored at key

    Args:
        key (str): The key to check.

    Returns:
        str: The type of key, or none when key doesn't exist
    """
    try:
        r = RedisConnectionManager.get_connection()
        key_type = r.type(key)
        info = {
            'key': key,
            'type': key_type,
            'ttl': r.ttl(key)
        }
        
        return info
    except RedisError as e:
        return {'error': str(e)}


@mcp.tool()
async def expire(name: str, expire_seconds: int) -> str:
    """Set an expiration time for a Redis key.

    Args:
        name: The Redis key.
        expire_seconds: Time in seconds after which the key should expire.

    Returns:
        A success message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        success = r.expire(name, expire_seconds)
        return f"Expiration set to {expire_seconds} seconds for '{name}'." if success else f"Key '{name}' does not exist."
    except RedisError as e:
        return f"Error setting expiration for key '{name}': {str(e)}"


@mcp.tool()
async def rename(old_key: str, new_key: str) -> Dict[str, Any]:
    """
    Renames a Redis key from old_key to new_key.

    Args:
        old_key (str): The current name of the Redis key to rename.
        new_key (str): The new name to assign to the key.

    Returns:
        Dict[str, Any]: A dictionary containing the result of the operation.
            On success: {"status": "success", "message": "..."}
            On error: {"error": "..."}
    """
    try:
        r = RedisConnectionManager.get_connection()

        # Check if the old key exists
        if not r.exists(old_key):
            return {"error": f"Key '{old_key}' does not exist."}

        # Rename the key
        r.rename(old_key, new_key)
        return {
            "status": "success",
            "message": f"Renamed key '{old_key}' to '{new_key}'"
        }

    except RedisError as e:
        return {"error": str(e)}


@mcp.tool()
async def scan(pattern: str = "*", count: int = 10, cursor: int = 0) -> dict:
    """
    使用SCAN命令进行Redis key的模式匹配查询。

    Args:
        pattern (str): key的匹配模式，默认为"*"
        count (int): 每次扫描返回的最大key数，默认为10
        cursor (int): 游标，首次查询传0，后续用上次返回的cursor

    Returns:
        dict: {"keys": 匹配到的key列表, "next_cursor": 下一个游标, "finished": 是否扫描结束}
    """
    try:
        r = RedisConnectionManager.get_connection()
        next_cursor, keys = r.scan(cursor=cursor, match=pattern, count=count)
        return {
            "keys": keys,
            "next_cursor": next_cursor,
            "finished": next_cursor == 0
        }
    except RedisError as e:
        return {"error": str(e)}