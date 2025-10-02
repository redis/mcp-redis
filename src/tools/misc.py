from typing import Any, Dict, Union, List

from redis.exceptions import RedisError

from src.common.connection import RedisConnectionManager
from src.common.server import mcp


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
        info = {"key": key, "type": key_type, "ttl": r.ttl(key)}

        return info
    except RedisError as e:
        return {"error": str(e)}


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
        return (
            f"Expiration set to {expire_seconds} seconds for '{name}'."
            if success
            else f"Key '{name}' does not exist."
        )
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
            "message": f"Renamed key '{old_key}' to '{new_key}'",
        }

    except RedisError as e:
        return {"error": str(e)}


@mcp.tool()
async def scan_keys(
    pattern: str = "*", count: int = 100, cursor: int = 0
) -> Union[str, Dict[str, Any]]:
    """
    Scan keys in the Redis database using the SCAN command (non-blocking, production-safe).

    ⚠️  IMPORTANT: This returns PARTIAL results from one iteration. Use scan_all_keys()
    to get ALL matching keys, or call this function multiple times with the returned cursor
    until cursor becomes 0.

    The SCAN command iterates through the keyspace in small chunks, making it safe to use
    on large databases without blocking other operations.

    Args:
        pattern: Pattern to match keys against (default is "*" for all keys).
                Common patterns: "user:*", "cache:*", "*:123", etc.
        count: Hint for the number of keys to return per iteration (default 100).
               Redis may return more or fewer keys than this hint.
        cursor: The cursor position to start scanning from (0 to start from beginning).
                To continue scanning, use the cursor value returned from previous call.

    Returns:
        A dictionary containing:
        - 'cursor': Next cursor position (0 means scan is complete)
        - 'keys': List of keys found in this iteration (PARTIAL RESULTS)
        - 'total_scanned': Number of keys returned in this batch
        - 'scan_complete': Boolean indicating if scan is finished
        Or an error message if something goes wrong.

    Example usage:
        First call: scan_keys("user:*") -> returns cursor=1234, keys=[...], scan_complete=False
        Next call: scan_keys("user:*", cursor=1234) -> continues from where it left off
        Final call: returns cursor=0, scan_complete=True when done
    """
    try:
        r = RedisConnectionManager.get_connection()
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=count)

        # Convert bytes to strings if needed
        decoded_keys = [
            key.decode("utf-8") if isinstance(key, bytes) else key for key in keys
        ]

        return {
            "cursor": cursor,
            "keys": decoded_keys,
            "total_scanned": len(decoded_keys),
            "scan_complete": cursor == 0,
        }
    except RedisError as e:
        return f"Error scanning keys with pattern '{pattern}': {str(e)}"


@mcp.tool()
async def scan_all_keys(
    pattern: str = "*", batch_size: int = 100
) -> Union[str, List[str]]:
    """
    Scan and return ALL keys matching a pattern using multiple SCAN iterations.

    This function automatically handles the SCAN cursor iteration to collect all matching keys.
    It's safer than KEYS * for large databases but will still collect all results in memory.

    ⚠️  WARNING: With very large datasets (millions of keys), this may consume significant memory.
    For large-scale operations, consider using scan_keys() with manual iteration instead.

    Args:
        pattern: Pattern to match keys against (default is "*" for all keys).
        batch_size: Number of keys to scan per iteration (default 100).

    Returns:
        A list of all keys matching the pattern or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        all_keys = []
        cursor = 0

        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=batch_size)

            # Convert bytes to strings if needed and add to results
            decoded_keys = [
                key.decode("utf-8") if isinstance(key, bytes) else key for key in keys
            ]
            all_keys.extend(decoded_keys)

            # Break when scan is complete (cursor returns to 0)
            if cursor == 0:
                break

        return all_keys
    except RedisError as e:
        return f"Error scanning all keys with pattern '{pattern}': {str(e)}"
