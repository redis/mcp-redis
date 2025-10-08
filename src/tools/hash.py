from typing import Optional
from src.common.connection import RedisConnectionManager
from redis.exceptions import RedisError
from src.common.server import mcp
import numpy as np


@mcp.tool()
async def hset(name: str, key: str, value: str | int | float, expire_seconds: int = None, host_id: Optional[str] = None) -> str:
    """Set a field in a hash stored at key with an optional expiration time.

    Args:
        name: The Redis hash key.
        key: The field name inside the hash.
        value: The value to set.
        expire_seconds: Optional; time in seconds after which the key should expire.
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.

    Returns:
        A success message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)
        r.hset(name, key, str(value))

        if expire_seconds is not None:
            r.expire(name, expire_seconds)

        return f"Field '{key}' set successfully in hash '{name}'." + (
            f" Expires in {expire_seconds} seconds." if expire_seconds else "")
    except RedisError as e:
        return f"Error setting field '{key}' in hash '{name}': {str(e)}"

@mcp.tool()
async def hget(name: str, key: str, host_id: Optional[str] = None) -> str:
    """Get the value of a field in a Redis hash.

    Args:
        name: The Redis hash key.
        key: The field name inside the hash.
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.

    Returns:
        The field value or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)
        value = r.hget(name, key)
        return value if value else f"Field '{key}' not found in hash '{name}'."
    except RedisError as e:
        return f"Error getting field '{key}' from hash '{name}': {str(e)}"

@mcp.tool()
async def hdel(name: str, key: str, host_id: Optional[str] = None) -> str:
    """Delete a field from a Redis hash.

    Args:
        name: The Redis hash key.
        key: The field name inside the hash.
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.

    Returns:
        A success message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)
        deleted = r.hdel(name, key)
        return f"Field '{key}' deleted from hash '{name}'." if deleted else f"Field '{key}' not found in hash '{name}'."
    except RedisError as e:
        return f"Error deleting field '{key}' from hash '{name}': {str(e)}"

@mcp.tool()
async def hgetall(name: str, host_id: Optional[str] = None) -> dict:
    """Get all fields and values from a Redis hash.

    Args:
        name: The Redis hash key.
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.

    Returns:
        A dictionary of field-value pairs or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)
        hash_data = r.hgetall(name)
        return {k: v for k, v in hash_data.items()} if hash_data else f"Hash '{name}' is empty or does not exist."
    except RedisError as e:
        return f"Error getting all fields from hash '{name}': {str(e)}"

@mcp.tool()
async def hexists(name: str, key: str, host_id: Optional[str] = None) -> bool:
    """Check if a field exists in a Redis hash.

    Args:
        name: The Redis hash key.
        key: The field name inside the hash.
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.

    Returns:
        True if the field exists, False otherwise.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)
        return r.hexists(name, key)
    except RedisError as e:
        return f"Error checking existence of field '{key}' in hash '{name}': {str(e)}"

@mcp.tool()
async def set_vector_in_hash(name: str, vector: list, vector_field: str = "vector", host_id: Optional[str] = None) -> bool:
    """Store a vector as a field in a Redis hash.

    Args:
        name: The Redis hash key.
        vector_field: The field name inside the hash. Unless specifically required, use the default field name
        vector: The vector (list of numbers) to store in the hash.
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.

    Returns:
        True if the vector was successfully stored, False otherwise.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id)

        # Convert the vector to a NumPy array, then to a binary blob using np.float32
        vector_array = np.array(vector, dtype=np.float32)
        binary_blob = vector_array.tobytes()

        r.hset(name, vector_field, binary_blob)
        return True
    except RedisError as e:
        return f"Error storing vector in hash '{name}' with field '{vector_field}': {str(e)}"


@mcp.tool()
async def get_vector_from_hash(name: str, vector_field: str = "vector", host_id: Optional[str] = None):
    """Retrieve a vector from a Redis hash and convert it back from binary blob.

    Args:
        name: The Redis hash key.
        vector_field: The field name inside the hash. Unless specifically required, use the default field name
        host_id (str, optional): Redis host identifier. If not provided, uses the default connection.

    Returns:
        The vector as a list of floats, or an error message if retrieval fails.
    """
    try:
        r = RedisConnectionManager.get_connection(host_id, decode_responses=False)

        # Retrieve the binary blob stored in the hash
        binary_blob = r.hget(name, vector_field)

        if binary_blob:
            # Convert the binary blob back to a NumPy array (assuming it's stored as float32)
            vector_array = np.frombuffer(binary_blob, dtype=np.float32)
            return vector_array.tolist()
        else:
            return f"Field '{vector_field}' not found in hash '{name}'."

    except RedisError as e:
        return f"Error retrieving vector field '{vector_field}' from hash '{name}': {str(e)}"
