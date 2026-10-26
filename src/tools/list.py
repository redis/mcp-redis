import json
from typing import Union, List, Optional

from redis.exceptions import RedisError
from redis.typing import FieldT

from src.common.connection import RedisConnectionManager
from src.common.server import mcp


@mcp.tool()
async def lpush(
    name: str, value: Union[FieldT, List[FieldT]], expire: Optional[int] = None
) -> str:
    """Push one or more values onto the left of a Redis list and optionally set an expiration time.

    Args:
        name: The name of the list
        value: A single value, or a list of values pushed in one LPUSH call.
            Values are inserted one after another at the head, so [a, b, c]
            results in the list starting with c, b, a.
        expire: Optional expiration time in seconds

    Returns:
        A string confirming the push, including the new list length for multiple values.
    """
    values = value if isinstance(value, list) else [value]
    if not values:
        return f"Error pushing value to list '{name}': no values provided."
    try:
        r = RedisConnectionManager.get_connection()
        length = r.lpush(name, *values)
        if expire:
            r.expire(name, expire)
        if isinstance(value, list):
            return f"Pushed {len(values)} value(s) to the left of list '{name}'. List length is now {length}."
        return f"Value '{value}' pushed to the left of list '{name}'."
    except RedisError as e:
        return f"Error pushing value to list '{name}': {str(e)}"


@mcp.tool()
async def rpush(
    name: str, value: Union[FieldT, List[FieldT]], expire: Optional[int] = None
) -> str:
    """Push one or more values onto the right of a Redis list and optionally set an expiration time.

    Args:
        name: The name of the list
        value: A single value, or a list of values pushed in one RPUSH call
        expire: Optional expiration time in seconds

    Returns:
        A string confirming the push, including the new list length for multiple values.
    """
    values = value if isinstance(value, list) else [value]
    if not values:
        return f"Error pushing value to list '{name}': no values provided."
    try:
        r = RedisConnectionManager.get_connection()
        length = r.rpush(name, *values)
        if expire:
            r.expire(name, expire)
        if isinstance(value, list):
            return f"Pushed {len(values)} value(s) to the right of list '{name}'. List length is now {length}."
        return f"Value '{value}' pushed to the right of list '{name}'."
    except RedisError as e:
        return f"Error pushing value to list '{name}': {str(e)}"


@mcp.tool()
async def lpop(name: str) -> str:
    """Remove and return the first element from a Redis list."""
    try:
        r = RedisConnectionManager.get_connection()
        value = r.lpop(name)
        return value if value else f"List '{name}' is empty or does not exist."
    except RedisError as e:
        return f"Error popping value from list '{name}': {str(e)}"


@mcp.tool()
async def rpop(name: str) -> str:
    """Remove and return the last element from a Redis list."""
    try:
        r = RedisConnectionManager.get_connection()
        value = r.rpop(name)
        return value if value else f"List '{name}' is empty or does not exist."
    except RedisError as e:
        return f"Error popping value from list '{name}': {str(e)}"


@mcp.tool()
async def lrange(name: str, start: int, stop: int) -> Union[str, List[str]]:
    """Get elements from a Redis list within a specific range.

    Returns:
    str: A JSON string containing the list of elements or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        values = r.lrange(name, start, stop)
        if not values:
            return f"List '{name}' is empty or does not exist."
        else:
            return json.dumps(values)
    except RedisError as e:
        return f"Error retrieving values from list '{name}': {str(e)}"


@mcp.tool()
async def llen(name: str) -> int:
    """Get the length of a Redis list."""
    try:
        r = RedisConnectionManager.get_connection()
        return r.llen(name)
    except RedisError as e:
        return f"Error retrieving length of list '{name}': {str(e)}"


@mcp.tool()
async def lrem(name: str, count: int, element: FieldT) -> str:
    """Remove elements from a Redis list.

    Args:
        name: The name of the list
        count: Number of elements to remove (0 = all, positive = from head, negative = from tail)
        element: The element value to remove

    Returns:
        A string indicating the number of elements removed.
    """
    try:
        r = RedisConnectionManager.get_connection()
        removed_count = r.lrem(name, count, element)

        if removed_count == 0:
            return f"Element '{element}' not found in list '{name}' or list does not exist."
        else:
            return f"Removed {removed_count} occurrence(s) of '{element}' from list '{name}'."

    except RedisError as e:
        return f"Error removing element from list '{name}': {str(e)}"
