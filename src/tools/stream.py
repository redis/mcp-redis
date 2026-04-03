from typing import Dict, Any, Optional, List

from redis.exceptions import RedisError

from src.common.connection import RedisConnectionManager
from src.common.server import mcp


@mcp.tool()
async def xadd(
    key: str, fields: Dict[str, Any], expiration: Optional[int] = None
) -> str:
    """Add an entry to a Redis stream with an optional expiration time.

    Args:
        key (str): The stream key.
        fields (dict): The fields and values for the stream entry.
        expiration (int, optional): Expiration time in seconds.

    Returns:
        str: The ID of the added entry or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        entry_id = r.xadd(key, fields)
        if expiration:
            r.expire(key, expiration)
        return f"Successfully added entry {entry_id} to {key}" + (
            f" with expiration {expiration} seconds" if expiration else ""
        )
    except RedisError as e:
        return f"Error adding to stream {key}: {str(e)}"


@mcp.tool()
async def xrange(key: str, count: int = 1) -> str:
    """Read entries from a Redis stream.

    Args:
        key (str): The stream key.
        count (int, optional): Number of entries to retrieve.

    Returns:
        str: The retrieved stream entries or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        entries = r.xrange(key, count=count)
        return str(entries) if entries else f"Stream {key} is empty or does not exist"
    except RedisError as e:
        return f"Error reading from stream {key}: {str(e)}"


@mcp.tool()
async def xdel(key: str, entry_id: str) -> str:
    """Delete an entry from a Redis stream.

    Args:
        key (str): The stream key.
        entry_id (str): The ID of the entry to delete.

    Returns:
        str: Confirmation message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        result = r.xdel(key, entry_id)
        return (
            f"Successfully deleted entry {entry_id} from {key}"
            if result
            else f"Entry {entry_id} not found in {key}"
        )
    except RedisError as e:
        return f"Error deleting from stream {key}: {str(e)}"


@mcp.tool()
async def xgroup_create(
    key: str,
    group_name: str,
    start_id: str = "$",
    mkstream: bool = True,
) -> str:
    """Create a consumer group for a Redis stream.

    Args:
        key (str): The stream key.
        group_name (str): The consumer group name.
        start_id (str, optional): Stream ID from which the group starts consuming.
        mkstream (bool, optional): Create the stream if it does not exist.

    Returns:
        str: Confirmation message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        r.xgroup_create(key, group_name, id=start_id, mkstream=mkstream)
        return f"Successfully created consumer group '{group_name}' on stream '{key}'"
    except RedisError as e:
        return (
            f"Error creating consumer group '{group_name}' on stream '{key}': {str(e)}"
        )


@mcp.tool()
async def xgroup_destroy(key: str, group_name: str) -> str:
    """Destroy a consumer group for a Redis stream.

    Args:
        key (str): The stream key.
        group_name (str): The consumer group name.

    Returns:
        str: Confirmation message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        result = r.xgroup_destroy(key, group_name)
        return (
            f"Successfully destroyed consumer group '{group_name}' on stream '{key}'"
            if result
            else f"Consumer group '{group_name}' not found on stream '{key}'"
        )
    except RedisError as e:
        return f"Error destroying consumer group '{group_name}' on stream '{key}': {str(e)}"


@mcp.tool()
async def xreadgroup(
    key: str,
    group_name: str,
    consumer_name: str,
    count: int = 1,
    block_ms: Optional[int] = None,
    stream_id: str = ">",
) -> str:
    """Read entries from a Redis stream using a consumer group.

    Args:
        key (str): The stream key.
        group_name (str): The consumer group name.
        consumer_name (str): The consumer name.
        count (int, optional): Maximum number of entries to retrieve.
        block_ms (int, optional): Maximum time to block waiting for entries.
            Use None for a non-blocking read. 0 is rejected because Redis treats
            BLOCK 0 as an indefinite wait.
        stream_id (str, optional): Stream ID to read from. Use ">" for new messages.

    Returns:
        str: The retrieved stream entries or an error message.
    """
    if count < 1:
        return "count must be greater than 0"
    if block_ms == 0:
        return "block_ms=0 is not allowed; use None for a non-blocking read or a positive timeout in milliseconds"
    if block_ms is not None and block_ms < 0:
        return "block_ms must be greater than 0 milliseconds when provided"
    if block_ms is not None and block_ms > 5000:
        return "block_ms must be less than or equal to 5000 milliseconds"

    try:
        r = RedisConnectionManager.get_connection()
        entries = r.xreadgroup(
            group_name,
            consumer_name,
            {key: stream_id},
            count=count,
            block=block_ms,
        )
        return (
            str(entries)
            if entries
            else (
                f"No entries available for consumer '{consumer_name}' in group "
                f"'{group_name}' on stream '{key}'"
            )
        )
    except RedisError as e:
        return (
            f"Error reading from stream {key} with consumer group '{group_name}': "
            f"{str(e)}"
        )


@mcp.tool()
async def xack(key: str, group_name: str, entry_ids: List[str]) -> str:
    """Acknowledge entries that were processed by a consumer group.

    Args:
        key (str): The stream key.
        group_name (str): The consumer group name.
        entry_ids (List[str]): Entry IDs to acknowledge.

    Returns:
        str: Confirmation message or an error message.
    """
    if not entry_ids:
        return "At least one entry ID is required to acknowledge stream entries"

    try:
        r = RedisConnectionManager.get_connection()
        acknowledged = r.xack(key, group_name, *entry_ids)
        return (
            f"Successfully acknowledged {acknowledged} entr"
            f"{'y' if acknowledged == 1 else 'ies'} in group '{group_name}' on stream '{key}'"
        )
    except RedisError as e:
        return (
            f"Error acknowledging entries for consumer group '{group_name}' on stream "
            f"'{key}': {str(e)}"
        )
