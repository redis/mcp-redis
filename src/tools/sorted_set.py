
from typing import Optional
from redis.exceptions import RedisError
import redis.asyncio as aioredis

from src.common.connection import current_tenant_id, tenant_redis_manager
from src.common.server import mcp


@mcp.tool()
async def zadd(
    key: str, score: float, member: str, expiration: Optional[int] = None
) -> str:
    """Add a member to a Redis sorted set with an optional expiration time.

    Args:
        key (str): The sorted set key.
        score (float): The score of the member.
        member (str): The member to add.
        expiration (int, optional): Expiration time in seconds.

    Returns:
        str: Confirmation message or an error message.
    """
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        await r.zadd(key, {member: score})
        if expiration:
            await r.expire(key, expiration)

        return f"Successfully added {member} to {key} with score {score} for tenant {tenant_id}" + (
            f" and expiration {expiration} seconds" if expiration else ""
        )
    except RedisError as e:
        return f"Error adding to sorted set {key}: {str(e)}"


@mcp.tool()
async def zrange(key: str, start: int, end: int, with_scores: bool = False) -> str:
    """Retrieve a range of members from a Redis sorted set.

    Args:
        key (str): The sorted set key.
        start (int): The starting index.
        end (int): The ending index.
        with_scores (bool, optional): Whether to include scores in the result.

    Returns:
        str: The sorted set members in the given range or an error message.
    """
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        members = await r.zrange(key, start, end, withscores=with_scores)
        return (
            str(members) if members else f"Sorted set {key} is empty or does not exist for tenant {tenant_id}"
        )
    except RedisError as e:
        return f"Error retrieving sorted set {key}: {str(e)}"


@mcp.tool()
async def zrem(key: str, member: str) -> str:
    """Remove a member from a Redis sorted set.

    Args:
        key (str): The sorted set key.
        member (str): The member to remove.

    Returns:
        str: Confirmation message or an error message.
    """
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        result = await r.zrem(key, member)
        return (
            f"Successfully removed {member} from {key} for tenant {tenant_id}"
            if result
            else f"Member {member} not found in {key} for tenant {tenant_id}"
        )
    except RedisError as e:
        return f"Error removing from sorted set {key}: {str(e)}"