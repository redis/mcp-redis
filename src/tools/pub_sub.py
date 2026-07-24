


import asyncio
from typing import Any, Dict
from redis.exceptions import RedisError
import redis.asyncio as aioredis

from src.common.connection import current_tenant_id, tenant_redis_manager
from src.common.server import mcp
from src.common.subscription_manager import (
    SubscriptionLimitExceededError,
    SubscriptionManager,
)


@mcp.tool()
async def publish(channel: str, message: str) -> str:
    """Publish a message to a Redis channel.

    Args:
        channel: The Redis channel to publish to.
        message: The message to send.

    Returns:
        A success message or an error message.
    """
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        await r.publish(channel, message)
        return f"Message published to channel '{channel}' for tenant {tenant_id}."
    except RedisError as e:
        return f"Error publishing message to channel '{channel}': {str(e)}"


@mcp.tool()
async def subscribe(channel: str) -> Dict[str, Any]:
    """Subscribe to a Redis channel and return a reusable subscription handle.

    Args:
        channel: The Redis channel to subscribe to.

    Returns:
        A dictionary containing the subscription ID or an error message.
    """
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return {"error": "No active tenant context detected for this tool execution."}

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        subscription = await asyncio.to_thread(
            SubscriptionManager.subscribe,
            r,
            channel,
        )
        return subscription
    except RedisError as e:
        return {"error": f"Error subscribing to channel '{channel}': {str(e)}"}
    except SubscriptionLimitExceededError as e:
        return {"error": str(e)}


@mcp.tool()
async def psubscribe(pattern: str) -> Dict[str, Any]:
    """Subscribe to Redis channels using a pattern.

    Args:
        pattern: The Redis channel pattern to subscribe to.

    Returns:
        A dictionary containing the subscription ID or an error message.
    """
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return {"error": "No active tenant context detected for this tool execution."}

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        subscription = await asyncio.to_thread(
            SubscriptionManager.psubscribe,
            r,
            pattern,
        )
        return subscription
    except RedisError as e:
        return {
            "error": f"Error subscribing to pattern '{pattern}': {str(e)}",
        }
    except SubscriptionLimitExceededError as e:
        return {"error": str(e)}


@mcp.tool()
async def read_messages(
    subscription_id: str, timeout_ms: int = 1000, max_messages: int = 10
) -> Dict[str, Any]:
    """Read pending pub/sub messages for an existing subscription."""
    if timeout_ms < 0:
        return {"error": "timeout_ms must be greater than or equal to 0"}
    if timeout_ms > 5000:
        return {"error": "timeout_ms must be less than or equal to 5000"}
    if max_messages < 1:
        return {"error": "max_messages must be greater than 0"}
    if max_messages > 100:
        return {"error": "max_messages must be less than or equal to 100"}

    try:
        return await asyncio.to_thread(
            SubscriptionManager.read_messages,
            subscription_id,
            timeout_ms,
            max_messages,
        )
    except KeyError:
        return {"error": f"Subscription '{subscription_id}' was not found"}
    except RedisError as e:
        return {
            "error": (
                f"Error reading messages for subscription '{subscription_id}': {str(e)}"
            )
        }


@mcp.tool()
async def unsubscribe(subscription_id: str) -> Dict[str, Any]:
    """Unsubscribe and close an existing pub/sub subscription."""
    try:
        return await asyncio.to_thread(
            SubscriptionManager.unsubscribe,
            subscription_id,
        )
    except KeyError:
        return {"error": f"Subscription '{subscription_id}' was not found"}
    except RedisError as e:
        return {
            "error": (
                f"Error unsubscribing from subscription '{subscription_id}': {str(e)}"
            )
        }