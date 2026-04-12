import asyncio
from typing import Any, Dict

from redis.exceptions import RedisError

from src.common.connection import RedisConnectionManager
from src.common.server import mcp
from src.common.subscription_manager import SubscriptionManager


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
        r = RedisConnectionManager.get_connection()
        r.publish(channel, message)
        return f"Message published to channel '{channel}'."
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
        r = RedisConnectionManager.get_connection()
        subscription = SubscriptionManager.subscribe(r, channel)
        return subscription
    except RedisError as e:
        return {"error": f"Error subscribing to channel '{channel}': {str(e)}"}


@mcp.tool()
async def psubscribe(pattern: str) -> Dict[str, Any]:
    """Subscribe to Redis channels using a pattern.

    Args:
        pattern: The Redis channel pattern to subscribe to.

    Returns:
        A dictionary containing the subscription ID or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        subscription = SubscriptionManager.psubscribe(r, pattern)
        return subscription
    except RedisError as e:
        return {
            "error": f"Error subscribing to pattern '{pattern}': {str(e)}",
        }


@mcp.tool()
async def read_messages(
    subscription_id: str, timeout_ms: int = 1000, max_messages: int = 10
) -> Dict[str, Any]:
    """Read pending pub/sub messages for an existing subscription.

    Args:
        subscription_id: The ID returned by subscribe() or psubscribe().
        timeout_ms: Time to wait for messages in milliseconds. Use 0 for non-blocking.
        max_messages: Maximum number of messages to return in one call.

    Returns:
        A dictionary containing the collected messages or an error message.
    """
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
    """Unsubscribe and close an existing pub/sub subscription.

    Args:
        subscription_id: The ID returned by subscribe() or psubscribe().

    Returns:
        A dictionary describing the closed subscription or an error message.
    """
    try:
        result = SubscriptionManager.unsubscribe(subscription_id)
        return result
    except KeyError:
        return {"error": f"Subscription '{subscription_id}' was not found"}
    except RedisError as e:
        return {
            "error": (
                f"Error unsubscribing from subscription '{subscription_id}': {str(e)}"
            )
        }
