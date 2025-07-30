from redis.exceptions import RedisError

from src.common.connection import RedisConnectionManager
from src.common.server import mcp


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
async def subscribe(channel: str) -> str:
    """Subscribe to a Redis channel.

    Args:
        channel: The Redis channel to subscribe to.

    Returns:
        A success message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        pubsub = r.pubsub()
        pubsub.subscribe(channel)
        return f"Subscribed to channel '{channel}'."
    except RedisError as e:
        return f"Error subscribing to channel '{channel}': {str(e)}"


@mcp.tool()
async def unsubscribe(channel: str) -> str:
    """Unsubscribe from a Redis channel.

    Args:
        channel: The Redis channel to unsubscribe from.

    Returns:
        A success message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        pubsub = r.pubsub()
        pubsub.unsubscribe(channel)
        return f"Unsubscribed from channel '{channel}'."
    except RedisError as e:
        return f"Error unsubscribing from channel '{channel}': {str(e)}"
