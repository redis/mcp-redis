"""
Unit tests for src/tools/pub_sub.py
"""

from unittest.mock import Mock, patch

import pytest
from redis.exceptions import ConnectionError, RedisError

from src.tools.pub_sub import publish, subscribe, unsubscribe
import src.tools.pub_sub as pub_sub_module


@pytest.fixture(autouse=True)
def reset_subscriptions():
    """Clear the subscription registry before and after each test."""
    pub_sub_module._subscriptions.clear()
    yield
    pub_sub_module._subscriptions.clear()


class TestPubSubOperations:
    """Test cases for Redis pub/sub operations."""

    @pytest.mark.asyncio
    async def test_publish_success(self, mock_redis_connection_manager):
        """Test successful publish operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.publish.return_value = (
            2  # Number of subscribers that received the message
        )

        result = await publish("test_channel", "Hello World")

        mock_redis.publish.assert_called_once_with("test_channel", "Hello World")
        assert "Message published to channel 'test_channel'" in result

    @pytest.mark.asyncio
    async def test_publish_no_subscribers(self, mock_redis_connection_manager):
        """Test publish operation with no subscribers."""
        mock_redis = mock_redis_connection_manager
        mock_redis.publish.return_value = 0  # No subscribers

        result = await publish("empty_channel", "Hello World")

        mock_redis.publish.assert_called_once_with("empty_channel", "Hello World")
        assert "Message published to channel 'empty_channel'" in result

    @pytest.mark.asyncio
    async def test_publish_redis_error(self, mock_redis_connection_manager):
        """Test publish operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.publish.side_effect = RedisError("Connection failed")

        result = await publish("test_channel", "Hello World")

        assert (
            "Error publishing message to channel 'test_channel': Connection failed"
            in result
        )

    @pytest.mark.asyncio
    async def test_publish_connection_error(self, mock_redis_connection_manager):
        """Test publish operation with connection error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.publish.side_effect = ConnectionError("Redis server unavailable")

        result = await publish("test_channel", "Hello World")

        assert (
            "Error publishing message to channel 'test_channel': Redis server unavailable"
            in result
        )

    @pytest.mark.asyncio
    async def test_publish_empty_message(self, mock_redis_connection_manager):
        """Test publish operation with empty message."""
        mock_redis = mock_redis_connection_manager
        mock_redis.publish.return_value = 1

        result = await publish("test_channel", "")

        mock_redis.publish.assert_called_once_with("test_channel", "")
        assert "Message published to channel 'test_channel'" in result

    @pytest.mark.asyncio
    async def test_publish_numeric_message(self, mock_redis_connection_manager):
        """Test publish operation with numeric message."""
        mock_redis = mock_redis_connection_manager
        mock_redis.publish.return_value = 1

        result = await publish("test_channel", 42)

        mock_redis.publish.assert_called_once_with("test_channel", 42)
        assert "Message published to channel 'test_channel'" in result

    @pytest.mark.asyncio
    async def test_publish_json_message(self, mock_redis_connection_manager):
        """Test publish operation with JSON-like message."""
        mock_redis = mock_redis_connection_manager
        mock_redis.publish.return_value = 3

        json_message = (
            '{"type": "notification", "data": {"user": "john", "action": "login"}}'
        )
        result = await publish("notifications", json_message)

        mock_redis.publish.assert_called_once_with("notifications", json_message)
        assert "Message published to channel 'notifications'" in result

    @pytest.mark.asyncio
    async def test_publish_unicode_message(self, mock_redis_connection_manager):
        """Test publish operation with unicode message."""
        mock_redis = mock_redis_connection_manager
        mock_redis.publish.return_value = 1

        unicode_message = "Hello 世界 🌍"
        result = await publish("test_channel", unicode_message)

        mock_redis.publish.assert_called_once_with("test_channel", unicode_message)
        assert "Message published to channel 'test_channel'" in result

    # --- subscribe ---

    @pytest.mark.asyncio
    async def test_subscribe_success(self, mock_redis_connection_manager):
        """Test successful subscribe operation stores pubsub in registry."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub
        mock_pubsub.subscribe.return_value = None

        result = await subscribe("test_channel")

        mock_redis.pubsub.assert_called_once()
        mock_pubsub.subscribe.assert_called_once_with("test_channel")
        assert "Subscribed to channel 'test_channel'" in result
        assert pub_sub_module._subscriptions["test_channel"] is mock_pubsub

    @pytest.mark.asyncio
    async def test_subscribe_already_subscribed(self, mock_redis_connection_manager):
        """Test that subscribing to the same channel twice is idempotent."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub

        await subscribe("test_channel")
        result = await subscribe("test_channel")

        assert mock_redis.pubsub.call_count == 1
        assert "Already subscribed to channel 'test_channel'" in result

    @pytest.mark.asyncio
    async def test_subscribe_redis_error(self, mock_redis_connection_manager):
        """Test subscribe operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.pubsub.side_effect = RedisError("Connection failed")

        result = await subscribe("test_channel")

        assert (
            "Error subscribing to channel 'test_channel': Connection failed" in result
        )
        assert "test_channel" not in pub_sub_module._subscriptions

    @pytest.mark.asyncio
    async def test_subscribe_pubsub_error_closes_connection(
        self, mock_redis_connection_manager
    ):
        """Test that a failed subscribe call closes the pubsub to avoid leaking."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub
        mock_pubsub.subscribe.side_effect = RedisError("Subscribe failed")

        result = await subscribe("test_channel")

        assert "Error subscribing to channel 'test_channel': Subscribe failed" in result
        mock_pubsub.close.assert_called_once()
        assert "test_channel" not in pub_sub_module._subscriptions

    @pytest.mark.asyncio
    async def test_subscribe_multiple_channels_pattern(
        self, mock_redis_connection_manager
    ):
        """Test subscribe operation with pattern-like channel name."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub
        mock_pubsub.subscribe.return_value = None

        pattern_channel = "notifications:*"
        result = await subscribe(pattern_channel)

        mock_pubsub.subscribe.assert_called_once_with(pattern_channel)
        assert f"Subscribed to channel '{pattern_channel}'" in result

    @pytest.mark.asyncio
    async def test_subscribe_with_special_characters(
        self, mock_redis_connection_manager
    ):
        """Test subscribe operation with special characters in channel name."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub
        mock_pubsub.subscribe.return_value = None

        special_channel = "channel:with:colons-and-dashes_and_underscores"
        result = await subscribe(special_channel)

        mock_pubsub.subscribe.assert_called_once_with(special_channel)
        assert f"Subscribed to channel '{special_channel}'" in result

    # --- unsubscribe ---

    @pytest.mark.asyncio
    async def test_unsubscribe_success(self, mock_redis_connection_manager):
        """Test successful unsubscribe closes the stored pubsub connection."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub
        mock_pubsub.subscribe.return_value = None

        await subscribe("test_channel")
        result = await unsubscribe("test_channel")

        mock_pubsub.close.assert_called_once()
        assert "Unsubscribed from channel 'test_channel'" in result
        assert "test_channel" not in pub_sub_module._subscriptions

    @pytest.mark.asyncio
    async def test_unsubscribe_uses_stored_pubsub_not_new_one(
        self, mock_redis_connection_manager
    ):
        """Test that unsubscribe does NOT create a new pubsub object."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub

        await subscribe("test_channel")
        assert mock_redis.pubsub.call_count == 1

        await unsubscribe("test_channel")

        # pubsub() must still have been called exactly once (from subscribe only)
        assert mock_redis.pubsub.call_count == 1

    @pytest.mark.asyncio
    async def test_unsubscribe_not_subscribed(self, mock_redis_connection_manager):
        """Test unsubscribe when no subscription exists returns a clear message."""
        result = await unsubscribe("unknown_channel")

        assert "Not subscribed to channel 'unknown_channel'" in result

    @pytest.mark.asyncio
    async def test_unsubscribe_redis_error(self, mock_redis_connection_manager):
        """Test unsubscribe when pubsub.close() raises a RedisError."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub
        mock_pubsub.subscribe.return_value = None
        mock_pubsub.close.side_effect = RedisError("Close failed")

        await subscribe("test_channel")
        result = await unsubscribe("test_channel")

        assert (
            "Error unsubscribing from channel 'test_channel': Close failed" in result
        )

    @pytest.mark.asyncio
    async def test_unsubscribe_removes_from_registry(
        self, mock_redis_connection_manager
    ):
        """Test that after unsubscribe the channel is removed from the registry."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub

        await subscribe("test_channel")
        assert "test_channel" in pub_sub_module._subscriptions

        await unsubscribe("test_channel")
        assert "test_channel" not in pub_sub_module._subscriptions

    # --- lifecycle ---

    @pytest.mark.asyncio
    async def test_subscribe_unsubscribe_resubscribe(
        self, mock_redis_connection_manager
    ):
        """Test that a channel can be re-subscribed after unsubscribe."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub_1 = Mock()
        mock_pubsub_2 = Mock()
        mock_redis.pubsub.side_effect = [mock_pubsub_1, mock_pubsub_2]

        await subscribe("test_channel")
        await unsubscribe("test_channel")
        result = await subscribe("test_channel")

        assert "Subscribed to channel 'test_channel'" in result
        assert pub_sub_module._subscriptions["test_channel"] is mock_pubsub_2

    @pytest.mark.asyncio
    async def test_publish_to_pattern_channel(self, mock_redis_connection_manager):
        """Test publish operation to pattern-like channel."""
        mock_redis = mock_redis_connection_manager
        mock_redis.publish.return_value = 5

        pattern_channel = "user:123:notifications"
        result = await publish(pattern_channel, "User notification")

        mock_redis.publish.assert_called_once_with(pattern_channel, "User notification")
        assert f"Message published to channel '{pattern_channel}'" in result

    @pytest.mark.asyncio
    async def test_publish_large_message(self, mock_redis_connection_manager):
        """Test publish operation with large message."""
        mock_redis = mock_redis_connection_manager
        mock_redis.publish.return_value = 1

        large_message = "x" * 10000  # 10KB message
        result = await publish("test_channel", large_message)

        mock_redis.publish.assert_called_once_with("test_channel", large_message)
        assert "Message published to channel 'test_channel'" in result

    @pytest.mark.asyncio
    async def test_connection_manager_called_correctly(self):
        """Test that RedisConnectionManager.get_connection is called correctly."""
        with patch(
            "src.tools.pub_sub.RedisConnectionManager.get_connection"
        ) as mock_get_conn:
            mock_redis = Mock()
            mock_redis.publish.return_value = 1
            mock_get_conn.return_value = mock_redis

            await publish("test_channel", "test_message")

            mock_get_conn.assert_called_once()

    @pytest.mark.asyncio
    async def test_function_signatures(self):
        """Test that functions have correct signatures."""
        import inspect

        # Test publish function signature
        publish_sig = inspect.signature(publish)
        publish_params = list(publish_sig.parameters.keys())
        assert publish_params == ["channel", "message"]

        # Test subscribe function signature
        subscribe_sig = inspect.signature(subscribe)
        subscribe_params = list(subscribe_sig.parameters.keys())
        assert subscribe_params == ["channel"]

        # Test unsubscribe function signature
        unsubscribe_sig = inspect.signature(unsubscribe)
        unsubscribe_params = list(unsubscribe_sig.parameters.keys())
        assert unsubscribe_params == ["channel"]
