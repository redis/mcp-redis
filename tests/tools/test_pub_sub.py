"""
Unit tests for src/tools/pub_sub.py
"""

from unittest.mock import Mock, patch

import pytest
from redis.exceptions import ConnectionError, RedisError

from src.common.subscription_manager import SubscriptionManager
from src.tools.pub_sub import psubscribe, publish, read_messages, subscribe, unsubscribe


class TestPubSubOperations:
    """Test cases for Redis pub/sub operations."""

    @pytest.mark.asyncio
    async def test_publish_success(self, mock_redis_connection_manager):
        """Test successful publish operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.publish.return_value = 2

        result = await publish("test_channel", "Hello World")

        mock_redis.publish.assert_called_once_with("test_channel", "Hello World")
        assert "Message published to channel 'test_channel'" in result

    @pytest.mark.asyncio
    async def test_publish_no_subscribers(self, mock_redis_connection_manager):
        """Test publish operation with no subscribers."""
        mock_redis = mock_redis_connection_manager
        mock_redis.publish.return_value = 0

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

    @pytest.mark.asyncio
    async def test_publish_large_message(self, mock_redis_connection_manager):
        """Test publish operation with large message."""
        mock_redis = mock_redis_connection_manager
        mock_redis.publish.return_value = 1

        large_message = "x" * 10000
        result = await publish("test_channel", large_message)

        mock_redis.publish.assert_called_once_with("test_channel", large_message)
        assert "Message published to channel 'test_channel'" in result

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
    async def test_subscribe_success(self, mock_redis_connection_manager):
        """Test successful channel subscribe operation."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub

        result = await subscribe("test_channel")

        subscription_id = result["subscription_id"]
        mock_redis.pubsub.assert_called_once()
        mock_pubsub.subscribe.assert_called_once_with("test_channel")
        assert result["status"] == "success"
        assert result["mode"] == "channel"
        assert result["targets"] == ["test_channel"]
        assert subscription_id in SubscriptionManager._subscriptions

    @pytest.mark.asyncio
    async def test_subscribe_redis_error(self, mock_redis_connection_manager):
        """Test subscribe operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.pubsub.side_effect = RedisError("Connection failed")

        result = await subscribe("test_channel")

        assert result == {
            "error": "Error subscribing to channel 'test_channel': Connection failed"
        }

    @pytest.mark.asyncio
    async def test_subscribe_pubsub_error(self, mock_redis_connection_manager):
        """Test subscribe operation when the subscribe call fails."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub
        mock_pubsub.subscribe.side_effect = RedisError("Subscribe failed")

        result = await subscribe("test_channel")

        mock_pubsub.close.assert_called_once()
        assert result == {
            "error": "Error subscribing to channel 'test_channel': Subscribe failed"
        }

    @pytest.mark.asyncio
    async def test_psubscribe_success(self, mock_redis_connection_manager):
        """Test successful pattern subscribe operation."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub

        result = await psubscribe("notifications:*")

        subscription_id = result["subscription_id"]
        mock_pubsub.psubscribe.assert_called_once_with("notifications:*")
        assert result["status"] == "success"
        assert result["mode"] == "pattern"
        assert result["targets"] == ["notifications:*"]
        assert subscription_id in SubscriptionManager._subscriptions

    @pytest.mark.asyncio
    async def test_psubscribe_redis_error(self, mock_redis_connection_manager):
        """Test pattern subscribe operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.pubsub.side_effect = RedisError("Connection failed")

        result = await psubscribe("notifications:*")

        assert result == {
            "error": "Error subscribing to pattern 'notifications:*': Connection failed"
        }

    @pytest.mark.asyncio
    async def test_read_messages_success(self, mock_redis_connection_manager):
        """Test reading messages from an active subscription."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_pubsub.get_message.side_effect = [
            {
                "type": "message",
                "channel": b"test_channel",
                "pattern": None,
                "data": b"Hello World",
            },
            None,
        ]
        mock_redis.pubsub.return_value = mock_pubsub

        subscription = await subscribe("test_channel")
        result = await read_messages(subscription["subscription_id"], max_messages=5)

        assert result == {
            "subscription_id": subscription["subscription_id"],
            "message_count": 1,
            "messages": [
                {
                    "type": "message",
                    "channel": "test_channel",
                    "pattern": None,
                    "data": "Hello World",
                }
            ],
        }
        assert mock_pubsub.get_message.call_count == 2

    @pytest.mark.asyncio
    async def test_read_messages_multiple_without_blocking(
        self, mock_redis_connection_manager
    ):
        """Test non-blocking reads consume multiple queued messages."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_pubsub.get_message.side_effect = [
            {
                "type": "message",
                "channel": b"test_channel",
                "pattern": None,
                "data": b"first",
            },
            {
                "type": "message",
                "channel": b"test_channel",
                "pattern": None,
                "data": b"second",
            },
            None,
        ]
        mock_redis.pubsub.return_value = mock_pubsub

        subscription = await subscribe("test_channel")
        result = await read_messages(
            subscription["subscription_id"], timeout_ms=0, max_messages=10
        )

        assert result["message_count"] == 2
        assert [message["data"] for message in result["messages"]] == [
            "first",
            "second",
        ]

    @pytest.mark.asyncio
    async def test_read_messages_returns_empty_list_when_no_messages(
        self, mock_redis_connection_manager
    ):
        """Test reading when no messages are currently available."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_pubsub.get_message.return_value = None
        mock_redis.pubsub.return_value = mock_pubsub

        subscription = await subscribe("test_channel")
        result = await read_messages(subscription["subscription_id"])

        assert result == {
            "subscription_id": subscription["subscription_id"],
            "message_count": 0,
            "messages": [],
        }

    @pytest.mark.asyncio
    async def test_read_messages_unknown_subscription(self):
        """Test reading with an unknown subscription ID."""
        result = await read_messages("missing-subscription")

        assert result == {"error": "Subscription 'missing-subscription' was not found"}

    @pytest.mark.asyncio
    async def test_read_messages_validation(self):
        """Test read_messages input validation."""
        assert await read_messages("sub-1", timeout_ms=-1) == {
            "error": "timeout_ms must be greater than or equal to 0"
        }
        assert await read_messages("sub-1", timeout_ms=6000) == {
            "error": "timeout_ms must be less than or equal to 5000"
        }
        assert await read_messages("sub-1", max_messages=0) == {
            "error": "max_messages must be greater than 0"
        }
        assert await read_messages("sub-1", max_messages=101) == {
            "error": "max_messages must be less than or equal to 100"
        }

    @pytest.mark.asyncio
    async def test_read_messages_redis_error(self, mock_redis_connection_manager):
        """Test read_messages when Redis returns an error."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_pubsub.get_message.side_effect = RedisError("Read failed")
        mock_redis.pubsub.return_value = mock_pubsub

        subscription = await subscribe("test_channel")
        result = await read_messages(subscription["subscription_id"])

        assert result == {
            "error": (
                f"Error reading messages for subscription "
                f"'{subscription['subscription_id']}': Read failed"
            )
        }

    @pytest.mark.asyncio
    async def test_read_messages_uses_asyncio_to_thread(self):
        """Test read_messages offloads blocking polling to a worker thread."""
        expected = {
            "subscription_id": "sub-123",
            "message_count": 0,
            "messages": [],
        }

        with (
            patch("src.tools.pub_sub.asyncio.to_thread") as mock_to_thread,
            patch("src.tools.pub_sub.SubscriptionManager.read_messages") as mock_read,
        ):
            mock_to_thread.return_value = expected

            result = await read_messages("sub-123", timeout_ms=250, max_messages=3)

            mock_to_thread.assert_awaited_once_with(mock_read, "sub-123", 250, 3)
            assert result == expected

    @pytest.mark.asyncio
    async def test_unsubscribe_success(self, mock_redis_connection_manager):
        """Test successful channel unsubscribe operation."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub

        subscription = await subscribe("test_channel")
        result = await unsubscribe(subscription["subscription_id"])

        mock_pubsub.unsubscribe.assert_called_once_with("test_channel")
        mock_pubsub.close.assert_called_once()
        assert result == {
            "status": "success",
            "subscription_id": subscription["subscription_id"],
            "mode": "channel",
            "targets": ["test_channel"],
        }
        assert subscription["subscription_id"] not in SubscriptionManager._subscriptions

    @pytest.mark.asyncio
    async def test_unsubscribe_pattern_subscription(
        self, mock_redis_connection_manager
    ):
        """Test successful pattern unsubscribe operation."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub

        subscription = await psubscribe("events:*")
        result = await unsubscribe(subscription["subscription_id"])

        mock_pubsub.punsubscribe.assert_called_once_with("events:*")
        mock_pubsub.close.assert_called_once()
        assert result["mode"] == "pattern"
        assert result["targets"] == ["events:*"]

    @pytest.mark.asyncio
    async def test_unsubscribe_unknown_subscription(self):
        """Test unsubscribe with an unknown subscription ID."""
        result = await unsubscribe("missing-subscription")

        assert result == {"error": "Subscription 'missing-subscription' was not found"}

    @pytest.mark.asyncio
    async def test_unsubscribe_redis_error(self, mock_redis_connection_manager):
        """Test unsubscribe when Redis returns an error."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_pubsub.unsubscribe.side_effect = RedisError("Unsubscribe failed")
        mock_redis.pubsub.return_value = mock_pubsub

        subscription = await subscribe("test_channel")
        result = await unsubscribe(subscription["subscription_id"])

        mock_pubsub.close.assert_called_once()
        assert result == {
            "error": (
                f"Error unsubscribing from subscription "
                f"'{subscription['subscription_id']}': Unsubscribe failed"
            )
        }

    @pytest.mark.asyncio
    async def test_subscribe_with_special_characters(
        self, mock_redis_connection_manager
    ):
        """Test subscribe operation with special characters in channel name."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub

        special_channel = "channel:with:colons-and-dashes_and_underscores"
        result = await subscribe(special_channel)

        mock_pubsub.subscribe.assert_called_once_with(special_channel)
        assert result["targets"] == [special_channel]

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

        publish_sig = inspect.signature(publish)
        assert list(publish_sig.parameters.keys()) == ["channel", "message"]

        subscribe_sig = inspect.signature(subscribe)
        assert list(subscribe_sig.parameters.keys()) == ["channel"]

        psubscribe_sig = inspect.signature(psubscribe)
        assert list(psubscribe_sig.parameters.keys()) == ["pattern"]

        read_messages_sig = inspect.signature(read_messages)
        assert list(read_messages_sig.parameters.keys()) == [
            "subscription_id",
            "timeout_ms",
            "max_messages",
        ]
        assert read_messages_sig.parameters["timeout_ms"].default == 1000
        assert read_messages_sig.parameters["max_messages"].default == 10

        unsubscribe_sig = inspect.signature(unsubscribe)
        assert list(unsubscribe_sig.parameters.keys()) == ["subscription_id"]
