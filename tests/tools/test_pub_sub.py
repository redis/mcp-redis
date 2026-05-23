"""
Unit tests for src/tools/pub_sub.py
"""

import threading
import time
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
    async def test_subscribe_rejects_when_subscription_limit_exceeded(
        self, mock_redis_connection_manager
    ):
        """Test subscribe returns a clear error when active subscription limit is reached."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        mock_redis.pubsub.return_value = mock_pubsub
        limit_error = (
            "Too many active subscriptions. Close unused subscriptions and try again."
        )

        with patch.object(SubscriptionManager, "MAX_ACTIVE_SUBSCRIPTIONS", 1):
            first = await subscribe("test_channel_1")
            second = await subscribe("test_channel_2")

        assert first["status"] == "success"
        assert second == {"error": limit_error}
        assert mock_pubsub.close.call_count == 1

    @pytest.mark.asyncio
    async def test_subscribe_cleans_stale_subscription_before_enforcing_limit(
        self, mock_redis_connection_manager
    ):
        """Test stale subscriptions are cleaned up before capacity check."""
        mock_redis = mock_redis_connection_manager
        stale_pubsub = Mock()
        fresh_pubsub = Mock()
        mock_redis.pubsub.side_effect = [stale_pubsub, fresh_pubsub]

        with (
            patch.object(SubscriptionManager, "MAX_ACTIVE_SUBSCRIPTIONS", 1),
            patch.object(SubscriptionManager, "STALE_SUBSCRIPTION_TTL_SECONDS", 60),
        ):
            first = await subscribe("test_channel_1")
            SubscriptionManager._subscriptions[
                first["subscription_id"]
            ].last_accessed_at = 0
            second = await subscribe("test_channel_2")

        assert first["status"] == "success"
        assert second["status"] == "success"
        stale_pubsub.close.assert_called_once()
        assert first["subscription_id"] not in SubscriptionManager._subscriptions
        assert second["subscription_id"] in SubscriptionManager._subscriptions

    @pytest.mark.asyncio
    async def test_subscribe_stale_cleanup_continues_when_close_fails(
        self, mock_redis_connection_manager
    ):
        """Test stale cleanup still processes remaining subscriptions after one close failure."""
        mock_redis = mock_redis_connection_manager
        failing_stale_pubsub = Mock()
        healthy_stale_pubsub = Mock()
        fresh_pubsub = Mock()

        failing_stale_pubsub.close.side_effect = [RedisError("close failed"), None]
        mock_redis.pubsub.side_effect = [
            failing_stale_pubsub,
            healthy_stale_pubsub,
            fresh_pubsub,
        ]

        with (
            patch.object(SubscriptionManager, "MAX_ACTIVE_SUBSCRIPTIONS", 3),
            patch.object(SubscriptionManager, "STALE_SUBSCRIPTION_TTL_SECONDS", 60),
        ):
            first = await subscribe("test_channel_1")
            second = await subscribe("test_channel_2")
            SubscriptionManager._subscriptions[
                first["subscription_id"]
            ].last_accessed_at = 0
            SubscriptionManager._subscriptions[
                second["subscription_id"]
            ].last_accessed_at = 0
            third = await subscribe("test_channel_3")

        assert third["status"] == "success"
        failing_stale_pubsub.close.assert_called_once()
        healthy_stale_pubsub.close.assert_called_once()
        assert first["subscription_id"] in SubscriptionManager._subscriptions
        assert second["subscription_id"] not in SubscriptionManager._subscriptions
        assert third["subscription_id"] in SubscriptionManager._subscriptions

    @pytest.mark.asyncio
    async def test_reset_closes_all_subscriptions_even_when_one_close_fails(
        self, mock_redis_connection_manager
    ):
        """Test reset attempts to close all subscriptions despite close failures."""
        mock_redis = mock_redis_connection_manager
        failing_pubsub = Mock()
        healthy_pubsub = Mock()
        mock_redis.pubsub.side_effect = [failing_pubsub, healthy_pubsub]
        failing_pubsub.close.side_effect = RedisError("close failed")

        first = await subscribe("test_channel_1")
        second = await subscribe("test_channel_2")

        SubscriptionManager.reset()

        failing_pubsub.close.assert_called_once()
        healthy_pubsub.close.assert_called_once()
        assert first["subscription_id"] not in SubscriptionManager._subscriptions
        assert second["subscription_id"] not in SubscriptionManager._subscriptions

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
    async def test_subscribe_uses_asyncio_to_thread(
        self, mock_redis_connection_manager
    ):
        """Test subscribe offloads blocking connection setup to a worker thread."""
        mock_redis = mock_redis_connection_manager
        expected = {
            "status": "success",
            "subscription_id": "sub-123",
            "mode": "channel",
            "targets": ["orders"],
        }

        with (
            patch("src.tools.pub_sub.asyncio.to_thread") as mock_to_thread,
            patch("src.tools.pub_sub.SubscriptionManager.subscribe") as mock_subscribe,
        ):
            mock_to_thread.return_value = expected

            result = await subscribe("orders")

            mock_to_thread.assert_awaited_once_with(
                mock_subscribe,
                mock_redis,
                "orders",
            )
            assert result == expected

    @pytest.mark.asyncio
    async def test_psubscribe_uses_asyncio_to_thread(
        self, mock_redis_connection_manager
    ):
        """Test psubscribe offloads blocking connection setup to a worker thread."""
        mock_redis = mock_redis_connection_manager
        expected = {
            "status": "success",
            "subscription_id": "sub-456",
            "mode": "pattern",
            "targets": ["orders:*"],
        }

        with (
            patch("src.tools.pub_sub.asyncio.to_thread") as mock_to_thread,
            patch("src.tools.pub_sub.SubscriptionManager.psubscribe") as mock_psub,
        ):
            mock_to_thread.return_value = expected

            result = await psubscribe("orders:*")

            mock_to_thread.assert_awaited_once_with(
                mock_psub,
                mock_redis,
                "orders:*",
            )
            assert result == expected

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
    async def test_read_messages_timeout_budget_starts_after_lock_acquisition(
        self, mock_redis_connection_manager
    ):
        """Test timeout budget is computed after acquiring subscription lock."""
        mock_redis = mock_redis_connection_manager
        mock_pubsub = Mock()
        captured_timeouts = []

        def capture_timeout(*args, **kwargs):
            captured_timeouts.append(kwargs.get("timeout"))
            return None

        mock_pubsub.get_message.side_effect = capture_timeout
        mock_redis.pubsub.return_value = mock_pubsub

        subscription = await subscribe("test_channel")
        managed_subscription = SubscriptionManager._subscriptions[
            subscription["subscription_id"]
        ]

        managed_subscription.lock.acquire()
        releaser = threading.Thread(
            target=lambda: (time.sleep(0.2), managed_subscription.lock.release())
        )
        releaser.start()

        try:
            result = await read_messages(
                subscription["subscription_id"], timeout_ms=300, max_messages=1
            )
        finally:
            releaser.join(timeout=1)
            if managed_subscription.lock.locked():
                managed_subscription.lock.release()

        assert result["message_count"] == 0
        assert captured_timeouts
        assert captured_timeouts[0] > 0.25

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
    async def test_unsubscribe_uses_asyncio_to_thread(self):
        """Test unsubscribe offloads blocking cleanup to a worker thread."""
        expected = {
            "status": "success",
            "subscription_id": "sub-123",
            "mode": "channel",
            "targets": ["orders"],
        }

        with (
            patch("src.tools.pub_sub.asyncio.to_thread") as mock_to_thread,
            patch("src.tools.pub_sub.SubscriptionManager.unsubscribe") as mock_unsub,
        ):
            mock_to_thread.return_value = expected

            result = await unsubscribe("sub-123")

            mock_to_thread.assert_awaited_once_with(mock_unsub, "sub-123")
            assert result == expected

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
