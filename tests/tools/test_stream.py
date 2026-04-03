"""
Unit tests for src/tools/stream.py
"""

from unittest.mock import Mock, patch

import pytest
from redis.exceptions import RedisError

from src.tools.stream import (
    xack,
    xadd,
    xdel,
    xgroup_create,
    xgroup_destroy,
    xrange,
    xreadgroup,
)


class TestStreamOperations:
    """Test cases for Redis stream operations."""

    @pytest.mark.asyncio
    async def test_xadd_success(self, mock_redis_connection_manager):
        """Test successful stream add operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xadd.return_value = "1234567890123-0"  # Stream entry ID

        fields = {"field1": "value1", "field2": "value2"}
        result = await xadd("test_stream", fields)

        mock_redis.xadd.assert_called_once_with("test_stream", fields)
        assert "Successfully added entry 1234567890123-0 to test_stream" in result
        assert "1234567890123-0" in result

    @pytest.mark.asyncio
    async def test_xadd_with_expiration(self, mock_redis_connection_manager):
        """Test stream add operation with expiration."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xadd.return_value = "1234567890124-0"
        mock_redis.expire.return_value = True

        fields = {"message": "test message"}
        result = await xadd("test_stream", fields, 60)

        mock_redis.xadd.assert_called_once_with("test_stream", fields)
        mock_redis.expire.assert_called_once_with("test_stream", 60)
        assert "with expiration 60 seconds" in result

    @pytest.mark.asyncio
    async def test_xadd_single_field(self, mock_redis_connection_manager):
        """Test stream add operation with single field."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xadd.return_value = "1234567890125-0"

        fields = {"message": "single field message"}
        result = await xadd("test_stream", fields)

        mock_redis.xadd.assert_called_once_with("test_stream", fields)
        assert "Successfully added entry 1234567890125-0 to test_stream" in result

    @pytest.mark.asyncio
    async def test_xadd_redis_error(self, mock_redis_connection_manager):
        """Test stream add operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xadd.side_effect = RedisError("Connection failed")

        fields = {"field1": "value1"}
        result = await xadd("test_stream", fields)

        assert "Error adding to stream test_stream: Connection failed" in result

    @pytest.mark.asyncio
    async def test_xadd_with_numeric_values(self, mock_redis_connection_manager):
        """Test stream add operation with numeric field values."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xadd.return_value = "1234567890126-0"

        fields = {"count": 42, "price": 19.99, "active": True}
        result = await xadd("test_stream", fields)

        mock_redis.xadd.assert_called_once_with("test_stream", fields)
        assert "Successfully added entry 1234567890126-0 to test_stream" in result

    @pytest.mark.asyncio
    async def test_xrange_success(self, mock_redis_connection_manager):
        """Test successful stream range operation."""
        mock_redis = mock_redis_connection_manager
        mock_entries = [
            ("1234567890123-0", {"field1": "value1", "field2": "value2"}),
            ("1234567890124-0", {"field1": "value3", "field2": "value4"}),
        ]
        mock_redis.xrange.return_value = mock_entries

        result = await xrange("test_stream")

        mock_redis.xrange.assert_called_once_with("test_stream", count=1)
        assert result == str(mock_entries)

    @pytest.mark.asyncio
    async def test_xrange_with_custom_count(self, mock_redis_connection_manager):
        """Test stream range operation with custom count."""
        mock_redis = mock_redis_connection_manager
        mock_entries = [
            ("1234567890123-0", {"message": "entry1"}),
            ("1234567890124-0", {"message": "entry2"}),
            ("1234567890125-0", {"message": "entry3"}),
        ]
        mock_redis.xrange.return_value = mock_entries

        result = await xrange("test_stream", 3)

        mock_redis.xrange.assert_called_once_with("test_stream", count=3)
        assert result == str(mock_entries)
        # Check the original mock_entries length
        assert len(mock_entries) == 3

    @pytest.mark.asyncio
    async def test_xrange_empty_stream(self, mock_redis_connection_manager):
        """Test stream range operation on empty stream."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xrange.return_value = []

        result = await xrange("empty_stream")

        assert "Stream empty_stream is empty or does not exist" in result

    @pytest.mark.asyncio
    async def test_xrange_redis_error(self, mock_redis_connection_manager):
        """Test stream range operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xrange.side_effect = RedisError("Connection failed")

        result = await xrange("test_stream")

        assert "Error reading from stream test_stream: Connection failed" in result

    @pytest.mark.asyncio
    async def test_xdel_success(self, mock_redis_connection_manager):
        """Test successful stream delete operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xdel.return_value = 1  # Number of entries deleted

        result = await xdel("test_stream", "1234567890123-0")

        mock_redis.xdel.assert_called_once_with("test_stream", "1234567890123-0")
        assert "Successfully deleted entry 1234567890123-0 from test_stream" in result

    @pytest.mark.asyncio
    async def test_xdel_entry_not_found(self, mock_redis_connection_manager):
        """Test stream delete operation when entry doesn't exist."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xdel.return_value = 0  # No entries deleted

        result = await xdel("test_stream", "nonexistent-entry-id")

        assert "Entry nonexistent-entry-id not found in test_stream" in result

    @pytest.mark.asyncio
    async def test_xdel_redis_error(self, mock_redis_connection_manager):
        """Test stream delete operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xdel.side_effect = RedisError("Connection failed")

        result = await xdel("test_stream", "1234567890123-0")

        assert "Error deleting from stream test_stream: Connection failed" in result

    @pytest.mark.asyncio
    async def test_xgroup_create_success(self, mock_redis_connection_manager):
        """Test successful consumer group creation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xgroup_create.return_value = True

        result = await xgroup_create("test_stream", "workers")

        mock_redis.xgroup_create.assert_called_once_with(
            "test_stream", "workers", id="$", mkstream=True
        )
        assert (
            result
            == "Successfully created consumer group 'workers' on stream 'test_stream'"
        )

    @pytest.mark.asyncio
    async def test_xgroup_create_with_custom_options(
        self, mock_redis_connection_manager
    ):
        """Test consumer group creation with explicit start ID and no stream creation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xgroup_create.return_value = True

        result = await xgroup_create(
            "test_stream", "workers", start_id="0-0", mkstream=False
        )

        mock_redis.xgroup_create.assert_called_once_with(
            "test_stream", "workers", id="0-0", mkstream=False
        )
        assert (
            result
            == "Successfully created consumer group 'workers' on stream 'test_stream'"
        )

    @pytest.mark.asyncio
    async def test_xgroup_create_redis_error(self, mock_redis_connection_manager):
        """Test consumer group creation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xgroup_create.side_effect = RedisError(
            "BUSYGROUP Consumer Group name already exists"
        )

        result = await xgroup_create("test_stream", "workers")

        assert (
            "Error creating consumer group 'workers' on stream 'test_stream': "
            "BUSYGROUP Consumer Group name already exists"
        ) == result

    @pytest.mark.asyncio
    async def test_xgroup_destroy_success(self, mock_redis_connection_manager):
        """Test successful consumer group destroy."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xgroup_destroy.return_value = 1

        result = await xgroup_destroy("test_stream", "workers")

        mock_redis.xgroup_destroy.assert_called_once_with("test_stream", "workers")
        assert (
            result
            == "Successfully destroyed consumer group 'workers' on stream 'test_stream'"
        )

    @pytest.mark.asyncio
    async def test_xgroup_destroy_group_not_found(self, mock_redis_connection_manager):
        """Test consumer group destroy when group does not exist."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xgroup_destroy.return_value = 0

        result = await xgroup_destroy("test_stream", "workers")

        mock_redis.xgroup_destroy.assert_called_once_with("test_stream", "workers")
        assert result == "Consumer group 'workers' not found on stream 'test_stream'"

    @pytest.mark.asyncio
    async def test_xgroup_destroy_redis_error(self, mock_redis_connection_manager):
        """Test consumer group destroy with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xgroup_destroy.side_effect = RedisError("NOGROUP No such key")

        result = await xgroup_destroy("test_stream", "workers")

        assert (
            "Error destroying consumer group 'workers' on stream 'test_stream': "
            "NOGROUP No such key"
        ) == result

    @pytest.mark.asyncio
    async def test_xreadgroup_success(self, mock_redis_connection_manager):
        """Test successful consumer-group read."""
        mock_redis = mock_redis_connection_manager
        mock_entries = [
            ("test_stream", [("1234567890123-0", {"field1": "value1"})]),
        ]
        mock_redis.xreadgroup.return_value = mock_entries

        result = await xreadgroup("test_stream", "workers", "consumer-1")

        mock_redis.xreadgroup.assert_called_once_with(
            "workers",
            "consumer-1",
            {"test_stream": ">"},
            count=1,
            block=None,
        )
        assert result == str(mock_entries)

    @pytest.mark.asyncio
    async def test_xreadgroup_with_count_and_block(self, mock_redis_connection_manager):
        """Test consumer-group read with count and blocking options."""
        mock_redis = mock_redis_connection_manager
        mock_entries = [
            ("test_stream", [("1234567890124-0", {"field1": "value2"})]),
        ]
        mock_redis.xreadgroup.return_value = mock_entries

        result = await xreadgroup(
            "test_stream",
            "workers",
            "consumer-1",
            count=5,
            block_ms=1000,
            stream_id="0",
        )

        mock_redis.xreadgroup.assert_called_once_with(
            "workers",
            "consumer-1",
            {"test_stream": "0"},
            count=5,
            block=1000,
        )
        assert result == str(mock_entries)

    @pytest.mark.asyncio
    async def test_xreadgroup_count_validation(self, mock_redis_connection_manager):
        """Test consumer-group read validates count > 0."""
        mock_redis = mock_redis_connection_manager

        result = await xreadgroup("test_stream", "workers", "consumer-1", count=0)

        mock_redis.xreadgroup.assert_not_called()
        assert result == "count must be greater than 0"

    @pytest.mark.asyncio
    async def test_xreadgroup_block_ms_negative_validation(
        self, mock_redis_connection_manager
    ):
        """Test consumer-group read rejects negative block_ms values."""
        mock_redis = mock_redis_connection_manager

        result = await xreadgroup("test_stream", "workers", "consumer-1", block_ms=-1)

        mock_redis.xreadgroup.assert_not_called()
        assert result == "block_ms must be greater than or equal to 0"

    @pytest.mark.asyncio
    async def test_xreadgroup_block_ms_zero_validation(
        self, mock_redis_connection_manager
    ):
        """Test consumer-group read rejects block_ms=0."""
        mock_redis = mock_redis_connection_manager

        result = await xreadgroup("test_stream", "workers", "consumer-1", block_ms=0)

        mock_redis.xreadgroup.assert_not_called()
        assert result == (
            "block_ms=0 is not allowed; use None for a non-blocking read or a "
            "positive timeout in milliseconds"
        )

    @pytest.mark.asyncio
    async def test_xreadgroup_block_ms_upper_bound_validation(
        self, mock_redis_connection_manager
    ):
        """Test consumer-group read rejects excessively large block_ms values."""
        mock_redis = mock_redis_connection_manager

        result = await xreadgroup("test_stream", "workers", "consumer-1", block_ms=300000)

        mock_redis.xreadgroup.assert_not_called()
        assert result == "block_ms must be less than or equal to 5000 milliseconds"

    @pytest.mark.asyncio
    async def test_xreadgroup_empty_result(self, mock_redis_connection_manager):
        """Test consumer-group read when no entries are available."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xreadgroup.return_value = []

        result = await xreadgroup("test_stream", "workers", "consumer-1")

        assert (
            result
            == "No entries available for consumer 'consumer-1' in group 'workers' on stream 'test_stream'"
        )

    @pytest.mark.asyncio
    async def test_xreadgroup_rejects_zero_block_ms(
        self, mock_redis_connection_manager
    ):
        """Test that block_ms=0 is rejected to avoid indefinite blocking."""
        mock_redis = mock_redis_connection_manager

        result = await xreadgroup("test_stream", "workers", "consumer-1", block_ms=0)

        mock_redis.xreadgroup.assert_not_called()
        assert result == (
            "block_ms=0 is not allowed; use None for a non-blocking read or a "
            "positive timeout in milliseconds"
        )

    @pytest.mark.asyncio
    async def test_xreadgroup_redis_error(self, mock_redis_connection_manager):
        """Test consumer-group read with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xreadgroup.side_effect = RedisError("NOGROUP No such key")

        result = await xreadgroup("test_stream", "workers", "consumer-1")

        assert (
            "Error reading from stream test_stream with consumer group 'workers': "
            "NOGROUP No such key"
        ) == result

    @pytest.mark.asyncio
    async def test_xack_success(self, mock_redis_connection_manager):
        """Test successful acknowledgment of stream entries."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xack.return_value = 2

        result = await xack(
            "test_stream", "workers", ["1234567890123-0", "1234567890124-0"]
        )

        mock_redis.xack.assert_called_once_with(
            "test_stream", "workers", "1234567890123-0", "1234567890124-0"
        )
        assert (
            result
            == "Successfully acknowledged 2 entries in group 'workers' on stream 'test_stream'"
        )

    @pytest.mark.asyncio
    async def test_xack_single_entry_success(self, mock_redis_connection_manager):
        """Test successful acknowledgment of a single stream entry."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xack.return_value = 1

        result = await xack("test_stream", "workers", ["1234567890123-0"])

        mock_redis.xack.assert_called_once_with(
            "test_stream", "workers", "1234567890123-0"
        )
        assert (
            result
            == "Successfully acknowledged 1 entry in group 'workers' on stream 'test_stream'"
        )

    @pytest.mark.asyncio
    async def test_xack_requires_entry_ids(self, mock_redis_connection_manager):
        """Test acknowledgment validation when no entry IDs are supplied."""
        mock_redis = mock_redis_connection_manager

        result = await xack("test_stream", "workers", [])

        mock_redis.xack.assert_not_called()
        assert (
            result == "At least one entry ID is required to acknowledge stream entries"
        )

    @pytest.mark.asyncio
    async def test_xack_redis_error(self, mock_redis_connection_manager):
        """Test acknowledgment with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xack.side_effect = RedisError("NOGROUP No such key")

        result = await xack("test_stream", "workers", ["1234567890123-0"])

        assert (
            "Error acknowledging entries for consumer group 'workers' on stream "
            "'test_stream': NOGROUP No such key"
        ) == result

    @pytest.mark.asyncio
    async def test_xadd_with_empty_fields(self, mock_redis_connection_manager):
        """Test stream add operation with empty fields dictionary."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xadd.return_value = "1234567890127-0"

        fields = {}
        result = await xadd("test_stream", fields)

        mock_redis.xadd.assert_called_once_with("test_stream", fields)
        assert "Successfully added entry 1234567890127-0 to test_stream" in result

    @pytest.mark.asyncio
    async def test_xadd_with_unicode_values(self, mock_redis_connection_manager):
        """Test stream add operation with unicode field values."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xadd.return_value = "1234567890128-0"

        fields = {"message": "Hello 世界 🌍", "user": "测试用户"}
        result = await xadd("test_stream", fields)

        mock_redis.xadd.assert_called_once_with("test_stream", fields)
        assert "Successfully added entry 1234567890128-0 to test_stream" in result

    @pytest.mark.asyncio
    async def test_xrange_large_count(self, mock_redis_connection_manager):
        """Test stream range operation with large count."""
        mock_redis = mock_redis_connection_manager
        mock_entries = [
            (f"123456789012{i}-0", {"data": f"entry_{i}"}) for i in range(100)
        ]
        mock_redis.xrange.return_value = mock_entries

        result = await xrange("test_stream", 100)

        mock_redis.xrange.assert_called_once_with("test_stream", count=100)
        # The function returns a string representation
        assert result == str(mock_entries)
        # Check the original mock_entries length
        assert len(mock_entries) == 100

    @pytest.mark.asyncio
    async def test_xdel_multiple_entries_behavior(self, mock_redis_connection_manager):
        """Test that xdel function handles single entry correctly."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xdel.return_value = 1

        result = await xdel("test_stream", "single-entry-id")

        # Should call xdel with single entry ID, not multiple
        mock_redis.xdel.assert_called_once_with("test_stream", "single-entry-id")
        assert "Successfully deleted entry single-entry-id from test_stream" in result

    @pytest.mark.asyncio
    async def test_xadd_expiration_error(self, mock_redis_connection_manager):
        """Test stream add operation when expiration fails."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xadd.return_value = "1234567890129-0"
        mock_redis.expire.side_effect = RedisError("Expire failed")

        fields = {"message": "test"}
        result = await xadd("test_stream", fields, 60)

        assert "Error adding to stream test_stream: Expire failed" in result

    @pytest.mark.asyncio
    async def test_xrange_single_entry(self, mock_redis_connection_manager):
        """Test stream range operation returning single entry."""
        mock_redis = mock_redis_connection_manager
        mock_entries = [("1234567890123-0", {"single": "entry"})]
        mock_redis.xrange.return_value = mock_entries

        result = await xrange("test_stream", 1)

        assert result == "[('1234567890123-0', {'single': 'entry'})]"
        # Check the original mock_entries length
        assert len(mock_entries) == 1

    @pytest.mark.asyncio
    async def test_connection_manager_called_correctly(self):
        """Test that RedisConnectionManager.get_connection is called correctly."""
        with patch(
            "src.tools.stream.RedisConnectionManager.get_connection"
        ) as mock_get_conn:
            mock_redis = Mock()
            mock_redis.xadd.return_value = "1234567890123-0"
            mock_get_conn.return_value = mock_redis

            await xadd("test_stream", {"field": "value"})

            mock_get_conn.assert_called_once()

    @pytest.mark.asyncio
    async def test_function_signatures(self):
        """Test that functions have correct signatures."""
        import inspect

        # Test xadd function signature
        xadd_sig = inspect.signature(xadd)
        xadd_params = list(xadd_sig.parameters.keys())
        assert xadd_params == ["key", "fields", "expiration"]
        assert xadd_sig.parameters["expiration"].default is None

        # Test xrange function signature
        xrange_sig = inspect.signature(xrange)
        xrange_params = list(xrange_sig.parameters.keys())
        assert xrange_params == ["key", "count"]
        assert xrange_sig.parameters["count"].default == 1

        # Test xdel function signature
        xdel_sig = inspect.signature(xdel)
        xdel_params = list(xdel_sig.parameters.keys())
        assert xdel_params == ["key", "entry_id"]

        # Test xgroup_create function signature
        xgroup_create_sig = inspect.signature(xgroup_create)
        xgroup_create_params = list(xgroup_create_sig.parameters.keys())
        assert xgroup_create_params == ["key", "group_name", "start_id", "mkstream"]
        assert xgroup_create_sig.parameters["start_id"].default == "$"
        assert xgroup_create_sig.parameters["mkstream"].default is True

        # Test xgroup_destroy function signature
        xgroup_destroy_sig = inspect.signature(xgroup_destroy)
        xgroup_destroy_params = list(xgroup_destroy_sig.parameters.keys())
        assert xgroup_destroy_params == ["key", "group_name"]

        # Test xreadgroup function signature
        xreadgroup_sig = inspect.signature(xreadgroup)
        xreadgroup_params = list(xreadgroup_sig.parameters.keys())
        assert xreadgroup_params == [
            "key",
            "group_name",
            "consumer_name",
            "count",
            "block_ms",
            "stream_id",
        ]
        assert xreadgroup_sig.parameters["count"].default == 1
        assert xreadgroup_sig.parameters["block_ms"].default is None
        assert xreadgroup_sig.parameters["stream_id"].default == ">"

        # Test xack function signature
        xack_sig = inspect.signature(xack)
        xack_params = list(xack_sig.parameters.keys())
        assert xack_params == ["key", "group_name", "entry_ids"]

    @pytest.mark.asyncio
    async def test_xadd_with_complex_fields(self, mock_redis_connection_manager):
        """Test stream add operation with complex field structure."""
        mock_redis = mock_redis_connection_manager
        mock_redis.xadd.return_value = "1234567890130-0"

        fields = {
            "event_type": "user_action",
            "user_id": "12345",
            "timestamp": "2024-01-01T12:00:00Z",
            "metadata": '{"browser": "chrome", "version": "120"}',
            "score": 95.5,
            "active": True,
        }
        result = await xadd("events_stream", fields)

        mock_redis.xadd.assert_called_once_with("events_stream", fields)
        assert "Successfully added entry 1234567890130-0 to events_stream" in result
