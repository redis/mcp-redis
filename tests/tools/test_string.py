"""
Unit tests for src/tools/string.py
"""

from unittest.mock import Mock, patch

import pytest
from redis.exceptions import ConnectionError, RedisError, TimeoutError

from src.tools.string import get, set


class TestStringOperations:
    """Test cases for Redis string operations."""

    @pytest.mark.asyncio
    async def test_set_success(self, mock_redis_connection_manager):
        """Test successful string set operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.set.return_value = True

        result = await set("test_key", "test_value")

        mock_redis.set.assert_called_once_with("test_key", b"test_value")
        assert "Successfully set test_key" in result

    @pytest.mark.asyncio
    async def test_set_with_expiration(self, mock_redis_connection_manager):
        """Test string set operation with expiration."""
        mock_redis = mock_redis_connection_manager
        mock_redis.setex.return_value = True

        result = await set("test_key", "test_value", 60)

        mock_redis.setex.assert_called_once_with("test_key", 60, b"test_value")
        assert "Successfully set test_key" in result
        assert "with expiration 60 seconds" in result

    @pytest.mark.asyncio
    async def test_set_redis_error(self, mock_redis_connection_manager):
        """Test string set operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.set.side_effect = RedisError("Connection failed")

        result = await set("test_key", "test_value")

        assert "Error setting key test_key: Connection failed" in result

    @pytest.mark.asyncio
    async def test_set_connection_error(self, mock_redis_connection_manager):
        """Test string set operation with connection error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.set.side_effect = ConnectionError("Redis server unavailable")

        result = await set("test_key", "test_value")

        assert "Error setting key test_key: Redis server unavailable" in result

    @pytest.mark.asyncio
    async def test_set_timeout_error(self, mock_redis_connection_manager):
        """Test string set operation with timeout error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.setex.side_effect = TimeoutError("Operation timed out")

        result = await set("test_key", "test_value", 30)

        assert "Error setting key test_key: Operation timed out" in result

    @pytest.mark.asyncio
    async def test_get_success(self, mock_redis_connection_manager):
        """Test successful string get operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.get.return_value = "test_value"

        result = await get("test_key")

        mock_redis.get.assert_called_once_with("test_key")
        assert result == "test_value"

    @pytest.mark.asyncio
    async def test_get_key_not_found(self, mock_redis_connection_manager):
        """Test string get operation when key doesn't exist."""
        mock_redis = mock_redis_connection_manager
        mock_redis.get.return_value = None

        result = await get("nonexistent_key")

        mock_redis.get.assert_called_once_with("nonexistent_key")
        assert "Key nonexistent_key does not exist" in result

    @pytest.mark.asyncio
    async def test_get_redis_error(self, mock_redis_connection_manager):
        """Test string get operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.get.side_effect = RedisError("Connection failed")

        result = await get("test_key")

        assert "Error retrieving key test_key: Connection failed" in result

    @pytest.mark.asyncio
    async def test_get_empty_string_value(self, mock_redis_connection_manager):
        """Test string get operation returning empty string."""
        mock_redis = mock_redis_connection_manager
        mock_redis.get.return_value = b""  # Redis returns bytes

        result = await get("test_key")

        # The implementation correctly handles empty bytes and returns empty string
        assert result == ""

    @pytest.mark.asyncio
    async def test_set_with_zero_expiration(self, mock_redis_connection_manager):
        """Test string set operation with zero expiration."""
        mock_redis = mock_redis_connection_manager
        mock_redis.set.return_value = True

        result = await set("test_key", "test_value", 0)

        # Should use regular set, not setex for zero expiration
        mock_redis.set.assert_called_once_with("test_key", b"test_value")
        assert "Successfully set test_key" in result

    @pytest.mark.asyncio
    async def test_set_with_negative_expiration(self, mock_redis_connection_manager):
        """Test string set operation with negative expiration."""
        mock_redis = mock_redis_connection_manager
        mock_redis.setex.return_value = True

        result = await set("test_key", "test_value", -1)

        # Negative expiration is truthy in Python, so setex is called
        mock_redis.setex.assert_called_once_with("test_key", -1, b"test_value")
        assert "Successfully set test_key" in result
        assert "with expiration -1 seconds" in result

    @pytest.mark.asyncio
    async def test_set_with_large_expiration(self, mock_redis_connection_manager):
        """Test string set operation with large expiration value."""
        mock_redis = mock_redis_connection_manager
        mock_redis.setex.return_value = True

        result = await set("test_key", "test_value", 86400)  # 24 hours

        mock_redis.setex.assert_called_once_with("test_key", 86400, b"test_value")
        assert "with expiration 86400 seconds" in result

    @pytest.mark.asyncio
    async def test_get_with_special_characters(self, mock_redis_connection_manager):
        """Test string get operation with special characters in key."""
        mock_redis = mock_redis_connection_manager
        mock_redis.get.return_value = "special_value"

        special_key = "test:key:with:colons"
        result = await get(special_key)

        mock_redis.get.assert_called_once_with(special_key)
        assert result == "special_value"

    @pytest.mark.asyncio
    async def test_set_with_unicode_value(self, mock_redis_connection_manager):
        """Test string set operation with unicode value."""
        mock_redis = mock_redis_connection_manager
        mock_redis.set.return_value = True

        unicode_value = "测试值 🚀"
        result = await set("test_key", unicode_value)

        mock_redis.set.assert_called_once_with(
            "test_key", unicode_value.encode("utf-8")
        )
        assert "Successfully set test_key" in result

    @pytest.mark.asyncio
    async def test_connection_manager_called_correctly(self):
        """Test that RedisConnectionManager.get_connection is called correctly."""
        with patch(
            "src.tools.string.RedisConnectionManager.get_connection"
        ) as mock_get_conn:
            mock_redis = Mock()
            mock_redis.set.return_value = True
            mock_get_conn.return_value = mock_redis

            await set("test_key", "test_value")

            mock_get_conn.assert_called_once()
            # Verify the actual call was made with bytes
            mock_redis.set.assert_called_once_with("test_key", b"test_value")

    @pytest.mark.asyncio
    async def test_function_signatures(self):
        """Test that functions have correct signatures."""
        import inspect

        # Test set function signature
        set_sig = inspect.signature(set)
        set_params = list(set_sig.parameters.keys())
        assert set_params == ["key", "value", "expiration"]
        assert set_sig.parameters["expiration"].default is None

        # Test get function signature
        get_sig = inspect.signature(get)
        get_params = list(get_sig.parameters.keys())
        assert get_params == ["key"]
