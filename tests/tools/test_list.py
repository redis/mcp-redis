"""
Unit tests for src/tools/list.py
"""

import pytest
from redis.exceptions import RedisError

from src.tools.list import llen, lpop, lpush, lrange, rpop, rpush, lrem


class TestListOperations:
    """Test cases for Redis list operations."""

    @pytest.mark.asyncio
    async def test_lpush_success(self, mock_redis_connection_manager):
        """Test successful left push operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lpush.return_value = 2  # New length of list

        result = await lpush("test_list", "value1")

        mock_redis.lpush.assert_called_once_with("test_list", "value1")
        assert "Value 'value1' pushed to the left of list 'test_list'" in result

    @pytest.mark.asyncio
    async def test_lpush_with_expiration(self, mock_redis_connection_manager):
        """Test left push operation with expiration."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lpush.return_value = 1
        mock_redis.expire.return_value = True

        result = await lpush("test_list", "value1", 60)

        mock_redis.lpush.assert_called_once_with("test_list", "value1")
        mock_redis.expire.assert_called_once_with("test_list", 60)
        # The implementation doesn't include expiration info in the message
        assert "Value 'value1' pushed to the left of list 'test_list'" in result

    @pytest.mark.asyncio
    async def test_lpush_redis_error(self, mock_redis_connection_manager):
        """Test left push operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lpush.side_effect = RedisError("Connection failed")

        result = await lpush("test_list", "value1")

        assert "Error pushing value to list 'test_list': Connection failed" in result

    @pytest.mark.asyncio
    async def test_rpush_success(self, mock_redis_connection_manager):
        """Test successful right push operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.rpush.return_value = 3

        result = await rpush("test_list", "value2")

        mock_redis.rpush.assert_called_once_with("test_list", "value2")
        assert "Value 'value2' pushed to the right of list 'test_list'" in result

    @pytest.mark.asyncio
    async def test_rpush_with_expiration(self, mock_redis_connection_manager):
        """Test right push operation with expiration."""
        mock_redis = mock_redis_connection_manager
        mock_redis.rpush.return_value = 1
        mock_redis.expire.return_value = True

        result = await rpush("test_list", "value2", 120)

        mock_redis.rpush.assert_called_once_with("test_list", "value2")
        mock_redis.expire.assert_called_once_with("test_list", 120)
        # The implementation doesn't include expiration info in the message
        assert "Value 'value2' pushed to the right of list 'test_list'" in result

    @pytest.mark.asyncio
    async def test_rpush_redis_error(self, mock_redis_connection_manager):
        """Test right push operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.rpush.side_effect = RedisError("Connection failed")

        result = await rpush("test_list", "value2")

        assert "Error pushing value to list 'test_list': Connection failed" in result

    @pytest.mark.asyncio
    async def test_lpop_success(self, mock_redis_connection_manager):
        """Test successful left pop operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lpop.return_value = "popped_value"

        result = await lpop("test_list")

        mock_redis.lpop.assert_called_once_with("test_list")
        assert result == "popped_value"

    @pytest.mark.asyncio
    async def test_lpop_empty_list(self, mock_redis_connection_manager):
        """Test left pop operation on empty list."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lpop.return_value = None

        result = await lpop("empty_list")

        assert "List 'empty_list' is empty" in result

    @pytest.mark.asyncio
    async def test_lpop_redis_error(self, mock_redis_connection_manager):
        """Test left pop operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lpop.side_effect = RedisError("Connection failed")

        result = await lpop("test_list")

        assert "Error popping value from list 'test_list': Connection failed" in result

    @pytest.mark.asyncio
    async def test_rpop_success(self, mock_redis_connection_manager):
        """Test successful right pop operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.rpop.return_value = "right_popped_value"

        result = await rpop("test_list")

        mock_redis.rpop.assert_called_once_with("test_list")
        assert result == "right_popped_value"

    @pytest.mark.asyncio
    async def test_rpop_empty_list(self, mock_redis_connection_manager):
        """Test right pop operation on empty list."""
        mock_redis = mock_redis_connection_manager
        mock_redis.rpop.return_value = None

        result = await rpop("empty_list")

        assert "List 'empty_list' is empty" in result

    @pytest.mark.asyncio
    async def test_rpop_redis_error(self, mock_redis_connection_manager):
        """Test right pop operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.rpop.side_effect = RedisError("Connection failed")

        result = await rpop("test_list")

        assert "Error popping value from list 'test_list': Connection failed" in result

    @pytest.mark.asyncio
    async def test_lrange_success(self, mock_redis_connection_manager):
        """Test successful list range operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lrange.return_value = ["item1", "item2", "item3"]

        result = await lrange("test_list", 0, 2)

        mock_redis.lrange.assert_called_once_with("test_list", 0, 2)
        assert result == '["item1", "item2", "item3"]'

    @pytest.mark.asyncio
    async def test_lrange_default_parameters(self, mock_redis_connection_manager):
        """Test list range operation with default parameters."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lrange.return_value = ["item1", "item2"]

        result = await lrange("test_list", 0, -1)

        mock_redis.lrange.assert_called_once_with("test_list", 0, -1)
        assert result == '["item1", "item2"]'

    @pytest.mark.asyncio
    async def test_lrange_empty_list(self, mock_redis_connection_manager):
        """Test list range operation on empty list."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lrange.return_value = []

        result = await lrange("empty_list", 0, -1)

        assert "List 'empty_list' is empty or does not exist" in result

    @pytest.mark.asyncio
    async def test_lrange_redis_error(self, mock_redis_connection_manager):
        """Test list range operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lrange.side_effect = RedisError("Connection failed")

        result = await lrange("test_list", 0, -1)

        assert (
            "Error retrieving values from list 'test_list': Connection failed" in result
        )

    @pytest.mark.asyncio
    async def test_llen_success(self, mock_redis_connection_manager):
        """Test successful list length operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.llen.return_value = 5

        result = await llen("test_list")

        mock_redis.llen.assert_called_once_with("test_list")
        assert result == 5

    @pytest.mark.asyncio
    async def test_llen_empty_list(self, mock_redis_connection_manager):
        """Test list length operation on empty list."""
        mock_redis = mock_redis_connection_manager
        mock_redis.llen.return_value = 0

        result = await llen("empty_list")

        assert result == 0

    @pytest.mark.asyncio
    async def test_llen_redis_error(self, mock_redis_connection_manager):
        """Test list length operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.llen.side_effect = RedisError("Connection failed")

        result = await llen("test_list")

        assert (
            "Error retrieving length of list 'test_list': Connection failed" in result
        )

    @pytest.mark.asyncio
    async def test_push_operations_with_numeric_values(
        self, mock_redis_connection_manager
    ):
        """Test push operations with numeric values."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lpush.return_value = 1
        mock_redis.rpush.return_value = 2

        # Test with integer
        result1 = await lpush("test_list", 42)
        mock_redis.lpush.assert_called_with("test_list", 42)

        # Test with float
        result2 = await rpush("test_list", 3.14)
        mock_redis.rpush.assert_called_with("test_list", 3.14)

        assert "pushed to the left of list" in result1
        assert "pushed to the right of list" in result2

    @pytest.mark.asyncio
    async def test_lrange_with_negative_indices(self, mock_redis_connection_manager):
        """Test list range operation with negative indices."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lrange.return_value = ["last_item"]

        result = await lrange("test_list", -1, -1)

        mock_redis.lrange.assert_called_once_with("test_list", -1, -1)
        assert result == '["last_item"]'

    @pytest.mark.asyncio
    async def test_expiration_error_handling(self, mock_redis_connection_manager):
        """Test expiration error handling in push operations."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lpush.return_value = 1
        mock_redis.expire.side_effect = RedisError("Expire failed")

        result = await lpush("test_list", "value", 60)

        # Should report the expire error
        assert "Error pushing value to list 'test_list': Expire failed" in result

    @pytest.mark.asyncio
    async def test_push_operations_return_new_length(
        self, mock_redis_connection_manager
    ):
        """Test that push operations handle return values correctly."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lpush.return_value = 3
        mock_redis.rpush.return_value = 4

        result1 = await lpush("test_list", "value1")
        result2 = await rpush("test_list", "value2")

        # Results should indicate successful push regardless of return value
        assert "pushed to the left of list" in result1
        assert "pushed to the right of list" in result2

    @pytest.mark.asyncio
    async def test_lrem_success_single_removal(self, mock_redis_connection_manager):
        """Test successful removal of a single matching element."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lrem.return_value = 1

        result = await lrem("test_list", 1, "value1")

        mock_redis.lrem.assert_called_once_with("test_list", 1, "value1")
        assert "Removed 1 occurrence(s) of 'value1' from list 'test_list'" in result

    @pytest.mark.asyncio
    async def test_lrem_success_multiple_removal(self, mock_redis_connection_manager):
        """Test successful removal of multiple matching elements."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lrem.return_value = 3

        result = await lrem("test_list", 0, "value1")

        mock_redis.lrem.assert_called_once_with("test_list", 0, "value1")
        assert "Removed 3 occurrence(s) of 'value1' from list 'test_list'" in result

    @pytest.mark.asyncio
    async def test_lrem_no_elements_removed(self, mock_redis_connection_manager):
        """Test when no elements are removed because the element is not found."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lrem.return_value = 0

        result = await lrem("test_list", 2, "missing_value")

        mock_redis.lrem.assert_called_once_with("test_list", 2, "missing_value")
        assert (
            "Element 'missing_value' not found in list 'test_list' or list does not exist."
            in result
        )

    @pytest.mark.asyncio
    async def test_lrem_negative_count(self, mock_redis_connection_manager):
        """Test removal with a negative count (from tail)."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lrem.return_value = 2

        result = await lrem("test_list", -2, "value_tail")

        mock_redis.lrem.assert_called_once_with("test_list", -2, "value_tail")
        assert "Removed 2 occurrence(s) of 'value_tail' from list 'test_list'" in result

    @pytest.mark.asyncio
    async def test_lrem_redis_error(self, mock_redis_connection_manager):
        """Test error handling during lrem operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.lrem.side_effect = RedisError("Connection failed")

        result = await lrem("test_list", 1, "value1")

        assert (
            "Error removing element from list 'test_list': Connection failed" in result
        )
