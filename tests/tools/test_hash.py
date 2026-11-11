"""
Unit tests for src/tools/hash.py
"""

import numpy as np
import pytest
from redis.exceptions import RedisError

from src.tools.hash import (
    get_vector_from_hash,
    hdel,
    hexists,
    hget,
    hgetall,
    hset,
    set_vector_in_hash,
)


class TestHashOperations:
    """Test cases for Redis hash operations."""

    @pytest.mark.asyncio
    async def test_hset_success(self, mock_redis_connection_manager):
        """Test successful hash set operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hset.return_value = 1

        result = await hset("test_hash", "field1", "value1")

        mock_redis.hset.assert_called_once_with("test_hash", "field1", "value1")
        assert "Field 'field1' set successfully in hash 'test_hash'." in result

    @pytest.mark.asyncio
    async def test_hset_with_expiration(self, mock_redis_connection_manager):
        """Test hash set operation with expiration."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hset.return_value = 1
        mock_redis.expire.return_value = True

        result = await hset("test_hash", "field1", "value1", 60)

        mock_redis.hset.assert_called_once_with("test_hash", "field1", "value1")
        mock_redis.expire.assert_called_once_with("test_hash", 60)
        assert "Expires in 60 seconds." in result

    @pytest.mark.asyncio
    async def test_hset_integer_value(self, mock_redis_connection_manager):
        """Test hash set operation with integer value."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hset.return_value = 1

        result = await hset("test_hash", "count", 42)

        mock_redis.hset.assert_called_once_with("test_hash", "count", "42")
        assert "Field 'count' set successfully in hash 'test_hash'." in result

    @pytest.mark.asyncio
    async def test_hset_float_value(self, mock_redis_connection_manager):
        """Test hash set operation with float value."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hset.return_value = 1

        result = await hset("test_hash", "price", 19.99)

        mock_redis.hset.assert_called_once_with("test_hash", "price", "19.99")
        assert "Field 'price' set successfully in hash 'test_hash'." in result

    @pytest.mark.asyncio
    async def test_hset_redis_error(self, mock_redis_connection_manager):
        """Test hash set operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hset.side_effect = RedisError("Connection failed")

        result = await hset("test_hash", "field1", "value1")

        assert (
            "Error setting field 'field1' in hash 'test_hash': Connection failed"
            in result
        )

    @pytest.mark.asyncio
    async def test_hget_success(self, mock_redis_connection_manager):
        """Test successful hash get operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hget.return_value = "value1"

        result = await hget("test_hash", "field1")

        mock_redis.hget.assert_called_once_with("test_hash", "field1")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_hget_field_not_found(self, mock_redis_connection_manager):
        """Test hash get operation when field doesn't exist."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hget.return_value = None

        result = await hget("test_hash", "nonexistent_field")

        assert "Field 'nonexistent_field' not found in hash 'test_hash'" in result

    @pytest.mark.asyncio
    async def test_hget_redis_error(self, mock_redis_connection_manager):
        """Test hash get operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hget.side_effect = RedisError("Connection failed")

        result = await hget("test_hash", "field1")

        assert (
            "Error getting field 'field1' from hash 'test_hash': Connection failed"
            in result
        )

    @pytest.mark.asyncio
    async def test_hgetall_success(self, mock_redis_connection_manager):
        """Test successful hash get all operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hgetall.return_value = {"field1": "value1", "field2": "value2"}

        result = await hgetall("test_hash")

        mock_redis.hgetall.assert_called_once_with("test_hash")
        assert result == {"field1": "value1", "field2": "value2"}

    @pytest.mark.asyncio
    async def test_hgetall_empty_hash(self, mock_redis_connection_manager):
        """Test hash get all operation on empty hash."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hgetall.return_value = {}

        result = await hgetall("empty_hash")

        assert "Hash 'empty_hash' is empty or does not exist" in result

    @pytest.mark.asyncio
    async def test_hgetall_redis_error(self, mock_redis_connection_manager):
        """Test hash get all operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hgetall.side_effect = RedisError("Connection failed")

        result = await hgetall("test_hash")

        assert (
            "Error getting all fields from hash 'test_hash': Connection failed"
            in result
        )

    @pytest.mark.asyncio
    async def test_hdel_success(self, mock_redis_connection_manager):
        """Test successful hash delete operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hdel.return_value = 1

        result = await hdel("test_hash", "field1")

        mock_redis.hdel.assert_called_once_with("test_hash", "field1")
        assert "Field 'field1' deleted from hash 'test_hash'." in result

    @pytest.mark.asyncio
    async def test_hdel_field_not_found(self, mock_redis_connection_manager):
        """Test hash delete operation when field doesn't exist."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hdel.return_value = 0

        result = await hdel("test_hash", "nonexistent_field")

        assert "Field 'nonexistent_field' not found in hash 'test_hash'" in result

    @pytest.mark.asyncio
    async def test_hdel_redis_error(self, mock_redis_connection_manager):
        """Test hash delete operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hdel.side_effect = RedisError("Connection failed")

        result = await hdel("test_hash", "field1")

        assert (
            "Error deleting field 'field1' from hash 'test_hash': Connection failed"
            in result
        )

    @pytest.mark.asyncio
    async def test_hexists_field_exists(self, mock_redis_connection_manager):
        """Test hash exists operation when field exists."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hexists.return_value = True

        result = await hexists("test_hash", "field1")

        mock_redis.hexists.assert_called_once_with("test_hash", "field1")
        assert result is True

    @pytest.mark.asyncio
    async def test_hexists_field_not_exists(self, mock_redis_connection_manager):
        """Test hash exists operation when field doesn't exist."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hexists.return_value = False

        result = await hexists("test_hash", "nonexistent_field")

        assert result is False

    @pytest.mark.asyncio
    async def test_hexists_redis_error(self, mock_redis_connection_manager):
        """Test hash exists operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hexists.side_effect = RedisError("Connection failed")

        result = await hexists("test_hash", "field1")

        assert (
            "Error checking existence of field 'field1' in hash 'test_hash': Connection failed"
            in result
        )

    @pytest.mark.asyncio
    async def test_set_vector_in_hash_success(
        self, mock_redis_connection_manager, mock_numpy_array
    ):
        """Test successful vector set operation in hash."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hset.return_value = 1

        vector = [0.1, 0.2, 0.3, 0.4, 0.5]
        result = await set_vector_in_hash("test_hash", vector)

        mock_numpy_array.assert_called_once_with(vector, dtype=np.float32)
        mock_redis.hset.assert_called_once_with(
            "test_hash", "vector", b"mock_binary_data"
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_set_vector_in_hash_custom_field(
        self, mock_redis_connection_manager, mock_numpy_array
    ):
        """Test vector set operation with custom field name."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hset.return_value = 1

        vector = [0.1, 0.2, 0.3]
        result = await set_vector_in_hash("test_hash", vector, "custom_vector")

        mock_redis.hset.assert_called_once_with(
            "test_hash", "custom_vector", b"mock_binary_data"
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_set_vector_in_hash_redis_error(
        self, mock_redis_connection_manager, mock_numpy_array
    ):
        """Test vector set operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hset.side_effect = RedisError("Connection failed")

        vector = [0.1, 0.2, 0.3]
        result = await set_vector_in_hash("test_hash", vector)

        assert (
            "Error storing vector in hash 'test_hash' with field 'vector': Connection failed"
            in result
        )

    @pytest.mark.asyncio
    async def test_get_vector_from_hash_success(
        self, mock_redis_connection_manager, mock_numpy_frombuffer
    ):
        """Test successful vector get operation from hash."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hget.return_value = b"mock_binary_data"

        result = await get_vector_from_hash("test_hash")

        mock_redis.hget.assert_called_once_with("test_hash", "vector")
        mock_numpy_frombuffer.assert_called_once_with(
            b"mock_binary_data", dtype=np.float32
        )
        assert result == [0.1, 0.2, 0.3]

    @pytest.mark.asyncio
    async def test_get_vector_from_hash_custom_field(
        self, mock_redis_connection_manager, mock_numpy_frombuffer
    ):
        """Test vector get operation with custom field name."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hget.return_value = b"mock_binary_data"

        result = await get_vector_from_hash("test_hash", "custom_vector")

        mock_redis.hget.assert_called_once_with("test_hash", "custom_vector")
        assert result == [0.1, 0.2, 0.3]

    @pytest.mark.asyncio
    async def test_get_vector_from_hash_not_found(self, mock_redis_connection_manager):
        """Test vector get operation when field doesn't exist."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hget.return_value = None

        result = await get_vector_from_hash("test_hash")

        assert "Field 'vector' not found in hash 'test_hash'." in result

    @pytest.mark.asyncio
    async def test_get_vector_from_hash_redis_error(
        self, mock_redis_connection_manager
    ):
        """Test vector get operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hget.side_effect = RedisError("Connection failed")

        result = await get_vector_from_hash("test_hash")

        assert (
            "Error retrieving vector field 'vector' from hash 'test_hash': Connection failed"
            in result
        )

    @pytest.mark.asyncio
    async def test_hset_expiration_error(self, mock_redis_connection_manager):
        """Test hash set operation when expiration fails."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hset.return_value = 1
        mock_redis.expire.side_effect = RedisError("Expire failed")

        result = await hset("test_hash", "field1", "value1", 60)

        # Should still report success for hset, but mention expire error
        assert (
            "Error setting field 'field1' in hash 'test_hash': Expire failed" in result
        )

    @pytest.mark.asyncio
    async def test_vector_operations_with_empty_vector(
        self, mock_redis_connection_manager, mock_numpy_array
    ):
        """Test vector operations with empty vector."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hset.return_value = 1

        empty_vector = []
        result = await set_vector_in_hash("test_hash", empty_vector)

        mock_numpy_array.assert_called_once_with(empty_vector, dtype=np.float32)
        assert result is True

    @pytest.mark.asyncio
    async def test_vector_operations_with_large_vector(
        self, mock_redis_connection_manager, mock_numpy_array
    ):
        """Test vector operations with large vector."""
        mock_redis = mock_redis_connection_manager
        mock_redis.hset.return_value = 1

        large_vector = [0.1] * 1000  # 1000-dimensional vector
        result = await set_vector_in_hash("test_hash", large_vector)

        mock_numpy_array.assert_called_once_with(large_vector, dtype=np.float32)
        assert result is True

    @pytest.mark.asyncio
    async def test_hget_empty_string(self, mock_redis_connection_manager):
        """An existing empty field is not a missing field."""
        mock_redis_connection_manager.hget.return_value = ""

        assert await hget("test_hash", "field1") == ""
