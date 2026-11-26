"""
Unit tests for src/tools/set.py
"""

from unittest.mock import Mock, patch

import pytest
from redis.exceptions import RedisError

from src.tools.set import sadd, smembers, srem


class TestSetOperations:
    """Test cases for Redis set operations."""

    @pytest.mark.asyncio
    async def test_sadd_success(self, mock_redis_connection_manager):
        """Test successful set add operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.sadd.return_value = 1  # Number of elements added

        result = await sadd("test_set", "member1")

        mock_redis.sadd.assert_called_once_with("test_set", "member1")
        assert "Value 'member1' added successfully to set 'test_set'" in result

    @pytest.mark.asyncio
    async def test_sadd_with_expiration(self, mock_redis_connection_manager):
        """Test set add operation with expiration."""
        mock_redis = mock_redis_connection_manager
        mock_redis.sadd.return_value = 1
        mock_redis.expire.return_value = True

        result = await sadd("test_set", "member1", 60)

        mock_redis.sadd.assert_called_once_with("test_set", "member1")
        mock_redis.expire.assert_called_once_with("test_set", 60)
        assert "Expires in 60 seconds" in result

    @pytest.mark.asyncio
    async def test_sadd_zero_expiration_does_not_delete_key(
        self, mock_redis_connection_manager
    ):
        """expire_seconds=0 must not call EXPIRE, which would delete the set."""
        mock_redis = mock_redis_connection_manager
        mock_redis.sadd.return_value = 1

        result = await sadd("test_set", "member1", 0)

        mock_redis.sadd.assert_called_once_with("test_set", "member1")
        mock_redis.expire.assert_not_called()
        assert "Expires" not in result

    @pytest.mark.asyncio
    async def test_sadd_member_already_exists(self, mock_redis_connection_manager):
        """Test set add operation when member already exists."""
        mock_redis = mock_redis_connection_manager
        mock_redis.sadd.return_value = 0  # Member already exists

        result = await sadd("test_set", "existing_member")

        assert "Value 'existing_member' added successfully to set 'test_set'" in result

    @pytest.mark.asyncio
    async def test_sadd_redis_error(self, mock_redis_connection_manager):
        """Test set add operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.sadd.side_effect = RedisError("Connection failed")

        result = await sadd("test_set", "member1")

        assert (
            "Error adding value 'member1' to set 'test_set': Connection failed"
            in result
        )

    @pytest.mark.asyncio
    async def test_sadd_numeric_member(self, mock_redis_connection_manager):
        """Test set add operation with numeric member."""
        mock_redis = mock_redis_connection_manager
        mock_redis.sadd.return_value = 1

        result = await sadd("test_set", 42)

        mock_redis.sadd.assert_called_once_with("test_set", 42)
        assert "Value '42' added successfully to set 'test_set'" in result

    @pytest.mark.asyncio
    async def test_srem_success(self, mock_redis_connection_manager):
        """Test successful set remove operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.srem.return_value = 1  # Number of elements removed

        result = await srem("test_set", "member1")

        mock_redis.srem.assert_called_once_with("test_set", "member1")
        assert "Value 'member1' removed from set 'test_set'" in result

    @pytest.mark.asyncio
    async def test_srem_member_not_exists(self, mock_redis_connection_manager):
        """Test set remove operation when member doesn't exist."""
        mock_redis = mock_redis_connection_manager
        mock_redis.srem.return_value = 0  # Member doesn't exist

        result = await srem("test_set", "nonexistent_member")

        assert "Value 'nonexistent_member' not found in set 'test_set'" in result

    @pytest.mark.asyncio
    async def test_srem_redis_error(self, mock_redis_connection_manager):
        """Test set remove operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.srem.side_effect = RedisError("Connection failed")

        result = await srem("test_set", "member1")

        assert (
            "Error removing value 'member1' from set 'test_set': Connection failed"
            in result
        )

    @pytest.mark.asyncio
    async def test_srem_numeric_member(self, mock_redis_connection_manager):
        """Test set remove operation with numeric member."""
        mock_redis = mock_redis_connection_manager
        mock_redis.srem.return_value = 1

        result = await srem("test_set", 42)

        mock_redis.srem.assert_called_once_with("test_set", 42)
        assert "Value '42' removed from set 'test_set'" in result

    @pytest.mark.asyncio
    async def test_smembers_success(self, mock_redis_connection_manager):
        """Test successful set members operation."""
        mock_redis = mock_redis_connection_manager
        mock_redis.smembers.return_value = {"member1", "member2", "member3"}

        result = await smembers("test_set")

        mock_redis.smembers.assert_called_once_with("test_set")
        assert set(result) == {"member1", "member2", "member3"}

    @pytest.mark.asyncio
    async def test_smembers_empty_set(self, mock_redis_connection_manager):
        """Test set members operation on empty set."""
        mock_redis = mock_redis_connection_manager
        mock_redis.smembers.return_value = set()

        result = await smembers("empty_set")

        assert "Set 'empty_set' is empty or does not exist" in result

    @pytest.mark.asyncio
    async def test_smembers_redis_error(self, mock_redis_connection_manager):
        """Test set members operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.smembers.side_effect = RedisError("Connection failed")

        result = await smembers("test_set")

        assert "Error retrieving members of set 'test_set': Connection failed" in result

    @pytest.mark.asyncio
    async def test_smembers_single_member(self, mock_redis_connection_manager):
        """Test set members operation with single member."""
        mock_redis = mock_redis_connection_manager
        mock_redis.smembers.return_value = {"single_member"}

        result = await smembers("test_set")

        assert result == ["single_member"]

    @pytest.mark.asyncio
    async def test_smembers_numeric_members(self, mock_redis_connection_manager):
        """Test set members operation with numeric members."""
        mock_redis = mock_redis_connection_manager
        mock_redis.smembers.return_value = {"1", "2", "3", "42"}

        result = await smembers("numeric_set")

        assert set(result) == {"1", "2", "3", "42"}

    @pytest.mark.asyncio
    async def test_sadd_expiration_error(self, mock_redis_connection_manager):
        """Test set add operation when expiration fails."""
        mock_redis = mock_redis_connection_manager
        mock_redis.sadd.return_value = 1
        mock_redis.expire.side_effect = RedisError("Expire failed")

        result = await sadd("test_set", "member1", 60)

        assert "Error adding value 'member1' to set 'test_set': Expire failed" in result

    @pytest.mark.asyncio
    async def test_sadd_with_special_characters(self, mock_redis_connection_manager):
        """Test set add operation with special characters in member."""
        mock_redis = mock_redis_connection_manager
        mock_redis.sadd.return_value = 1

        special_member = "member:with:colons"
        result = await sadd("test_set", special_member)

        mock_redis.sadd.assert_called_once_with("test_set", special_member)
        assert (
            f"Value '{special_member}' added successfully to set 'test_set'" in result
        )

    @pytest.mark.asyncio
    async def test_sadd_with_unicode_member(self, mock_redis_connection_manager):
        """Test set add operation with unicode member."""
        mock_redis = mock_redis_connection_manager
        mock_redis.sadd.return_value = 1

        unicode_member = "测试成员 🚀"
        result = await sadd("test_set", unicode_member)

        mock_redis.sadd.assert_called_once_with("test_set", unicode_member)
        assert (
            f"Value '{unicode_member}' added successfully to set 'test_set'" in result
        )

    @pytest.mark.asyncio
    async def test_smembers_large_set(self, mock_redis_connection_manager):
        """Test set members operation with large set."""
        mock_redis = mock_redis_connection_manager
        large_set = {f"member_{i}" for i in range(1000)}
        mock_redis.smembers.return_value = large_set

        result = await smembers("large_set")

        # smembers returns a list, not a set
        assert isinstance(result, list)
        assert len(result) == 1000

    @pytest.mark.asyncio
    async def test_srem_multiple_members_behavior(self, mock_redis_connection_manager):
        """Test that srem function handles single member correctly."""
        mock_redis = mock_redis_connection_manager
        mock_redis.srem.return_value = 1

        result = await srem("test_set", "single_member")

        # Should call srem with single member, not multiple members
        mock_redis.srem.assert_called_once_with("test_set", "single_member")
        assert "Value 'single_member' removed from set 'test_set'" in result

    @pytest.mark.asyncio
    async def test_connection_manager_called_correctly(self):
        """Test that RedisConnectionManager.get_connection is called correctly."""
        with patch(
            "src.tools.set.RedisConnectionManager.get_connection"
        ) as mock_get_conn:
            mock_redis = Mock()
            mock_redis.sadd.return_value = 1
            mock_get_conn.return_value = mock_redis

            await sadd("test_set", "member1")

            mock_get_conn.assert_called_once()

    @pytest.mark.asyncio
    async def test_function_signatures(self):
        """Test that functions have correct signatures."""
        import inspect

        # Test sadd function signature
        sadd_sig = inspect.signature(sadd)
        sadd_params = list(sadd_sig.parameters.keys())
        assert sadd_params == ["name", "value", "expire_seconds"]
        assert sadd_sig.parameters["expire_seconds"].default is None

        # Test srem function signature
        srem_sig = inspect.signature(srem)
        srem_params = list(srem_sig.parameters.keys())
        assert srem_params == ["name", "value"]

        # Test smembers function signature
        smembers_sig = inspect.signature(smembers)
        smembers_params = list(smembers_sig.parameters.keys())
        assert smembers_params == ["name"]
