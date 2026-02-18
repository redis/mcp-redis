"""
Unit tests for src/tools/redis_query_engine.py
"""

import json
from unittest.mock import Mock, patch

import pytest
from redis.commands.search.field import VectorField
from redis.commands.search.index_definition import IndexDefinition
from redis.commands.search.query import Query
from redis.exceptions import RedisError

from src.tools.redis_query_engine import (
    create_vector_index_hash,
    get_index_info,
    get_indexes,
    vector_search_hash,
    hybrid_search,
)


class TestRedisQueryEngineOperations:
    """Test cases for Redis query engine operations."""

    @pytest.mark.asyncio
    async def test_get_indexes_success(self, mock_redis_connection_manager):
        """Test successful get indexes operation."""
        mock_redis = mock_redis_connection_manager
        mock_indexes = ["index1", "index2", "vector_index"]
        mock_redis.execute_command.return_value = mock_indexes

        result = await get_indexes()

        mock_redis.execute_command.assert_called_once_with("FT._LIST")
        assert result == json.dumps(mock_indexes)

    @pytest.mark.asyncio
    async def test_get_indexes_empty(self, mock_redis_connection_manager):
        """Test get indexes operation with no indexes."""
        mock_redis = mock_redis_connection_manager
        mock_redis.execute_command.return_value = []

        result = await get_indexes()

        assert result == json.dumps([])

    @pytest.mark.asyncio
    async def test_get_indexes_redis_error(self, mock_redis_connection_manager):
        """Test get indexes operation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_redis.execute_command.side_effect = RedisError("Search module not loaded")

        result = await get_indexes()

        assert "Error retrieving indexes: Search module not loaded" in result

    @pytest.mark.asyncio
    async def test_create_vector_index_hash_success(
        self, mock_redis_connection_manager
    ):
        """Test successful vector index creation."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft
        mock_ft.create_index.return_value = "OK"

        result = await create_vector_index_hash()

        mock_redis.ft.assert_called_once_with("vector_index")
        mock_ft.create_index.assert_called_once()

        # Verify the create_index call arguments
        call_args = mock_ft.create_index.call_args
        fields = call_args[0][0]  # First positional argument (fields)
        definition = call_args[1]["definition"]  # Keyword argument

        assert len(fields) == 1
        assert isinstance(fields[0], VectorField)
        assert fields[0].name == "vector"
        assert isinstance(definition, IndexDefinition)

        assert "Index 'vector_index' created successfully." in result

    @pytest.mark.asyncio
    async def test_create_vector_index_hash_custom_params(
        self, mock_redis_connection_manager
    ):
        """Test vector index creation with custom parameters."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft
        mock_ft.create_index.return_value = "OK"

        result = await create_vector_index_hash(
            index_name="custom_index",
            vector_field="embedding",
            dim=512,
            distance_metric="COSINE",
        )

        mock_redis.ft.assert_called_once_with("custom_index")

        # Verify the field configuration
        call_args = mock_ft.create_index.call_args
        fields = call_args[0][0]

        assert fields[0].name == "embedding"
        assert "Index 'custom_index' created successfully." in result

    @pytest.mark.asyncio
    async def test_create_vector_index_hash_redis_error(
        self, mock_redis_connection_manager
    ):
        """Test vector index creation with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft
        mock_ft.create_index.side_effect = RedisError("Index already exists")

        result = await create_vector_index_hash()

        assert "Error creating index 'vector_index': Index already exists" in result

    @pytest.mark.asyncio
    async def test_vector_search_hash_success(
        self, mock_redis_connection_manager, sample_vector
    ):
        """Test successful vector search operation."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft

        # Mock search results
        mock_doc1 = Mock()
        mock_doc1.__dict__ = {"id": "doc1", "vector": "binary_data", "score": "0.95"}
        mock_doc2 = Mock()
        mock_doc2.__dict__ = {"id": "doc2", "vector": "binary_data", "score": "0.87"}

        mock_result = Mock()
        mock_result.docs = [mock_doc1, mock_doc2]
        mock_ft.search.return_value = mock_result

        with patch("numpy.array") as mock_np_array:
            mock_np_array.return_value.tobytes.return_value = b"query_vector_bytes"

            result = await vector_search_hash(sample_vector)

            mock_redis.ft.assert_called_once_with("vector_index")
            mock_ft.search.assert_called_once()

            # Verify the search query
            search_call_args = mock_ft.search.call_args[0][0]
            assert isinstance(search_call_args, Query)

            assert isinstance(result, list)
            assert len(result) == 2

    @pytest.mark.asyncio
    async def test_vector_search_hash_custom_params(
        self, mock_redis_connection_manager, sample_vector
    ):
        """Test vector search with custom parameters."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft

        mock_result = Mock()
        mock_result.docs = []
        mock_ft.search.return_value = mock_result

        with patch("numpy.array") as mock_np_array:
            mock_np_array.return_value.tobytes.return_value = b"query_vector_bytes"

            result = await vector_search_hash(
                query_vector=sample_vector,
                index_name="custom_index",
                vector_field="embedding",
                k=10,
                return_fields=["title", "content"],
            )

            mock_redis.ft.assert_called_once_with("custom_index")
            assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_vector_search_hash_no_results(
        self, mock_redis_connection_manager, sample_vector
    ):
        """Test vector search with no results."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft

        mock_result = Mock()
        mock_result.docs = []
        mock_ft.search.return_value = mock_result

        with patch("numpy.array") as mock_np_array:
            mock_np_array.return_value.tobytes.return_value = b"query_vector_bytes"

            result = await vector_search_hash(sample_vector)

            assert result == []  # Empty list when no results

    @pytest.mark.asyncio
    async def test_vector_search_hash_redis_error(
        self, mock_redis_connection_manager, sample_vector
    ):
        """Test vector search with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft
        mock_ft.search.side_effect = RedisError("Index not found")

        with patch("numpy.array") as mock_np_array:
            mock_np_array.return_value.astype.return_value.tobytes.return_value = (
                b"query_vector_bytes"
            )

            result = await vector_search_hash(sample_vector)

            assert (
                "Error performing vector search on index 'vector_index': Index not found"
                in result
            )

    @pytest.mark.asyncio
    async def test_get_index_info_success(self, mock_redis_connection_manager):
        """Test successful get index info operation."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft

        mock_info = {
            "index_name": "vector_index",
            "index_options": [],
            "index_definition": ["key_type", "HASH", "prefixes", ["doc:"]],
            "attributes": [
                ["identifier", "vector", "attribute", "vector", "type", "VECTOR"]
            ],
            "num_docs": "100",
            "max_doc_id": "100",
            "num_terms": "0",
            "num_records": "100",
            "inverted_sz_mb": "0.00",
            "vector_index_sz_mb": "1.50",
            "total_inverted_index_blocks": "0",
            "offset_vectors_sz_mb": "0.00",
            "doc_table_size_mb": "0.01",
            "sortable_values_size_mb": "0.00",
            "key_table_size_mb": "0.00",
        }
        mock_ft.info.return_value = mock_info

        result = await get_index_info("vector_index")

        mock_redis.ft.assert_called_once_with("vector_index")
        mock_ft.info.assert_called_once()
        # get_index_info returns a JSON string representation
        assert result == json.dumps(mock_info, ensure_ascii=False, indent=2)

    @pytest.mark.asyncio
    async def test_get_index_info_default_index(self, mock_redis_connection_manager):
        """Test get index info with default index name."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft
        mock_ft.info.return_value = {"index_name": "vector_index"}

        result = await get_index_info("vector_index")

        mock_redis.ft.assert_called_once_with("vector_index")
        # get_index_info returns a JSON string representation
        assert result == json.dumps(
            {"index_name": "vector_index"}, ensure_ascii=False, indent=2
        )

    @pytest.mark.asyncio
    async def test_get_index_info_redis_error(self, mock_redis_connection_manager):
        """Test get index info with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft
        mock_ft.info.side_effect = RedisError("Index not found")

        result = await get_index_info("nonexistent_index")

        assert "Error retrieving index info: Index not found" in result

    @pytest.mark.asyncio
    async def test_create_vector_index_different_metrics(
        self, mock_redis_connection_manager
    ):
        """Test vector index creation with different distance metrics."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft
        mock_ft.create_index.return_value = "OK"

        # Test L2 metric
        await create_vector_index_hash(distance_metric="L2")
        mock_ft.create_index.assert_called()

        # Test IP metric
        mock_ft.reset_mock()
        await create_vector_index_hash(distance_metric="IP")
        mock_ft.create_index.assert_called()

    @pytest.mark.asyncio
    async def test_vector_search_with_large_k(
        self, mock_redis_connection_manager, sample_vector
    ):
        """Test vector search with large k value."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft

        mock_result = Mock()
        mock_result.docs = []
        mock_ft.search.return_value = mock_result

        with patch("numpy.array") as mock_np_array:
            mock_np_array.return_value.astype.return_value.tobytes.return_value = (
                b"query_vector_bytes"
            )

            result = await vector_search_hash(sample_vector, k=1000)
            assert result == []  # Empty list when no results

            # Should handle large k values
            mock_ft.search.assert_called_once()

    @pytest.mark.asyncio
    async def test_connection_manager_called_correctly(self):
        """Test that RedisConnectionManager.get_connection is called correctly."""
        with patch(
            "src.tools.redis_query_engine.RedisConnectionManager.get_connection"
        ) as mock_get_conn:
            mock_redis = Mock()
            mock_redis.execute_command.return_value = []
            mock_get_conn.return_value = mock_redis

            await get_indexes()

            mock_get_conn.assert_called_once()

    @pytest.mark.asyncio
    async def test_hybrid_search_success(
        self, mock_redis_connection_manager, sample_vector
    ):
        """Test successful hybrid search with a filter expression."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft

        mock_doc1 = Mock()
        mock_doc1.__dict__ = {"id": "doc1", "score": "0.95", "category": "news"}
        mock_doc2 = Mock()
        mock_doc2.__dict__ = {"id": "doc2", "score": "0.87", "category": "news"}

        mock_result = Mock()
        mock_result.docs = [mock_doc1, mock_doc2]
        mock_ft.search.return_value = mock_result

        with patch("numpy.array") as mock_np_array:
            mock_np_array.return_value.tobytes.return_value = b"query_vector_bytes"

            result = await hybrid_search(
                query_vector=sample_vector,
                filter_expression="@category:{news}",
            )

            mock_redis.ft.assert_called_once_with("vector_index")
            mock_ft.search.assert_called_once()

            search_call_args = mock_ft.search.call_args[0][0]
            assert isinstance(search_call_args, Query)

            assert isinstance(result, list)
            assert len(result) == 2

    @pytest.mark.asyncio
    async def test_hybrid_search_no_filter(
        self, mock_redis_connection_manager, sample_vector
    ):
        """Test hybrid search with default '*' filter behaves like pure vector search."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft

        mock_doc = Mock()
        mock_doc.__dict__ = {"id": "doc1", "score": "0.91"}

        mock_result = Mock()
        mock_result.docs = [mock_doc]
        mock_ft.search.return_value = mock_result

        with patch("numpy.array") as mock_np_array:
            mock_np_array.return_value.tobytes.return_value = b"query_vector_bytes"

            result = await hybrid_search(query_vector=sample_vector)

            # Verify the query string contains the default wildcard filter
            search_call_args = mock_ft.search.call_args[0][0]
            assert "(*)" in search_call_args.query_string()

            assert isinstance(result, list)
            assert len(result) == 1

    @pytest.mark.asyncio
    async def test_hybrid_search_numeric_range_filter(
        self, mock_redis_connection_manager, sample_vector
    ):
        """Test hybrid search with a numeric range filter expression."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft

        mock_result = Mock()
        mock_result.docs = []
        mock_ft.search.return_value = mock_result

        with patch("numpy.array") as mock_np_array:
            mock_np_array.return_value.tobytes.return_value = b"query_vector_bytes"

            result = await hybrid_search(
                query_vector=sample_vector,
                filter_expression="@year:[2020 2024]",
            )

            search_call_args = mock_ft.search.call_args[0][0]
            assert "@year:[2020 2024]" in search_call_args.query_string()
            assert result == []

    @pytest.mark.asyncio
    async def test_hybrid_search_custom_params(
        self, mock_redis_connection_manager, sample_vector
    ):
        """Test hybrid search with custom index, field, k, and return_fields."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft

        mock_result = Mock()
        mock_result.docs = []
        mock_ft.search.return_value = mock_result

        with patch("numpy.array") as mock_np_array:
            mock_np_array.return_value.tobytes.return_value = b"query_vector_bytes"

            result = await hybrid_search(
                query_vector=sample_vector,
                filter_expression="@lang:{en}",
                index_name="custom_index",
                vector_field="embedding",
                k=10,
                return_fields=["title", "content"],
            )

            mock_redis.ft.assert_called_once_with("custom_index")
            assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_hybrid_search_no_results(
        self, mock_redis_connection_manager, sample_vector
    ):
        """Test hybrid search returns empty list when filter matches nothing."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft

        mock_result = Mock()
        mock_result.docs = []
        mock_ft.search.return_value = mock_result

        with patch("numpy.array") as mock_np_array:
            mock_np_array.return_value.tobytes.return_value = b"query_vector_bytes"

            result = await hybrid_search(
                query_vector=sample_vector,
                filter_expression="@category:{nonexistent}",
            )

            assert result == []

    @pytest.mark.asyncio
    async def test_hybrid_search_redis_error(
        self, mock_redis_connection_manager, sample_vector
    ):
        """Test hybrid search with Redis error."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft
        mock_ft.search.side_effect = RedisError("Index not found")

        with patch("numpy.array") as mock_np_array:
            mock_np_array.return_value.tobytes.return_value = b"query_vector_bytes"

            result = await hybrid_search(sample_vector)

            assert (
                "Error performing hybrid search on index 'vector_index': Index not found"
                in result
            )

    @pytest.mark.asyncio
    async def test_hybrid_search_combined_filter(
        self, mock_redis_connection_manager, sample_vector
    ):
        """Test hybrid search with combined tag and range filter."""
        mock_redis = mock_redis_connection_manager
        mock_ft = Mock()
        mock_redis.ft.return_value = mock_ft

        mock_doc = Mock()
        mock_doc.__dict__ = {
            "id": "doc1",
            "score": "0.88",
            "lang": "en",
            "year": "2023",
        }

        mock_result = Mock()
        mock_result.docs = [mock_doc]
        mock_ft.search.return_value = mock_result

        with patch("numpy.array") as mock_np_array:
            mock_np_array.return_value.tobytes.return_value = b"query_vector_bytes"

            result = await hybrid_search(
                query_vector=sample_vector,
                filter_expression="@lang:{en} @year:[2022 +inf]",
            )

            search_call_args = mock_ft.search.call_args[0][0]
            assert "@lang:{en} @year:[2022 +inf]" in search_call_args.query_string()

            assert len(result) == 1
            assert result[0]["id"] == "doc1"
