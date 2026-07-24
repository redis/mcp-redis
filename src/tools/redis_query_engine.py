


import json
from typing import List, Optional, Union, Dict, Any

import numpy as np
from redis.commands.search.field import VectorField
from redis.commands.search.index_definition import IndexDefinition
from redis.commands.search.query import Query
from redis.exceptions import RedisError
import redis.asyncio as aioredis

from src.common.connection import current_tenant_id, tenant_redis_manager
from src.common.server import mcp


@mcp.tool()
async def get_indexes() -> str:
    """List of indexes in the Redis database for the active tenant.

    Returns:
        str: A JSON string containing the list of indexes or an error message.
    """
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        indexes = await r.execute_command("FT._LIST")
        # Decode bytes if returned as raw byte strings
        decoded_indexes = [
            idx.decode("utf-8") if isinstance(idx, bytes) else idx for idx in indexes
        ]
        return json.dumps(decoded_indexes)
    except RedisError as e:
        return f"Error retrieving indexes: {str(e)}"


@mcp.tool()
async def get_index_info(index_name: str) -> str:
    """Retrieve schema and information about a specific Redis index using FT.INFO.

    Args:
        index_name (str): The name of the index to retrieve information about.

    Returns:
        str: Information about the specified index or an error message.
    """
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        info = await r.ft(index_name).info()
        return json.dumps(info, ensure_ascii=False, indent=2)
    except RedisError as e:
        return f"Error retrieving index info for '{index_name}': {str(e)}"


@mcp.tool()
async def get_indexed_keys_number(index_name: str) -> str:
    """Retrieve the number of indexed keys by the index.

    Args:
        index_name (str): The name of the index to retrieve information about.

    Returns:
        str: Number of indexed keys as a string
    """
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        search_result = await r.ft(index_name).search(Query("*"))
        return str(search_result.total)
    except RedisError as e:
        return f"Error retrieving number of keys for '{index_name}': {str(e)}"


@mcp.tool()
async def create_vector_index_hash(
    index_name: str = "vector_index",
    prefix: str = "doc:",
    vector_field: str = "vector",
    dim: int = 1536,
    distance_metric: str = "COSINE",
) -> str:
    """Create a Redis 8 vector similarity index using HNSW on a Redis hash.

    Args:
        index_name: The name of the Redis index to create.
        prefix: The key prefix used to identify documents to index (e.g., 'doc:').
        vector_field: The name of the vector field to be indexed for similarity search.
        dim: The dimensionality of the vectors stored under the vector_field.
        distance_metric: The distance function to use (e.g., 'COSINE', 'L2', 'IP').

    Returns:
        A string indicating whether the index was created successfully or an error message.
    """
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        index_def = IndexDefinition(prefix=[prefix])
        schema = VectorField(
            vector_field,
            "HNSW",
            {"TYPE": "FLOAT32", "DIM": dim, "DISTANCE_METRIC": distance_metric},
        )

        await r.ft(index_name).create_index([schema], definition=index_def)
        return f"Index '{index_name}' created successfully for tenant {tenant_id}."
    except RedisError as e:
        return f"Error creating index '{index_name}': {str(e)}"


@mcp.tool()
async def vector_search_hash(
    query_vector: List[float],
    index_name: str = "vector_index",
    vector_field: str = "vector",
    k: int = 5,
    return_fields: Optional[List[str]] = None,
) -> Union[List[Dict[str, Any]], str]:
    """Perform a KNN vector similarity search on vectors stored in hash data structures.

    Args:
        query_vector: List of floats to use as the query vector.
        index_name: Name of the Redis index.
        vector_field: Name of the indexed vector field.
        k: Number of nearest neighbors to return.
        return_fields: List of fields to return (optional).

    Returns:
        A list of matched documents or an error message.
    """
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        vector_blob = np.array(query_vector, dtype=np.float32).tobytes()

        base_query = f"*=>[KNN {k} @{vector_field} $vec_param AS score]"
        query = (
            Query(base_query)
            .sort_by("score")
            .paging(0, k)
            .return_fields("id", "score", *return_fields or [])
            .dialect(2)
        )

        results = await r.ft(index_name).search(
            query, query_params={"vec_param": vector_blob}
        )

        return [doc.__dict__ for doc in results.docs]
    except RedisError as e:
        return f"Error performing vector search on index '{index_name}': {str(e)}"


@mcp.tool()
async def hybrid_search(
    query_vector: List[float],
    filter_expression: str = "*",
    index_name: str = "vector_index",
    vector_field: str = "vector",
    k: int = 5,
    return_fields: Optional[List[str]] = None,
) -> Union[List[Dict[str, Any]], str]:
    """Perform a hybrid search combining a Redis filter expression with KNN vector similarity.

    Args:
        query_vector: List of floats to use as the query vector.
        filter_expression: Redis filter expression to restrict candidates before KNN ranking.
        index_name: Name of the Redis index.
        vector_field: Name of the indexed vector field.
        k: Number of nearest neighbors to return.
        return_fields: Additional fields to include in results (optional).

    Returns:
        A list of matched documents with their similarity score, or an error message.
    """
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        vector_blob = np.array(query_vector, dtype=np.float32).tobytes()

        base_query = (
            f"({filter_expression})=>[KNN {k} @{vector_field} $vec_param AS score]"
        )
        query = (
            Query(base_query)
            .sort_by("score")
            .paging(0, k)
            .return_fields("id", "score", *return_fields or [])
            .dialect(2)
        )

        results = await r.ft(index_name).search(
            query, query_params={"vec_param": vector_blob}
        )

        return [doc.__dict__ for doc in results.docs]
    except RedisError as e:
        return f"Error performing hybrid search on index '{index_name}': {str(e)}"