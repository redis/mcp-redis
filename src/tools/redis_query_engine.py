import json
from typing import List, Optional, Union, Dict, Any

import numpy as np
from redis.commands.search.field import VectorField
from redis.commands.search.index_definition import IndexDefinition
from redis.commands.search.query import Query
from redis.exceptions import RedisError

from src.common.connection import RedisConnectionManager
from src.common.server import mcp


@mcp.tool()
async def get_indexes() -> str:
    """List of indexes in the Redis database

    Returns:
        str: A JSON string containing the list of indexes or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        return json.dumps(r.execute_command("FT._LIST"))
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
        r = RedisConnectionManager.get_connection()
        info = r.ft(index_name).info()
        return json.dumps(info, ensure_ascii=False, indent=2)
    except RedisError as e:
        return f"Error retrieving index info: {str(e)}"


@mcp.tool()
async def get_indexed_keys_number(index_name: str) -> str:
    """Retrieve the number of indexed keys by the index

    Args:
        index_name (str): The name of the index to retrieve information about.

    Returns:
        str: Number of indexed keys as a string
    """
    try:
        r = RedisConnectionManager.get_connection()
        total = r.ft(index_name).search(Query("*")).total
        return str(total)
    except RedisError as e:
        return f"Error retrieving number of keys: {str(e)}"


@mcp.tool()
async def create_vector_index_hash(
    index_name: str = "vector_index",
    prefix: str = "doc:",
    vector_field: str = "vector",
    dim: int = 1536,
    distance_metric: str = "COSINE",
) -> str:
    """
    Create a Redis 8 vector similarity index using HNSW on a Redis hash.

    This function sets up a Redis index for approximate nearest neighbor (ANN)
    search using the HNSW algorithm and float32 vector embeddings.

    Args:
        index_name: The name of the Redis index to create. Unless specifically required, use the default name for the index.
        prefix: The key prefix used to identify documents to index (e.g., 'doc:'). Unless specifically required, use the default prefix.
        vector_field: The name of the vector field to be indexed for similarity search. Unless specifically required, use the default field name
        dim: The dimensionality of the vectors stored under the vector_field.
        distance_metric: The distance function to use (e.g., 'COSINE', 'L2', 'IP').

    Returns:
        A string indicating whether the index was created successfully or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()

        index_def = IndexDefinition(prefix=[prefix])
        schema = VectorField(
            vector_field,
            "HNSW",
            {"TYPE": "FLOAT32", "DIM": dim, "DISTANCE_METRIC": distance_metric},
        )

        r.ft(index_name).create_index([schema], definition=index_def)
        return f"Index '{index_name}' created successfully."
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
    """
    Perform a KNN vector similarity search using Redis 8 or later version on vectors stored in hash data structures.

    Args:
        query_vector: List of floats to use as the query vector.
        index_name: Name of the Redis index. Unless specifically specified, use the default index name.
        vector_field: Name of the indexed vector field. Unless specifically required, use the default field name
        k: Number of nearest neighbors to return.
        return_fields: List of fields to return (optional).

    Returns:
        A list of matched documents or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()

        # Convert query vector to float32 binary blob
        vector_blob = np.array(query_vector, dtype=np.float32).tobytes()

        # Build the KNN query
        base_query = f"*=>[KNN {k} @{vector_field} $vec_param AS score]"
        query = (
            Query(base_query)
            .sort_by("score")
            .paging(0, k)
            .return_fields("id", "score", *return_fields or [])
            .dialect(2)
        )

        # Perform the search with vector parameter
        results = r.ft(index_name).search(
            query, query_params={"vec_param": vector_blob}
        )

        # Format and return the results
        return [doc.__dict__ for doc in results.docs]
    except RedisError as e:
        return f"Error performing vector search on index '{index_name}': {str(e)}"
