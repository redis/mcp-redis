import json
from typing import Optional
from src.common.connection import RedisConnectionManager
from redis.exceptions import RedisError
from src.common.server import mcp
from redis.commands.search.query import Query
from redis.commands.search.field import VectorField
from redis.commands.search.index_definition import IndexDefinition
import numpy as np


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
async def get_index_info(index_name: str) -> str | dict:
    """Retrieve schema and information about a specific Redis index using FT.INFO.

    Args:
        index_name (str): The name of the index to retrieve information about.

    Returns:
        str: Information about the specified index or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        return r.ft(index_name).info()
    except RedisError as e:
        return f"Error retrieving index info: {str(e)}"


@mcp.tool()
async def get_indexed_keys_number(index_name: str) -> int:
    """Retrieve the number of indexed keys by the index

    Args:
        index_name (str): The name of the index to retrieve information about.

    Returns:
        int: Number of indexed keys
    """
    try:
        r = RedisConnectionManager.get_connection()
        return r.ft(index_name).search(Query("*")).total
    except RedisError as e:
        return f"Error retrieving number of keys: {str(e)}"


@mcp.tool()
async def create_vector_index_hash(index_name: str = "vector_index",
                       prefix: str = "doc:",
                       vector_field: str = "vector",
                       dim: int = 1536,
                       distance_metric: str = "COSINE") -> str:
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
        schema = (
            VectorField(
                vector_field,
                "HNSW",
                {
                    "TYPE": "FLOAT32",
                    "DIM": dim,
                    "DISTANCE_METRIC": distance_metric
                }
            )
        )

        r.ft(index_name).create_index([schema], definition=index_def)
        return f"Index '{index_name}' created successfully."
    except RedisError as e:
        return f"Error creating index '{index_name}': {str(e)}"


@mcp.tool()
async def vector_search_hash(query_vector: list,
                            index_name: str = "vector_index",
                            vector_field: str = "vector",
                            k: int = 5,
                            return_fields: list = None) -> list:
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
        query = Query(base_query).sort_by("score").paging(0, k).return_fields("id", "score", *return_fields or []).dialect(2)

        # Perform the search with vector parameter
        results = r.ft(index_name).search(query, query_params={"vec_param": vector_blob})

        # Format and return the results
        return [doc.__dict__ for doc in results.docs]
    except RedisError as e:
        return f"Error performing vector search on index '{index_name}': {str(e)}"


def _get_index_dialect(r, index_name: str) -> int:
    """
    Get the dialect version of a Redis search index.
    
    Args:
        r: Redis connection
        index_name: Name of the index
        
    Returns:
        int: Dialect version (1, 2, 3, or 4). Defaults to 1 if not detected.
    """
    try:
        # Get index info
        info = r.ft(index_name).info()
        
        # Check if dialect is specified in index info
        if 'dialect' in info:
            return int(info['dialect'])
        
        # Try to detect dialect by checking for dialect-specific features
        # This is a fallback method when dialect isn't explicitly stated
        
        # Check for dialect 4 features (introduced in Redis 7.2)
        # Dialect 4 supports more advanced vector search features
        if 'vector_fields' in info or any('VECTOR' in str(field) for field in info.get('attributes', [])):
            return 4
            
        # Check for dialect 3 features (introduced in Redis 7.0)
        # Dialect 3 supports JSON path queries
        if any('JSONPath' in str(field) for field in info.get('attributes', [])):
            return 3
            
        # Check for dialect 2 features (default in Redis 6.2+)
        # Dialect 2 supports more query operators and syntax
        if 'stopwords' in info or 'max_text_fields' in info:
            return 2
            
        # Default to dialect 1 (legacy)
        return 1
        
    except Exception:
        # If we can't determine dialect, default to 1 (most compatible)
        return 1


@mcp.tool()
async def text_search(query_text: str, 
                     index_name: str, 
                     return_fields: list = None,
                     limit: int = 10,
                     offset: int = 0,
                     sort_by: str = None,
                     sort_ascending: bool = True,
                     dialect: int = None) -> str:
    """
    Perform a general text search using Redis FT.SEARCH command with automatic dialect detection.
    
    This function allows you to search through indexed text fields using Redis Search.
    It automatically detects the dialect of the index and adjusts the query accordingly.
    RediSearch supports different dialects (1, 2, 3, 4) with varying query syntax and capabilities.
    
    Args:
        query_text: The search query string. Syntax depends on dialect:
                   
                   Dialect 1 (Legacy):
                   - Simple terms: "hello world"
                   - Field search: "@title:redis"
                   - Phrase search: "\"exact phrase\""
                   - Boolean: "redis search" (implicit AND)
                   
                   Dialect 2+ (Modern):
                   - All dialect 1 features plus:
                   - Explicit boolean: "redis AND search", "redis OR query"
                   - Negation: "redis -search"
                   - Wildcards: "hel*", "red?"
                   - Numeric ranges: "@price:[10 20]"
                   - Geo queries: "@location:[lng lat radius unit]"
                   
                   Dialect 3+ (JSON support):
                   - JSONPath queries: "@$.user.name:john"
                   
                   Dialect 4+ (Advanced vectors):
                   - Enhanced vector search syntax
                   
        index_name: The name of the Redis search index to query against.
        return_fields: Optional list of fields to return in results. If None, returns all fields.
        limit: Maximum number of results to return (default: 10).
        offset: Number of results to skip for pagination (default: 0).
        sort_by: Optional field name to sort results by.
        sort_ascending: Sort direction, True for ascending, False for descending (default: True).
        dialect: Optional explicit dialect to use. If None, will auto-detect from index.
    
    Returns:
        str: JSON string containing search results with document data, metadata, and dialect info, or error message.
    
    Example queries by dialect:
        Dialect 1: "redis database", "@title:redis"
        Dialect 2: "redis AND search", "@price:[10 50]", "red*"
        Dialect 3: "@$.user.name:john AND @$.status:active"
        Dialect 4: Enhanced vector and hybrid search queries
    """
    try:
        r = RedisConnectionManager.get_connection()
        
        # Determine dialect
        if dialect is None:
            detected_dialect = _get_index_dialect(r, index_name)
        else:
            detected_dialect = dialect
            
        # Build the query
        query = Query(query_text)
        
        # Set dialect for the query
        query = query.dialect(detected_dialect)
        
        # Add pagination
        query = query.paging(offset, limit)
        
        # Add return fields if specified
        if return_fields:
            query = query.return_fields(*return_fields)
        
        # Add sorting if specified
        if sort_by:
            query = query.sort_by(sort_by, asc=sort_ascending)
        
        # Execute the search
        results = r.ft(index_name).search(query)
        
        # Format the results
        formatted_results = {
            "total": results.total,
            "docs": [doc.__dict__ for doc in results.docs],
            "query": query_text,
            "dialect": detected_dialect,
            "offset": offset,
            "limit": limit,
            "index_name": index_name
        }
        
        return json.dumps(formatted_results, indent=2)
        
    except RedisError as e:
        return f"Error performing text search on index '{index_name}': {str(e)}"


@mcp.tool()
async def get_index_dialect(index_name: str) -> str:
    """
    Get the dialect version of a Redis search index.
    
    RediSearch supports different dialects with varying capabilities:
    - Dialect 1 (Legacy): Basic text search, field queries, phrase search
    - Dialect 2 (Modern): Boolean operators, wildcards, numeric ranges, geo queries
    - Dialect 3 (JSON): JSONPath queries, enhanced JSON support
    - Dialect 4 (Advanced): Enhanced vector search, latest features
    
    Args:
        index_name: The name of the Redis search index.
        
    Returns:
        str: JSON string containing dialect information and capabilities, or error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        
        # Get the dialect
        dialect = _get_index_dialect(r, index_name)
        
        # Get index info for additional context
        info = r.ft(index_name).info()
        
        # Define capabilities by dialect
        capabilities = {
            1: {
                "description": "Legacy dialect - basic text search",
                "features": [
                    "Simple term search",
                    "Field-specific search (@field:value)",
                    "Phrase search (\"exact phrase\")",
                    "Implicit AND between terms"
                ]
            },
            2: {
                "description": "Modern dialect - enhanced query syntax",
                "features": [
                    "All dialect 1 features",
                    "Explicit boolean operators (AND, OR, NOT)",
                    "Negation (-term)",
                    "Wildcards (*, ?)",
                    "Numeric ranges (@field:[min max])",
                    "Geo queries (@location:[lng lat radius unit])",
                    "Parentheses for grouping"
                ]
            },
            3: {
                "description": "JSON dialect - JSONPath support",
                "features": [
                    "All dialect 2 features",
                    "JSONPath queries (@$.path:value)",
                    "Enhanced JSON field indexing",
                    "Nested object search"
                ]
            },
            4: {
                "description": "Advanced dialect - latest features",
                "features": [
                    "All dialect 3 features",
                    "Enhanced vector search syntax",
                    "Hybrid search capabilities",
                    "Latest RediSearch features"
                ]
            }
        }
        
        result = {
            "index_name": index_name,
            "dialect": dialect,
            "capabilities": capabilities.get(dialect, {"description": "Unknown dialect", "features": []}),
            "index_info": {
                "num_docs": info.get('num_docs', 0),
                "max_doc_id": info.get('max_doc_id', 0),
                "num_terms": info.get('num_terms', 0),
                "num_records": info.get('num_records', 0),
                "inverted_sz_mb": info.get('inverted_sz_mb', 0),
                "vector_index_sz_mb": info.get('vector_index_sz_mb', 0),
                "total_inverted_index_blocks": info.get('total_inverted_index_blocks', 0),
                "offset_vectors_sz_mb": info.get('offset_vectors_sz_mb', 0),
                "doc_table_size_mb": info.get('doc_table_size_mb', 0),
                "sortable_values_size_mb": info.get('sortable_values_size_mb', 0),
                "key_table_size_mb": info.get('key_table_size_mb', 0)
            }
        }
        
        return json.dumps(result, indent=2)
        
    except RedisError as e:
        return f"Error getting dialect for index '{index_name}': {str(e)}"
