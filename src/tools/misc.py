

import asyncio
from typing import Any, Dict, Union, List
import aiohttp
from redis.exceptions import RedisError
import redis.asyncio as aioredis

from src.common.connection import current_tenant_id, tenant_redis_manager
from src.common.server import mcp
from src.common.config import MCP_DOCS_SEARCH_URL
from src.version import __version__

DOCS_SEARCH_TIMEOUT_SECONDS = 10


@mcp.tool()
async def delete(key: str) -> str:
    """Delete a Redis key.

    Args:
        key (str): The key to delete.

    Returns:
        str: Confirmation message or an error message.
    """
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        result = await r.delete(key)
        return f"Successfully deleted {key} for tenant {tenant_id}" if result else f"Key {key} not found for tenant {tenant_id}"
    except RedisError as e:
        return f"Error deleting key {key}: {str(e)}"


@mcp.tool()
async def type(key: str) -> Dict[str, Any]:
    """Returns the string representation of the type of the value stored at key."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return {"error": "No active tenant context detected for this tool execution."}

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        key_type = await r.type(key)
        ttl = await r.ttl(key)

        return {
            "key": key,
            "type": key_type.decode("utf-8") if isinstance(key_type, bytes) else key_type,
            "ttl": ttl,
            "tenant_id": tenant_id
        }
    except RedisError as e:
        return {"error": str(e)}


@mcp.tool()
async def expire(name: str, expire_seconds: int) -> str:
    """Set an expiration time for a Redis key."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        success = await r.expire(name, expire_seconds)
        return (
            f"Expiration set to {expire_seconds} seconds for '{name}' (tenant {tenant_id})."
            if success
            else f"Key '{name}' does not exist for tenant {tenant_id}."
        )
    except RedisError as e:
        return f"Error setting expiration for key '{name}': {str(e)}"


@mcp.tool()
async def rename(old_key: str, new_key: str) -> Dict[str, Any]:
    """Renames a Redis key from old_key to new_key."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return {"error": "No active tenant context detected for this tool execution."}

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        if not await r.exists(old_key):
            return {"error": f"Key '{old_key}' does not exist for tenant {tenant_id}."}

        await r.rename(old_key, new_key)
        return {
            "status": "success",
            "message": f"Renamed key '{old_key}' to '{new_key}' for tenant {tenant_id}",
        }
    except RedisError as e:
        return {"error": str(e)}


@mcp.tool()
async def scan_keys(
    pattern: str = "*", count: int = 100, cursor: int = 0
) -> Union[str, Dict[str, Any]]:
    """Scan keys in the Redis database using the SCAN command (non-blocking, production-safe)."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        new_cursor, keys = await r.scan(cursor=cursor, match=pattern, count=count)

        decoded_keys = [
            key.decode("utf-8") if isinstance(key, bytes) else key for key in keys
        ]

        return {
            "cursor": new_cursor,
            "keys": decoded_keys,
            "total_scanned": len(decoded_keys),
            "scan_complete": new_cursor == 0,
            "tenant_id": tenant_id
        }
    except RedisError as e:
        return f"Error scanning keys with pattern '{pattern}': {str(e)}"


@mcp.tool()
async def scan_all_keys(
    pattern: str = "*", batch_size: int = 100
) -> Union[str, List[str]]:
    """Scan and return ALL keys matching a pattern using multiple SCAN iterations."""
    try:
        try:
            tenant_id = current_tenant_id.get()
        except LookupError:
            return "Error: No active tenant context detected for this tool execution."

        r: aioredis.Redis = await tenant_redis_manager.get_client()

        all_keys = []
        cursor = 0

        while True:
            cursor, keys = await r.scan(cursor=cursor, match=pattern, count=batch_size)

            decoded_keys = [
                key.decode("utf-8") if isinstance(key, bytes) else key for key in keys
            ]
            all_keys.extend(decoded_keys)

            if cursor == 0:
                break

        return all_keys
    except RedisError as e:
        return f"Error scanning all keys with pattern '{pattern}': {str(e)}"


@mcp.tool()
async def search_redis_documents(
    question: str,
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """Search Redis documentation and knowledge base."""
    if not MCP_DOCS_SEARCH_URL:
        return {"error": "MCP_DOCS_SEARCH_URL environment variable is not configured"}

    if not question.strip():
        return {"error": "Question parameter cannot be empty"}

    try:
        headers = {
            "Accept": "application/json",
            "User-Agent": f"Redis-MCP-Server/{__version__}",
        }
        timeout = aiohttp.ClientTimeout(total=DOCS_SEARCH_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                url=MCP_DOCS_SEARCH_URL, params={"q": question}, headers=headers
            ) as response:
                if response.status >= 400:
                    try:
                        error_payload = await response.json()
                        return {
                            "error": f"Docs search request failed with status {response.status}",
                            "details": error_payload,
                        }
                    except aiohttp.ContentTypeError:
                        text_content = await response.text()
                        return {
                            "error": f"Docs search request failed with status {response.status}: {text_content}"
                        }

                try:
                    result = await response.json()
                    return result
                except aiohttp.ContentTypeError:
                    text_content = await response.text()
                    return {"error": f"Non-JSON response: {text_content}"}

    except (asyncio.TimeoutError, TimeoutError):
        return {
            "error": f"Docs search request timed out after {DOCS_SEARCH_TIMEOUT_SECONDS} seconds"
        }
    except aiohttp.ClientError as e:
        return {"error": f"HTTP client error: {str(e)}"}
    except Exception as e:
        return {"error": f"Unexpected error calling docs API: {str(e)}"}