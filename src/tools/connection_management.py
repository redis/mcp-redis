from typing import Optional, Dict, Any
from src.common.connection import RedisConnectionPool
from src.common.config import build_redis_config
from src.common.server import mcp
import urllib.parse


@mcp.tool()
async def connect(
    url: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    db: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    ssl: Optional[bool] = None,
    ssl_ca_path: Optional[str] = None,
    ssl_keyfile: Optional[str] = None,
    ssl_certfile: Optional[str] = None,
    ssl_cert_reqs: Optional[str] = None,
    ssl_ca_certs: Optional[str] = None,
    cluster_mode: Optional[bool] = None,
    host_id: Optional[str] = None
) -> str:
    """Connect to a Redis server and add it to the connection pool.
    
    Args:
        url: Redis connection URI (redis://user:pass@host:port/db or rediss:// for SSL)
        host: Redis host (default: 127.0.0.1)
        port: Redis port (default: 6379)
        db: Redis database number (default: 0)
        username: Redis username
        password: Redis password
        ssl: Use SSL connection
        ssl_ca_path: Path to CA certificate file
        ssl_keyfile: Path to SSL key file
        ssl_certfile: Path to SSL certificate file
        ssl_cert_reqs: SSL certificate requirements (default: required)
        ssl_ca_certs: Path to CA certificates file
        cluster_mode: Enable Redis cluster mode
        host_id: Custom identifier for this connection (auto-generated if not provided)
    
    Returns:
        Success message with connection details or error message.
    """
    try:
        # Build configuration using unified logic
        config, generated_host_id = build_redis_config(
            url=url, host=host, port=port, db=db, username=username,
            password=password, ssl=ssl, ssl_ca_path=ssl_ca_path,
            ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile,
            ssl_cert_reqs=ssl_cert_reqs, ssl_ca_certs=ssl_ca_certs,
            cluster_mode=cluster_mode, host_id=host_id
        )
        
        # Use provided host_id or generated one
        final_host_id = host_id or generated_host_id
        
        # Add connection to pool
        result = RedisConnectionPool.add_connection_to_pool(final_host_id, config)
        
        return f"{result}. Host identifier: '{final_host_id}'"
        
    except Exception as e:
        return f"Failed to connect to Redis: {str(e)}"


@mcp.tool()
async def list_connections() -> Dict[str, Any]:
    """List all active Redis connections in the pool.
    
    Returns:
        Dictionary containing details of all active connections.
    """
    try:
        connections = RedisConnectionPool.list_connections_in_pool()
        
        if not connections:
            return {"message": "No active connections", "connections": {}}
        
        return {
            "message": f"Found {len(connections)} active connection(s)",
            "connections": connections
        }
        
    except Exception as e:
        return {"error": f"Failed to list connections: {str(e)}"}


@mcp.tool()
async def disconnect(host_id: str) -> str:
    """Disconnect from a Redis server and remove it from the connection pool.
    
    Args:
        host_id: The identifier of the connection to remove
    
    Returns:
        Success message or error message.
    """
    try:
        result = RedisConnectionPool.remove_connection_from_pool(host_id)
        return result
        
    except Exception as e:
        return f"Failed to disconnect from {host_id}: {str(e)}"


@mcp.tool()
async def switch_default_connection(host_id: str) -> str:
    """Switch the default connection to a different host.
    
    Args:
        host_id: The identifier of the connection to set as default
    
    Returns:
        Success message or error message.
    """
    try:
        pool = RedisConnectionPool.get_instance()
        
        # Check if connection exists
        if host_id not in pool._connections:
            available = list(pool._connections.keys())
            return f"Connection '{host_id}' not found. Available connections: {available}"
        
        # Set as default
        pool._default_host = host_id
        return f"Default connection switched to '{host_id}'"
        
    except Exception as e:
        return f"Failed to switch default connection: {str(e)}"


@mcp.tool()
async def get_connection(host_id: Optional[str] = None) -> Dict[str, Any]:
    """Get details for a specific Redis connection or the default connection.
    
    Args:
        host_id: The identifier of the connection to get details for. If not provided, uses the default connection.
    
    Returns:
        Dictionary containing connection details or error message.
    """
    try:
        details = RedisConnectionPool.get_connection_details_from_pool(host_id)
        
        if "error" in details:
            return {"error": details["error"]}
        
        return {
            "message": f"Connection details for '{details['host_id']}'",
            "connection": details
        }
        
    except Exception as e:
        return {"error": f"Failed to get connection details: {str(e)}"}
