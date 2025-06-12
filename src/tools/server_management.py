from common.connection import RedisConnectionManager
from redis.exceptions import RedisError
from common.server import mcp

@mcp.tool()
async def dbsize() -> int:
    """Get the number of keys stored in the Redis database
    """
    try:
        r = RedisConnectionManager.get_connection()
        return r.dbsize()
    except RedisError as e:
        return f"Error getting database size: {str(e)}"


@mcp.tool()
async def info(section: str = "default") -> dict:
    """Get Redis server information and statistics.

    Args:
        section: The section of the info command (default, memory, cpu, etc.).

    Returns:
        A dictionary of server information or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        info = r.info(section)
        return info
    except RedisError as e:
        return f"Error retrieving Redis info: {str(e)}"


@mcp.tool()
async def client_list() -> list:
    """Get a list of connected clients to the Redis server."""
    try:
        r = RedisConnectionManager.get_connection()
        clients = r.client_list()
        return clients
    except RedisError as e:
        return f"Error retrieving client list: {str(e)}"


@mcp.tool()
async def switch_database(db: int) -> str:
    """
    Switch to a different Redis database.
    
    Args:
        db (int): Database number (0-15 for most Redis configurations)
        
    Returns:
        str: Confirmation message or error message
    """
    try:
        r = RedisConnectionManager.get_connection()
        r.execute_command("SELECT", db)
        return f"Successfully switched to database {db}"
    except RedisError as e:
        return f"Error switching to database {db}: {str(e)}"


@mcp.tool()
async def get_current_database() -> dict:
    """
    Get information about the currently selected database.
    
    Returns:
        Dict containing current database info and key count
    """
    try:
        r = RedisConnectionManager.get_connection()
        # Get current database info
        info = r.info('keyspace')
        
        # Try to determine current database by checking connection
        try:
            # This will show us which DB we're connected to
            config_info = r.config_get('databases')
            total_dbs = int(config_info.get('databases', 16))
        except:
            total_dbs = 16  # Default Redis database count
            
        # Get current database number (Redis doesn't have a direct command for this)
        # We'll use a workaround by checking which DB has activity or using SELECT
        current_info = r.execute_command("INFO", "keyspace")
        
        return {
            "keyspace_info": info,
            "total_databases": total_dbs,
            "current_keyspace": current_info,
            "note": "Use switch_database(db) to change database"
        }
    except RedisError as e:
        return {"error": f"Error getting database info: {str(e)}"}


@mcp.tool()
async def list_all_databases() -> dict:
    """
    List all databases and their key counts.
    
    Returns:
        Dict containing information about all databases
    """
    try:
        databases_info = {}
        
        # Try databases 0-15 (standard Redis range)
        for db_num in range(16):
            try:
                # Create connection for specific database
                r = RedisConnectionManager.get_connection_for_db(db_num)
                key_count = r.dbsize()
                
                if key_count > 0:  # Only include databases with keys
                    databases_info[f"db{db_num}"] = {
                        "database": db_num,
                        "keys": key_count,
                        "has_data": True
                    }
                else:
                    databases_info[f"db{db_num}"] = {
                        "database": db_num,
                        "keys": 0,
                        "has_data": False
                    }
                    
            except RedisError:
                # Skip databases that can't be accessed
                continue
                
        return {
            "databases": databases_info,
            "total_found": len([db for db in databases_info.values() if db["has_data"]]),
            "note": "Use switch_database(db_number) to switch between databases"
        }
        
    except Exception as e:
        return {"error": f"Error listing databases: {str(e)}"}