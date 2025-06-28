import os
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server with env configurable host/port
mcp = FastMCP(
    "Redis MCP Server",
    dependencies=["redis", "dotenv", "numpy"],
    host=os.getenv('MCP_HOST', '0.0.0.0'),
    port=int(os.getenv('MCP_PORT', '8000'))
)