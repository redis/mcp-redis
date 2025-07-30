from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("Redis MCP Server", dependencies=["redis", "dotenv", "numpy"])
