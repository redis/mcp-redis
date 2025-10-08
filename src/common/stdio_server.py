# Copyright Redis Contributors
# SPDX-License-Identifier: MIT


async def serve_stdio() -> None:
    """Serve the MCP server using stdio transport."""
    # Import the existing FastMCP server
    from src.common.server import mcp
    
    # FastMCP handles stdio transport natively
    await mcp.run_stdio_async()
