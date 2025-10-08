# Copyright Redis Contributors
# SPDX-License-Identifier: MIT


async def serve_streaming(
    host: str = '0.0.0.0',
    port: int = 8000,
) -> None:
    """Serve the MCP server using streaming (SSE/Streamable HTTP) transport."""
    # Import the existing FastMCP server
    from src.common.server import mcp
    
    # Update host and port settings
    mcp.settings.host = host
    mcp.settings.port = port
    
    # FastMCP handles streamable HTTP transport natively
    await mcp.run_streamable_http_async()
