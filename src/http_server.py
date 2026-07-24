

"""HTTP API layer for Redis MCP Server - Production Streamable HTTP Architecture"""
import asyncio
import contextlib
import logging

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from src.common.server import mcp


# Import context tokens and lifecycle connection managers
from src.common.connection import current_tenant_id, current_mcp_id, tenant_redis_manager
from src.common.tenant_db import tenant_db_manager


logger = logging.getLogger(__name__)


async def periodic_idle_tenant_cleanup():
    """
    Background worker that runs every 10 seconds to evict inactive 
    tenant PostgreSQL and Redis connection pools (idle timeout > 10m).
    """
    while True:
        try:
            await asyncio.sleep(10)

            # Clean Redis idle pools
            try:
                await tenant_redis_manager.close_idle_pools()
            except Exception as e:
                logger.error(f"Error evicting idle Redis pools: {e}", exc_info=True)

            # Clean DB idle pools
            try:
                await tenant_db_manager.close_idle_pools()
            except Exception as e:
                logger.error(f"Error evicting idle DB pools: {e}", exc_info=True)

        except asyncio.CancelledError:
            # Clean exit on task cancellation during lifespan shutdown
            logger.info("Background idle cleanup worker cancelled. Stopping loop.")
            raise


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the clean startup and shutdown lifecycles of the application.
    """
    logger.info("Starting Redis MCP server lifespan management...")
    
    # 1. Start the periodic background task for idle tenant pool cleanup
    cleanup_task = asyncio.create_task(periodic_idle_tenant_cleanup())

    try:
        # 2. FastMCP uses session_manager.run() to control the background async engine loop
        async with mcp.session_manager.run():
            logger.info("FastMCP stream engine session successfully bound.")
            yield

    finally:
        # 3. Cancel the background worker loop on server shutdown
        cleanup_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await cleanup_task

        # 4. Lifecycle Shutdown Block: Prevents socket leaks on server exit
        logger.info("[LIFECYCLE] Initiating graceful draining of all isolated tenant connectivity frameworks...")

        try:
            # Drop both Redis and Postgres tenant pools cleanly
            await tenant_redis_manager.shutdown_all_pools()
            await tenant_db_manager.shutdown_all_pools()
            logger.info("[LIFECYCLE] All dynamic tenant pools closed successfully.")
        except Exception as e:
            logger.error(f"[LIFECYCLE ERROR] Failed to drop tenant structural pools: {str(e)}", exc_info=True)

        logger.info("Application shutdown complete.")




class MultiTenantMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        # Skip tenant verification for internal/docs pathways
        if request.url.path in ["/health", "/metrics", "/docs", "/openapi.json"]:
            return await call_next(request)

        # FIX FOR SPRING BOOT RESTTEMPLATE:
        # Silently rewrite /mcp to /mcp/ in ASGI scope so FastAPI won't send a 307 Redirect.
        if request.scope["path"] == "/mcp":
            request.scope["path"] = "/mcp/"
            request.scope["raw_path"] = b"/mcp/"

        tenant_id = request.headers.get("X-Tenant-ID")
        if not tenant_id or not tenant_id.strip():
            logger.warning(f"Rejected request to {request.url.path} - Missing X-Tenant-ID header.")
            return JSONResponse(
                status_code=400,
                content={"error": "Missing or empty mandatory 'X-Tenant-ID' header."}
            )

        # Extract optional X-MCP-ID header sent from Java
        mcp_id = request.headers.get("X-MCP-ID")

        # Set thread-local context tokens
        tenant_token = current_tenant_id.set(tenant_id.strip())
        mcp_token = current_mcp_id.set(mcp_id.strip() if mcp_id else None)

        logger.info(f"[TENANT TRACKER] Processing request for Tenant: '{tenant_id}' | MCP ID: '{mcp_id}' on path: {request.url.path}")

        try:
            response = await call_next(request)
            return response
        except Exception as e:
            logger.error(f"Execution failure for tenant '{tenant_id}': {str(e)}", exc_info=True)
            return JSONResponse(
                status_code=500,
                content={"error": "Internal server execution failure within tenant environment pool."}
            )
        finally:
            current_tenant_id.reset(tenant_token)
            current_mcp_id.reset(mcp_token)


# Instantiate the web server
app = FastAPI(
    title="Redis Multi-Tenant MCP Server",
    description="A production streamable HTTP server for handling isolated tenant Redis environments.",
    version="1.0",
    lifespan=lifespan,
)


# Register the middleware layer smoothly
app.add_middleware(MultiTenantMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["Mcp-Session-Id"],
)

# Mount the MCP server's streamable HTTP application at the /mcp path
app.mount("/mcp", mcp.streamable_http_app())