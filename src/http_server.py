# """HTTP API layer for Redis MCP Server - Production Streamable HTTP Architecture"""
# import contextlib
# import logging

# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
# from starlette.middleware.base import BaseHTTPMiddleware
# from fastapi import FastAPI, Request, Response  # <-- Added Request and Response here
# from fastapi.responses import JSONResponse

# from src.common.server import mcp


# # Import context tokens and lifecycle connection managers
# from src.common.connection import current_tenant_id  , tenant_redis_manager
# # from src.common.tenant_db import tenant_db_manager


# logger = logging.getLogger(__name__)


# # @contextlib.asynccontextmanager
# # async def lifespan(app: FastAPI):
# #     """
# #     Manages the clean startup and shutdown lifecycles of the application.
# #     """
# #     logger.info("Starting Redis MCP server lifespan management...")
    
# #     # FastMCP uses session_manager.run() to control the background async engine loop
# #     async with mcp.session_manager.run():
# #         logger.info("FastMCP stream engine session successfully bound.")
# #         yield
        
# #     logger.info("Application shutdown complete.")




# # @contextlib.asynccontextmanager
# # async def lifespan(app: FastAPI):
# #     """
# #     Manages the clean startup and shutdown lifecycles of the application.
# #     """
# #     logger.info("Starting Redis MCP server lifespan management...")
    
# #     # FastMCP uses session_manager.run() to control the background async engine loop
# #     async with mcp.session_manager.run():
# #         logger.info("FastMCP stream engine session successfully bound.")
# #         yield
        
# #     # --- Lifecycle Shutdown Block ---
# #     logger.info("Draining all isolated tenant connectivity frameworks...")
# #     try:
# #         # await tenant_redis_manager.shutdown_all_pools()
# #         # await tenant_db_manager.shutdown_all_pools()
# #         logger.info("All tenant pools closed successfully.")
# #     except Exception as e:
# #         logger.error(f"Error occurred during tenant infrastructure pool teardown: {str(e)}", exc_info=True)
        
# #     logger.info("Application shutdown complete.")

# async def periodic_idle_tenant_cleanup():
#     """
#     Background worker that runs every 60 seconds to evict inactive 
#     tenant PostgreSQL and Redis connection pools (idle timeout > 10m).
#     """
#     while True:
#         try:
#             await asyncio.sleep(60)

#             # Clean Redis idle pools
#             try:
#                 await tenant_redis_manager.close_idle_pools()
#             except Exception as e:
#                 logger.error(f"Error evicting idle Redis pools: {e}", exc_info=True)

#             # Clean DB idle pools
#             try:
#                 await tenant_db_manager.close_idle_pools()
#             except Exception as e:
#                 logger.error(f"Error evicting idle DB pools: {e}", exc_info=True)

#         except asyncio.CancelledError:
#             # Clean exit on task cancellation during lifespan shutdown
#             logger.info("Background idle cleanup worker cancelled. Stopping loop.")
#             raise


# @contextlib.asynccontextmanager
# async def lifespan(app: FastAPI):
#     """
#     Manages the clean startup and shutdown lifecycles of the application.
#     """
#     logger.info("Starting Redis MCP server lifespan management...")
    
#     try:
#         # FastMCP uses session_manager.run() to control the background async engine loop
#         async with mcp.session_manager.run():
#             logger.info("FastMCP stream engine session successfully bound.")
#             yield
            
#     finally:
#         # --- Lifecycle Shutdown Block ---
#         # This code is guaranteed to run on server exit, preventing resource leaks
#         print("🛑 [LIFECYCLE] Initiating graceful draining of all isolated tenant connectivity frameworks...", flush=True)
#         logger.info("Draining all isolated tenant connectivity frameworks...")
        
#         try:
#             # tenant_redis_manager internally calls tenant_db_manager down the chain
#             await tenant_redis_manager.shutdown_all_pools()
#             print("✨ [LIFECYCLE] All dynamic tenant pools closed successfully.", flush=True)
#             logger.info("All tenant pools closed successfully.")
#         except Exception as e:
#             print(f"❌ [LIFECYCLE ERROR] Failed to drop tenant structural pools: {str(e)}", flush=True)
#             logger.error(f"Error occurred during tenant infrastructure pool teardown: {str(e)}", exc_info=True)
            
#         logger.info("Application shutdown complete.")

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
from src.common.connection import current_tenant_id, tenant_redis_manager
from src.common.tenant_db import tenant_db_manager

logger = logging.getLogger(__name__)


async def periodic_idle_tenant_cleanup():
    """
    Background worker that runs every 60 seconds to evict inactive 
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
        print("🛑 [LIFECYCLE] Initiating graceful draining of all isolated tenant connectivity frameworks...", flush=True)
        logger.info("Draining all isolated tenant connectivity frameworks...")

        try:
            # Drop both Redis and Postgres tenant pools cleanly
            await tenant_redis_manager.shutdown_all_pools()
            await tenant_db_manager.shutdown_all_pools()
            print("✨ [LIFECYCLE] All dynamic tenant pools closed successfully.", flush=True)
            logger.info("All tenant pools closed successfully.")
        except Exception as e:
            print(f"❌ [LIFECYCLE ERROR] Failed to drop tenant structural pools: {str(e)}", flush=True)
            logger.error(f"Error occurred during tenant infrastructure pool teardown: {str(e)}", exc_info=True)

        logger.info("Application shutdown complete.")



# --- 1. Define the Middleware Layer ---
class MultiTenantMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        # Skip tenant verification for internal/docs pathways
        if request.url.path in ["/health", "/metrics", "/docs", "/openapi.json"]:
            return await call_next(request)

        tenant_id = request.headers.get("X-Tenant-ID")
        if not tenant_id or not tenant_id.strip():
            logger.warning(f"Rejected request to {request.url.path} - Missing X-Tenant-ID header.")
            return JSONResponse(
                status_code=400,
                content={"error": "Missing or empty mandatory 'X-Tenant-ID' header."}
            )

        # Secure context isolation ring-fence
        token = current_tenant_id.set(tenant_id.strip())

        # Using a raw print statement to completely bypass logger layout/buffering
        print(f"\n>>>> [TENANT HOOK] Connection established for: {tenant_id} on {request.url.path} <<<<\n", flush=True)
        # ─── ADD THIS LINE FOR TESTING LOGS ────────────────────────────────────
        logger.info(f"🔑 [TENANT TRACKER] Processing request for Tenant ID: '{tenant_id}' on path: {request.url.path}")
        # ────────────────────────────────────────────────────────────────
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
            # Safely release the context trace frame
            current_tenant_id.reset(token)


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