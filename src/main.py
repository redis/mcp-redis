

import sys
import logging
import uvicorn

from src.common.logging_utils import configure_logging
from src.http_server import app as http_app


# 1. Initialize logging FIRST
configure_logging()

logger = logging.getLogger("mcp.server")

# 2. Then start your app/FastMCP setup
logger.info("Launching Multi-Tenant Core on http://0.0.0.0:8000...")



if __name__ == "__main__":
    import argparse

    # Initialize runtime logs
    configure_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Multi-Tenant Async Redis Proxy Server")
    
    # Capture the network interface bindings you passed via your CLI execution
    parser.add_argument("--http-host", default="0.0.0.0", help="HTTP Server Bind Host")
    parser.add_argument("--http-port", type=int, default=8000, help="HTTP Server Bind Port")
    args = parser.parse_args()

    print(f"Launching Multi-Tenant Core via CLI script runner on http://{args.http_host}:{args.http_port}...", flush=True)
    logger.info(f"Starting HTTP server on {args.http_host}:{args.http_port}")

    # Pass the actual app object instance straight into the Uvicorn engine
    uvicorn.run(
        http_app, 
        host=args.http_host, 
        port=args.http_port, 
        log_level="debug"
    )