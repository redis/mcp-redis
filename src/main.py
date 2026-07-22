# # import sys
# # import logging

# # import click
# # import uvicorn # New import for running the HTTP server

# # from src.common.config import (
# #     parse_redis_uri,
# #     set_redis_config_from_cli,
# #     set_entraid_config_from_cli,
# # )
# # from src.common.logging_utils import configure_logging
# # from src.http_server import app as http_app # Import the FastAPI app


# # @click.command()
# # @click.option(
# #     "--url",
# #     help="Redis connection URI (redis://user:pass@host:port/db or rediss:// for SSL)",
# # )
# # @click.option("--host", default="127.0.0.1", help="Redis host")
# # @click.option("--port", default=6379, type=int, help="Redis port")
# # @click.option("--db", default=0, type=int, help="Redis database number")
# # @click.option("--username", help="Redis username")
# # @click.option("--password", help="Redis password")
# # @click.option("--ssl", is_flag=True, help="Use SSL connection")
# # @click.option("--ssl-ca-path", help="Path to CA certificate file")
# # @click.option("--ssl-keyfile", help="Path to SSL key file")
# # @click.option("--ssl-certfile", help="Path to SSL certificate file")
# # @click.option(
# #     "--ssl-cert-reqs", default="required", help="SSL certificate requirements"
# # )
# # @click.option("--ssl-ca-certs", help="Path to CA certificates file")
# # @click.option("--cluster-mode", is_flag=True, help="Enable Redis cluster mode")
# # # Entra ID Authentication Options
# # @click.option(
# #     "--entraid-auth-flow",
# #     type=click.Choice(["service_principal", "managed_identity", "default_credential"]),
# #     help="Entra ID authentication flow",
# # )
# # @click.option(
# #     "--entraid-client-id",
# #     help="Entra ID client ID (for service principal or user-assigned managed identity)",
# # )
# # @click.option(
# #     "--entraid-client-secret", help="Entra ID client secret (for service principal)"
# # )
# # @click.option("--entraid-tenant-id", help="Entra ID tenant ID (for service principal)")
# # @click.option(
# #     "--entraid-identity-type",
# #     type=click.Choice(["system_assigned", "user_assigned"]),
# #     default="system_assigned",
# #     help="Managed identity type",
# # )
# # @click.option(
# #     "--entraid-scopes",
# #     default="https://redis.azure.com/.default",
# #     help="Entra ID scopes (comma-separated)",
# # )
# # @click.option(
# #     "--entraid-resource", default="https://redis.azure.com/", help="Entra ID resource"
# # )
# # @click.option(
# #     "--entraid-token-refresh-ratio",
# #     type=float,
# #     default=0.9,
# #     help="Token expiration refresh ratio",
# # )
# # @click.option(
# #     "--entraid-retry-max-attempts",
# #     type=int,
# #     default=3,
# #     help="Maximum retry attempts for token requests",
# # )
# # @click.option(
# #     "--entraid-retry-delay-ms",
# #     type=int,
# #     default=100,
# #     help="Retry delay in milliseconds",
# # )
# # @click.option(
# #     "--http-host",
# #     default="0.0.0.0",
# #     help="Host address for the HTTP server (e.g., 0.0.0.0)",
# # )
# # @click.option(
# #     "--http-port",
# #     default=8000,
# #     type=int,
# #     help="Port for the HTTP server (e.g., 8000)",
# # )
# # def cli(
# #     url,
# #     host,
# #     port,
# #     db,
# #     username,
# #     password,
# #     ssl,
# #     ssl_ca_path,
# #     ssl_keyfile,
# #     ssl_certfile,
# #     ssl_cert_reqs,
# #     ssl_ca_certs,
# #     cluster_mode,
# #     entraid_auth_flow,
# #     entraid_client_id,
# #     entraid_client_secret,
# #     entraid_tenant_id,
# #     entraid_identity_type,
# #     entraid_scopes,
# #     entraid_resource,
# #     entraid_token_refresh_ratio,
# #     entraid_retry_max_attempts,
# #     entraid_retry_delay_ms,
# # ):
# #     """Redis MCP Server - Model Context Protocol server for Redis."""

# #     # Handle Redis URI if provided (and not empty)
# #     # Note: gemini-cli passes the raw "${REDIS_URL}" string when the env var is not set

# #     if url and url.strip() and url.strip() != "${REDIS_URL}":
# #         try:
# #             uri_config = parse_redis_uri(url)
# #             set_redis_config_from_cli(uri_config)
# #         except ValueError as e:
# #             click.echo(f"Error parsing Redis URI: {e}", err=True)
# #             sys.exit(1)
# #     else:
# #         # Set individual Redis parameters
# #         config = {
# #             "host": host,
# #             "port": port,
# #             "db": db,
# #             "ssl": ssl,
# #             "cluster_mode": cluster_mode,
# #         }

# #         if username:
# #             config["username"] = username
# #         if password:
# #             config["password"] = password
# #         if ssl_ca_path:
# #             config["ssl_ca_path"] = ssl_ca_path
# #         if ssl_keyfile:
# #             config["ssl_keyfile"] = ssl_keyfile
# #         if ssl_certfile:
# #             config["ssl_certfile"] = ssl_certfile
# #         if ssl_cert_reqs:
# #             config["ssl_cert_reqs"] = ssl_cert_reqs
# #         if ssl_ca_certs:
# #             config["ssl_ca_certs"] = ssl_ca_certs

# #         set_redis_config_from_cli(config)

# #     # Handle Entra ID authentication configuration
# #     entraid_config = {}
# #     if entraid_auth_flow:
# #         entraid_config["auth_flow"] = entraid_auth_flow
# #     if entraid_client_id:
# #         entraid_config["client_id"] = entraid_client_id
# #     if entraid_client_secret:
# #         entraid_config["client_secret"] = entraid_client_secret
# #     if entraid_tenant_id:
# #         entraid_config["tenant_id"] = entraid_tenant_id
# #     if entraid_identity_type:
# #         entraid_config["identity_type"] = entraid_identity_type
# #     if entraid_scopes:
# #         entraid_config["scopes"] = entraid_scopes
# #     if entraid_resource:
# #         entraid_config["resource"] = entraid_resource
# #     if entraid_token_refresh_ratio is not None:
# #         entraid_config["token_expiration_refresh_ratio"] = entraid_token_refresh_ratio
# #     if entraid_retry_max_attempts is not None:
# #         entraid_config["retry_max_attempts"] = entraid_retry_max_attempts
# #     if entraid_retry_delay_ms is not None:
# #         entraid_config["retry_delay_ms"] = entraid_retry_delay_ms

# #     # For user-assigned managed identity, use client_id as user_assigned_identity_client_id
# #     if (
# #         entraid_auth_flow == "managed_identity"
# #         and entraid_identity_type == "user_assigned"
# #         and entraid_client_id
# #     ):
# #         entraid_config["user_assigned_identity_client_id"] = entraid_client_id

# #     if entraid_config:
# #         set_entraid_config_from_cli(entraid_config)

# #     # Start the HTTP server using uvicorn
# #     _logger.info(f"Starting HTTP server on {http_host}:{http_port}")
# #     uvicorn.run(http_app, host=http_host, port=http_port)


# # if __name__ == "__main__":







# import sys
# import logging
# import click
# import uvicorn

# # from src.common.config import (
# #     parse_redis_uri,
# #     set_redis_config_from_cli,
# #     set_entraid_config_from_cli,
# # )
# from src.common.logging_utils import configure_logging
# from src.http_server import app as http_app


# @click.command()
# @click.option(
#     "--url",
#     help="Redis connection URI (redis://user:pass@host:port/db or rediss:// for SSL)",
# )
# @click.option("--host", default="127.0.0.1", help="Redis host")
# @click.option("--port", default=6379, type=int, help="Redis port")
# @click.option("--db", default=0, type=int, help="Redis database number")
# @click.option("--username", help="Redis username")
# @click.option("--password", help="Redis password")
# @click.option("--ssl", is_flag=True, help="Use SSL connection")
# @click.option("--ssl-ca-path", help="Path to CA certificate file")
# @click.option("--ssl-keyfile", help="Path to SSL key file")
# @click.option("--ssl-certfile", help="Path to SSL certificate file")
# @click.option(
#     "--ssl-cert-reqs", default="required", help="SSL certificate requirements"
# )
# @click.option("--ssl-ca-certs", help="Path to CA certificates file")
# @click.option("--cluster-mode", is_flag=True, help="Enable Redis cluster mode")
# # Entra ID Authentication Options
# @click.option(
#     "--entraid-auth-flow",
#     type=click.Choice(["service_principal", "managed_identity", "default_credential"]),
#     help="Entra ID authentication flow",
# )
# @click.option(
#     "--entraid-client-id",
#     help="Entra ID client ID (for service principal or user-assigned managed identity)",
# )
# @click.option(
#     "--entraid-client-secret", help="Entra ID client secret (for service principal)"
# )
# @click.option("--entraid-tenant-id", help="Entra ID tenant ID (for service principal)")
# @click.option(
#     "--entraid-identity-type",
#     type=click.Choice(["system_assigned", "user_assigned"]),
#     default="system_assigned",
#     help="Managed identity type",
# )
# @click.option(
#     "--entraid-scopes",
#     default="https://redis.azure.com/.default",
#     help="Entra ID scopes (comma-separated)",
# )
# @click.option(
#     "--entraid-resource", default="https://redis.azure.com/", help="Entra ID resource"
# )
# @click.option(
#     "--entraid-token-refresh-ratio",
#     type=float,
#     default=0.9,
#     help="Token expiration refresh ratio",
# )
# @click.option(
#     "--entraid-retry-max-attempts",
#     type=int,
#     default=3,
#     help="Maximum retry attempts for token requests",
# )
# @click.option(
#     "--entraid-retry-delay-ms",
#     type=int,
#     default=100,
#     help="Retry delay in milliseconds",
# )
# @click.option(
#     "--http-host",
#     default="0.0.0.0",
#     help="Host address for the HTTP server (e.g., 0.0.0.0)",
# )
# @click.option(
#     "--http-port",
#     default=8000,
#     type=int,
#     help="Port for the HTTP server (e.g., 8000)",
# )
# def cli(
#     url,
#     host,
#     port,
#     db,
#     username,
#     password,
#     ssl,
#     ssl_ca_path,
#     ssl_keyfile,
#     ssl_certfile,
#     ssl_cert_reqs,
#     ssl_ca_certs,
#     cluster_mode,
#     entraid_auth_flow,
#     entraid_client_id,
#     entraid_client_secret,
#     entraid_tenant_id,
#     entraid_identity_type,
#     entraid_scopes,
#     entraid_resource,
#     entraid_token_refresh_ratio,
#     entraid_retry_max_attempts,
#     entraid_retry_delay_ms,
#     http_host,
#     http_port,
# ):
#     """Redis MCP Server - Model Context Protocol server for Redis."""
    
#     # Initialize the runtime application logs
#     configure_logging()
#     logger = logging.getLogger(__name__)

#     # Handle Redis URI if provided (and not empty)
#     if url and url.strip() and url.strip() != "${REDIS_URL}":
#         try:
#             uri_config = parse_redis_uri(url)
#             set_redis_config_from_cli(uri_config)
#         except ValueError as e:
#             click.echo(f"Error parsing Redis URI: {e}", err=True)
#             sys.exit(1)
#     else:
#         # Set individual Redis parameters
#         config = {
#             "host": host,
#             "port": port,
#             "db": db,
#             "ssl": ssl,
#             "cluster_mode": cluster_mode,
#         }

#         if username:
#             config["username"] = username
#         if password:
#             config["password"] = password
#         if ssl_ca_path:
#             config["ssl_ca_path"] = ssl_ca_path
#         if ssl_keyfile:
#             config["ssl_keyfile"] = ssl_keyfile
#         if ssl_certfile:
#             config["ssl_certfile"] = ssl_certfile
#         if ssl_cert_reqs:
#             config["ssl_cert_reqs"] = ssl_cert_reqs
#         if ssl_ca_certs:
#             config["ssl_ca_certs"] = ssl_ca_certs

#         set_redis_config_from_cli(config)

#     # Handle Entra ID authentication configuration
#     entraid_config = {}
#     if entraid_auth_flow:
#         entraid_config["auth_flow"] = entraid_auth_flow
#     if entraid_client_id:
#         entraid_config["client_id"] = entraid_client_id
#     if entraid_client_secret:
#         entraid_config["client_secret"] = entraid_client_secret
#     if entraid_tenant_id:
#         entraid_config["tenant_id"] = entraid_tenant_id
#     if entraid_identity_type:
#         entraid_config["identity_type"] = entraid_identity_type
#     if entraid_scopes:
#         entraid_config["scopes"] = entraid_scopes
#     if entraid_resource:
#         entraid_config["resource"] = entraid_resource
#     if entraid_token_refresh_ratio is not None:
#         entraid_config["token_expiration_refresh_ratio"] = entraid_token_refresh_ratio
#     if entraid_retry_max_attempts is not None:
#         entraid_config["retry_max_attempts"] = entraid_retry_max_attempts
#     if entraid_retry_delay_ms is not None:
#         entraid_config["retry_delay_ms"] = entraid_retry_delay_ms

#     if (
#         entraid_auth_flow == "managed_identity"
#         and entraid_identity_type == "user_assigned"
#         and entraid_client_id
#     ):
#         entraid_config["user_assigned_identity_client_id"] = entraid_client_id

#     if entraid_config:
#         set_entraid_config_from_cli(entraid_config)

#     # Start the HTTP server using uvicorn
#     logger.info(f"Starting HTTP server on {http_host}:{http_port}")
#     uvicorn.run(http_app, host=http_host, port=http_port)


# # if __name__ == "__main__":
# #     cli()



# # src/main.py (Bottom block adjustment)

# if __name__ == "__main__":
#     import argparse
#     import uvicorn

#     parser = argparse.ArgumentParser(description="Multi-Tenant Async Redis Proxy Server")
#     parser.add_init_lock = None  # Internal reference indicator if needed
    
#     # Keep the network interface bindings you passed via CLI
#     parser.add_argument("--http-host", default="0.0.0.0", help="HTTP Server Bind Host")
#     parser.add_argument("--http-port", type=int, default=8000, help="HTTP Server Bind Port")
#     args = parser.parse_args()

#     print(f"🚀 Launching Multi-Tenant Core via CLI script runner on http://{args.http_host}:{args.http_port}...", flush=True)

#     # Boot straight into the local Uvicorn instance using the global app reference
#     uvicorn.run(
#         "src.main:app", 
#         host=args.http_host, 
#         port=args.http_port, 
#         reload=True,
#         log_level="debug"
#     )




import sys
import logging
import uvicorn

from src.common.logging_utils import configure_logging
from src.http_server import app as http_app

# import logging
# import sys

# # Configure the root logger format and level
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
#     handlers=[
#         logging.StreamHandler(sys.stdout)
#     ]
# )

# # Optional: Set your mcp namespace specifically to INFO or DEBUG
# logging.getLogger("mcp").setLevel(logging.INFO)



import logging
import sys

# # Remove any existing handlers added by external libraries
# for handler in logging.root.handlers[:]:
#     logging.root.removeHandler(handler)

# # Configure a clean, standard single-line log format
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
#     handlers=[logging.StreamHandler(sys.stdout)],
#     force=True
# )

# # Suppress debug/verbose output from noisy underlying libraries
# logging.getLogger("uvicorn.access").setLevel(logging.INFO)
# logging.getLogger("mcp").setLevel(logging.INFO)


# # Set base log level for your application
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)-5s | %(message)s",
#     datefmt="%H:%M:%S",
#     force=True
# )

# # Silence internal framework noise
# logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
# logging.getLogger("mcp.server").setLevel(logging.WARNING)


# import logging
# import tempfile
# import sys
# from pathlib import Path

# #Create a 'logs' directory relative to where the server is executed
# LOG_DIR = Path.cwd() / "logs"
# LOG_DIR.mkdir(exist_ok=True)
# LOG_FILE = LOG_DIR / "app.txt"

# # Configure root logger
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)-5s | [%(name)s] %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
#     handlers=[
#         # 1. Clean console output
#         logging.StreamHandler(sys.stdout),
#         # 2. Local log file inside your project folder
#         logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
#     ],
#     force=True
# )

# # Keep console output clean by muting HTTP access noise
# logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
# logging.getLogger("mcp.server").setLevel(logging.WARNING)

# print(f"📄 Logs are writing to: {LOG_FILE}")



import logging
import sys
from pathlib import Path

# Fix 1: Guarantee the log path relative to main.py directory
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "app_logs.txt"

# Configure root logger
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-5s | [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ],
    force=True
)

logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("mcp.server").setLevel(logging.WARNING)

# Test write to verify it works instantly
logging.info("--- APP LOG START ---")
print(f"📄 Writing logs to: {LOG_FILE}")

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

    print(f"🚀 Launching Multi-Tenant Core via CLI script runner on http://{args.http_host}:{args.http_port}...", flush=True)
    logger.info(f"Starting HTTP server on {args.http_host}:{args.http_port}")

    # Pass the actual app object instance straight into the Uvicorn engine
    uvicorn.run(
        http_app, 
        host=args.http_host, 
        port=args.http_port, 
        log_level="debug"
    )