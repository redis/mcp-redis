import sys
import logging

import click

from src.common.config import (
    parse_redis_uri,
    set_redis_config_from_cli,
    set_entraid_config_from_cli,
)
from src.common.server import mcp
from src.common.logging_utils import configure_logging


class RedisMCPServer:
    def __init__(self):
        # Configure logging on server initialization (idempotent)
        configure_logging()
        self._logger = logging.getLogger(__name__)
        self._logger.info("Starting the Redis MCP Server")

    def run(self):
        mcp.run()


@click.command()
@click.option(
    "--url",
    help="Redis connection URI (redis://user:pass@host:port/db or rediss:// for SSL)",
)
@click.option("--host", default="127.0.0.1", help="Redis host")
@click.option("--port", default=6379, type=int, help="Redis port")
@click.option("--db", default=0, type=int, help="Redis database number")
@click.option("--username", help="Redis username")
@click.option("--password", help="Redis password")
@click.option("--ssl", is_flag=True, help="Use SSL connection")
@click.option("--ssl-ca-path", help="Path to CA certificate file")
@click.option("--ssl-keyfile", help="Path to SSL key file")
@click.option("--ssl-certfile", help="Path to SSL certificate file")
@click.option(
    "--ssl-cert-reqs", default="required", help="SSL certificate requirements"
)
@click.option("--ssl-ca-certs", help="Path to CA certificates file")
@click.option("--cluster-mode", is_flag=True, help="Enable Redis cluster mode")
# Entra ID Authentication Options
@click.option(
    "--entraid-auth-flow",
    type=click.Choice(["service_principal", "managed_identity", "default_credential"]),
    help="Entra ID authentication flow",
)
@click.option(
    "--entraid-client-id",
    help="Entra ID client ID (for service principal or user-assigned managed identity)",
)
@click.option(
    "--entraid-client-secret", help="Entra ID client secret (for service principal)"
)
@click.option("--entraid-tenant-id", help="Entra ID tenant ID (for service principal)")
@click.option(
    "--entraid-identity-type",
    type=click.Choice(["system_assigned", "user_assigned"]),
    default="system_assigned",
    help="Managed identity type",
)
@click.option(
    "--entraid-scopes",
    default="https://redis.azure.com/.default",
    help="Entra ID scopes (comma-separated)",
)
@click.option(
    "--entraid-resource", default="https://redis.azure.com/", help="Entra ID resource"
)
@click.option(
    "--entraid-token-refresh-ratio",
    type=float,
    default=0.9,
    help="Token expiration refresh ratio",
)
@click.option(
    "--entraid-retry-max-attempts",
    type=int,
    default=3,
    help="Maximum retry attempts for token requests",
)
@click.option(
    "--entraid-retry-delay-ms",
    type=int,
    default=100,
    help="Retry delay in milliseconds",
)
def cli(
    url,
    host,
    port,
    db,
    username,
    password,
    ssl,
    ssl_ca_path,
    ssl_keyfile,
    ssl_certfile,
    ssl_cert_reqs,
    ssl_ca_certs,
    cluster_mode,
    entraid_auth_flow,
    entraid_client_id,
    entraid_client_secret,
    entraid_tenant_id,
    entraid_identity_type,
    entraid_scopes,
    entraid_resource,
    entraid_token_refresh_ratio,
    entraid_retry_max_attempts,
    entraid_retry_delay_ms,
):
    """Redis MCP Server - Model Context Protocol server for Redis."""

    # Handle Redis URI if provided (and not empty)
    # Note: gemini-cli passes the raw "${REDIS_URL}" string when the env var is not set

    if url and url.strip() and url.strip() != "${REDIS_URL}":
        try:
            uri_config = parse_redis_uri(url)
            set_redis_config_from_cli(uri_config)
        except ValueError as e:
            click.echo(f"Error parsing Redis URI: {e}", err=True)
            sys.exit(1)
    else:
        # Set individual Redis parameters
        config = {
            "host": host,
            "port": port,
            "db": db,
            "ssl": ssl,
            "cluster_mode": cluster_mode,
        }

        if username:
            config["username"] = username
        if password:
            config["password"] = password
        if ssl_ca_path:
            config["ssl_ca_path"] = ssl_ca_path
        if ssl_keyfile:
            config["ssl_keyfile"] = ssl_keyfile
        if ssl_certfile:
            config["ssl_certfile"] = ssl_certfile
        if ssl_cert_reqs:
            config["ssl_cert_reqs"] = ssl_cert_reqs
        if ssl_ca_certs:
            config["ssl_ca_certs"] = ssl_ca_certs

        set_redis_config_from_cli(config)

    # Handle Entra ID authentication configuration
    entraid_config = {}
    if entraid_auth_flow:
        entraid_config["auth_flow"] = entraid_auth_flow
    if entraid_client_id:
        entraid_config["client_id"] = entraid_client_id
    if entraid_client_secret:
        entraid_config["client_secret"] = entraid_client_secret
    if entraid_tenant_id:
        entraid_config["tenant_id"] = entraid_tenant_id
    if entraid_identity_type:
        entraid_config["identity_type"] = entraid_identity_type
    if entraid_scopes:
        entraid_config["scopes"] = entraid_scopes
    if entraid_resource:
        entraid_config["resource"] = entraid_resource
    if entraid_token_refresh_ratio is not None:
        entraid_config["token_expiration_refresh_ratio"] = entraid_token_refresh_ratio
    if entraid_retry_max_attempts is not None:
        entraid_config["retry_max_attempts"] = entraid_retry_max_attempts
    if entraid_retry_delay_ms is not None:
        entraid_config["retry_delay_ms"] = entraid_retry_delay_ms

    # For user-assigned managed identity, use client_id as user_assigned_identity_client_id
    if (
        entraid_auth_flow == "managed_identity"
        and entraid_identity_type == "user_assigned"
        and entraid_client_id
    ):
        entraid_config["user_assigned_identity_client_id"] = entraid_client_id

    if entraid_config:
        set_entraid_config_from_cli(entraid_config)

    # Start the server
    server = RedisMCPServer()
    server.run()


def main():
    """Legacy main function for backward compatibility."""
    server = RedisMCPServer()
    server.run()


if __name__ == "__main__":
    main()
