import sys
import logging

import click

from src.common.config import (
    parse_redis_uri,
    set_redis_config_from_cli,
    set_entraid_config_from_cli,
    validate_redis_config,
    DEFAULT_SENTINEL_PORT,
)
from src.common.server import mcp
from src.common.logging_utils import configure_logging


class RedisMCPServer:
    def __init__(self, transport="stdio", http_host="127.0.0.1", http_port=8000):
        configure_logging()
        self._logger = logging.getLogger(__name__)
        self.transport = self._normalize_transport(transport)
        self.http_host = http_host
        self.http_port = http_port
        self._logger.info(
            "Starting the Redis MCP Server with transport: %s on %s:%s",
            self.transport,
            self.http_host,
            self.http_port,
        )

    @staticmethod
    def _normalize_transport(transport):
        # FastMCP exposes streamable HTTP using the "streamable-http" transport name.
        # Accept "http" as a user-facing alias because MCP clients and metadata commonly
        # describe this deployment mode as HTTP.
        if transport == "http":
            return "streamable-http"
        return transport

    def run(self):
        mcp.settings.host = self.http_host
        mcp.settings.port = self.http_port
        mcp.run(transport=self.transport)


@click.command()
@click.option(
    "--url",
    help="Redis connection URI (redis://user:pass@host:port/db or rediss:// for SSL)",
)
@click.option(
    "--transport",
    type=click.Choice(["stdio", "http", "sse", "streamable-http"]),
    default="stdio",
    envvar="TRANSPORT",
    help="Transport protocol (stdio, http, sse, or streamable-http)",
)
@click.option(
    "--http-port",
    type=int,
    default=8000,
    envvar=["HTTP_PORT", "FASTMCP_PORT"],
    help="Port for HTTP/SSE transport",
)
@click.option(
    "--http-host",
    default="127.0.0.1",
    envvar=["HTTP_HOST", "FASTMCP_HOST"],
    help="Host for HTTP/SSE transport",
)
@click.option("--host", default="127.0.0.1", envvar="REDIS_HOST", help="Redis host")
@click.option("--port", default=6379, envvar="REDIS_PORT", type=int, help="Redis port")
@click.option(
    "--db", default=0, envvar="REDIS_DB", type=int, help="Redis database number"
)
@click.option("--username", envvar="REDIS_USERNAME", help="Redis username")
@click.option("--password", envvar="REDIS_PWD", help="Redis password")
@click.option("--ssl", is_flag=True, envvar="REDIS_SSL", help="Use SSL connection")
@click.option(
    "--ssl-ca-path", envvar="REDIS_SSL_CA_PATH", help="Path to CA certificate file"
)
@click.option("--ssl-keyfile", envvar="REDIS_SSL_KEYFILE", help="Path to SSL key file")
@click.option(
    "--ssl-certfile", envvar="REDIS_SSL_CERTFILE", help="Path to SSL certificate file"
)
@click.option(
    "--ssl-cert-reqs",
    default="required",
    envvar="REDIS_SSL_CERT_REQS",
    help="SSL certificate requirements",
)
@click.option(
    "--ssl-ca-certs", envvar="REDIS_SSL_CA_CERTS", help="Path to CA certificates file"
)
@click.option(
    "--topology",
    type=click.Choice(["standalone", "sentinel", "cluster"]),
    envvar="REDIS_TOPOLOGY",
    help="Redis topology mode",
)
@click.option(
    "--cluster-mode",
    is_flag=True,
    envvar="REDIS_CLUSTER_MODE",
    help="Enable Redis cluster mode (legacy compatibility flag)",
)
@click.option(
    "--sentinel-master-name",
    envvar="REDIS_SENTINEL_MASTER_NAME",
    help="Sentinel master name to resolve",
)
@click.option(
    "--sentinel-nodes",
    envvar="REDIS_SENTINEL_NODES",
    help="Comma-separated Sentinel nodes in host:port form",
)
@click.option(
    "--sentinel-host",
    envvar="REDIS_SENTINEL_HOST",
    help="Single Sentinel host (alternative to --sentinel-nodes)",
)
@click.option(
    "--sentinel-port",
    envvar="REDIS_SENTINEL_PORT",
    type=int,
    default=DEFAULT_SENTINEL_PORT,
    help="Single Sentinel port when using --sentinel-host",
)
@click.option(
    "--sentinel-username",
    envvar="REDIS_SENTINEL_USERNAME",
    help="Sentinel username if Sentinel itself requires authentication",
)
@click.option(
    "--sentinel-password",
    envvar="REDIS_SENTINEL_PWD",
    help="Sentinel password if Sentinel itself requires authentication",
)
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
    transport,
    http_port,
    http_host,
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
    topology,
    cluster_mode,
    sentinel_master_name,
    sentinel_nodes,
    sentinel_host,
    sentinel_port,
    sentinel_username,
    sentinel_password,
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
            "sentinel_master_name": sentinel_master_name,
            "sentinel_username": sentinel_username,
            "sentinel_password": sentinel_password,
        }

        # Only include topology in config when explicitly set, so the after-loop
        # guard in set_redis_config_from_cli can properly handle --cluster-mode
        # without --topology
        if topology is not None:
            config["topology"] = topology

        if sentinel_nodes:
            config["sentinel_nodes"] = sentinel_nodes
        elif sentinel_host:
            config["sentinel_nodes"] = [(sentinel_host, sentinel_port)]
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

    is_valid, error_message = validate_redis_config()
    if not is_valid:
        click.echo(f"Error validating Redis configuration: {error_message}", err=True)
        sys.exit(1)

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
    server = RedisMCPServer(
        transport=transport,
        http_host=http_host,
        http_port=http_port,
    )
    server.run()


def main():
    """Legacy main function for backward compatibility."""
    server = RedisMCPServer(transport="stdio")
    server.run()


if __name__ == "__main__":
    main()
