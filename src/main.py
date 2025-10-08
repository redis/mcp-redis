import sys
import click
import asyncio

from src.common.config import build_redis_config
from src.common.connection import RedisConnectionPool
from src.common.stdio_server import serve_stdio
from src.common.streaming_server import serve_streaming
import src.tools.server_management
import src.tools.misc
import src.tools.redis_query_engine
import src.tools.hash
import src.tools.list
import src.tools.string
import src.tools.json
import src.tools.sorted_set
import src.tools.set
import src.tools.stream
import src.tools.pub_sub
import src.tools.connection_management


@click.command()
@click.option('--transport', default='stdio', type=click.Choice(['stdio', 'streamable-http']), 
              help='Transport method (stdio or streamable-http)')
@click.option('--http-host', default='127.0.0.1', help='HTTP server host (for streamable-http transport)')
@click.option('--http-port', default=8000, type=int, help='HTTP server port (for streamable-http transport)')
@click.option('--url', help='Redis connection URI (redis://user:pass@host:port/db or rediss:// for SSL)')
@click.option('--host', default='127.0.0.1', help='Redis host')
@click.option('--port', default=6379, type=int, help='Redis port')
@click.option('--db', default=0, type=int, help='Redis database number')
@click.option('--username', help='Redis username')
@click.option('--password', help='Redis password')
@click.option('--ssl', is_flag=True, help='Use SSL connection')
@click.option('--ssl-ca-path', help='Path to CA certificate file')
@click.option('--ssl-keyfile', help='Path to SSL key file')
@click.option('--ssl-certfile', help='Path to SSL certificate file')
@click.option('--ssl-cert-reqs', default='required', help='SSL certificate requirements')
@click.option('--ssl-ca-certs', help='Path to CA certificates file')
@click.option('--cluster-mode', is_flag=True, help='Enable Redis cluster mode')
def cli(transport, http_host, http_port, url, host, port, db, username, password,
        ssl, ssl_ca_path, ssl_keyfile, ssl_certfile,
        ssl_cert_reqs, ssl_ca_certs, cluster_mode):
    """Redis MCP Server - Model Context Protocol server for Redis."""
    
    try:
        # Build configuration using unified logic - URL takes precedence but individual params can override
        config, host_id = build_redis_config(
            url=url, host=host, port=port, db=db, username=username, password=password,
            ssl=ssl, ssl_ca_path=ssl_ca_path, ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile, ssl_cert_reqs=ssl_cert_reqs,
            ssl_ca_certs=ssl_ca_certs, cluster_mode=cluster_mode
        )
        
        # Add connection directly to pool
        RedisConnectionPool.add_connection_to_pool(host_id, config)
        
    except ValueError as e:
        click.echo(f"Error parsing Redis configuration: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error connecting to Redis: {e}", err=True)
        sys.exit(1)

    # Start the appropriate server
    if transport == "streamable-http":
        asyncio.run(serve_streaming(host=http_host, port=http_port))
    else:
        asyncio.run(serve_stdio())


def main():
    """Main entry point for backward compatibility."""
    cli()


if __name__ == "__main__":
    main()
