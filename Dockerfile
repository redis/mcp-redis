FROM python:3.14-slim

LABEL io.modelcontextprotocol.server.name="io.github.redis/mcp-redis"
LABEL description="Redis MCP Server with support for multiple transport protocols"
LABEL transport.supported="stdio,http,sse,streamable-http"

RUN pip install --upgrade uv

WORKDIR /app
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked

# Support environment variable configuration for transport and bind address.
ENV TRANSPORT=streamable-http
ENV HTTP_HOST=0.0.0.0
ENV HTTP_PORT=8000

ENTRYPOINT ["uv", "run", "redis-mcp-server"]
