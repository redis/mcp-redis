

# Use a stable slim Python image (3.12 or 3.11)
FROM python:3.12-slim AS builder

LABEL io.modelcontextprotocol.server.name="io.github.redis/mcp-redis"

# Install uv directly from its official image for faster, reliable setup
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Enable bytecode compilation for faster startup
ENV UV_COMPILE_BYTECODE=1
# Copy only lockfile and project setup first to maximize layer caching
COPY pyproject.toml uv.lock ./

# Install dependencies using cached mounts
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Copy application source code
COPY . /app

# Sync the project itself
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Place uv environment path into system PATH
ENV PATH="/app/.venv/bin:$PATH"

# Expose HTTP port for Spring Boot gateway communication
EXPOSE 8000

# Set default host and port environment variables
ENV HTTP_HOST=0.0.0.0
ENV HTTP_PORT=8000

# Execute server with HTTP bindings enabled
CMD ["uv", "run", "src/main.py", "--http-host", "0.0.0.0", "--http-port", "8000"]