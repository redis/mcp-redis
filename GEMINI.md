# Redis MCP Server Extension

This extension provides a natural language interface for managing and searching data in Redis through the Model Context Protocol (MCP).

## What this extension provides

The Redis MCP Server enables AI agents to efficiently interact with Redis databases using natural language commands. You can:

- **Store and retrieve data**: Cache items, store session data, manage configuration values
- **Work with data structures**: Manage hashes, lists, sets, sorted sets, and streams
- **Search and filter**: Perform efficient data retrieval and searching operations
- **Pub/Sub messaging**: Publish and subscribe to real-time message channels
- **JSON operations**: Store, retrieve, and manipulate JSON documents
- **Vector search**: Manage vector indexes and perform similarity searches

## Available Tools

### String Operations
- Set, get, and manage string values with optional expiration
- Useful for caching, session data, and simple configuration

### Hash Operations  
- Store field-value pairs within a single key
- Support for vector embeddings storage
- Ideal for user profiles, product information, and structured objects

### List Operations
- Append, pop, and manage list items
- Perfect for queues, message brokers, and activity logs

### Set Operations
- Add, remove, and list unique set members
- Perform set operations like intersection and union
- Great for tracking unique values and tags

### Sorted Set Operations
- Manage score-based ordered data
- Ideal for leaderboards, priority queues, and time-based analytics

### Pub/Sub Operations
- Publish messages to channels and subscribe to receive them
- Real-time notifications and chat applications

### Stream Operations
- Add, read, and delete from data streams
- Event sourcing, activity feeds, and sensor data logging

### JSON Operations
- Store, retrieve, and manipulate JSON documents
- Complex nested data structures with path-based access

### Vector Search
- Manage vector indexes and perform similarity searches
- AI/ML applications and semantic search

### Server Management
- Retrieve database information and statistics
- Monitor Redis server status and performance

## Usage Examples

You can interact with Redis using natural language:

- "Store this user session data with a 1-hour expiration"
- "Add this item to the shopping cart list"
- "Search for similar vectors in the product embeddings"
- "Publish a notification to the alerts channel"
- "Get the top 10 scores from the leaderboard"
- "Cache this API response for 5 minutes"

## Configuration

The extension connects to Redis using a Redis URL. Default configuration connects to `redis://127.0.0.1:6379/0`.

### Primary Configuration: Redis URL

Set the `REDIS_URL` environment variable to configure your Redis connection:

```bash
export REDIS_URL=redis://[username:password@]host:port/database
```

### Configuration Examples

**Local Redis (no authentication):**
```bash
export REDIS_URL=redis://127.0.0.1:6379/0
# or
export REDIS_URL=redis://localhost:6379/0
```

**Redis with password:**
```bash
export REDIS_URL=redis://:mypassword@localhost:6379/0
```

**Redis with username and password:**
```bash
export REDIS_URL=redis://myuser:mypassword@localhost:6379/0
```

**Redis Cloud:**
```bash
export REDIS_URL=redis://default:abc123@redis-12345.c1.us-east-1.ec2.cloud.redislabs.com:12345/0
```

**Redis with SSL:**
```bash
export REDIS_URL=rediss://user:pass@secure-redis.com:6380/0
```

**Redis with SSL and certificates:**
```bash
export REDIS_URL=rediss://user:pass@host:6380/0?ssl_cert_reqs=required&ssl_ca_certs=/path/to/ca.pem
```

**AWS ElastiCache:**
```bash
export REDIS_URL=redis://my-cluster.abc123.cache.amazonaws.com:6379/0
```

**Azure Cache for Redis:**
```bash
export REDIS_URL=rediss://mycache.redis.cache.windows.net:6380/0?ssl_cert_reqs=required
```

### Backward Compatibility: Individual Environment Variables

If `REDIS_URL` is not set, the extension will fall back to individual environment variables:

- `REDIS_HOST` - Redis hostname (default: 127.0.0.1)
- `REDIS_PORT` - Redis port (default: 6379)
- `REDIS_DB` - Database number (default: 0)
- `REDIS_USERNAME` - Redis username (optional)
- `REDIS_PWD` - Redis password (optional)
- `REDIS_SSL` - Enable SSL: "true" or "false" (default: false)
- `REDIS_SSL_CA_PATH` - Path to CA certificate file
- `REDIS_SSL_KEYFILE` - Path to SSL key file
- `REDIS_SSL_CERTFILE` - Path to SSL certificate file
- `REDIS_SSL_CERT_REQS` - SSL certificate requirements (default: "required")
- `REDIS_SSL_CA_CERTS` - Path to CA certificates file
- `REDIS_CLUSTER_MODE` - Enable cluster mode: "true" or "false" (default: false)

**Example using individual variables:**
```bash
export REDIS_HOST=my-redis-server.com
export REDIS_PORT=6379
export REDIS_PWD=mypassword
export REDIS_SSL=true
```

### Configuration Priority

1. **`REDIS_URL`** (highest priority) - If set, this will be used exclusively
2. **Individual environment variables** - Used as fallback when `REDIS_URL` is not set
3. **Built-in defaults** - Used when no configuration is provided

### Configuration Methods

1. **Environment Variables**: Set variables in your shell or system
2. **`.env` File**: Create a `.env` file in your project directory
3. **System Environment**: Set variables at the system level
4. **Shell Profile**: Add exports to your `.bashrc`, `.zshrc`, etc.

### No Configuration Required

If you don't set any configuration, the extension will automatically connect to a local Redis instance at `redis://127.0.0.1:6379/0`.

### Advanced SSL Configuration

For production environments with custom SSL certificates, you can use query parameters in the Redis URL:

```bash
export REDIS_URL=rediss://user:pass@host:6380/0?ssl_cert_reqs=required&ssl_ca_path=/path/to/ca.pem&ssl_keyfile=/path/to/key.pem&ssl_certfile=/path/to/cert.pem
```

Supported SSL query parameters:
- `ssl_cert_reqs` - Certificate requirements: "required", "optional", "none"
- `ssl_ca_certs` - Path to CA certificates file
- `ssl_ca_path` - Path to CA certificate file
- `ssl_keyfile` - Path to SSL private key file
- `ssl_certfile` - Path to SSL certificate file

For detailed configuration options and Redis URL format, see the main Redis MCP Server documentation.
