# Redis MCP Server
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.13%2B-blue)](https://www.python.org/downloads/)
[![smithery badge](https://smithery.ai/badge/@redis/mcp-redis)](https://smithery.ai/server/@redis/mcp-redis)
[![Verified on MseeP](https://mseep.ai/badge.svg)](https://mseep.ai/app/70102150-efe0-4705-9f7d-87980109a279)

## Overview
The Redis MCP Server is a **natural language interface** designed for agentic applications to efficiently manage and search data in Redis. It integrates seamlessly with **MCP (Model Content Protocol) clients**, enabling AI-driven workflows to interact with structured and unstructured data in Redis. Using this MCP Server, you can ask questions like:

- "Store the entire conversation in a stream"
- "Cache this item"
- "Store the session with an expiration time"
- "Index and search this vector"

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Tools](#tools)
- [Installation](#installation)
  - [Quick Start with uvx](#quick-start-with-uvx)
  - [Development Installation](#development-installation)
  - [With Docker](#with-docker)
- [Configuration](#configuration)
  - [Redis ACL](#redis-acl)
  - [Configuration via command line arguments](#configuration-via-command-line-arguments)
  - [Configuration via Environment Variables](#configuration-via-environment-variables)
- [Integrations](#integrations)
  - [OpenAI Agents SDK](#openai-agents-sdk)
  - [Augment](#augment)
  - [Claude Desktop](#claude-desktop)
  - [VS Code with GitHub Copilot](#vs-code-with-github-copilot)
- [Testing](#testing)
- [Example Use Cases](#example-use-cases)
- [Contributing](#contributing)
- [License](#license)
- [Badges](#badges)
- [Contact](#contact)


## Features
- **Natural Language Queries**: Enables AI agents to query and update Redis using natural language.
- **Seamless MCP Integration**: Works with any **MCP client** for smooth communication.
- **Full Redis Support**: Handles **hashes, lists, sets, sorted sets, streams**, and more.
- **Search & Filtering**: Supports efficient data retrieval and searching in Redis.
- **Scalable & Lightweight**: Designed for **high-performance** data operations.

## Tools

This MCP Server provides tools to manage the data stored in Redis.

- `string` tools to set, get strings with expiration. Useful for storing simple configuration values, session data, or caching responses.
- `hash` tools to store field-value pairs within a single key. The hash can store vector embeddings. Useful for representing objects with multiple attributes, user profiles, or product information where fields can be accessed individually.
- `list` tools with common operations to append and pop items. Useful for queues, message brokers, or maintaining a list of most recent actions.
- `set` tools to add, remove and list set members. Useful for tracking unique values like user IDs or tags, and for performing set operations like intersection.
- `sorted set` tools to manage data for e.g. leaderboards, priority queues, or time-based analytics with score-based ordering.
- `pub/sub` functionality to publish messages to channels and subscribe to receive them. Useful for real-time notifications, chat applications, or distributing updates to multiple clients.
- `streams` tools to add, read, and delete from data streams. Useful for event sourcing, activity feeds, or sensor data logging with consumer groups support.
- `JSON` tools to store, retrieve, and manipulate JSON documents in Redis. Useful for complex nested data structures, document databases, or configuration management with path-based access.

Additional tools.

- `query engine` tools to manage vector indexes and perform vector search
- `server management` tool to retrieve information about the database

## Installation

The Redis MCP Server supports both `stdio` and `streamable-http` [transports](https://modelcontextprotocol.io/docs/concepts/transports).

> No PyPi package is available at the moment.

### Quick Start with uvx 

The easiest way to use the Redis MCP Server is with `uvx`, which allows you to run it directly from GitHub (from a branch, or use a tagged release). It is recommended to use a tagged release, the `main` branch is under active development and may contain breaking changes. As an example, you can execute the following command to run the `0.2.0` release:

```commandline
uvx --from git+https://github.com/redis/mcp-redis.git@0.2.0 redis-mcp-server --url redis://localhost:6379/0
```

Check the release notes for the latest version in the [Releases](https://github.com/redis/mcp-redis/releases) section.
Additional examples are provided below.

```sh
# Run with Redis URI
uvx --from git+https://github.com/redis/mcp-redis.git redis-mcp-server --url redis://localhost:6379/0

# Run with Redis URI and SSL 
uvx --from git+https://github.com/redis/mcp-redis.git redis-mcp-server --url "rediss://<USERNAME>:<PASSWORD>@<HOST>:<PORT>?ssl_cert_reqs=required&ssl_ca_certs=<PATH_TO_CERT>"

# Run with individual parameters
uvx --from git+https://github.com/redis/mcp-redis.git redis-mcp-server --host localhost --port 6379 --password mypassword

# Run with streamable HTTP transport (default on http://127.0.0.1:8000/mcp)
uvx --from git+https://github.com/redis/mcp-redis.git redis-mcp-server --transport streamable-http --url redis://localhost:6379/0

# Run with streamable HTTP on custom host/port
uvx --from git+https://github.com/redis/mcp-redis.git redis-mcp-server --transport streamable-http --http-host 0.0.0.0 --http-port 8080 --url redis://localhost:6379/0

# See all options
uvx --from git+https://github.com/redis/mcp-redis.git redis-mcp-server --help
```

### Running with Streamable HTTP

```sh
# Development mode with streamable HTTP
uv run redis-mcp-server --transport streamable-http --url redis://localhost:6379/0

# Production mode with custom host and port
uv run redis-mcp-server --transport streamable-http --http-host 0.0.0.0 --http-port 8000 --url redis://localhost:6379/0
```

### Development Installation

For development or if you prefer to clone the repository:

```sh
# Clone the repository
git clone https://github.com/redis/mcp-redis.git
cd mcp-redis

# Install dependencies using uv
uv venv
source .venv/bin/activate
uv sync

# Run with CLI interface
uv run redis-mcp-server --help

# Run with stdio transport (default)
uv run src/main.py

# Run with streamable HTTP transport
uv run src/main.py --transport streamable-http --http-host 127.0.0.1 --http-port 8000

# Or run the main file directly (uses environment variables)
uv run src/main.py
```

Once you cloned the repository, installed the dependencies and verified you can run the server, you can configure Claude Desktop or any other MCP Client to use this MCP Server running the main file directly (it uses environment variables). This is usually preferred for development.
The following example is for Claude Desktop, but the same applies to any other MCP Client.

1. Specify your Redis credentials and TLS configuration
2. Retrieve your `uv` command full path (e.g. `which uv`)
3. Edit the `claude_desktop_config.json` configuration file
   - on a MacOS, at `~/Library/Application\ Support/Claude/`

```json
{
    "mcpServers": {
        "redis": {
            "command": "<full_path_uv_command>",
            "args": [
                "--directory",
                "<your_mcp_server_directory>",
                "run",
                "src/main.py"
            ],
            "env": {
                "REDIS_HOST": "<your_redis_database_hostname>",
                "REDIS_PORT": "<your_redis_database_port>",
                "REDIS_PWD": "<your_redis_database_password>",
                "REDIS_SSL": True|False,
                "REDIS_CA_PATH": "<your_redis_ca_path>",
                "REDIS_CLUSTER_MODE": True|False
            }
        }
    }
}
```

You can troubleshoot problems by tailing the log file.

```commandline
tail -f ~/Library/Logs/Claude/mcp-server-redis.log
```

### With Docker

You can use a dockerized deployment of this server. You can either build your own image or use the official [Redis MCP Docker](https://hub.docker.com/r/mcp/redis) image.

If you'd like to build your own image, the Redis MCP Server provides a Dockerfile. Build this server's image with:

```commandline
docker build -t mcp-redis .
```

Finally, configure the client to create the container at start-up. An example for Claude Desktop is provided below. Edit the `claude_desktop_config.json` and add:

```json
{
  "mcpServers": {
    "redis": {
      "command": "docker",
      "args": ["run",
                "--rm",
                "--name",
                "redis-mcp-server",
                "-i",
                "-e", "REDIS_HOST=<redis_hostname>",
                "-e", "REDIS_PORT=<redis_port>",
                "-e", "REDIS_USERNAME=<redis_username>",
                "-e", "REDIS_PWD=<redis_password>",
                "mcp-redis"]
    }
  }
}
```

To use the official [Redis MCP Docker](https://hub.docker.com/r/mcp/redis) image, just replace your image name (`mcp-redis` in the example above) with `mcp/redis`.

## Configuration

The Redis MCP Server can be configured in two ways: via command line arguments or via environment variables.
The precedence is: command line arguments > environment variables > default values.

### Redis ACL

You can configure Redis ACL to restrict the access to the Redis database. For example, to create a read-only user:

```
127.0.0.1:6379> ACL SETUSER readonlyuser on >mypassword ~* +@read -@write
```

Configure the user via command line arguments or environment variables.

### Configuration via command line arguments

When using the CLI interface, you can configure the server with command line arguments:

```sh
# Basic Redis connection
uvx --from git+https://github.com/redis/mcp-redis.git redis-mcp-server \
  --host localhost \
  --port 6379 \
  --password mypassword

# Using Redis URI (simpler)
uvx --from git+https://github.com/redis/mcp-redis.git redis-mcp-server \
  --url redis://user:pass@localhost:6379/0

# SSL connection
uvx --from git+https://github.com/redis/mcp-redis.git redis-mcp-server \
  --url rediss://user:pass@redis.example.com:6379/0

# See all available options
uvx --from git+https://github.com/redis/mcp-redis.git redis-mcp-server --help
```

**Available CLI Options:**
- `--url` - Redis connection URI (redis://user:pass@host:port/db)
- `--host` - Redis hostname (default: 127.0.0.1)
- `--port` - Redis port (default: 6379)
- `--db` - Redis database number (default: 0)
- `--username` - Redis username
- `--password` - Redis password
- `--ssl` - Enable SSL connection
- `--ssl-ca-path` - Path to CA certificate file
- `--ssl-keyfile` - Path to SSL key file
- `--ssl-certfile` - Path to SSL certificate file
- `--ssl-cert-reqs` - SSL certificate requirements (default: required)
- `--ssl-ca-certs` - Path to CA certificates file
- `--cluster-mode` - Enable Redis cluster mode

### Configuration via Environment Variables

If desired, you can use environment variables. Defaults are provided for all variables.

| Name                 | Description                                               | Default Value |
|----------------------|-----------------------------------------------------------|---------------|
| `REDIS_HOST`         | Redis IP or hostname                                      | `"127.0.0.1"` |
| `REDIS_PORT`         | Redis port                                                | `6379`        |
| `REDIS_DB`           | Database                                                  | 0             |
| `REDIS_USERNAME`     | Default database username                                 | `"default"`   |
| `REDIS_PWD`          | Default database password                                 | ""            |
| `REDIS_SSL`          | Enables or disables SSL/TLS                               | `False`       |
| `REDIS_CA_PATH`      | CA certificate for verifying server                       | None          |
| `REDIS_SSL_KEYFILE`  | Client's private key file for client authentication       | None          |
| `REDIS_SSL_CERTFILE` | Client's certificate file for client authentication       | None          |
| `REDIS_CERT_REQS`    | Whether the client should verify the server's certificate | `"required"`  |
| `REDIS_CA_CERTS`     | Path to the trusted CA certificates file                  | None          |
| `REDIS_CLUSTER_MODE` | Enable Redis Cluster mode                                 | `False`       |


There are several ways to set environment variables:

1. **Using a `.env` File**:  
Place a `.env` file in your project directory with key-value pairs for each environment variable. Tools like `python-dotenv`, `pipenv`, and `uv` can automatically load these variables when running your application. This is a convenient and secure way to manage configuration, as it keeps sensitive data out of your shell history and version control (if `.env` is in `.gitignore`).
For example, create a `.env` file with the following content from the `.env.example` file provided in the repository:

```bash
cp .env.example .env
```

Then edit the `.env` file to set your Redis configuration:

OR,

2. **Setting Variables in the Shell**:  
You can export environment variables directly in your shell before running your application. For example:

```sh
export REDIS_HOST=your_redis_host
export REDIS_PORT=6379
# Other variables will be set similarly...
```

This method is useful for temporary overrides or quick testing.


## Integrations

Integrating this MCP Server to development frameworks like OpenAI Agents SDK, or with tools like Claude Desktop, VS Code, or Augment is described in the following sections.

### OpenAI Agents SDK

Integrate this MCP Server with the OpenAI Agents SDK. Read the [documents](https://openai.github.io/openai-agents-python/mcp/) to learn more about the integration of the SDK with MCP.

Install the Python SDK.

```commandline
pip install openai-agents
```

Configure the OpenAI token:

```commandline
export OPENAI_API_KEY="<openai_token>"
```

And run the [application](./examples/redis_assistant.py).

```commandline
python3.13 redis_assistant.py
```

You can troubleshoot your agent workflows using the [OpenAI dashboard](https://platform.openai.com/traces/).

### Augment

You can configure the Redis MCP Server in Augment by importing the server via JSON:

```json
{
  "mcpServers": {
    "Redis MCP Server": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/redis/mcp-redis.git",
        "redis-mcp-server",
        "--url",
        "redis://localhost:6379/0"
      ]
    }
  }
}
```

### Claude Desktop

The simplest way to configure MCP clients is using `uvx`. Add the following JSON to your `claude_desktop_config.json`, remember to provide the full path to `uvx`.

```json
{
    "mcpServers": {
        "redis-mcp-server": {
            "type": "stdio",
            "command": "/Users/mortensi/.local/bin/uvx",
            "args": [
                "--from", "git+https://github.com/redis/mcp-redis.git",
                "redis-mcp-server",
                "--url", "redis://localhost:6379/0"
            ]
        }
    }
}
```

If you'd like to test the [Redis MCP Server](https://smithery.ai/server/@redis/mcp-redis) via Smithery, you can configure Claude Desktop automatically:

```bash
npx -y @smithery/cli install @redis/mcp-redis --client claude
```

Follow the prompt and provide the details to configure the server and connect to Redis (e.g. using a Redis Cloud database).
The procedure will create the proper configuration in the `claude_desktop_config.json` configuration file.

### VS Code with GitHub Copilot

To use the Redis MCP Server with VS Code, you must enable the [agent mode](https://code.visualstudio.com/docs/copilot/chat/chat-agent-mode) tools. Add the following to your `settings.json`:

```json
{
  "chat.agent.enabled": true
}
```

You can start the GitHub desired version of the Redis MCP server using `uvx` by adding the following JSON to your `settings.json`:

```json
"mcp": {
    "servers": {
        "Redis MCP Server": {
        "type": "stdio",
        "command": "uvx", 
        "args": [
            "--from", "git+https://github.com/redis/mcp-redis.git",
            "redis-mcp-server",
            "--url", "redis://localhost:6379/0"
        ]
        },
    }
},
```

Alternatively, you can start the server using `uv` and configure your `mcp.json` or `settings.json`. This is usually desired for development.

```json
{
  "servers": {
    "redis": {
      "type": "stdio",
      "command": "<full_path_uv_command>",
      "args": [
        "--directory",
        "<your_mcp_server_directory>",
        "run",
        "src/main.py"
      ],
      "env": {
        "REDIS_HOST": "<your_redis_database_hostname>",
        "REDIS_PORT": "<your_redis_database_port>",
        "REDIS_USERNAME": "<your_redis_database_username>",
        "REDIS_PWD": "<your_redis_database_password>",
      }
    }
  }
}
```

```json
{
  "mcp": {
    "servers": {
      "redis": {
        "type": "stdio",
        "command": "<full_path_uv_command>",
        "args": [
          "--directory",
          "<your_mcp_server_directory>",
          "run",
          "src/main.py"
        ],
        "env": {
          "REDIS_HOST": "<your_redis_database_hostname>",
          "REDIS_PORT": "<your_redis_database_port>",
          "REDIS_USERNAME": "<your_redis_database_username>",
          "REDIS_PWD": "<your_redis_database_password>",
        }
      }
    }
  }
}
```

For more information, see the [VS Code documentation](https://code.visualstudio.com/docs/copilot/chat/mcp-servers).


## Testing

You can use the [MCP Inspector](https://modelcontextprotocol.io/docs/tools/inspector) for visual debugging of this MCP Server.

```sh
npx @modelcontextprotocol/inspector uv run src/main.py
```

## Example Use Cases
- **AI Assistants**: Enable LLMs to fetch, store, and process data in Redis.
- **Chatbots & Virtual Agents**: Retrieve session data, manage queues, and personalize responses.
- **Data Search & Analytics**: Query Redis for **real-time insights and fast lookups**.
- **Event Processing**: Manage event streams with **Redis Streams**.

## Contributing
1. Fork the repo
2. Create a new branch (`feature-branch`)
3. Commit your changes
4. Push to your branch and submit a PR!

## License
This project is licensed under the **MIT License**.

## Badges

<a href="https://glama.ai/mcp/servers/@redis/mcp-redis">
  <img width="380" height="200" src="https://glama.ai/mcp/servers/@redis/mcp-redis/badge" alt="Redis Server MCP server" />
</a>

## Contact
For questions or support, reach out via [GitHub Issues](https://github.com/redis/mcp-redis/issues).
