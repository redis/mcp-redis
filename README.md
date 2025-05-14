[![MseeP.ai Security Assessment Badge](https://mseep.net/pr/redis-mcp-redis-badge.png)](https://mseep.ai/app/redis-mcp-redis)

# Redis MCP Server
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.13%2B-blue)](https://www.python.org/downloads/)
[![smithery badge](https://smithery.ai/badge/@redis/mcp-redis)](https://smithery.ai/server/@redis/mcp-redis)


<a href="https://glama.ai/mcp/servers/@redis/mcp-redis">
  <img width="380" height="200" src="https://glama.ai/mcp/servers/@redis/mcp-redis/badge" alt="Redis Server MCP server" />
</a>

## Overview
The Redis MCP Server is a **natural language interface** designed for agentic applications to efficiently manage and search data in Redis. It integrates seamlessly with **MCP (Model Content Protocol) clients**, enabling AI-driven workflows to interact with structured and unstructured data in Redis. Using this MCP Server, you can ask questions like:

- "Store the entire conversation in a stream"
- "Cache this item"
- "Store the session with an expiration time"
- "Index and search this vector"

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

Follow these instructions to install the server.

```sh
# Clone the repository
git clone https://github.com/redis/mcp-redis.git
cd mcp-redis

# Install dependencies using uv
uv venv
source .venv/bin/activate
uv sync
```

## Configuration

To configure this Redis MCP Server, consider the following environment variables:

| Name                 | Description                                               | Default Value |
|----------------------|-----------------------------------------------------------|---------------|
| `REDIS_HOST`         | Redis IP or hostname                                      | `"127.0.0.1"` |
| `REDIS_PORT`         | Redis port                                                | `6379`        |
| `REDIS_USERNAME`     | Default database username                                 | `"default"`   |
| `REDIS_PWD`          | Default database password                                 | ""            |
| `REDIS_SSL`          | Enables or disables SSL/TLS                               | `False`       |
| `REDIS_CA_PATH`      | CA certificate for verifying server                       | None          |
| `REDIS_SSL_KEYFILE`  | Client's private key file for client authentication       | None          |
| `REDIS_SSL_CERTFILE` | Client's certificate file for client authentication       | None          |
| `REDIS_CERT_REQS`    | Whether the client should verify the server's certificate | `"required"`  |
| `REDIS_CA_CERTS`     | Path to the trusted CA certificates file                  | None          |
| `REDIS_CLUSTER_MODE` | Enable Redis Cluster mode                                 | `False`       |
| `MCP_TRANSPORT`      | Use the `stdio` or `sse` transport                        | `stdio`       |


## Transports

This MCP server can be configured to handle requests locally, running as a process and communicating with the MCP client via `stdin` and `stdout`.
This is the default configuration. The `sse` transport is also configurable so the server is available over the network.
Configure the `MCP_TRANSPORT` variable accordingly.

```commandline
export MCP_TRANSPORT="sse"
```

Then start the server.

```commandline
uv run src/main.py
```

Test the server:

```commandline
curl -i http://127.0.0.1:8000/sse
HTTP/1.1 200 OK
```

Integrate with your favorite tool or client. The VS Code configuration for GitHub Copilot is:

```commandline
"mcp": {
    "servers": {
        "redis-mcp": {
            "type": "sse",
            "url": "http://127.0.0.1:8000/sse"
        },
    }
},
```


## Integration with OpenAI Agents SDK

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

## Integration with Claude Desktop

### Via Smithery

If you'd like to test the [Redis MCP Server](https://smithery.ai/server/@redis/mcp-redis) deployed [by Smithery](https://smithery.ai/docs/deployments), you can configure Claude Desktop automatically:

```bash
npx -y @smithery/cli install @redis/mcp-redis --client claude
```

Follow the prompt and provide the details to configure the server and connect to Redis (e.g. using a Redis Cloud database).
The procedure will create the proper configuration in the `claude_desktop_config.json` configuration file.

### Manual configuration

You can configure Claude Desktop to use this MCP Server.

1. Specify your Redis credentials and TLS configuration
2. Retrieve your `uv` command full path (e.g. `which uv`)
3. Edit the `claude_desktop_config.json` configuration file
   - on a MacOS, at `~/Library/Application\ Support/Claude/`

```commandline
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

### Using with Docker

You can use a dockerized deployment of this server. You can either build your own image or use the official [Redis MCP Docker](https://hub.docker.com/r/mcp/redis) image.

If you'd like to build your own image, the Redis MCP Server provides a Dockerfile. Build this server's image with:

```commandline
docker build -t mcp-redis .
```

Finally, configure Claude Desktop to create the container at start-up. Edit the `claude_desktop_config.json` and add:

```commandline
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

### Troubleshooting

You can troubleshoot problems by tailing the log file.

```commandline
tail -f ~/Library/Logs/Claude/mcp-server-redis.log
```

## Integration with VS Code

To use the Redis MCP Server with VS Code, you need:

1. Enable the [agent mode](https://code.visualstudio.com/docs/copilot/chat/chat-agent-mode) tools. Add the following to your `settings.json`:

```commandline
{
  "chat.agent.enabled": true
}
```

2. Add the Redis MCP Server configuration to your `mcp.json` or `settings.json`:

```commandline
// Example .vscode/mcp.json
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

```commandline
// Example settings.json
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

## Contact
For questions or support, reach out via [GitHub Issues](https://github.com/redis/mcp-redis/issues).
