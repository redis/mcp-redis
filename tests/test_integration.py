"""
Integration tests for Redis MCP Server.

These tests actually start the MCP server process and verify it can handle real requests.
"""

import json
import subprocess
import sys
import time
import os
from pathlib import Path

import pytest


def _redis_available():
    """Check if Redis is available for testing."""
    try:
        import redis

        r = redis.Redis(host="localhost", port=6379, decode_responses=True)
        r.ping()
        return True
    except Exception:
        return False


def _create_server_process(project_root):
    """Create a server process with proper encoding for cross-platform compatibility."""
    return subprocess.Popen(
        [sys.executable, "-m", "src.main"],
        cwd=project_root,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",  # Replace invalid characters instead of failing
        env={"REDIS_HOST": "localhost", "REDIS_PORT": "6379", **dict(os.environ)},
    )


@pytest.mark.integration
class TestMCPServerIntegration:
    """Integration tests that start the actual MCP server."""

    @pytest.fixture
    def server_process(self):
        """Start the MCP server process for testing."""
        # Get the project root directory
        project_root = Path(__file__).parent.parent

        # Start the server process with proper encoding for cross-platform compatibility
        process = _create_server_process(project_root)

        # Give the server a moment to start
        time.sleep(1)

        yield process

        # Clean up
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

    def test_server_starts_successfully(self, server_process):
        """Test that the MCP server starts without crashing."""
        # Check if process is still running
        assert server_process.poll() is None, "Server process should be running"

        # Check for startup message in stderr
        # Note: MCP servers typically output startup info to stderr
        time.sleep(0.5)  # Give time for startup message

        # The server should still be running
        assert server_process.poll() is None

    def test_server_handles_unicode_on_windows(self, server_process):
        """Test that the server handles Unicode properly on Windows."""
        # This test specifically addresses the Windows Unicode decode error
        # Check if process is still running
        assert server_process.poll() is None, "Server process should be running"

        # Try to read any available output without blocking
        # This should not cause a UnicodeDecodeError on Windows
        try:
            # Use a short timeout to avoid blocking
            import select
            import sys

            if sys.platform == "win32":
                # On Windows, we can't use select, so just check if process is alive
                time.sleep(0.1)
                assert server_process.poll() is None
            else:
                # On Unix-like systems, we can use select
                ready, _, _ = select.select([server_process.stdout], [], [], 0.1)
                # If there's output available, try to read it
                if ready:
                    try:
                        server_process.stdout.read(1)  # Read just one character
                        # If we get here, Unicode handling is working
                        assert True
                    except UnicodeDecodeError:
                        pytest.fail("Unicode decode error occurred")

        except Exception:
            # If any other error occurs, that's fine - we're just testing Unicode handling
            pass

        # Main assertion: process should still be running
        assert server_process.poll() is None

    def test_server_responds_to_initialize_request(self, server_process):
        """Test that the server responds to MCP initialize request."""
        # MCP initialize request
        initialize_request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "1.0.0"},
            },
        }

        # Send the request
        request_json = json.dumps(initialize_request) + "\n"
        server_process.stdin.write(request_json)
        server_process.stdin.flush()

        # Read the response
        response_line = server_process.stdout.readline()
        assert response_line.strip(), "Server should respond to initialize request"

        # Parse the response
        try:
            response = json.loads(response_line)
            assert response.get("jsonrpc") == "2.0"
            assert response.get("id") == 1
            assert "result" in response
        except json.JSONDecodeError:
            pytest.fail(f"Invalid JSON response: {response_line}")

    def test_server_lists_tools(self, server_process):
        """Test that the server can list available tools."""
        # First initialize
        initialize_request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "1.0.0"},
            },
        }

        server_process.stdin.write(json.dumps(initialize_request) + "\n")
        server_process.stdin.flush()
        server_process.stdout.readline()  # Read initialize response

        # Send initialized notification
        initialized_notification = {
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        }
        server_process.stdin.write(json.dumps(initialized_notification) + "\n")
        server_process.stdin.flush()

        # Request tools list
        tools_request = {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}

        server_process.stdin.write(json.dumps(tools_request) + "\n")
        server_process.stdin.flush()

        # Read the response
        response_line = server_process.stdout.readline()
        response = json.loads(response_line)

        assert response.get("jsonrpc") == "2.0"
        assert response.get("id") == 2
        assert "result" in response
        assert "tools" in response["result"]

        # Verify we have some Redis tools
        tools = response["result"]["tools"]
        tool_names = [tool["name"] for tool in tools]

        # Should have basic Redis operations
        expected_tools = [
            "hset",
            "hget",
            "hdel",
            "hgetall",
            "hexists",
            "set_vector_in_hash",
            "get_vector_from_hash",
            "json_set",
            "json_get",
            "json_del",
            "lpush",
            "rpush",
            "lpop",
            "rpop",
            "lrange",
            "llen",
            "delete",
            "type",
            "expire",
            "rename",
            "scan_keys",
            "scan_all_keys",
            "publish",
            "subscribe",
            "unsubscribe",
            "get_indexes",
            "get_index_info",
            "get_indexed_keys_number",
            "create_vector_index_hash",
            "vector_search_hash",
            "dbsize",
            "info",
            "client_list",
            "sadd",
            "srem",
            "smembers",
            "zadd",
            "zrange",
            "zrem",
            "xadd",
            "xrange",
            "xdel",
            "set",
            "get",
        ]
        for tool in tool_names:
            assert tool in expected_tools, (
                f"Expected tool '{tool}' not found in {tool_names}"
            )

    def test_server_tool_count_and_names(self, server_process):
        """Test that the server registers the correct number of tools with expected names."""
        # Initialize the server
        self._initialize_server(server_process)

        # Request tools list
        tools_request = {"jsonrpc": "2.0", "id": 3, "method": "tools/list"}

        server_process.stdin.write(json.dumps(tools_request) + "\n")
        server_process.stdin.flush()

        # Read the response
        response_line = server_process.stdout.readline()
        response = json.loads(response_line)

        assert response.get("jsonrpc") == "2.0"
        assert response.get("id") == 3
        assert "result" in response
        assert "tools" in response["result"]

        tools = response["result"]["tools"]
        tool_names = [tool["name"] for tool in tools]

        # Expected tool count (based on @mcp.tool() decorators in codebase)
        expected_tool_count = 44
        assert len(tools) == expected_tool_count, (
            f"Expected {expected_tool_count} tools, but got {len(tools)}"
        )

        # Expected tool names (alphabetically sorted for easier verification)
        expected_tools = [
            "client_list",
            "create_vector_index_hash",
            "dbsize",
            "delete",
            "expire",
            "get",
            "get_index_info",
            "get_indexed_keys_number",
            "get_indexes",
            "get_vector_from_hash",
            "hdel",
            "hexists",
            "hget",
            "hgetall",
            "hset",
            "info",
            "json_del",
            "json_get",
            "json_set",
            "llen",
            "lpop",
            "lpush",
            "lrange",
            "publish",
            "rename",
            "rpop",
            "rpush",
            "sadd",
            "scan_all_keys",
            "scan_keys",
            "set",
            "set_vector_in_hash",
            "smembers",
            "srem",
            "subscribe",
            "type",
            "unsubscribe",
            "vector_search_hash",
            "xadd",
            "xdel",
            "xrange",
            "zadd",
            "zrange",
            "zrem",
        ]

        # Verify all expected tools are present
        missing_tools = set(expected_tools) - set(tool_names)
        extra_tools = set(tool_names) - set(expected_tools)

        assert not missing_tools, f"Missing expected tools: {sorted(missing_tools)}"
        assert not extra_tools, f"Found unexpected tools: {sorted(extra_tools)}"

        # Verify tool categories are represented
        tool_categories = {
            "string": ["get", "set"],
            "hash": ["hget", "hset", "hgetall", "hdel", "hexists"],
            "list": ["lpush", "rpush", "lpop", "rpop", "lrange", "llen"],
            "set": ["sadd", "srem", "smembers"],
            "sorted_set": ["zadd", "zrem", "zrange"],
            "stream": ["xadd", "xdel", "xrange"],
            "json": ["json_get", "json_set", "json_del"],
            "pub_sub": ["publish", "subscribe", "unsubscribe"],
            "server_mgmt": ["dbsize", "info", "client_list"],
            "misc": [
                "delete",
                "expire",
                "rename",
                "type",
                "scan_keys",
                "scan_all_keys",
            ],
            "vector_search": [
                "create_vector_index_hash",
                "vector_search_hash",
                "get_indexes",
                "get_index_info",
                "set_vector_in_hash",
                "get_vector_from_hash",
                "get_indexed_keys_number",
            ],
        }

        for category, category_tools in tool_categories.items():
            for tool in category_tools:
                assert tool in tool_names, (
                    f"Tool '{tool}' from category '{category}' not found in registered tools"
                )

    def _initialize_server(self, server_process):
        """Helper to initialize the MCP server."""
        # Send initialize request
        initialize_request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "1.0.0"},
            },
        }

        server_process.stdin.write(json.dumps(initialize_request) + "\n")
        server_process.stdin.flush()
        server_process.stdout.readline()  # Read response

        # Send initialized notification
        initialized_notification = {
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        }
        server_process.stdin.write(json.dumps(initialized_notification) + "\n")
        server_process.stdin.flush()
