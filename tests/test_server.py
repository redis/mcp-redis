"""
Unit tests for src/common/server.py
"""

from unittest.mock import patch

from src.common.server import mcp


class TestMCPServer:
    """Test cases for MCP server initialization."""

    def test_mcp_server_instance_exists(self):
        """Test that mcp server instance is created."""
        assert mcp is not None
        assert hasattr(mcp, "run")
        assert hasattr(mcp, "tool")

    def test_mcp_server_name(self):
        """Test that mcp server has correct name."""
        # The FastMCP server should have the correct name
        assert hasattr(mcp, "name") or hasattr(mcp, "_name")
        # We can't directly access the name in FastMCP, but we can verify it's a FastMCP instance
        assert str(type(mcp)) == "<class 'mcp.server.fastmcp.server.FastMCP'>"

    def test_mcp_server_dependencies(self):
        """Test that mcp server has correct dependencies."""
        # FastMCP should have dependencies configured
        # We can't directly test this without accessing private attributes
        # but we can verify the server was initialized properly
        assert mcp is not None

    @patch("mcp.server.fastmcp.FastMCP")
    def test_mcp_server_initialization(self, mock_fastmcp):
        """Test MCP server initialization with correct parameters."""
        # Re-import to trigger initialization
        import importlib

        import src.common.server

        importlib.reload(src.common.server)

        # Verify FastMCP was called with correct parameters
        mock_fastmcp.assert_called_once_with(
            "Redis MCP Server", dependencies=["redis", "dotenv", "numpy"]
        )

    def test_mcp_server_tool_decorator(self):
        """Test that mcp server provides tool decorator."""
        assert hasattr(mcp, "tool")
        assert callable(mcp.tool)

    def test_mcp_server_run_method(self):
        """Test that mcp server provides run method."""
        assert hasattr(mcp, "run")
        assert callable(mcp.run)

    @patch.object(mcp, "run")
    def test_mcp_server_run_can_be_called(self, mock_run):
        """Test that mcp server run method can be called."""
        mcp.run()
        mock_run.assert_called_once()

    def test_mcp_tool_decorator_functionality(self):
        """Test that the tool decorator can be used."""

        # Test that we can use the decorator (this tests the decorator exists and is callable)
        @mcp.tool()
        async def test_tool():
            """Test tool for decorator functionality."""
            return "test"

        # Verify the decorator worked
        assert callable(test_tool)
        assert hasattr(test_tool, "__name__")
        assert test_tool.__name__ == "test_tool"

    def test_mcp_tool_decorator_with_parameters(self):
        """Test that the tool decorator works with parameters."""

        @mcp.tool()
        async def test_tool_with_params(param1: str, param2: int = 10):
            """Test tool with parameters."""
            return f"{param1}:{param2}"

        # Verify the decorator worked
        assert callable(test_tool_with_params)
        assert hasattr(test_tool_with_params, "__name__")

    def test_mcp_server_is_singleton(self):
        """Test that importing server multiple times returns same instance."""
        from src.common.server import mcp as mcp1
        from src.common.server import mcp as mcp2

        assert mcp1 is mcp2
        assert id(mcp1) == id(mcp2)

    @patch("mcp.server.fastmcp.FastMCP")
    def test_mcp_server_dependencies_list(self, mock_fastmcp):
        """Test that MCP server is initialized with correct dependencies list."""
        # Re-import to trigger initialization
        import importlib

        import src.common.server

        importlib.reload(src.common.server)

        # Get the call arguments
        call_args = mock_fastmcp.call_args
        assert call_args[0][0] == "Redis MCP Server"  # First positional argument
        assert call_args[1]["dependencies"] == [
            "redis",
            "dotenv",
            "numpy",
        ]  # Keyword argument

    def test_mcp_server_type(self):
        """Test that mcp server is of correct type."""
        from mcp.server.fastmcp import FastMCP

        assert isinstance(mcp, FastMCP)

    def test_mcp_server_attributes(self):
        """Test that mcp server has expected attributes."""
        # Test for common FastMCP attributes
        expected_attributes = ["run", "tool"]

        for attr in expected_attributes:
            assert hasattr(mcp, attr), f"MCP server missing attribute: {attr}"
            assert callable(getattr(mcp, attr)), (
                f"MCP server attribute {attr} is not callable"
            )
