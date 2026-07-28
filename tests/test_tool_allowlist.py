"""
Unit tests for the MCP_REDIS_ALLOWED_TOOLS allowlist in src/common/server.py
"""

import pytest
from mcp.server.fastmcp import FastMCP

from src.common.server import (
    ALLOWED_TOOLS_ENV_VAR,
    apply_tool_allowlist,
    parse_allowed_tools,
    select_disallowed_tools,
)


REGISTERED = ["get_value", "set_value", "delete_value"]


def build_server() -> FastMCP:
    """A server with one read tool and two write tools."""
    server = FastMCP("Test Server")

    async def get_value(key: str) -> str:
        """Read a value."""
        return key

    async def set_value(key: str, value: str) -> str:
        """Write a value."""
        return value

    async def delete_value(key: str) -> str:
        """Delete a value."""
        return key

    for fn in (get_value, set_value, delete_value):
        server.add_tool(fn)

    return server


async def tool_names(server: FastMCP) -> list:
    return sorted(tool.name for tool in await server.list_tools())


class TestParseAllowedTools:
    """Test cases for parsing the allowlist."""

    def test_returns_none_when_unset(self):
        assert parse_allowed_tools(None) is None

    def test_splits_on_commas(self):
        assert parse_allowed_tools("get_value,set_value") == {"get_value", "set_value"}

    def test_strips_surrounding_whitespace(self):
        assert parse_allowed_tools(" get_value , set_value ") == {
            "get_value",
            "set_value",
        }

    def test_ignores_empty_entries(self):
        assert parse_allowed_tools("get_value,,set_value,") == {
            "get_value",
            "set_value",
        }

    def test_empty_string_allows_nothing(self):
        """An empty allowlist is still an allowlist, unlike an unset one."""
        assert parse_allowed_tools("") == set()


class TestSelectDisallowedTools:
    """Test cases for resolving the allowlist against registered tools."""

    def test_returns_tools_outside_the_allowlist(self):
        assert select_disallowed_tools({"get_value"}, REGISTERED) == [
            "delete_value",
            "set_value",
        ]

    def test_returns_nothing_when_every_tool_is_allowed(self):
        assert select_disallowed_tools(set(REGISTERED), REGISTERED) == []

    def test_returns_every_tool_for_an_empty_allowlist(self):
        assert select_disallowed_tools(set(), REGISTERED) == [
            "delete_value",
            "get_value",
            "set_value",
        ]

    def test_rejects_a_misspelled_name(self):
        with pytest.raises(ValueError) as excinfo:
            select_disallowed_tools({"get_value", "get_valu"}, REGISTERED)

        message = str(excinfo.value)
        assert "get_valu" in message
        assert ALLOWED_TOOLS_ENV_VAR in message

    def test_reports_every_unknown_name_sorted(self):
        with pytest.raises(ValueError) as excinfo:
            select_disallowed_tools({"zzz_missing", "aaa_missing"}, REGISTERED)

        assert "aaa_missing, zzz_missing" in str(excinfo.value)

    def test_error_lists_the_available_tools(self):
        with pytest.raises(ValueError) as excinfo:
            select_disallowed_tools({"nope"}, REGISTERED)

        message = str(excinfo.value)
        for name in REGISTERED:
            assert name in message


class TestApplyToolAllowlist:
    """Test cases for applying the allowlist to a server."""

    async def test_keeps_every_tool_when_unset(self):
        server = build_server()
        apply_tool_allowlist(server, None)

        assert await tool_names(server) == [
            "delete_value",
            "get_value",
            "set_value",
        ]

    async def test_omits_unnamed_tools_from_tools_list(self):
        """The point of the allowlist: omitted tools are gone, not merely refused."""
        server = build_server()
        apply_tool_allowlist(server, "get_value,set_value")

        assert await tool_names(server) == ["get_value", "set_value"]

    async def test_keeps_a_single_named_tool(self):
        server = build_server()
        apply_tool_allowlist(server, "get_value")

        assert await tool_names(server) == ["get_value"]

    async def test_empty_allowlist_removes_every_tool(self):
        server = build_server()
        apply_tool_allowlist(server, "")

        assert await tool_names(server) == []

    async def test_unknown_name_raises_and_leaves_the_server_untouched(self):
        server = build_server()

        with pytest.raises(ValueError):
            apply_tool_allowlist(server, "get_value,get_valu")

        assert await tool_names(server) == [
            "delete_value",
            "get_value",
            "set_value",
        ]
