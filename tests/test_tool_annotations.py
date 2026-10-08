"""
Tests that every registered tool declares its MCP safety hints.

The point of the module under test is that an unannotated tool is read by a
client as destructive and open-world, so a tool whose hints nobody answered for
has to fail here rather than ship with the protocol's defaults.
"""

import pytest

from src.common.server import mcp

# What each writing tool is declared to be. Spelled out rather than derived from
# src/common/annotations.py a second time, so that changing a preset has to be
# restated here: these hints are what a client shows the user before it runs
# the tool, and they should not move by accident.
#
# Read-only tools are deliberately absent: the test below requires every tool
# missing from this table to be read-only, which is the stronger statement.
# The tools that reach something other than the configured Redis instance.
# `search_redis_documents` is an HTTP request to the docs service at
# MCP_DOCS_SEARCH_URL, which is the open-ended set of entities the hint is
# about; everything else is bounded by the one instance.
OPEN_WORLD_TOOLS = {"search_redis_documents"}

WRITE_TOOLS = {
    # hash
    "hset": {"destructiveHint": True, "idempotentHint": True},
    "hdel": {"destructiveHint": True, "idempotentHint": True},
    "set_vector_in_hash": {"destructiveHint": True, "idempotentHint": True},
    # json
    "json_set": {"destructiveHint": True, "idempotentHint": True},
    "json_del": {"destructiveHint": True, "idempotentHint": True},
    # list
    "lpush": {"destructiveHint": False, "idempotentHint": False},
    "rpush": {"destructiveHint": False, "idempotentHint": False},
    "lpop": {"destructiveHint": True, "idempotentHint": False},
    "rpop": {"destructiveHint": True, "idempotentHint": False},
    "lrem": {"destructiveHint": True, "idempotentHint": False},
    # misc
    "delete": {"destructiveHint": True, "idempotentHint": True},
    "expire": {"destructiveHint": True, "idempotentHint": True},
    "rename": {"destructiveHint": True, "idempotentHint": False},
    # pub/sub
    "publish": {"destructiveHint": False, "idempotentHint": False},
    "subscribe": {"destructiveHint": False, "idempotentHint": False},
    "psubscribe": {"destructiveHint": False, "idempotentHint": False},
    "read_messages": {"destructiveHint": False, "idempotentHint": False},
    "unsubscribe": {"destructiveHint": True, "idempotentHint": True},
    # query engine
    "create_vector_index_hash": {"destructiveHint": False, "idempotentHint": False},
    # set
    "sadd": {"destructiveHint": False, "idempotentHint": True},
    "srem": {"destructiveHint": True, "idempotentHint": True},
    # sorted set
    "zadd": {"destructiveHint": False, "idempotentHint": True},
    "zrem": {"destructiveHint": True, "idempotentHint": True},
    # stream
    "xadd": {"destructiveHint": False, "idempotentHint": False},
    "xdel": {"destructiveHint": True, "idempotentHint": True},
    "xgroup_create": {"destructiveHint": False, "idempotentHint": False},
    "xgroup_destroy": {"destructiveHint": True, "idempotentHint": True},
    "xreadgroup": {"destructiveHint": False, "idempotentHint": False},
    "xack": {"destructiveHint": False, "idempotentHint": True},
    # string
    "set": {"destructiveHint": True, "idempotentHint": True},
}


def _tool_names_defined_by_this_server() -> set:
    """The names of the tools that live under src/tools.

    Other test modules register throwaway tools on the same module-level `mcp`
    instance, and those are not this server's to annotate, so they are filtered
    out by the module their function was defined in.
    """
    return {
        tool.name
        for tool in mcp._tool_manager.list_tools()
        if getattr(tool.fn, "__module__", "").startswith("src.tools.")
    }


@pytest.fixture
async def annotations_by_tool():
    """The annotations every tool is registered with, keyed by tool name."""
    own = _tool_names_defined_by_this_server()
    tools = await mcp.list_tools()
    return {tool.name: tool.annotations for tool in tools if tool.name in own}


class TestToolAnnotations:
    """Test cases for the MCP annotations on registered tools."""

    async def test_every_tool_is_annotated(self, annotations_by_tool):
        """Every registered tool carries annotations."""
        unannotated = sorted(
            name
            for name, annotations in annotations_by_tool.items()
            if annotations is None
        )

        assert unannotated == []

    async def test_every_tool_sets_the_three_default_dangerous_hints(
        self, annotations_by_tool
    ):
        """readOnlyHint, destructiveHint and openWorldHint are never left unset.

        These are the three whose defaults are the dangerous ones, so an unset
        hint is a claim the tool never meant to make.
        """
        for name, annotations in annotations_by_tool.items():
            assert isinstance(annotations.readOnlyHint, bool), name
            assert isinstance(annotations.destructiveHint, bool), name
            assert isinstance(annotations.openWorldHint, bool), name

    async def test_only_the_docs_search_reaches_beyond_the_configured_redis(
        self, annotations_by_tool
    ):
        """Only OPEN_WORLD_TOOLS leaves the configured Redis instance.

        Stated as an exact set rather than a floor, so that a new tool calling
        out to some other service has to be added here deliberately, and so
        that a tool losing its outside call has its hint corrected.
        """
        open_world = {
            name
            for name, annotations in annotations_by_tool.items()
            if annotations.openWorldHint
        }

        assert open_world == OPEN_WORLD_TOOLS

    async def test_writing_tools_match_the_declared_table(self, annotations_by_tool):
        """Each writing tool carries the hints WRITE_TOOLS declares for it."""
        for name, expected in WRITE_TOOLS.items():
            annotations = annotations_by_tool.get(name)

            assert annotations is not None, f"{name} is no longer registered"
            assert annotations.readOnlyHint is False, name
            assert annotations.destructiveHint is expected["destructiveHint"], name
            assert annotations.idempotentHint is expected["idempotentHint"], name

    async def test_every_other_tool_is_read_only(self, annotations_by_tool):
        """A tool absent from WRITE_TOOLS reads data and does not change it."""
        reads = [name for name in annotations_by_tool if name not in WRITE_TOOLS]

        assert reads, "no read-only tools found; the fixture is probably empty"
        for name in reads:
            annotations = annotations_by_tool[name]
            assert annotations.readOnlyHint is True, name
            assert annotations.destructiveHint is False, name

    async def test_declared_writing_tools_all_exist(self, annotations_by_tool):
        """WRITE_TOOLS does not outlive the tools it describes."""
        gone = sorted(name for name in WRITE_TOOLS if name not in annotations_by_tool)

        assert gone == []
