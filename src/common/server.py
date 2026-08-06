import importlib
import os
import pkgutil
from typing import Iterable, List, Optional

from mcp.server.fastmcp import FastMCP

ALLOWED_TOOLS_ENV_VAR = "MCP_REDIS_ALLOWED_TOOLS"


def load_tools():
    import src.tools as tools_pkg

    for _, module_name, _ in pkgutil.iter_modules(tools_pkg.__path__):
        importlib.import_module(f"src.tools.{module_name}")


def parse_allowed_tools(allowed_tools: Optional[str]) -> Optional[set]:
    """Parse a comma-separated allowlist of tool names.

    Returns None when no allowlist is configured, meaning every tool stays
    available.
    """
    if allowed_tools is None:
        return None

    names = {name.strip() for name in allowed_tools.split(",")}
    names.discard("")
    return names


def select_disallowed_tools(allowed: set, registered: Iterable[str]) -> List[str]:
    """Return the registered tool names that the allowlist does not cover.

    Raises:
        ValueError: if the allowlist names a tool the server does not provide.
            Dropping such a name silently would leave a narrower set of tools
            than was configured, which is indistinguishable from the tool
            simply not being used.
    """
    registered = set(registered)

    unknown = sorted(allowed - registered)
    if unknown:
        raise ValueError(
            f"{ALLOWED_TOOLS_ENV_VAR} names unknown "
            f"{'tool' if len(unknown) == 1 else 'tools'}: {', '.join(unknown)}. "
            f"Available tools: {', '.join(sorted(registered))}"
        )

    return sorted(registered - allowed)


def apply_tool_allowlist(server: FastMCP, allowed_tools: Optional[str]) -> None:
    """Unregister every tool that the allowlist does not name.

    Unregistering rather than rejecting at call time keeps the omitted tools
    out of `tools/list` entirely, so an agent cannot plan around a tool it is
    not allowed to use.
    """
    allowed = parse_allowed_tools(allowed_tools)
    if allowed is None:
        return

    # FastMCP exposes no synchronous accessor for its registry; `list_tools()`
    # is a coroutine and this runs at import time.
    registered = [tool.name for tool in server._tool_manager.list_tools()]

    for name in select_disallowed_tools(allowed, registered):
        server.remove_tool(name)


# Initialize FastMCP server
mcp = FastMCP(
    "Redis MCP Server", dependencies=["redis", "python-dotenv", "numpy", "aiohttp"]
)

# Load tools
load_tools()

# Restrict the exposed tools when an allowlist is configured
apply_tool_allowlist(mcp, os.getenv(ALLOWED_TOOLS_ENV_VAR))
