import importlib
import pkgutil
from mcp.server.fastmcp import FastMCP


def load_tools():
    import src.tools as tools_pkg

    for _, module_name, _ in pkgutil.iter_modules(tools_pkg.__path__):
        importlib.import_module(f"src.tools.{module_name}")


# Initialize FastMCP server
mcp = FastMCP("Redis MCP Server", dependencies=["redis", "dotenv", "numpy"])

# Load tools
load_tools()
