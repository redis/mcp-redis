
import logging
import os
import sys


def resolve_log_level() -> int:
    """Resolve desired log level from MCP_REDIS_LOG_LEVEL environment variable."""
    name = os.getenv("MCP_REDIS_LOG_LEVEL")
    if name:
        s = name.strip()
        try:
            return int(s)
        except ValueError:
            pass
        level = getattr(logging, s.upper(), None)
        if isinstance(level, int):
            return level
    return logging.INFO


def configure_logging() -> int:
    """Configure clean single-line stderr logging.

    Removes third-party wrapping handlers (e.g. RichHandler installed by FastMCP/Typer)
    and enforces a standard clean log layout across all modules.
    """
    level = resolve_log_level()
    root = logging.getLogger()

    root.setLevel(level)

    # Standard clean log formatter for terminal output
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-5s | %(message)s",
        datefmt="%H:%M:%S"
    )

    # Remove all default/Rich handlers attached by FastMCP or third-party packages
    # that cause vertical column wrapping
    for h in list(root.handlers):
        root.removeHandler(h)

    # Create a single clean StreamHandler pointing to sys.stderr
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(level)
    stderr_handler.setFormatter(formatter)
    root.addHandler(stderr_handler)

    # Ensure Uvicorn uses the root handlers and doesn't re-wrap logs
    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access", "mcp"):
        u_logger = logging.getLogger(logger_name)
        u_logger.handlers = []
        u_logger.propagate = True
        u_logger.setLevel(level)

    logging.captureWarnings(True)
    return level