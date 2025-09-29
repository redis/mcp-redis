import logging
import sys
import pytest

from src.common.logging_utils import resolve_log_level, configure_logging


@pytest.fixture()
def preserve_logging():
    """Snapshot and restore the root logger state to avoid cross-test interference."""
    root = logging.getLogger()
    saved_level = root.level
    saved_handlers = list(root.handlers)
    saved_handler_levels = [h.level for h in saved_handlers]
    try:
        yield
    finally:
        # Remove any handlers added during the test
        for h in list(root.handlers):
            try:
                root.removeHandler(h)
            except Exception:
                pass
        # Restore original handlers and their levels
        for h, lvl in zip(saved_handlers, saved_handler_levels):
            try:
                root.addHandler(h)
                h.setLevel(lvl)
            except Exception:
                pass
        # Restore original root level
        root.setLevel(saved_level)
        # Best-effort: disable warnings capture enabled by configure_logging
        try:
            logging.captureWarnings(False)
        except Exception:
            pass


def test_resolve_log_level_default_warning(monkeypatch):
    monkeypatch.delenv("MCP_REDIS_LOG_LEVEL", raising=False)
    assert resolve_log_level() == logging.WARNING


def test_resolve_log_level_parses_name_and_alias(monkeypatch):
    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "info")
    assert resolve_log_level() == logging.INFO
    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "WARN")
    assert resolve_log_level() == logging.WARNING
    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "fatal")
    assert resolve_log_level() == logging.CRITICAL


def test_resolve_log_level_parses_numeric(monkeypatch):
    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "10")
    assert resolve_log_level() == 10
    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "+20")
    assert resolve_log_level() == 20


def test_configure_logging_adds_stderr_handler_when_none(monkeypatch, preserve_logging):
    # Ensure no handlers exist before configuring
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "INFO")
    level = configure_logging()

    assert level == logging.INFO
    assert len(root.handlers) == 1, (
        "Should add exactly one stderr handler when none exist"
    )
    handler = root.handlers[0]
    assert isinstance(handler, logging.StreamHandler)
    # StreamHandler exposes the underlying stream attribute
    assert getattr(handler, "stream", None) is sys.stderr
    assert handler.level == logging.INFO
    assert root.level == logging.INFO


def test_configure_logging_only_lowers_restrictive_handlers(
    monkeypatch, preserve_logging
):
    root = logging.getLogger()
    # Start from a clean handler set
    for h in list(root.handlers):
        root.removeHandler(h)

    # Add two handlers: one restrictive WARNING, one permissive NOTSET
    h_warning = logging.StreamHandler(sys.stderr)
    h_warning.setLevel(logging.WARNING)
    root.addHandler(h_warning)

    h_notset = logging.StreamHandler(sys.stderr)
    h_notset.setLevel(logging.NOTSET)
    root.addHandler(h_notset)

    # Request DEBUG
    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "DEBUG")
    configure_logging()

    # The WARNING handler should be lowered to DEBUG; NOTSET should remain NOTSET
    assert h_warning.level == logging.DEBUG
    assert h_notset.level == logging.NOTSET


def test_configure_logging_does_not_raise_handler_threshold(
    monkeypatch, preserve_logging
):
    root = logging.getLogger()
    # Clean handlers
    for h in list(root.handlers):
        root.removeHandler(h)

    # Add a handler at WARNING and then set env to ERROR
    h_warning = logging.StreamHandler(sys.stderr)
    h_warning.setLevel(logging.WARNING)
    root.addHandler(h_warning)

    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "ERROR")
    configure_logging()

    # Handler should remain at WARNING (30), not be raised to ERROR (40)
    assert h_warning.level == logging.WARNING
    # Root level should reflect ERROR
    assert root.level == logging.ERROR


def test_configure_logging_does_not_add_handler_if_exists(
    monkeypatch, preserve_logging
):
    root = logging.getLogger()
    # Start with one existing handler
    for h in list(root.handlers):
        root.removeHandler(h)
    existing = logging.StreamHandler(sys.stderr)
    root.addHandler(existing)

    monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "INFO")
    configure_logging()

    # Should not add another handler
    assert len(root.handlers) == 1
    assert root.handlers[0] is existing
