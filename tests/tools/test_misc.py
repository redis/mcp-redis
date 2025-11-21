import pytest

import src.tools.misc as misc
from src.tools.misc import search_documents


@pytest.mark.asyncio
async def test_search_documents_url_not_configured(monkeypatch):
    """Return a clear error if MCP_DOCS_SEARCH_URL is not set."""
    monkeypatch.setattr(misc, "MCP_DOCS_SEARCH_URL", "")

    result = await search_documents("What is Redis?")

    assert isinstance(result, dict)
    assert (
        result["error"] == "MCP_DOCS_SEARCH_URL environment variable is not configured"
    )


@pytest.mark.asyncio
async def test_search_documents_empty_question(monkeypatch):
    """Reject empty/whitespace-only questions."""
    monkeypatch.setattr(misc, "MCP_DOCS_SEARCH_URL", "https://example.com/docs")

    result = await search_documents("   ")

    assert isinstance(result, dict)
    assert result["error"] == "Question parameter cannot be empty"


@pytest.mark.asyncio
async def test_search_documents_success_json_response(monkeypatch):
    """Return parsed JSON when the docs API responds with JSON."""

    class DummyResponse:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return {"results": [{"title": "Redis Intro"}]}

    class DummySession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, *_, **__):  # pragma: no cover - trivial wrapper
            return DummyResponse()

    monkeypatch.setattr(misc, "MCP_DOCS_SEARCH_URL", "https://example.com/docs")
    monkeypatch.setattr(misc.aiohttp, "ClientSession", DummySession)

    result = await search_documents("What is a Redis stream?")

    assert isinstance(result, dict)
    assert result["results"][0]["title"] == "Redis Intro"


@pytest.mark.asyncio
async def test_search_documents_non_json_response(monkeypatch):
    """If the response is not JSON, surface the text content in an error."""

    class DummyContentTypeError(Exception):
        pass

    class DummyResponse:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            raise DummyContentTypeError("Not JSON")

        async def text(self):
            return "<html>not json</html>"

    class DummySession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, *_, **__):  # pragma: no cover - trivial wrapper
            return DummyResponse()

    monkeypatch.setattr(misc, "MCP_DOCS_SEARCH_URL", "https://example.com/docs")
    # Patch aiohttp.ContentTypeError to our dummy so the except block matches
    monkeypatch.setattr(misc.aiohttp, "ContentTypeError", DummyContentTypeError)
    monkeypatch.setattr(misc.aiohttp, "ClientSession", DummySession)

    result = await search_documents("Explain Redis JSON")

    assert isinstance(result, dict)
    assert "Non-JSON response" in result["error"]
    assert "not json" in result["error"]


@pytest.mark.asyncio
async def test_search_documents_http_client_error(monkeypatch):
    """HTTP client errors are caught and returned in an error dict."""

    class DummyClientError(Exception):
        pass

    class ErrorResponse:
        async def __aenter__(self):
            raise DummyClientError("boom")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class DummySession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, *_, **__):  # pragma: no cover - trivial wrapper
            return ErrorResponse()

    monkeypatch.setattr(misc, "MCP_DOCS_SEARCH_URL", "https://example.com/docs")
    monkeypatch.setattr(misc.aiohttp, "ClientError", DummyClientError)
    monkeypatch.setattr(misc.aiohttp, "ClientSession", DummySession)

    result = await search_documents("What is Redis?")

    assert isinstance(result, dict)
    assert result["error"] == "HTTP client error: boom"
