"""
Tools that return a scalar on success return an error string when Redis
fails. Their return annotations must allow that string, otherwise FastMCP's
output-schema validation replaces the Redis error with a pydantic error.
"""

import pytest
from redis.exceptions import RedisError

from src.common.server import mcp, load_tools

load_tools()


SCALAR_TOOLS = [
    ("hexists", {"name": "h", "key": "k"}, True),
    ("llen", {"name": "l"}, 3),
    ("dbsize", {}, 5),
]


class TestScalarToolOutputs:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("tool, args, _value", SCALAR_TOOLS)
    async def test_redis_error_is_returned_not_a_validation_error(
        self, mock_redis_connection_manager, tool, args, _value
    ):
        getattr(mock_redis_connection_manager, tool).side_effect = RedisError(
            "Connection failed"
        )

        _content, structured = await mcp.call_tool(tool, args)

        assert "Connection failed" in structured["result"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("tool, args, value", SCALAR_TOOLS)
    async def test_success_keeps_the_native_type(
        self, mock_redis_connection_manager, tool, args, value
    ):
        getattr(mock_redis_connection_manager, tool).return_value = value

        _content, structured = await mcp.call_tool(tool, args)

        assert structured["result"] == value
        assert type(structured["result"]) is type(value)
