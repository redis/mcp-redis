"""Explicit Redis protocol selection without changing client defaults."""

import os
import subprocess
import sys
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

from src.common import config, connection
from src.main import cli


@pytest.fixture
def protocol_config(monkeypatch, redis_config):
    """Isolate configuration and disable real credential acquisition."""
    values = {**redis_config, "protocol": None}
    monkeypatch.setattr(config, "REDIS_CFG", values)
    monkeypatch.setattr(connection, "REDIS_CFG", values)
    monkeypatch.setattr(connection, "is_entraid_auth_enabled", lambda: False)
    return values


@pytest.mark.parametrize("protocol", [None, 2, 3])
@pytest.mark.parametrize("cluster", [False, True])
@pytest.mark.parametrize("decode_responses", [False, True])
def test_connection_protocol(
    monkeypatch, protocol_config, protocol, cluster, decode_responses
):
    """Pass explicit protocols to both clients, but omit the default."""
    protocol_config.update(protocol=protocol, cluster_mode=cluster)
    constructor = Mock()
    if cluster:
        monkeypatch.setattr(connection.redis.cluster, "RedisCluster", constructor)
    else:
        monkeypatch.setattr(connection.redis, "Redis", constructor)
    result = connection.RedisConnectionManager.get_connection(decode_responses)
    assert result is constructor.return_value
    constructor.assert_called_once()
    kwargs = constructor.call_args.kwargs
    assert kwargs["decode_responses"] is decode_responses
    if protocol is None:
        assert "protocol" not in kwargs
    else:
        assert kwargs["protocol"] == protocol
        assert isinstance(kwargs["protocol"], int)


@pytest.mark.parametrize("protocol", [2, 3, "2", "3"])
def test_config_protocol_is_integer(protocol_config, protocol):
    """Configuration normalization retains the redis-py integer contract."""
    config.set_redis_config_from_cli({"protocol": protocol})
    assert protocol_config["protocol"] == int(protocol)
    assert isinstance(protocol_config["protocol"], int)


@pytest.mark.parametrize("protocol", [1, 4, True, 2.5, "", "invalid"])
def test_invalid_config_protocol_is_rejected(protocol_config, protocol):
    """Reject unsupported values without updating the current protocol."""
    with pytest.raises(ValueError, match="protocol must be 2 or 3"):
        config.set_redis_config_from_cli({"protocol": protocol})
    assert protocol_config["protocol"] is None


@pytest.mark.parametrize("value", [None, "2", "3", "1", "4", "", "invalid"])
def test_environment_protocol(value):
    """Read the environment at import, without reloading shared test globals."""
    env = os.environ.copy()
    env.pop("REDIS_PROTOCOL", None)
    if value is not None:
        env["REDIS_PROTOCOL"] = value
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from src.common.config import REDIS_CFG; print(REDIS_CFG['protocol'])",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
        timeout=10,
    )
    if value in (None, "2", "3"):
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == (value or "None")
    else:
        assert result.returncode != 0
        assert "protocol must be 2 or 3" in result.stderr


@pytest.mark.parametrize("url", [None, "redis://localhost:6379/0"])
@pytest.mark.parametrize("protocol", [None, 2, 3])
def test_cli_protocol_precedence(monkeypatch, protocol_config, url, protocol):
    """CLI values override the environment even when a URI is supplied."""
    protocol_config["protocol"] = 3
    server = Mock()
    monkeypatch.setattr("src.main.RedisMCPServer", server)
    args = ["--url", url] if url else []
    if protocol is not None:
        args += ["--protocol", str(protocol)]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    assert protocol_config["protocol"] == (protocol if protocol is not None else 3)
    server.return_value.run.assert_called_once()


@pytest.mark.parametrize("value", ["1", "4", "invalid"])
def test_invalid_cli_protocol_does_not_start_server(monkeypatch, value):
    """Click rejects unsupported protocols before starting MCP."""
    server = Mock()
    monkeypatch.setattr("src.main.RedisMCPServer", server)
    result = CliRunner().invoke(cli, ["--protocol", value])
    assert result.exit_code == 2
    assert "Invalid value" in result.output
    server.assert_not_called()
