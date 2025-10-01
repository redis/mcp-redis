"""
Unit tests for src/main.py
"""

import logging

from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from src.main import RedisMCPServer, cli


class TestRedisMCPServer:
    """Test cases for RedisMCPServer class."""

    def test_init_logs_startup_message(self, capsys, caplog, monkeypatch):
        """Startup should emit an INFO log; client may route it via handlers.
        Accept either stderr output or log record text.
        """
        monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "INFO")

        with caplog.at_level(logging.INFO):
            server = RedisMCPServer()
            assert server is not None

        captured = capsys.readouterr()
        stderr_text = captured.err or ""
        log_text = caplog.text or ""  # collected by pytest logging handler
        combined = stderr_text + "\n" + log_text
        assert "Starting the Redis MCP Server" in combined

    @patch("src.main.mcp.run")
    def test_run_calls_mcp_run(self, mock_mcp_run):
        """Test that RedisMCPServer.run() calls mcp.run()."""
        server = RedisMCPServer()
        server.run()
        mock_mcp_run.assert_called_once()

    @patch("src.main.mcp.run")
    def test_run_propagates_exceptions(self, mock_mcp_run):
        """Test that exceptions from mcp.run() are propagated."""
        mock_mcp_run.side_effect = Exception("MCP run failed")
        server = RedisMCPServer()

        with pytest.raises(Exception, match="MCP run failed"):
            server.run()


class TestCLI:
    """Test cases for CLI interface."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    @patch("src.main.parse_redis_uri")
    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_cli_with_url_parameter(
        self, mock_server_class, mock_set_config, mock_parse_uri
    ):
        """Test CLI with --url parameter."""
        mock_parse_uri.return_value = {"host": "localhost", "port": 6379}
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(cli, ["--url", "redis://localhost:6379/0"])

        assert result.exit_code == 0
        mock_parse_uri.assert_called_once_with("redis://localhost:6379/0")
        mock_set_config.assert_called_once_with({"host": "localhost", "port": 6379})
        mock_server_class.assert_called_once()
        mock_server.run.assert_called_once()

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_cli_with_individual_parameters(self, mock_server_class, mock_set_config):
        """Test CLI with individual connection parameters."""
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(
            cli,
            [
                "--host",
                "redis.example.com",
                "--port",
                "6380",
                "--db",
                "1",
                "--username",
                "testuser",
                "--password",
                "testpass",
                "--ssl",
            ],
        )

        assert result.exit_code == 0
        mock_set_config.assert_called_once()

        # Verify the config passed to set_redis_config_from_cli
        call_args = mock_set_config.call_args[0][0]
        assert call_args["host"] == "redis.example.com"
        assert call_args["port"] == 6380
        assert call_args["db"] == 1
        assert call_args["username"] == "testuser"
        assert call_args["password"] == "testpass"
        assert call_args["ssl"] is True

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_cli_with_ssl_parameters(self, mock_server_class, mock_set_config):
        """Test CLI with SSL-specific parameters."""
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(
            cli,
            [
                "--ssl",
                "--ssl-ca-path",
                "/path/to/ca.pem",
                "--ssl-keyfile",
                "/path/to/key.pem",
                "--ssl-certfile",
                "/path/to/cert.pem",
                "--ssl-cert-reqs",
                "optional",
                "--ssl-ca-certs",
                "/path/to/ca-bundle.pem",
            ],
        )

        assert result.exit_code == 0
        call_args = mock_set_config.call_args[0][0]
        assert call_args["ssl"] is True
        assert call_args["ssl_ca_path"] == "/path/to/ca.pem"
        assert call_args["ssl_keyfile"] == "/path/to/key.pem"
        assert call_args["ssl_certfile"] == "/path/to/cert.pem"
        assert call_args["ssl_cert_reqs"] == "optional"
        assert call_args["ssl_ca_certs"] == "/path/to/ca-bundle.pem"

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_cli_with_cluster_mode(self, mock_server_class, mock_set_config):
        """Test CLI with cluster mode enabled."""
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(cli, ["--cluster-mode"])

        assert result.exit_code == 0
        call_args = mock_set_config.call_args[0][0]
        assert call_args["cluster_mode"] is True

    @patch("src.main.parse_redis_uri")
    def test_cli_with_invalid_url(self, mock_parse_uri):
        """Test CLI with invalid Redis URL."""
        mock_parse_uri.side_effect = ValueError("Invalid Redis URI")

        result = self.runner.invoke(cli, ["--url", "invalid://url"])

        assert result.exit_code != 0
        assert "Invalid Redis URI" in result.output

    @patch("src.main.RedisMCPServer")
    def test_cli_server_initialization_failure(self, mock_server_class):
        """Test CLI when server initialization fails."""
        mock_server_class.side_effect = Exception("Server init failed")

        result = self.runner.invoke(cli, [])

        assert result.exit_code != 0

    @patch("src.main.RedisMCPServer")
    def test_cli_server_run_failure(self, mock_server_class):
        """Test CLI when server run fails."""
        mock_server = Mock()
        mock_server.run.side_effect = Exception("Server run failed")
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(cli, [])

        assert result.exit_code != 0

    def test_cli_help(self):
        """Test CLI help output."""
        result = self.runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        assert "Redis connection URI" in result.output
        assert "--host" in result.output
        assert "--port" in result.output
        assert "--ssl" in result.output

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_cli_default_values(self, mock_server_class, mock_set_config):
        """Test CLI with default values."""
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(cli, [])

        assert result.exit_code == 0
        # Should be called with empty config when no parameters provided
        mock_set_config.assert_called_once()
        call_args = mock_set_config.call_args[0][0]

        # Check that only non-None values are in the config
        for key, value in call_args.items():
            if value is not None:
                # These should be the default values or explicitly set values
                assert isinstance(value, (str, int, bool))

    @patch("src.main.parse_redis_uri")
    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_cli_url_overrides_individual_params(
        self, mock_server_class, mock_set_config, mock_parse_uri
    ):
        """Test that --url parameter takes precedence over individual parameters."""
        mock_parse_uri.return_value = {"host": "uri-host", "port": 9999}
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(
            cli,
            [
                "--url",
                "redis://uri-host:9999/0",
                "--host",
                "individual-host",
                "--port",
                "6379",
            ],
        )

        assert result.exit_code == 0
        mock_parse_uri.assert_called_once_with("redis://uri-host:9999/0")
        # Should use URI config, not individual parameters
        call_args = mock_set_config.call_args[0][0]
        assert call_args["host"] == "uri-host"
        assert call_args["port"] == 9999
