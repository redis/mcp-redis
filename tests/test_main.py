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
    def test_run_sets_http_host_port_and_normalizes_http_transport(self, mock_mcp_run):
        """HTTP settings should be applied before starting FastMCP."""
        server = RedisMCPServer(
            transport="http",
            http_host="0.0.0.0",
            http_port=9000,
        )

        server.run()

        assert server.transport == "streamable-http"
        assert server.http_host == "0.0.0.0"
        assert server.http_port == 9000
        assert server._normalize_transport("http") == "streamable-http"
        assert server._normalize_transport("sse") == "sse"
        mock_mcp_run.assert_called_once_with(transport="streamable-http")

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
        mock_parse_uri.return_value = {
            "host": "localhost",
            "port": 6379,
            "topology": "standalone",
            "cluster_mode": False,
        }
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(cli, ["--url", "redis://localhost:6379/0"])

        assert result.exit_code == 0
        mock_parse_uri.assert_called_once_with("redis://localhost:6379/0")
        mock_set_config.assert_called_once_with(
            {
                "host": "localhost",
                "port": 6379,
                "topology": "standalone",
                "cluster_mode": False,
            }
        )
        mock_server_class.assert_called_once_with(
            transport="stdio", http_host="127.0.0.1", http_port=8000
        )
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
        assert call_args["topology"] is None

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
        assert call_args["topology"] is None

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_cli_with_sentinel_topology(self, mock_server_class, mock_set_config):
        """Test CLI sentinel topology parameters."""
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(
            cli,
            [
                "--topology",
                "sentinel",
                "--sentinel-master-name",
                "mymaster",
                "--sentinel-nodes",
                "host1:26379,host2:26380",
            ],
        )

        assert result.exit_code == 0
        call_args = mock_set_config.call_args[0][0]
        assert call_args["topology"] == "sentinel"
        assert call_args["sentinel_master_name"] == "mymaster"
        assert call_args["sentinel_nodes"] == "host1:26379,host2:26380"

    @patch("src.main.parse_redis_uri")
    def test_cli_with_invalid_sentinel_config(self, mock_parse_uri):
        """Sentinel topology should require master name and nodes."""
        mock_parse_uri.return_value = {
            "host": "localhost",
            "port": 6379,
            "topology": "standalone",
            "cluster_mode": False,
        }

        result = self.runner.invoke(cli, ["--topology", "sentinel"])

        assert result.exit_code != 0
        assert "Error validating Redis configuration" in result.output

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
                assert isinstance(value, (str, int, bool, list))

    @patch("src.main.parse_redis_uri")
    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_cli_url_overrides_individual_params(
        self, mock_server_class, mock_set_config, mock_parse_uri
    ):
        """Test that --url parameter takes precedence over individual parameters."""
        mock_parse_uri.return_value = {
            "host": "uri-host",
            "port": 9999,
            "topology": "cluster",
            "cluster_mode": True,
        }
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
        assert call_args["topology"] == "cluster"
        assert call_args["cluster_mode"] is True


class TestTransportProtocols:
    """Test cases for different transport protocols."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_http_transport_with_custom_host_port(
        self, mock_server_class, mock_set_config
    ):
        """Test HTTP transport with custom host and port."""
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(
            cli,
            [
                "--transport",
                "http",
                "--http-host",
                "0.0.0.0",
                "--http-port",
                "9000",
                "--url",
                "redis://localhost:6379/0",
            ],
        )

        assert result.exit_code == 0
        mock_server_class.assert_called_once_with(
            transport="http", http_host="0.0.0.0", http_port=9000
        )
        mock_server.run.assert_called_once()

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_sse_transport_with_default_host_port(
        self, mock_server_class, mock_set_config
    ):
        """Test SSE transport with default host and port."""
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(
            cli,
            ["--transport", "sse", "--url", "redis://localhost:6379/0"],
        )

        assert result.exit_code == 0
        mock_server_class.assert_called_once_with(
            transport="sse", http_host="127.0.0.1", http_port=8000
        )
        mock_server.run.assert_called_once()

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_streamable_http_transport(self, mock_server_class, mock_set_config):
        """Test streamable-http transport."""
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(
            cli,
            [
                "--transport",
                "streamable-http",
                "--http-host",
                "192.168.1.100",
                "--http-port",
                "8080",
                "--host",
                "redis.example.com",
                "--port",
                "6379",
            ],
        )

        assert result.exit_code == 0
        mock_server_class.assert_called_once_with(
            transport="streamable-http", http_host="192.168.1.100", http_port=8080
        )
        mock_server.run.assert_called_once()

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_invalid_transport_option(self, mock_server_class, mock_set_config):
        """Test that invalid transport option is rejected."""
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(
            cli,
            ["--transport", "invalid-transport", "--url", "redis://localhost:6379/0"],
        )

        assert result.exit_code != 0
        assert (
            "Invalid value for '--transport'" in result.output
            or "Invalid choice" in result.output
        )

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_stdio_transport_default_behavior(self, mock_server_class, mock_set_config):
        """Test that stdio is the default transport."""
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(cli, ["--url", "redis://localhost:6379/0"])

        assert result.exit_code == 0
        mock_server_class.assert_called_once_with(
            transport="stdio", http_host="127.0.0.1", http_port=8000
        )
        mock_server.run.assert_called_once()

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_http_port_validation(self, mock_server_class, mock_set_config):
        """Test HTTP port parameter validation."""
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        # Test valid port
        result = self.runner.invoke(
            cli,
            [
                "--transport",
                "http",
                "--http-port",
                "8080",
                "--url",
                "redis://localhost:6379/0",
            ],
        )
        assert result.exit_code == 0

        # Test invalid port (should be rejected by Click)
        result = self.runner.invoke(
            cli,
            [
                "--transport",
                "http",
                "--http-port",
                "invalid",
                "--url",
                "redis://localhost:6379/0",
            ],
        )
        assert result.exit_code != 0

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_http_host_validation(self, mock_server_class, mock_set_config):
        """Test HTTP host parameter with different formats."""
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        # Test with IP address
        result = self.runner.invoke(
            cli,
            [
                "--transport",
                "sse",
                "--http-host",
                "10.0.0.1",
                "--url",
                "redis://localhost:6379/0",
            ],
        )
        assert result.exit_code == 0

        # Test with hostname
        result = self.runner.invoke(
            cli,
            [
                "--transport",
                "http",
                "--http-host",
                "mcp-server.example.com",
                "--url",
                "redis://localhost:6379/0",
            ],
        )
        assert result.exit_code == 0

        # Test with localhost
        result = self.runner.invoke(
            cli,
            [
                "--transport",
                "streamable-http",
                "--http-host",
                "localhost",
                "--url",
                "redis://localhost:6379/0",
            ],
        )
        assert result.exit_code == 0
