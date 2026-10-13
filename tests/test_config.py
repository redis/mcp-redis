"""
Unit tests for src/common/config.py
"""

import os
from unittest.mock import patch

import pytest

from src.common.config import (
    REDIS_CFG,
    parse_redis_uri,
    set_redis_config_from_cli,
    validate_redis_config,
)


class TestParseRedisURI:
    """Test cases for parse_redis_uri function."""

    def test_parse_basic_redis_uri(self):
        """Test parsing basic Redis URI."""
        uri = "redis://localhost:6379/0"
        result = parse_redis_uri(uri)

        expected = {
            "ssl": False,
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "topology": "standalone",
            "cluster_mode": False,
        }
        assert result == expected

    def test_parse_redis_uri_with_auth(self):
        """Test parsing Redis URI with authentication."""
        uri = "redis://user:pass@localhost:6379/1"
        result = parse_redis_uri(uri)

        expected = {
            "ssl": False,
            "host": "localhost",
            "port": 6379,
            "db": 1,
            "topology": "standalone",
            "cluster_mode": False,
            "username": "user",
            "password": "pass",
        }
        assert result == expected

    def test_parse_rediss_uri(self):
        """Test parsing Redis SSL URI."""
        uri = "rediss://user:pass@redis.example.com:6380/2"
        result = parse_redis_uri(uri)

        expected = {
            "ssl": True,
            "host": "redis.example.com",
            "port": 6380,
            "db": 2,
            "topology": "standalone",
            "cluster_mode": False,
            "username": "user",
            "password": "pass",
        }
        assert result == expected

    def test_parse_uri_with_query_parameters(self):
        """Test parsing URI with query parameters."""
        uri = "redis://localhost:6379/0?ssl_cert_reqs=optional&ssl_ca_certs=/path/to/ca.pem"
        result = parse_redis_uri(uri)

        assert result["ssl"] is False
        assert result["host"] == "localhost"
        assert result["port"] == 6379
        assert result["db"] == 0
        assert result["ssl_cert_reqs"] == "optional"
        assert result["ssl_ca_certs"] == "/path/to/ca.pem"

    def test_parse_uri_with_db_in_query(self):
        """Test parsing URI with database number in query parameters."""
        uri = "redis://localhost:6379?db=5"
        result = parse_redis_uri(uri)

        assert result["db"] == 5

    def test_parse_uri_with_ssl_parameters(self):
        """Test parsing URI with SSL-related query parameters."""
        uri = "rediss://localhost:6379/0?ssl_keyfile=/key.pem&ssl_certfile=/cert.pem&ssl_ca_path=/ca.pem"
        result = parse_redis_uri(uri)

        assert result["ssl"] is True
        assert result["ssl_keyfile"] == "/key.pem"
        assert result["ssl_certfile"] == "/cert.pem"
        assert result["ssl_ca_path"] == "/ca.pem"

    def test_parse_uri_defaults(self):
        """Test parsing URI with default values."""
        uri = "redis://example.com"
        result = parse_redis_uri(uri)

        assert result["host"] == "example.com"
        assert result["port"] == 6379  # Default port
        assert result["db"] == 0  # Default database

    def test_parse_uri_no_path(self):
        """Test parsing URI without path."""
        uri = "redis://localhost:6379"
        result = parse_redis_uri(uri)

        assert result["db"] == 0

    def test_parse_uri_root_path(self):
        """Test parsing URI with root path."""
        uri = "redis://localhost:6379/"
        result = parse_redis_uri(uri)

        assert result["db"] == 0

    def test_parse_uri_invalid_db_in_path(self):
        """Test parsing URI with invalid database number in path."""
        uri = "redis://localhost:6379/invalid"
        result = parse_redis_uri(uri)

        assert result["db"] == 0  # Should default to 0

    def test_parse_uri_invalid_db_in_query(self):
        """Test parsing URI with invalid database number in query."""
        uri = "redis://localhost:6379?db=invalid"
        result = parse_redis_uri(uri)

        # Should not have db key or should be handled gracefully
        assert "db" not in result or result["db"] == 0

    def test_parse_uri_unsupported_scheme(self):
        """Test parsing URI with unsupported scheme."""
        uri = "http://localhost:6379/0"

        with pytest.raises(ValueError, match="Unsupported scheme: http"):
            parse_redis_uri(uri)

    def test_parse_uri_with_topology_cluster(self):
        """Cluster topology should be parsed from query params."""
        uri = "redis://localhost:6379/0?topology=cluster"
        result = parse_redis_uri(uri)

        assert result["topology"] == "cluster"
        assert result["cluster_mode"] is True

    def test_parse_uri_with_legacy_cluster_mode(self):
        """Legacy cluster_mode query param should still work."""
        uri = "redis://localhost:6379/0?cluster_mode=true"
        result = parse_redis_uri(uri)

        assert result["topology"] == "cluster"
        assert result["cluster_mode"] is True

    def test_parse_uri_with_sentinel_configuration(self):
        """Sentinel query params should be parsed."""
        uri = (
            "redis://localhost:6379/0?"
            "topology=sentinel&sentinel_master_name=mymaster&"
            "sentinel_nodes=host1:26379,host2:26380"
        )
        result = parse_redis_uri(uri)

        assert result["topology"] == "sentinel"
        assert result["cluster_mode"] is False
        assert result["sentinel_master_name"] == "mymaster"
        assert result["sentinel_nodes"] == [("host1", 26379), ("host2", 26380)]


class TestSetRedisConfigFromCLI:
    """Test cases for set_redis_config_from_cli function."""

    def setup_method(self):
        """Set up test fixtures."""
        # Store original config
        self.original_config = REDIS_CFG.copy()

    def teardown_method(self):
        """Restore original config."""
        REDIS_CFG.clear()
        REDIS_CFG.update(self.original_config)

    def test_set_string_values(self):
        """Test setting string configuration values."""
        config = {
            "host": "redis.example.com",
            "username": "testuser",
            "password": "testpass",
        }

        set_redis_config_from_cli(config)

        assert REDIS_CFG["host"] == "redis.example.com"
        assert REDIS_CFG["username"] == "testuser"
        assert REDIS_CFG["password"] == "testpass"

    def test_set_integer_values(self):
        """Test setting integer configuration values."""
        config = {"port": 6380, "db": 2}

        set_redis_config_from_cli(config)

        assert REDIS_CFG["port"] == 6380
        assert isinstance(REDIS_CFG["port"], int)
        assert REDIS_CFG["db"] == 2
        assert isinstance(REDIS_CFG["db"], int)

    def test_set_boolean_values(self):
        """Test setting boolean configuration values."""
        config = {"ssl": True, "cluster_mode": False, "topology": "standalone"}

        set_redis_config_from_cli(config)

        assert REDIS_CFG["ssl"] is True
        assert isinstance(REDIS_CFG["ssl"], bool)
        assert REDIS_CFG["cluster_mode"] is False
        assert isinstance(REDIS_CFG["cluster_mode"], bool)
        assert REDIS_CFG["topology"] == "standalone"

    def test_set_none_values(self):
        """Test setting None configuration values."""
        config = {"ssl_ca_path": None, "ssl_keyfile": None}

        set_redis_config_from_cli(config)

        assert REDIS_CFG["ssl_ca_path"] is None
        assert REDIS_CFG["ssl_keyfile"] is None

    def test_set_mixed_values(self):
        """Test setting mixed configuration values."""
        config = {
            "host": "localhost",
            "port": 6379,
            "ssl": True,
            "ssl_ca_path": "/path/to/ca.pem",
            "cluster_mode": False,
            "topology": "standalone",
            "username": None,
        }

        set_redis_config_from_cli(config)

        assert REDIS_CFG["host"] == "localhost"
        assert REDIS_CFG["port"] == 6379
        assert REDIS_CFG["ssl"] is True
        assert REDIS_CFG["ssl_ca_path"] == "/path/to/ca.pem"
        assert REDIS_CFG["cluster_mode"] is False
        assert REDIS_CFG["topology"] == "standalone"
        assert REDIS_CFG["username"] is None

    def test_convert_string_integers(self):
        """Test converting string integers to integers."""
        config = {"port": "6380", "db": "1"}

        set_redis_config_from_cli(config)

        assert REDIS_CFG["port"] == 6380
        assert isinstance(REDIS_CFG["port"], int)
        assert REDIS_CFG["db"] == 1
        assert isinstance(REDIS_CFG["db"], int)

    def test_convert_other_booleans_to_strings(self):
        """Test converting non-ssl/cluster_mode booleans to strings."""
        # This tests the behavior where other boolean values are converted to strings
        # for environment compatibility
        config = {"some_other_bool": True}

        set_redis_config_from_cli(config)

        # This would be converted to string for environment compatibility
        assert REDIS_CFG["some_other_bool"] == "true"

    def test_set_topology_to_cluster_sets_legacy_flag(self):
        """Topology updates should keep cluster_mode in sync."""
        set_redis_config_from_cli({"topology": "cluster"})

        assert REDIS_CFG["topology"] == "cluster"
        assert REDIS_CFG["cluster_mode"] is True

    def test_set_sentinel_nodes_parses_string(self):
        """Sentinel nodes should be normalized into host/port tuples."""
        set_redis_config_from_cli(
            {
                "topology": "sentinel",
                "sentinel_nodes": "host1:26379,host2:26380",
                "sentinel_master_name": "mymaster",
            }
        )

        assert REDIS_CFG["topology"] == "sentinel"
        assert REDIS_CFG["cluster_mode"] is False
        assert REDIS_CFG["sentinel_nodes"] == [("host1", 26379), ("host2", 26380)]

    def test_validate_sentinel_requires_master_name(self):
        """Sentinel config must provide a master name."""
        is_valid, error_message = validate_redis_config(
            {
                "topology": "sentinel",
                "cluster_mode": False,
                "sentinel_nodes": [("host1", 26379)],
                "sentinel_master_name": None,
            }
        )

        assert is_valid is False
        assert "sentinel_master_name" in error_message

    def test_validate_sentinel_requires_nodes(self):
        """Sentinel config must provide at least one node."""
        is_valid, error_message = validate_redis_config(
            {
                "topology": "sentinel",
                "cluster_mode": False,
                "sentinel_nodes": [],
                "sentinel_master_name": "mymaster",
            }
        )

        assert is_valid is False
        assert "sentinel node" in error_message

    def test_validate_conflicting_cluster_and_sentinel(self):
        """Cluster topology should not accept sentinel-only settings."""
        is_valid, error_message = validate_redis_config(
            {
                "topology": "cluster",
                "cluster_mode": True,
                "sentinel_master_name": "mymaster",
                "sentinel_nodes": [("host1", 26379)],
            }
        )

        assert is_valid is False
        assert "cannot be combined" in error_message

    def test_empty_config(self):
        """Test setting empty configuration."""
        original_config = REDIS_CFG.copy()
        config = {}

        set_redis_config_from_cli(config)

        # Config should remain unchanged
        assert REDIS_CFG == original_config


@patch.dict(os.environ, {}, clear=True)
class TestRedisConfigDefaults:
    """Test cases for REDIS_CFG default values."""

    @patch("src.common.config.load_dotenv")
    def test_default_config_values(self, mock_load_dotenv):
        """Test default configuration values when no environment variables are set."""
        # Re-import to get fresh config
        import importlib

        import src.common.config

        importlib.reload(src.common.config)

        config = src.common.config.REDIS_CFG

        assert config["host"] == "127.0.0.1"
        assert config["port"] == 6379
        assert config["username"] is None
        assert config["password"] == ""
        assert config["ssl"] is False
        assert config["cluster_mode"] is False
        assert config["topology"] == "standalone"
        assert config["db"] == 0
        assert config["sentinel_master_name"] is None
        assert config["sentinel_nodes"] == []

    @patch.dict(
        os.environ,
        {
            "REDIS_HOST": "redis.example.com",
            "REDIS_PORT": "6380",
            "REDIS_SSL": "true",
            "REDIS_CLUSTER_MODE": "1",
        },
    )
    @patch("src.common.config.load_dotenv")
    def test_config_from_environment(self, mock_load_dotenv):
        """Test configuration loading from environment variables."""
        # Re-import to get fresh config
        import importlib

        import src.common.config

        importlib.reload(src.common.config)

        config = src.common.config.REDIS_CFG

        assert config["host"] == "redis.example.com"
        assert config["port"] == 6380
        assert config["ssl"] is True
        assert config["cluster_mode"] is True
        assert config["topology"] == "cluster"

    @patch.dict(
        os.environ,
        {
            "REDIS_TOPOLOGY": "sentinel",
            "REDIS_SENTINEL_MASTER_NAME": "mymaster",
            "REDIS_SENTINEL_NODES": "host1:26379,host2:26380",
        },
    )
    @patch("src.common.config.load_dotenv")
    def test_sentinel_config_from_environment(self, mock_load_dotenv):
        """Sentinel configuration should load from environment variables."""
        import importlib

        import src.common.config

        importlib.reload(src.common.config)

        config = src.common.config.REDIS_CFG

        assert config["topology"] == "sentinel"
        assert config["cluster_mode"] is False
        assert config["sentinel_master_name"] == "mymaster"
        assert config["sentinel_nodes"] == [("host1", 26379), ("host2", 26380)]
