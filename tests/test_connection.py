"""
Unit tests for src/common/connection.py
"""

from unittest.mock import Mock, patch

import pytest
from redis.exceptions import ConnectionError

from src.common.connection import RedisConnectionManager


class TestRedisConnectionManager:
    """Test cases for RedisConnectionManager class."""

    def setup_method(self):
        """Set up test fixtures."""
        # Reset singleton instance before each test
        RedisConnectionManager._instance = None

    def teardown_method(self):
        """Clean up after each test."""
        # Reset singleton instance after each test
        RedisConnectionManager._instance = None

    @patch("src.common.connection.redis.Redis")
    @patch("src.common.connection.REDIS_CFG")
    def test_get_connection_standalone_mode(self, mock_config, mock_redis_class):
        """Test getting connection in standalone mode."""
        mock_config.__getitem__.side_effect = lambda key: {
            "cluster_mode": False,
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "username": None,
            "password": "",
            "ssl": False,
            "ssl_ca_path": None,
            "ssl_keyfile": None,
            "ssl_certfile": None,
            "ssl_cert_reqs": "required",
            "ssl_ca_certs": None,
            "ssl_check_hostname": True,
        }[key]

        mock_redis_instance = Mock()
        mock_redis_class.return_value = mock_redis_instance

        connection = RedisConnectionManager.get_connection()

        assert connection == mock_redis_instance
        mock_redis_class.assert_called_once()

        # Verify connection parameters
        call_args = mock_redis_class.call_args[1]
        assert call_args["host"] == "localhost"
        assert call_args["port"] == 6379
        assert call_args["db"] == 0
        assert call_args["decode_responses"] is True
        assert call_args["max_connections"] == 10
        assert "lib_name" in call_args

    @patch("src.common.connection.redis.cluster.RedisCluster")
    @patch("src.common.connection.REDIS_CFG")
    def test_get_connection_cluster_mode(self, mock_config, mock_cluster_class):
        """Test getting connection in cluster mode."""
        mock_config.__getitem__.side_effect = lambda key: {
            "cluster_mode": True,
            "host": "localhost",
            "port": 6379,
            "username": "testuser",
            "password": "testpass",
            "ssl": True,
            "ssl_ca_path": "/path/to/ca.pem",
            "ssl_keyfile": "/path/to/key.pem",
            "ssl_certfile": "/path/to/cert.pem",
            "ssl_cert_reqs": "required",
            "ssl_ca_certs": "/path/to/ca-bundle.pem",
            "ssl_check_hostname": True,
        }[key]

        mock_cluster_instance = Mock()
        mock_cluster_class.return_value = mock_cluster_instance

        connection = RedisConnectionManager.get_connection()

        assert connection == mock_cluster_instance
        mock_cluster_class.assert_called_once()

        # Verify connection parameters
        call_args = mock_cluster_class.call_args[1]
        assert call_args["host"] == "localhost"
        assert call_args["port"] == 6379
        assert call_args["username"] == "testuser"
        assert call_args["password"] == "testpass"
        assert call_args["ssl"] is True
        assert call_args["ssl_ca_path"] == "/path/to/ca.pem"
        assert call_args["decode_responses"] is True
        assert call_args["max_connections_per_node"] == 10
        assert "lib_name" in call_args

    @patch("src.common.connection.redis.Redis")
    @patch("src.common.connection.REDIS_CFG")
    def test_get_connection_singleton_behavior(self, mock_config, mock_redis_class):
        """Test that get_connection returns the same instance (singleton behavior)."""
        mock_config.__getitem__.side_effect = lambda key: {
            "cluster_mode": False,
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "username": None,
            "password": "",
            "ssl": False,
            "ssl_ca_path": None,
            "ssl_keyfile": None,
            "ssl_certfile": None,
            "ssl_cert_reqs": "required",
            "ssl_ca_certs": None,
            "ssl_check_hostname": True,
        }[key]

        mock_redis_instance = Mock()
        mock_redis_class.return_value = mock_redis_instance

        # First call
        connection1 = RedisConnectionManager.get_connection()
        # Second call
        connection2 = RedisConnectionManager.get_connection()

        assert connection1 == connection2
        assert connection1 == mock_redis_instance
        # Redis class should only be called once
        mock_redis_class.assert_called_once()

    @patch("src.common.connection.redis.Redis")
    @patch("src.common.connection.REDIS_CFG")
    def test_get_connection_with_decode_responses_false(
        self, mock_config, mock_redis_class
    ):
        """Test getting connection with decode_responses=False."""
        mock_config.__getitem__.side_effect = lambda key: {
            "cluster_mode": False,
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "username": None,
            "password": "",
            "ssl": False,
            "ssl_ca_path": None,
            "ssl_keyfile": None,
            "ssl_certfile": None,
            "ssl_cert_reqs": "required",
            "ssl_ca_certs": None,
            "ssl_check_hostname": True,
        }[key]

        mock_redis_instance = Mock()
        mock_redis_class.return_value = mock_redis_instance

        connection = RedisConnectionManager.get_connection(decode_responses=False)
        assert connection == mock_redis_instance

        call_args = mock_redis_class.call_args[1]
        assert call_args["decode_responses"] is False

    @patch("src.common.connection.redis.Redis")
    @patch("src.common.connection.REDIS_CFG")
    def test_get_connection_with_ssl_configuration(self, mock_config, mock_redis_class):
        """Test getting connection with SSL configuration."""
        mock_config.__getitem__.side_effect = lambda key: {
            "cluster_mode": False,
            "host": "redis.example.com",
            "port": 6380,
            "db": 1,
            "username": "ssluser",
            "password": "sslpass",
            "ssl": True,
            "ssl_ca_path": "/path/to/ca.pem",
            "ssl_keyfile": "/path/to/key.pem",
            "ssl_certfile": "/path/to/cert.pem",
            "ssl_cert_reqs": "optional",
            "ssl_ca_certs": "/path/to/ca-bundle.pem",
            "ssl_check_hostname": True,
        }[key]

        mock_redis_instance = Mock()
        mock_redis_class.return_value = mock_redis_instance

        connection = RedisConnectionManager.get_connection()
        assert connection == mock_redis_instance

        call_args = mock_redis_class.call_args[1]
        assert call_args["ssl"] is True
        assert call_args["ssl_ca_path"] == "/path/to/ca.pem"
        assert call_args["ssl_keyfile"] == "/path/to/key.pem"
        assert call_args["ssl_certfile"] == "/path/to/cert.pem"
        assert call_args["ssl_cert_reqs"] == "optional"
        assert call_args["ssl_ca_certs"] == "/path/to/ca-bundle.pem"

    @patch("src.common.connection.redis.Redis")
    @patch("src.common.connection.REDIS_CFG")
    def test_get_connection_includes_version_in_lib_name(
        self, mock_config, mock_redis_class
    ):
        """Test that connection includes version information in lib_name."""
        mock_config.__getitem__.side_effect = lambda key: {
            "cluster_mode": False,
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "username": None,
            "password": "",
            "ssl": False,
            "ssl_ca_path": None,
            "ssl_keyfile": None,
            "ssl_certfile": None,
            "ssl_cert_reqs": "required",
            "ssl_ca_certs": None,
            "ssl_check_hostname": True,
        }[key]

        mock_redis_instance = Mock()
        mock_redis_class.return_value = mock_redis_instance

        with patch("src.common.connection.__version__", "1.0.0"):
            connection = RedisConnectionManager.get_connection()

            assert connection == mock_redis_instance

            call_args = mock_redis_class.call_args[1]
            assert "redis-py(mcp-server_v1.0.0)" in call_args["lib_name"]

    @patch("src.common.connection.redis.Redis")
    @patch("src.common.connection.REDIS_CFG")
    def test_connection_error_handling(self, mock_config, mock_redis_class):
        """Test connection error handling."""
        mock_config.__getitem__.side_effect = lambda key: {
            "cluster_mode": False,
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "username": None,
            "password": "",
            "ssl": False,
            "ssl_ca_path": None,
            "ssl_keyfile": None,
            "ssl_certfile": None,
            "ssl_cert_reqs": "required",
            "ssl_ca_certs": None,
            "ssl_check_hostname": True,
        }[key]

        # Mock Redis constructor to raise ConnectionError
        mock_redis_class.side_effect = ConnectionError("Connection refused")

        with pytest.raises(ConnectionError, match="Connection refused"):
            RedisConnectionManager.get_connection()

    @patch("src.common.connection.redis.cluster.RedisCluster")
    @patch("src.common.connection.REDIS_CFG")
    def test_cluster_connection_error_handling(self, mock_config, mock_cluster_class):
        """Test cluster connection error handling."""
        mock_config.__getitem__.side_effect = lambda key: {
            "cluster_mode": True,
            "host": "localhost",
            "port": 6379,
            "username": None,
            "password": "",
            "ssl": False,
            "ssl_ca_path": None,
            "ssl_keyfile": None,
            "ssl_certfile": None,
            "ssl_cert_reqs": "required",
            "ssl_ca_certs": None,
            "ssl_check_hostname": True,
        }[key]

        # Mock RedisCluster constructor to raise ConnectionError
        mock_cluster_class.side_effect = ConnectionError("Cluster connection failed")

        with pytest.raises(ConnectionError, match="Cluster connection failed"):
            RedisConnectionManager.get_connection()

    def test_reset_instance(self):
        """Test that the singleton instance can be reset."""
        # Set up a mock instance
        mock_instance = Mock()
        RedisConnectionManager._instance = mock_instance

        # Verify instance is set
        assert RedisConnectionManager._instance == mock_instance

        # Reset instance
        RedisConnectionManager._instance = None

        # Verify instance is reset
        assert RedisConnectionManager._instance is None

    @patch("src.common.connection.redis.Redis")
    @patch("src.common.connection.REDIS_CFG")
    def test_connection_parameters_filtering(self, mock_config, mock_redis_class):
        """Test that None values are properly handled in connection parameters."""
        mock_config.__getitem__.side_effect = lambda key: {
            "cluster_mode": False,
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "username": None,  # This should be passed as None
            "password": "",  # This should be passed as empty string
            "ssl": False,
            "ssl_ca_path": None,
            "ssl_keyfile": None,
            "ssl_certfile": None,
            "ssl_cert_reqs": "required",
            "ssl_ca_certs": None,
            "ssl_check_hostname": True,
        }[key]

        mock_redis_instance = Mock()
        mock_redis_class.return_value = mock_redis_instance

        connection = RedisConnectionManager.get_connection()

        assert connection == mock_redis_instance

        call_args = mock_redis_class.call_args[1]
        assert call_args["username"] is None
        assert call_args["password"] == ""
        assert call_args["ssl_ca_path"] is None
