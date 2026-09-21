"""
Unit tests for src/common/connection.py
"""

from unittest.mock import Mock, patch

import pytest
import redis
from redis.exceptions import ConnectionError

from src.common.connection import RedisConnectionManager


def make_config(**overrides):
    config = {
        "topology": "standalone",
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
        "sentinel_master_name": None,
        "sentinel_nodes": [],
        "sentinel_username": None,
        "sentinel_password": None,
    }
    config.update(overrides)
    return config


class TestRedisConnectionManager:
    """Test cases for RedisConnectionManager class."""

    def setup_method(self):
        RedisConnectionManager._instance = None

    def teardown_method(self):
        RedisConnectionManager._instance = None

    @patch("src.common.connection.redis.Redis")
    @patch("src.common.connection.REDIS_CFG", new_callable=lambda: make_config())
    def test_get_connection_standalone_mode(self, mock_config, mock_redis_class):
        """Test getting connection in standalone mode."""
        mock_redis_instance = Mock()
        mock_redis_class.return_value = mock_redis_instance

        connection = RedisConnectionManager.get_connection()

        assert connection == mock_redis_instance
        mock_redis_class.assert_called_once()

        call_args = mock_redis_class.call_args[1]
        assert call_args["host"] == "localhost"
        assert call_args["port"] == 6379
        assert call_args["db"] == 0
        assert call_args["decode_responses"] is True
        assert call_args["max_connections"] == 10
        assert "lib_name" in call_args

    @patch("src.common.connection.redis.cluster.RedisCluster")
    @patch(
        "src.common.connection.REDIS_CFG",
        new_callable=lambda: make_config(
            topology="cluster",
            cluster_mode=True,
            username="testuser",
            password="testpass",
            ssl=True,
            ssl_ca_path="/path/to/ca.pem",
            ssl_keyfile="/path/to/key.pem",
            ssl_certfile="/path/to/cert.pem",
            ssl_ca_certs="/path/to/ca-bundle.pem",
        ),
    )
    def test_get_connection_cluster_mode(self, mock_config, mock_cluster_class):
        """Test getting connection in cluster mode."""
        mock_cluster_instance = Mock()
        mock_cluster_class.return_value = mock_cluster_instance

        connection = RedisConnectionManager.get_connection()

        assert connection == mock_cluster_instance
        mock_cluster_class.assert_called_once()

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

    @patch("src.common.connection.Sentinel")
    @patch(
        "src.common.connection.REDIS_CFG",
        new_callable=lambda: make_config(
            topology="sentinel",
            sentinel_master_name="mymaster",
            sentinel_nodes=[("host1", 26379), ("host2", 26380)],
            username="redis-user",
            password="redis-pass",
            sentinel_username="sentinel-user",
            sentinel_password="sentinel-pass",
            ssl=True,
        ),
    )
    def test_get_connection_sentinel_mode(self, mock_config, mock_sentinel_class):
        """Sentinel mode should resolve a master connection."""
        mock_master_connection = Mock()
        mock_sentinel_instance = Mock()
        mock_sentinel_instance.master_for.return_value = mock_master_connection
        mock_sentinel_class.return_value = mock_sentinel_instance

        connection = RedisConnectionManager.get_connection()

        assert connection == mock_master_connection
        mock_sentinel_class.assert_called_once()
        sentinel_call = mock_sentinel_class.call_args
        assert sentinel_call.args[0] == [("host1", 26379), ("host2", 26380)]
        assert sentinel_call.kwargs["sentinel_kwargs"] == {
            "username": "sentinel-user",
            "password": "sentinel-pass",
        }
        assert sentinel_call.kwargs["username"] == "redis-user"
        assert sentinel_call.kwargs["password"] == "redis-pass"
        assert sentinel_call.kwargs["ssl"] is True
        assert sentinel_call.kwargs["db"] == 0
        mock_sentinel_instance.master_for.assert_called_once_with(
            "mymaster",
            redis_class=redis.Redis,
        )

    @patch("src.common.connection.redis.Redis")
    @patch("src.common.connection.REDIS_CFG", new_callable=lambda: make_config())
    def test_get_connection_singleton_behavior(self, mock_config, mock_redis_class):
        """Test that get_connection returns the same instance."""
        mock_redis_instance = Mock()
        mock_redis_class.return_value = mock_redis_instance

        connection1 = RedisConnectionManager.get_connection()
        connection2 = RedisConnectionManager.get_connection()

        assert connection1 == connection2
        mock_redis_class.assert_called_once()

    @patch("src.common.connection.redis.Redis")
    @patch("src.common.connection.REDIS_CFG", new_callable=lambda: make_config())
    def test_get_connection_with_decode_responses_false(
        self, mock_config, mock_redis_class
    ):
        """Test getting connection with decode_responses=False."""
        mock_redis_instance = Mock()
        mock_redis_class.return_value = mock_redis_instance

        connection = RedisConnectionManager.get_connection(decode_responses=False)
        assert connection == mock_redis_instance

        call_args = mock_redis_class.call_args[1]
        assert call_args["decode_responses"] is False

    @patch(
        "src.common.connection.redis.Redis",
        side_effect=ConnectionError("Connection refused"),
    )
    @patch("src.common.connection.REDIS_CFG", new_callable=lambda: make_config())
    def test_connection_error_handling(self, mock_config, mock_redis_class):
        """Test standalone connection error handling."""
        with pytest.raises(ConnectionError, match="Connection refused"):
            RedisConnectionManager.get_connection()

    @patch(
        "src.common.connection.redis.cluster.RedisCluster",
        side_effect=ConnectionError("Cluster connection failed"),
    )
    @patch(
        "src.common.connection.REDIS_CFG",
        new_callable=lambda: make_config(topology="cluster", cluster_mode=True),
    )
    def test_cluster_connection_error_handling(self, mock_config, mock_cluster_class):
        """Test cluster connection error handling."""
        with pytest.raises(ConnectionError, match="Cluster connection failed"):
            RedisConnectionManager.get_connection()

    @patch("src.common.connection.Sentinel", side_effect=ConnectionError("Sentinel failed"))
    @patch(
        "src.common.connection.REDIS_CFG",
        new_callable=lambda: make_config(
            topology="sentinel",
            sentinel_master_name="mymaster",
            sentinel_nodes=[("host1", 26379)],
        ),
    )
    def test_sentinel_connection_error_handling(self, mock_config, mock_sentinel_class):
        """Test sentinel connection error handling."""
        with pytest.raises(ConnectionError, match="Sentinel failed"):
            RedisConnectionManager.get_connection()

    def test_reset_instance(self):
        """Test that the singleton instance can be reset."""
        mock_instance = Mock()
        RedisConnectionManager._instance = mock_instance

        assert RedisConnectionManager._instance == mock_instance

        RedisConnectionManager._instance = None

        assert RedisConnectionManager._instance is None
