"""
Pytest configuration and fixtures for Redis MCP Server tests.
"""

from unittest.mock import Mock, patch

import pytest
import redis
from redis.exceptions import ConnectionError, RedisError, TimeoutError


@pytest.fixture
def mock_redis():
    """Create a mock Redis connection."""
    mock = Mock(spec=redis.Redis)
    return mock


@pytest.fixture
def mock_redis_cluster():
    """Create a mock Redis Cluster connection."""
    mock = Mock(spec=redis.cluster.RedisCluster)
    return mock


@pytest.fixture
def mock_redis_connection_manager():
    """Mock the RedisConnectionManager to return a mock Redis connection."""
    with patch(
        "src.common.connection.RedisConnectionManager.get_connection"
    ) as mock_get_conn:
        mock_redis = Mock(spec=redis.Redis)
        mock_get_conn.return_value = mock_redis
        yield mock_redis


@pytest.fixture
def redis_config():
    """Sample Redis configuration for testing."""
    return {
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
        "cluster_mode": False,
    }


@pytest.fixture
def redis_uri_samples():
    """Sample Redis URIs for testing."""
    return {
        "basic": "redis://localhost:6379/0",
        "with_auth": "redis://user:pass@localhost:6379/0",
        "ssl": "rediss://user:pass@localhost:6379/0",
        "with_query": "redis://localhost:6379/0?ssl_cert_reqs=required",
        "cluster": "redis://localhost:6379/0?cluster_mode=true",
    }


@pytest.fixture
def sample_vector():
    """Sample vector for testing vector operations."""
    return [0.1, 0.2, 0.3, 0.4, 0.5]


@pytest.fixture
def sample_json_data():
    """Sample JSON data for testing."""
    return {
        "name": "John Doe",
        "age": 30,
        "city": "New York",
        "hobbies": ["reading", "swimming"],
    }


@pytest.fixture
def redis_error_scenarios():
    """Common Redis error scenarios for testing."""
    return {
        "connection_error": ConnectionError("Connection refused"),
        "timeout_error": TimeoutError("Operation timed out"),
        "generic_error": RedisError("Generic Redis error"),
        "auth_error": RedisError("NOAUTH Authentication required"),
        "wrong_type": RedisError(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        ),
    }


@pytest.fixture(autouse=True)
def reset_connection_manager():
    """Reset the RedisConnectionManager singleton before each test."""
    from src.common.connection import RedisConnectionManager

    RedisConnectionManager._instance = None
    yield
    RedisConnectionManager._instance = None


@pytest.fixture(autouse=True)
def reset_subscription_manager():
    """Reset the pub/sub subscription registry before each test."""
    from src.common.subscription_manager import SubscriptionManager

    SubscriptionManager.reset()
    yield
    SubscriptionManager.reset()


@pytest.fixture
def mock_numpy_array():
    """Mock numpy array for vector testing."""
    with patch("numpy.array") as mock_array:
        mock_array.return_value.tobytes.return_value = b"mock_binary_data"
        yield mock_array


@pytest.fixture
def mock_numpy_frombuffer():
    """Mock numpy frombuffer for vector testing."""
    with patch("numpy.frombuffer") as mock_frombuffer:
        mock_frombuffer.return_value.tolist.return_value = [0.1, 0.2, 0.3]
        yield mock_frombuffer


# Async test helpers
@pytest.fixture
def event_loop():
    """Create an event loop for async tests."""
    import asyncio

    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# Mark configurations
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
