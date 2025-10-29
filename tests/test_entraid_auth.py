"""
Unit tests for Entra ID authentication in src/common/entraid_auth.py
"""

from unittest.mock import Mock, patch

import pytest

from src.common.entraid_auth import (
    create_credential_provider,
    EntraIDAuthenticationError,
    _create_token_manager_config,
    _create_service_principal_provider,
    _create_managed_identity_provider,
    _create_default_credential_provider,
)


class TestCreateCredentialProvider:
    """Test cases for create_credential_provider function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.original_entraid_cfg = {}
        self.original_entraid_available = None

    def teardown_method(self):
        """Restore original state."""
        pass

    @patch("src.common.entraid_auth.is_entraid_auth_enabled")
    def test_returns_none_when_auth_disabled(self, mock_is_enabled):
        """Test that None is returned when Entra ID auth is disabled."""
        mock_is_enabled.return_value = False

        result = create_credential_provider()

        assert result is None
        mock_is_enabled.assert_called_once()

    @patch("src.common.entraid_auth.ENTRAID_AVAILABLE", False)
    @patch("src.common.entraid_auth.is_entraid_auth_enabled")
    def test_raises_error_when_package_not_available(self, mock_is_enabled):
        """Test that error is raised when redis-entraid package is not available."""
        mock_is_enabled.return_value = True

        with pytest.raises(EntraIDAuthenticationError) as exc_info:
            create_credential_provider()

        assert "redis-entraid package is required" in str(exc_info.value)

    @patch("src.common.entraid_auth.validate_entraid_config")
    @patch("src.common.entraid_auth.is_entraid_auth_enabled")
    def test_raises_error_on_invalid_config(self, mock_is_enabled, mock_validate):
        """Test that error is raised when configuration is invalid."""
        mock_is_enabled.return_value = True
        mock_validate.return_value = (False, "Invalid configuration")

        with pytest.raises(EntraIDAuthenticationError) as exc_info:
            create_credential_provider()

        assert "Invalid Entra ID configuration" in str(exc_info.value)

    @patch("src.common.entraid_auth._create_service_principal_provider")
    @patch("src.common.entraid_auth._create_token_manager_config")
    @patch.dict("src.common.entraid_auth.ENTRAID_CFG", {"auth_flow": "service_principal"})
    @patch("src.common.entraid_auth.validate_entraid_config")
    @patch("src.common.entraid_auth.is_entraid_auth_enabled")
    def test_creates_service_principal_provider(
        self,
        mock_is_enabled,
        mock_validate,
        mock_create_token_config,
        mock_create_sp_provider,
    ):
        """Test creating service principal credential provider."""
        mock_is_enabled.return_value = True
        mock_validate.return_value = (True, "")
        mock_token_config = Mock()
        mock_create_token_config.return_value = mock_token_config
        mock_provider = Mock()
        mock_create_sp_provider.return_value = mock_provider

        result = create_credential_provider()

        assert result == mock_provider
        mock_create_sp_provider.assert_called_once_with(mock_token_config)

    @patch("src.common.entraid_auth._create_managed_identity_provider")
    @patch("src.common.entraid_auth._create_token_manager_config")
    @patch.dict("src.common.entraid_auth.ENTRAID_CFG", {"auth_flow": "managed_identity"})
    @patch("src.common.entraid_auth.validate_entraid_config")
    @patch("src.common.entraid_auth.is_entraid_auth_enabled")
    def test_creates_managed_identity_provider(
        self,
        mock_is_enabled,
        mock_validate,
        mock_create_token_config,
        mock_create_mi_provider,
    ):
        """Test creating managed identity credential provider."""
        mock_is_enabled.return_value = True
        mock_validate.return_value = (True, "")
        mock_token_config = Mock()
        mock_create_token_config.return_value = mock_token_config
        mock_provider = Mock()
        mock_create_mi_provider.return_value = mock_provider

        result = create_credential_provider()

        assert result == mock_provider
        mock_create_mi_provider.assert_called_once_with(mock_token_config)

    @patch("src.common.entraid_auth._create_default_credential_provider")
    @patch("src.common.entraid_auth._create_token_manager_config")
    @patch.dict("src.common.entraid_auth.ENTRAID_CFG", {"auth_flow": "default_credential"})
    @patch("src.common.entraid_auth.validate_entraid_config")
    @patch("src.common.entraid_auth.is_entraid_auth_enabled")
    def test_creates_default_credential_provider(
        self,
        mock_is_enabled,
        mock_validate,
        mock_create_token_config,
        mock_create_dc_provider,
    ):
        """Test creating default credential provider."""
        mock_is_enabled.return_value = True
        mock_validate.return_value = (True, "")
        mock_token_config = Mock()
        mock_create_token_config.return_value = mock_token_config
        mock_provider = Mock()
        mock_create_dc_provider.return_value = mock_provider

        result = create_credential_provider()

        assert result == mock_provider
        mock_create_dc_provider.assert_called_once_with(mock_token_config)

    @patch("src.common.entraid_auth._create_token_manager_config")
    @patch.dict("src.common.entraid_auth.ENTRAID_CFG", {"auth_flow": "unsupported_flow"})
    @patch("src.common.entraid_auth.validate_entraid_config")
    @patch("src.common.entraid_auth.is_entraid_auth_enabled")
    def test_raises_error_on_unsupported_flow(
        self, mock_is_enabled, mock_validate, mock_create_token_config
    ):
        """Test that error is raised for unsupported auth flow."""
        mock_is_enabled.return_value = True
        mock_validate.return_value = (True, "")
        mock_create_token_config.return_value = Mock()

        with pytest.raises(EntraIDAuthenticationError) as exc_info:
            create_credential_provider()

        assert "Unsupported authentication flow" in str(exc_info.value)


class TestCreateTokenManagerConfig:
    """Test cases for _create_token_manager_config function."""

    @patch("src.common.entraid_auth.ENTRAID_CFG")
    @patch("src.common.entraid_auth.TokenManagerConfig")
    @patch("src.common.entraid_auth.RetryPolicy")
    def test_creates_token_manager_config(
        self, mock_retry_policy_class, mock_token_config_class, mock_entraid_cfg
    ):
        """Test creating token manager configuration."""
        mock_entraid_cfg.__getitem__.side_effect = lambda key: {
            "retry_max_attempts": 3,
            "retry_delay_ms": 100,
            "token_expiration_refresh_ratio": 0.9,
            "lower_refresh_bound_millis": 30000,
            "token_request_execution_timeout_ms": 10000,
        }[key]

        mock_retry_policy = Mock()
        mock_retry_policy_class.return_value = mock_retry_policy
        mock_token_config = Mock()
        mock_token_config_class.return_value = mock_token_config

        result = _create_token_manager_config()

        # Verify RetryPolicy was created with correct parameters
        mock_retry_policy_class.assert_called_once_with(
            max_attempts=3, delay_in_ms=100
        )

        # Verify TokenManagerConfig was created with correct parameters
        mock_token_config_class.assert_called_once_with(
            expiration_refresh_ratio=0.9,
            lower_refresh_bound_millis=30000,
            token_request_execution_timeout_in_ms=10000,
            retry_policy=mock_retry_policy,
        )

        assert result == mock_token_config


class TestCreateServicePrincipalProvider:
    """Test cases for _create_service_principal_provider function."""

    @patch("src.common.entraid_auth.create_from_service_principal")
    @patch("src.common.entraid_auth.ENTRAID_CFG")
    def test_creates_service_principal_provider(
        self, mock_entraid_cfg, mock_create_sp
    ):
        """Test creating service principal provider."""
        mock_entraid_cfg.__getitem__.side_effect = lambda key: {
            "client_id": "test-client-id",
            "client_secret": "test-secret",
            "tenant_id": "test-tenant-id",
        }[key]

        mock_token_config = Mock()
        mock_provider = Mock()
        mock_create_sp.return_value = mock_provider

        result = _create_service_principal_provider(mock_token_config)

        mock_create_sp.assert_called_once_with(
            client_id="test-client-id",
            client_credential="test-secret",
            tenant_id="test-tenant-id",
            token_manager_config=mock_token_config,
        )

        assert result == mock_provider


class TestCreateManagedIdentityProvider:
    """Test cases for _create_managed_identity_provider function."""

    @patch("src.common.entraid_auth.create_from_managed_identity")
    @patch("src.common.entraid_auth.ManagedIdentityType")
    @patch("src.common.entraid_auth.ENTRAID_CFG")
    def test_creates_system_assigned_managed_identity_provider(
        self, mock_entraid_cfg, mock_identity_type_class, mock_create_mi
    ):
        """Test creating system-assigned managed identity provider."""
        mock_entraid_cfg.__getitem__.side_effect = lambda key: {
            "identity_type": "system_assigned",
            "resource": "https://redis.azure.com/",
        }[key]

        mock_identity_type = Mock()
        mock_identity_type_class.SYSTEM_ASSIGNED = mock_identity_type
        mock_token_config = Mock()
        mock_provider = Mock()
        mock_create_mi.return_value = mock_provider

        result = _create_managed_identity_provider(mock_token_config)

        mock_create_mi.assert_called_once_with(
            identity_type=mock_identity_type,
            resource="https://redis.azure.com/",
            token_manager_config=mock_token_config,
        )

        assert result == mock_provider

    @patch("src.common.entraid_auth.create_from_managed_identity")
    @patch("src.common.entraid_auth.ManagedIdentityType")
    @patch("src.common.entraid_auth.ENTRAID_CFG")
    def test_creates_user_assigned_managed_identity_provider(
        self, mock_entraid_cfg, mock_identity_type_class, mock_create_mi
    ):
        """Test creating user-assigned managed identity provider."""
        mock_entraid_cfg.__getitem__.side_effect = lambda key: {
            "identity_type": "user_assigned",
            "resource": "https://redis.azure.com/",
            "user_assigned_identity_client_id": "test-user-assigned-id",
        }[key]

        mock_identity_type = Mock()
        mock_identity_type_class.USER_ASSIGNED = mock_identity_type
        mock_token_config = Mock()
        mock_provider = Mock()
        mock_create_mi.return_value = mock_provider

        result = _create_managed_identity_provider(mock_token_config)

        mock_create_mi.assert_called_once_with(
            identity_type=mock_identity_type,
            resource="https://redis.azure.com/",
            client_id="test-user-assigned-id",
            token_manager_config=mock_token_config,
        )

        assert result == mock_provider

    @patch("src.common.entraid_auth.ENTRAID_CFG")
    def test_raises_error_on_invalid_identity_type(self, mock_entraid_cfg):
        """Test that error is raised for invalid identity type."""
        mock_entraid_cfg.__getitem__.side_effect = lambda key: {
            "identity_type": "invalid_type",
        }[key]

        mock_token_config = Mock()

        with pytest.raises(EntraIDAuthenticationError) as exc_info:
            _create_managed_identity_provider(mock_token_config)

        assert "Invalid identity type" in str(exc_info.value)


class TestCreateDefaultCredentialProvider:
    """Test cases for _create_default_credential_provider function."""

    @patch("src.common.entraid_auth.create_from_default_azure_credential")
    @patch("src.common.entraid_auth.ENTRAID_CFG")
    def test_creates_default_credential_provider_single_scope(
        self, mock_entraid_cfg, mock_create_dc
    ):
        """Test creating default credential provider with single scope."""
        mock_entraid_cfg.__getitem__.side_effect = lambda key: {
            "scopes": "https://redis.azure.com/.default",
        }[key]

        mock_token_config = Mock()
        mock_provider = Mock()
        mock_create_dc.return_value = mock_provider

        result = _create_default_credential_provider(mock_token_config)

        mock_create_dc.assert_called_once_with(
            scopes=("https://redis.azure.com/.default",),
            token_manager_config=mock_token_config,
        )

        assert result == mock_provider

    @patch("src.common.entraid_auth.create_from_default_azure_credential")
    @patch("src.common.entraid_auth.ENTRAID_CFG")
    def test_creates_default_credential_provider_multiple_scopes(
        self, mock_entraid_cfg, mock_create_dc
    ):
        """Test creating default credential provider with multiple scopes."""
        mock_entraid_cfg.__getitem__.side_effect = lambda key: {
            "scopes": "https://redis.azure.com/.default, https://other.scope/.default",
        }[key]

        mock_token_config = Mock()
        mock_provider = Mock()
        mock_create_dc.return_value = mock_provider

        result = _create_default_credential_provider(mock_token_config)

        mock_create_dc.assert_called_once_with(
            scopes=(
                "https://redis.azure.com/.default",
                "https://other.scope/.default",
            ),
            token_manager_config=mock_token_config,
        )

        assert result == mock_provider

    @patch("src.common.entraid_auth.create_from_default_azure_credential")
    @patch("src.common.entraid_auth.ENTRAID_CFG")
    def test_creates_default_credential_provider_with_whitespace(
        self, mock_entraid_cfg, mock_create_dc
    ):
        """Test that scopes with whitespace are properly trimmed."""
        mock_entraid_cfg.__getitem__.side_effect = lambda key: {
            "scopes": "  https://redis.azure.com/.default  ,  https://other.scope/.default  ",
        }[key]

        mock_token_config = Mock()
        mock_provider = Mock()
        mock_create_dc.return_value = mock_provider

        result = _create_default_credential_provider(mock_token_config)

        mock_create_dc.assert_called_once_with(
            scopes=(
                "https://redis.azure.com/.default",
                "https://other.scope/.default",
            ),
            token_manager_config=mock_token_config,
        )

        assert result == mock_provider


class TestEntraIDAuthenticationError:
    """Test cases for EntraIDAuthenticationError exception."""

    def test_exception_can_be_raised(self):
        """Test that EntraIDAuthenticationError can be raised."""
        with pytest.raises(EntraIDAuthenticationError):
            raise EntraIDAuthenticationError("Test error message")

    def test_exception_message(self):
        """Test that exception message is preserved."""
        error_msg = "Test error message"
        with pytest.raises(EntraIDAuthenticationError) as exc_info:
            raise EntraIDAuthenticationError(error_msg)

        assert str(exc_info.value) == error_msg

    def test_exception_is_exception_subclass(self):
        """Test that EntraIDAuthenticationError is an Exception subclass."""
        assert issubclass(EntraIDAuthenticationError, Exception)

