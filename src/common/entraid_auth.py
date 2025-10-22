"""
Entra ID authentication provider factory for Redis MCP Server.

This module provides factory methods to create credential providers for different
Azure authentication flows based on configuration.
"""

import logging

from src.common.config import (
    ENTRAID_CFG,
    is_entraid_auth_enabled,
    validate_entraid_config,
)

_logger = logging.getLogger(__name__)

# Reduce Azure SDK logging verbosity
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)
logging.getLogger("azure.identity").setLevel(logging.WARNING)
logging.getLogger("redis.auth.token_manager").setLevel(logging.WARNING)

# Import redis-entraid components only when needed
try:
    from redis_entraid.cred_provider import (
        create_from_default_azure_credential,
        create_from_managed_identity,
        create_from_service_principal,
        ManagedIdentityType,
        TokenManagerConfig,
        RetryPolicy,
    )

    ENTRAID_AVAILABLE = True
except ImportError:
    _logger.warning(
        "redis-entraid package not available. Entra ID authentication will be disabled."
    )
    ENTRAID_AVAILABLE = False


class EntraIDAuthenticationError(Exception):
    """Exception raised for Entra ID authentication configuration errors."""

    pass


def create_credential_provider():
    """
    Create an Entra ID credential provider based on the current configuration.

    Returns:
        Credential provider instance or None if Entra ID auth is not configured.

    Raises:
        EntraIDAuthenticationError: If configuration is invalid or required packages are missing.
    """
    if not is_entraid_auth_enabled():
        return None

    if not ENTRAID_AVAILABLE:
        raise EntraIDAuthenticationError(
            "redis-entraid package is required for Entra ID authentication. "
            "Install it with: pip install redis-entraid"
        )

    # Validate configuration
    is_valid, error_message = validate_entraid_config()
    if not is_valid:
        raise EntraIDAuthenticationError(
            f"Invalid Entra ID configuration: {error_message}"
        )

    auth_flow = ENTRAID_CFG["auth_flow"]

    try:
        # Create token manager configuration
        token_manager_config = _create_token_manager_config()

        if auth_flow == "service_principal":
            return _create_service_principal_provider(token_manager_config)
        elif auth_flow == "managed_identity":
            return _create_managed_identity_provider(token_manager_config)
        elif auth_flow == "default_credential":
            return _create_default_credential_provider(token_manager_config)
        else:
            raise EntraIDAuthenticationError(
                f"Unsupported authentication flow: {auth_flow}"
            )

    except Exception as e:
        _logger.error("Failed to create Entra ID credential provider: %s", e)
        raise EntraIDAuthenticationError(f"Failed to create credential provider: {e}")


def _create_token_manager_config():
    """Create TokenManagerConfig from current configuration."""
    retry_policy = RetryPolicy(
        max_attempts=ENTRAID_CFG["retry_max_attempts"],
        delay_in_ms=ENTRAID_CFG["retry_delay_ms"],
    )

    return TokenManagerConfig(
        expiration_refresh_ratio=ENTRAID_CFG["token_expiration_refresh_ratio"],
        lower_refresh_bound_millis=ENTRAID_CFG["lower_refresh_bound_millis"],
        token_request_execution_timeout_in_ms=ENTRAID_CFG[
            "token_request_execution_timeout_ms"
        ],
        retry_policy=retry_policy,
    )


def _create_service_principal_provider(token_manager_config):
    """Create service principal credential provider."""

    return create_from_service_principal(
        client_id=ENTRAID_CFG["client_id"],
        client_credential=ENTRAID_CFG["client_secret"],
        tenant_id=ENTRAID_CFG["tenant_id"],
        token_manager_config=token_manager_config,
    )


def _create_managed_identity_provider(token_manager_config):
    """Create managed identity credential provider."""
    identity_type_str = ENTRAID_CFG["identity_type"]

    if identity_type_str == "system_assigned":
        identity_type = ManagedIdentityType.SYSTEM_ASSIGNED

        return create_from_managed_identity(
            identity_type=identity_type,
            resource=ENTRAID_CFG["resource"],
            token_manager_config=token_manager_config,
        )

    elif identity_type_str == "user_assigned":
        identity_type = ManagedIdentityType.USER_ASSIGNED

        return create_from_managed_identity(
            identity_type=identity_type,
            resource=ENTRAID_CFG["resource"],
            client_id=ENTRAID_CFG["user_assigned_identity_client_id"],
            token_manager_config=token_manager_config,
        )

    else:
        raise EntraIDAuthenticationError(f"Invalid identity type: {identity_type_str}")


def _create_default_credential_provider(token_manager_config):
    """Create default Azure credential provider."""

    # Parse scopes from configuration
    scopes_str = ENTRAID_CFG["scopes"]
    scopes = tuple(scope.strip() for scope in scopes_str.split(","))

    return create_from_default_azure_credential(
        scopes=scopes, token_manager_config=token_manager_config
    )
