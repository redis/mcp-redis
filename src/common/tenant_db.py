
import os
import json
import asyncio
import logging
import base64
import hashlib
from typing import Dict, Any, Optional
import asyncpg
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from src.common.config import RedisMCPConfig
from src.common.lru_cache import AsyncLRUCache

from src.common.constants import (
    DEFAULT_MAX_POOLS,
    DEFAULT_IDLE_TIMEOUT,
    DEFAULT_POSTGRES_MIN_SIZE,
    DEFAULT_POSTGRES_MAX_SIZE,
    DEFAULT_POSTGRES_TIMEOUT,
)

logger = logging.getLogger("mcp.tenant_db")

CENTRAL_DB_URL = os.environ.get("CENTRAL_DATABASE_URL", "postgresql://postgres:Admin12@localhost:5432/global")
ENCRYPTION_SECRET = os.environ.get("CREDENTIAL_ENCRYPTION_KEY", "MySecure32CharacterEncryptKey!!!")



def decrypt_aes_cbc(ciphertext_b64: str, key_str: str) -> str:
    """Tries standard Spring Boot / Java AES decryption strategies."""
    if not ciphertext_b64 or not key_str:
        return ciphertext_b64

    try:
        data = base64.b64decode(ciphertext_b64)
    except Exception:
        return ciphertext_b64

    raw_key = key_str.encode('utf-8')[:32].ljust(32, b'\x00')
    sha256_key = hashlib.sha256(key_str.encode('utf-8')).digest()
    candidate_keys = [raw_key, sha256_key]

    for key in candidate_keys:
        # Strategy 1: CBC with Zero IV
        try:
            cipher = Cipher(algorithms.AES(key), modes.CBC(b'\x00' * 16))
            decryptor = cipher.decryptor()
            padded_plain = decryptor.update(data) + decryptor.finalize()
            unpadder = padding.PKCS7(128).unpadder()
            return (unpadder.update(padded_plain) + unpadder.finalize()).decode('utf-8')
        except Exception:
            pass

        # Strategy 2: CBC with Prepended IV
        if len(data) > 16:
            try:
                iv = data[:16]
                ciphertext = data[16:]
                cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
                decryptor = cipher.decryptor()
                padded_plain = decryptor.update(ciphertext) + decryptor.finalize()
                unpadder = padding.PKCS7(128).unpadder()
                return (unpadder.update(padded_plain) + unpadder.finalize()).decode('utf-8')
            except Exception:
                pass

        # Strategy 3: ECB
        try:
            cipher = Cipher(algorithms.AES(key), modes.ECB())
            decryptor = cipher.decryptor()
            padded_plain = decryptor.update(data) + decryptor.finalize()
            unpadder = padding.PKCS7(128).unpadder()
            return (unpadder.update(padded_plain) + unpadder.finalize()).decode('utf-8')
        except Exception:
            pass

    logger.warning("Failed to decrypt password with provided secret key. Returning raw string.")
    return ciphertext_b64


class CentralRegistryManager:
    """Manages an async connection pool to the CENTRAL global Postgres registry database."""
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self._pool_init_lock = asyncio.Lock()

    async def _ensure_pool(self):
        if self.pool is not None:
            return
        async with self._pool_init_lock:
            if self.pool is not None:
                return
            
            logger.info("🗄️  [POSTGRES GLOBAL REGISTRY] Initializing connection pool targeting: %s", CENTRAL_DB_URL)
            try:
             

                self.pool = await asyncpg.create_pool(
                    dsn=CENTRAL_DB_URL,
                    min_size=DEFAULT_POSTGRES_MIN_SIZE, # 
                    max_size=DEFAULT_POSTGRES_MAX_SIZE, # 
                    timeout=DEFAULT_POSTGRES_TIMEOUT    # 
                )
                logger.info("[POSTGRES GLOBAL REGISTRY] Central database connection pool created successfully!")
            except Exception as e:
                err_msg = f"Failed establishing pool for Postgres Global Central Registry: {str(e)}"
                logger.error("[POSTGRES GLOBAL REGISTRY ERROR] %s", err_msg)
                raise ConnectionError(err_msg) from e

    async def fetch_tenant_db_credentials(self, tenant_id: str) -> Dict[str, Any]:
        await self._ensure_pool()
        assert self.pool is not None

        try:
            logger.info("[POSTGRES GLOBAL REGISTRY] Looking up Postgres credentials for tenant suffix: '%s'", tenant_id)
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT suffix, write_db_username, write_db_password 
                    FROM tenants 
                    WHERE suffix = $1;
                    """,
                    tenant_id
                )
                if not row:
                    err_msg = f"No tenant DB credentials registered in Global Registry for suffix: '{tenant_id}'"
                    logger.error("[POSTGRES GLOBAL REGISTRY ERROR] %s", err_msg)
                    raise LookupError(err_msg)

                raw_password = row['write_db_password']
                password = decrypt_aes_cbc(raw_password, ENCRYPTION_SECRET) if (raw_password and ENCRYPTION_SECRET) else raw_password

                return {
                    "host": os.environ.get("PG_HOST", "localhost"),
                    "port": 5432,
                    "user": row['write_db_username'],
                    "password": password,
                    "database": row['suffix']
                }
        except Exception as e:
            if not isinstance(e, LookupError):
                err_msg = f"Database query failure while querying Global Registry for tenant '{tenant_id}': {str(e)}"
                logger.error("[POSTGRES GLOBAL REGISTRY ERROR] %s", err_msg)
            raise


async def _close_pg_pool_callback(tenant_id: str, pool: asyncpg.Pool):
    """Callback function executed when a Postgres tenant pool is evicted."""
    await pool.close()
    logger.info("   ↳ [POSTGRES TENANT DB] Closed connection pool for tenant database: '%s'", tenant_id)


class PerTenantDBManager:
    """Manages dynamic connection pools to isolated tenant Postgres databases using AsyncLRUCache."""
    def __init__(self, central_registry: CentralRegistryManager, max_pools: int = 20, idle_timeout: int = DEFAULT_IDLE_TIMEOUT):
        self._central_registry = central_registry
        self.pools = AsyncLRUCache[asyncpg.Pool](
            max_size=max_pools,
            ttl_seconds=idle_timeout,
            on_evict=_close_pg_pool_callback
        )
        self._creation_locks: Dict[str, asyncio.Lock] = {}
        self._lock = asyncio.Lock()

    async def _get_tenant_pool(self, tenant_id: str) -> asyncpg.Pool:
        pool = await self.pools.get(tenant_id)
        if pool:
            return pool

        async with self._lock:
            if tenant_id not in self._creation_locks:
                self._creation_locks[tenant_id] = asyncio.Lock()

        async with self._creation_locks[tenant_id]:
            # Double check after acquiring tenant lock
            pool = await self.pools.get(tenant_id)
            if pool:
                return pool

            logger.info("[POSTGRES TENANT DB] Cold starting isolated Postgres Pool for tenant database: '%s'", tenant_id)
            
            # Step 1: Query global registry for tenant DB credentials
            creds = await self._central_registry.fetch_tenant_db_credentials(tenant_id)
            
            # Step 2: Establish connection pool to tenant's specific Postgres DB
            try:
                new_pool = await asyncpg.create_pool(
                    **creds, 
                    min_size=DEFAULT_POSTGRES_MIN_SIZE, 
                    max_size=DEFAULT_POSTGRES_MAX_SIZE,
                    command_timeout=DEFAULT_POSTGRES_TIMEOUT,
                    timeout=5.0
                )
                logger.info("[POSTGRES TENANT DB] Connected to isolated tenant database '%s' @ %s:%s", creds['database'], creds['host'], creds['port'])
                await self.pools.put(tenant_id, new_pool)
                return new_pool
            except Exception as e:
                err_msg = f"Failed creating Postgres connection pool targeting tenant database '{tenant_id}': {str(e)}"
                logger.error("[POSTGRES TENANT DB ERROR] %s", err_msg)
                raise ConnectionError(err_msg) from e

    async def get_mcp_config_by_id(self, tenant_id: str, mcp_id: Optional[str] = None) -> Dict[str, Any]:
        """Queries 'mcp_registry' strictly by unique 'mcp_id' (UUID) inside tenant's DB."""
        tenant_pool = await self._get_tenant_pool(tenant_id)
        
        try:
            async with tenant_pool.acquire() as conn:
                logger.info("[MCP CONFIG METADATA] Querying 'mcp_registry' for MCP ID '%s' in tenant DB '%s'", mcp_id, tenant_id)
                
                if mcp_id:
                    row = await conn.fetchrow(
                        "SELECT config FROM mcp_registry WHERE id = $1 LIMIT 1;", 
                        mcp_id
                    )
                else:
                    # Fallback to first available configuration if mcp_id is not provided
                    row = await conn.fetchrow(
                        "SELECT config FROM mcp_registry LIMIT 1;"
                    )
                
                if not row or row['config'] is None:
                    err_msg = f"No configuration entry found for MCP ID '{mcp_id}' in tenant DB '{tenant_id}'"
                    logger.error("[MCP CONFIG METADATA ERROR] %s", err_msg)
                    raise LookupError(err_msg)

                raw_config = row['config']

                # Parse & validate config (URL string, JSON string, or dict)
                validated = RedisMCPConfig.from_any(raw_config)

                return {
                    "REDIS_HOST": validated.host,
                    "REDIS_PORT": validated.port,
                    "REDIS_DB": validated.db,
                    "REDIS_USERNAME": validated.username,
                    "REDIS_PWD": validated.password,
                    "REDIS_CLUSTER_MODE": validated.cluster_mode,
                    "REDIS_SSL": validated.ssl.enabled,
                    "REDIS_SSL_CA_PATH": validated.ssl.ssl_ca_path,
                    "REDIS_SSL_KEYFILE": validated.ssl.ssl_keyfile,
                    "REDIS_SSL_CERTFILE": validated.ssl.ssl_certfile,
                    "REDIS_SSL_CERT_REQS": validated.ssl.ssl_cert_reqs,
                    "REDIS_SSL_CA_CERTS": validated.ssl.ssl_ca_certs,
                }
        except Exception as e:
            if not isinstance(e, LookupError):
                err_msg = f"Failed querying 'mcp_registry' for MCP ID '{mcp_id}' in tenant DB '{tenant_id}': {str(e)}"
                logger.error("[MCP CONFIG METADATA ERROR] %s", err_msg)
            raise

    async def close_idle_pools(self):
        """Triggered by background task to reclaim idle Postgres pools."""
        await self.pools.evict_idle()

    async def shutdown_all_pools(self):
        """Triggered on server shutdown to clear all Postgres connection pools."""
        logger.info("[POSTGRES TENANT DB] Tearing down active tenant database pools...")
        await self.pools.clear_all()
        self._creation_locks.clear()
        
        if self._central_registry.pool:
            await self._central_registry.pool.close()
            logger.info("[POSTGRES GLOBAL REGISTRY] Disconnected Central Registry pool.")
            self._central_registry.pool = None


central_registry = CentralRegistryManager()
tenant_db_manager = PerTenantDBManager(central_registry, max_pools=DEFAULT_MAX_POOLS, idle_timeout=DEFAULT_IDLE_TIMEOUT)