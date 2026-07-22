
# # # src/common/tenant_db.py
# # import os
# # import asyncio
# # import logging
# # import base64
# # from typing import Dict, Any
# # import hashlib
# # import time
# # import asyncpg
# # from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
# # from cryptography.hazmat.primitives import padding

# # logger = logging.getLogger("mcp.tenant_db")

# # # Read central metadata registry configurations from environment variables
# # CENTRAL_DB_URL = os.environ.get("CENTRAL_DATABASE_URL", "postgresql://postgres:Admin12@localhost:5432/global")
# # ENCRYPTION_SECRET = os.environ.get("CREDENTIAL_ENCRYPTION_KEY", "MySecure32CharacterEncryptKey!!!")



# # def decrypt_aes_cbc(ciphertext_b64: str, key_str: str) -> str:
# #     """
# #     Tries multiple common Spring Boot / Java AES decryption strategies 
# #     (AES-CBC with Zero-IV, IV-prefix, SHA256 derived key, and AES-ECB).
# #     """
# #     if not ciphertext_b64 or not key_str:
# #         return ciphertext_b64

# #     try:
# #         data = base64.b64decode(ciphertext_b64)
# #     except Exception:
# #         return ciphertext_b64

# #     # Candidate keys: Raw key bytes vs SHA256 hashed key bytes
# #     raw_key = key_str.encode('utf-8')[:32].ljust(32, b'\x00')
# #     sha256_key = hashlib.sha256(key_str.encode('utf-8')).digest()
    
# #     candidate_keys = [raw_key, sha256_key]

# #     for key in candidate_keys:
# #         # Strategy 1: CBC with Zero IV (16 zero bytes) - Most common Java/Spring implementation
# #         try:
# #             cipher = Cipher(algorithms.AES(key), modes.CBC(b'\x00' * 16))
# #             decryptor = cipher.decryptor()
# #             padded_plain = decryptor.update(data) + decryptor.finalize()
# #             unpadder = padding.PKCS7(128).unpadder()
# #             return (unpadder.update(padded_plain) + unpadder.finalize()).decode('utf-8')
# #         except Exception:
# #             pass

# #         # Strategy 2: CBC with IV embedded in the first 16 bytes
# #         if len(data) > 16:
# #             try:
# #                 iv = data[:16]
# #                 ciphertext = data[16:]
# #                 cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
# #                 decryptor = cipher.decryptor()
# #                 padded_plain = decryptor.update(ciphertext) + decryptor.finalize()
# #                 unpadder = padding.PKCS7(128).unpadder()
# #                 return (unpadder.update(padded_plain) + unpadder.finalize()).decode('utf-8')
# #             except Exception:
# #                 pass

# #         # Strategy 3: ECB Mode (Legacy Spring Boot default)
# #         try:
# #             cipher = Cipher(algorithms.AES(key), modes.ECB())
# #             decryptor = cipher.decryptor()
# #             padded_plain = decryptor.update(data) + decryptor.finalize()
# #             unpadder = padding.PKCS7(128).unpadder()
# #             return (unpadder.update(padded_plain) + unpadder.finalize()).decode('utf-8')
# #         except Exception:
# #             pass

# #     logger.warning("Failed to decrypt password with provided secret key. Returning raw string.")
# #     return ciphertext_b64

# # class CentralRegistryManager:
# #     """
# #     Manages an async connection pool to the CENTRAL 'global_registry' database.
# #     Queries the custom schema using 'suffix' as the primary tenant tracking token.
# #     """
# #     def __init__(self):
# #         self.pool = None
# #         self._pool_init_lock = asyncio.Lock()

# #     async def _ensure_pool(self):
# #         if self.pool is not None:
# #             return
# #         async with self._pool_init_lock:
# #             if self.pool is not None:
# #                 return
            
# #             print(f"🗄️  Connecting to central DB at: {CENTRAL_DB_URL}", flush=True)
# #             try:
# #                 self.pool = await asyncpg.create_pool(
# #                     dsn=CENTRAL_DB_URL,
# #                     min_size=1,
# #                     max_size=5,
# #                     timeout=10.0
# #                 )
# #                 print("✅ Connected to central DB pool!", flush=True)
# #             except Exception as e:
# #                 print(f"❌ Failed to create pool: {type(e).__name__}: {e}", flush=True)
# #                 raise

# #     async def fetch_tenant_db_credentials(self, tenant_id: str) -> Dict[str, Any]:
# #         """Fetches the DB credentials using your real schema layout."""
# #         await self._ensure_pool()
# #         async with self.pool.acquire() as conn:
# #             row = await conn.fetchrow(
# #                 """
# #                 SELECT suffix, write_db_username, write_db_password 
# #                 FROM tenants 
# #                 WHERE suffix = $1;
# #                 """,
# #                 tenant_id
# #             )
# #             if not row:
# #                 raise LookupError(f"No active tenant DB registration found for suffix identifier: '{tenant_id}'")

# #             raw_password = row['write_db_password']
# #             print(f"🔑 [DEBUG 1] Raw creds for '{tenant_id}': User='{row['write_db_username']}', RawPass='{raw_password}', DB='{row['suffix']}'", flush=True)

# #             # Decrypt the Spring Boot AES password
# #             if raw_password and ENCRYPTION_SECRET:
# #                 password = decrypt_aes_cbc(raw_password, ENCRYPTION_SECRET)
# #             else:
# #                 password = raw_password

# #             print(f"🔑 [DEBUG 2] Decrypted creds for '{tenant_id}': User='{row['write_db_username']}', Pass='{password}', DB='{row['suffix']}'", flush=True)
            
# #             return {
# #                 "host": os.environ.get("PG_HOST", "localhost"),
# #                 "port": 5432,
# #                 "user": row['write_db_username'],
# #                 "password": password,
# #                 "database": row['suffix']
# #             }


# # class PerTenantDBManager:
# #     """
# #     Manages connection pools to each tenant's individual, isolated PostgreSQL database.
# #     """
# #     def __init__(self, central_registry: CentralRegistryManager ,  idle_timeout: int = 600):
# #         self._central_registry = central_registry
# #         self._tenant_pools: Dict[str, asyncpg.Pool] = {}
# #         self._tenant_pool_locks: Dict[str, asyncio.Lock] = {}
# #         self._last_accessed: Dict[str, float] = {}  # Track activity for eviction
# #         self._idle_timeout = idle_timeout  # Seconds (default: 10 mins)
# #         self._lock = asyncio.Lock()

# #     async def _get_tenant_pool(self, tenant_id: str) -> asyncpg.Pool:
# #         """Lazily creates and caches an asyncpg.Pool for a specific tenant's database."""

# #         await self.close_idle_pools()

# #         self._last_accessed[tenant_id] = time.time()

# #         if tenant_id in self._tenant_pools:
# #             return self._tenant_pools[tenant_id]

# #         async with self._lock:
# #             if tenant_id not in self._tenant_pool_locks:
# #                 self._tenant_pool_locks[tenant_id] = asyncio.Lock()

# #         async with self._tenant_pool_locks[tenant_id]:
# #             if tenant_id in self._tenant_pools:
# #                 return self._tenant_pools[tenant_id]

# #             print(f"❄️  [TENANT DB] Cold starting isolated Postgres Pool for Tenant: '{tenant_id}'", flush=True)
# #             creds = await self._central_registry.fetch_tenant_db_credentials(tenant_id)
# #             pool = await asyncpg.create_pool(
# #                 **creds, 
# #                 min_size=1, 
# #                 max_size=3,
# #                 command_timeout=15.0,  # Query execution timeout
# #                 timeout=5.0           # Pool acquisition timeout
# #                 )
# #             self._tenant_pools[tenant_id] = pool
# #             return pool

# #     async def get_redis_config_for_tenant(self, tenant_id: str) -> Dict[str, Any]:
# #         """
# #         Connects to the tenant's private DB and fetches their Redis config.
# #         """
# #         tenant_pool = await self._get_tenant_pool(tenant_id)
# #         async with tenant_pool.acquire() as conn:
# #             print(f"🔍 [TENANT DB] Querying isolated table 'redis_config_registry' inside DB '{tenant_id}'", flush=True)
# #             row = await conn.fetchrow(
# #                 """
# #                 SELECT host, port, db, username, password, ssl, cluster_mode, entraid_auth_enabled
# #                 FROM redis_config_registry LIMIT 1;
# #                 """
# #             )
# #             if not row:
# #                 raise LookupError(f"No Redis configuration found in database for tenant: '{tenant_id}'")

# #             print(f"🔍 [TENANT DB] Result isolated table 'redis_config_registry' Row DB '{row}'", flush=True)
            
# #             return {
# #                 "REDIS_HOST": row['host'],
# #                 "REDIS_PORT": int(row['port']),
# #                 "REDIS_DB": int(row['db']) if row['db'] is not None else 0,
# #                 "REDIS_USERNAME": row['username'],
# #                 "REDIS_PWD": row['password'], 
# #                 "REDIS_SSL": bool(row['ssl']),
# #                 "REDIS_CLUSTER_MODE": bool(row['cluster_mode']),
# #                 "REDIS_ENTRAID_AUTH": bool(row['entraid_auth_enabled'])
# #             }

# #     async def shutdown_all_pools(self):
# #         """Closes all cached tenant database connection pools cleanly on server teardown."""
# #         print(f"🛑 [TENANT DB] Tearing down {len(self._tenant_pools)} isolated database connection pools...", flush=True)
# #         for tenant_id, pool in self._tenant_pools.items():
# #             await pool.close()
# #             print(f"   ↳ Disconnected Postgres pool for tenant '{tenant_id}'", flush=True)
# #         self._tenant_pools.clear()
        
# #         if self._central_registry.pool:
# #             await self._central_registry.pool.close()
# #             print("   ↳ Disconnected Global Central Registry pool.", flush=True)
# #             self._central_registry.pool = None

    
# #     async def close_idle_pools(self):
# #         """Background task to close pools for tenants that haven't sent queries recently."""
# #         now = time.time()
# #         to_remove = [
# #             t_id for t_id, last_time in self._last_accessed.items()
# #             if now - last_time > self._idle_timeout and t_id in self._tenant_pools
# #         ]
        
# #         for tenant_id in to_remove:
# #             async with self._tenant_pool_locks.get(tenant_id, asyncio.Lock()):
# #                 pool = self._tenant_pools.pop(tenant_id, None)
# #                 if pool:
# #                     await pool.close()
# #                     self._last_accessed.pop(tenant_id, None)
# #                     print(f"🧹 [TENANT DB] Closed idle Postgres pool for tenant: '{tenant_id}'", flush=True)


# # central_registry = CentralRegistryManager()
# # tenant_db_manager = PerTenantDBManager(central_registry)







# # src/common/tenant_db.py
# import os
# import asyncio
# import logging
# import base64
# import hashlib
# import time
# from typing import Dict, Any
# import asyncpg
# from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
# from cryptography.hazmat.primitives import padding

# from src.common.lru_cache import AsyncLRUCache

# logger = logging.getLogger("mcp.tenant_db")

# CENTRAL_DB_URL = os.environ.get("CENTRAL_DATABASE_URL", "postgresql://postgres:Admin12@localhost:5432/global")
# ENCRYPTION_SECRET = os.environ.get("CREDENTIAL_ENCRYPTION_KEY", "MySecure32CharacterEncryptKey!!!")


# def decrypt_aes_cbc(ciphertext_b64: str, key_str: str) -> str:
#     """Tries standard Spring Boot / Java AES decryption strategies."""
#     if not ciphertext_b64 or not key_str:
#         return ciphertext_b64

#     try:
#         data = base64.b64decode(ciphertext_b64)
#     except Exception:
#         return ciphertext_b64

#     raw_key = key_str.encode('utf-8')[:32].ljust(32, b'\x00')
#     sha256_key = hashlib.sha256(key_str.encode('utf-8')).digest()
#     candidate_keys = [raw_key, sha256_key]

#     for key in candidate_keys:
#         try:
#             cipher = Cipher(algorithms.AES(key), modes.CBC(b'\x00' * 16))
#             decryptor = cipher.decryptor()
#             padded_plain = decryptor.update(data) + decryptor.finalize()
#             unpadder = padding.PKCS7(128).unpadder()
#             return (unpadder.update(padded_plain) + unpadder.finalize()).decode('utf-8')
#         except Exception:
#             pass

#         if len(data) > 16:
#             try:
#                 iv = data[:16]
#                 ciphertext = data[16:]
#                 cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
#                 decryptor = cipher.decryptor()
#                 padded_plain = decryptor.update(ciphertext) + decryptor.finalize()
#                 unpadder = padding.PKCS7(128).unpadder()
#                 return (unpadder.update(padded_plain) + unpadder.finalize()).decode('utf-8')
#             except Exception:
#                 pass

#         try:
#             cipher = Cipher(algorithms.AES(key), modes.ECB())
#             decryptor = cipher.decryptor()
#             padded_plain = decryptor.update(data) + decryptor.finalize()
#             unpadder = padding.PKCS7(128).unpadder()
#             return (unpadder.update(padded_plain) + unpadder.finalize()).decode('utf-8')
#         except Exception:
#             pass

#     logger.warning("Failed to decrypt password with provided secret key. Returning raw string.")
#     return ciphertext_b64


# class CentralRegistryManager:
#     """Manages an async connection pool to the CENTRAL global registry database."""
#     def __init__(self):
#         self.pool = None
#         self._pool_init_lock = asyncio.Lock()

#     async def _ensure_pool(self):
#         if self.pool is not None:
#             return
#         async with self._pool_init_lock:
#             if self.pool is not None:
#                 return
            
#             print(f"🗄️  Connecting to central DB at: {CENTRAL_DB_URL}", flush=True)
#             try:
#                 self.pool = await asyncpg.create_pool(
#                     dsn=CENTRAL_DB_URL,
#                     min_size=1,
#                     max_size=5,
#                     timeout=10.0
#                 )
#                 print("✅ Connected to central DB pool!", flush=True)
#             except Exception as e:
#                 print(f"❌ Failed to create pool: {type(e).__name__}: {e}", flush=True)
#                 raise

#     async def fetch_tenant_db_credentials(self, tenant_id: str) -> Dict[str, Any]:
#         await self._ensure_pool()
#         async with self.pool.acquire() as conn:
#             row = await conn.fetchrow(
#                 """
#                 SELECT suffix, write_db_username, write_db_password 
#                 FROM tenants 
#                 WHERE suffix = $1;
#                 """,
#                 tenant_id
#             )
#             if not row:
#                 raise LookupError(f"No active tenant DB registration found for suffix identifier: '{tenant_id}'")

#             raw_password = row['write_db_password']
#             password = decrypt_aes_cbc(raw_password, ENCRYPTION_SECRET) if (raw_password and ENCRYPTION_SECRET) else raw_password

#             return {
#                 "host": os.environ.get("PG_HOST", "localhost"),
#                 "port": 5432,
#                 "user": row['write_db_username'],
#                 "password": password,
#                 "database": row['suffix']
#             }


# async def _close_pg_pool_callback(tenant_id: str, pool: asyncpg.Pool):
#     """Callback function executed when a Postgres pool is evicted."""
#     await pool.close()
#     print(f"   ↳ [PG DISCONNECT] Disconnected Postgres pool for tenant: '{tenant_id}'", flush=True)


# class PerTenantDBManager:
#     """Manages dynamic connection pools to isolated tenant databases using AsyncLRUCache."""
#     def __init__(self, central_registry: CentralRegistryManager, max_pools: int = 20, idle_timeout: int = 600):
#         self._central_registry = central_registry
#         self.pools = AsyncLRUCache[asyncpg.Pool](
#             max_size=max_pools,
#             ttl_seconds=idle_timeout,
#             on_evict=_close_pg_pool_callback
#         )
#         self._creation_locks: Dict[str, asyncio.Lock] = {}
#         self._lock = asyncio.Lock()

#     async def _get_tenant_pool(self, tenant_id: str) -> asyncpg.Pool:
#         pool = await self.pools.get(tenant_id)
#         if pool:
#             return pool

#         async with self._lock:
#             if tenant_id not in self._creation_locks:
#                 self._creation_locks[tenant_id] = asyncio.Lock()

#         async with self._creation_locks[tenant_id]:
#             # Double check after acquiring tenant lock
#             pool = await self.pools.get(tenant_id)
#             if pool:
#                 return pool

#             print(f"❄️  [TENANT DB] Cold starting isolated Postgres Pool for Tenant: '{tenant_id}'", flush=True)
#             creds = await self._central_registry.fetch_tenant_db_credentials(tenant_id)
#             new_pool = await asyncpg.create_pool(
#                 **creds, 
#                 min_size=1, 
#                 max_size=3,
#                 command_timeout=15.0,
#                 timeout=5.0
#             )
#             await self.pools.put(tenant_id, new_pool)
#             return new_pool

#     async def get_redis_config_for_tenant(self, tenant_id: str) -> Dict[str, Any]:
#         tenant_pool = await self._get_tenant_pool(tenant_id)
#         async with tenant_pool.acquire() as conn:
#             print(f"🔍 [TENANT DB] Querying isolated table 'redis_config_registry' inside DB '{tenant_id}'", flush=True)
#             row = await conn.fetchrow(
#                 """
#                 SELECT host, port, db, username, password, ssl, cluster_mode, entraid_auth_enabled
#                 FROM redis_config_registry LIMIT 1;
#                 """
#             )
#             if not row:
#                 raise LookupError(f"No Redis configuration found in database for tenant: '{tenant_id}'")

#             return {
#                 "REDIS_HOST": row['host'],
#                 "REDIS_PORT": int(row['port']),
#                 "REDIS_DB": int(row['db']) if row['db'] is not None else 0,
#                 "REDIS_USERNAME": row['username'],
#                 "REDIS_PWD": row['password'], 
#                 "REDIS_SSL": bool(row['ssl']),
#                 "REDIS_CLUSTER_MODE": bool(row['cluster_mode']),
#                 "REDIS_ENTRAID_AUTH": bool(row['entraid_auth_enabled'])
#             }

#     async def close_idle_pools(self):
#         """Triggered by background task to reclaim idle pools."""
#         await self.pools.evict_idle()

#     async def shutdown_all_pools(self):
#         """Triggered on server shutdown to clear all connection pools."""
#         print(f"🛑 [TENANT DB] Tearing down active database pools...", flush=True)
#         await self.pools.clear_all()
        
#         if self._central_registry.pool:
#             await self._central_registry.pool.close()
#             print("   ↳ Disconnected Global Central Registry pool.", flush=True)
#             self._central_registry.pool = None


# central_registry = CentralRegistryManager()
# tenant_db_manager = PerTenantDBManager(central_registry, idle_timeout=15)








import os
import asyncio
import logging
import base64
import hashlib
from typing import Dict, Any, Optional
import asyncpg
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding

from src.common.lru_cache import AsyncLRUCache

logger = logging.getLogger("mcp.tenant_db")

CENTRAL_DB_URL = os.environ.get("CENTRAL_DATABASE_URL", "postgresql://postgres:Admin12@localhost:5432/global")
ENCRYPTION_SECRET = os.environ.get("CREDENTIAL_ENCRYPTION_KEY", "MySecure32CharacterEncryptKey!!!")
DEFAULT_IDLE_TIMEOUT = int(os.environ.get("TENANT_POOL_IDLE_TIMEOUT", "30"))


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
    """Manages an async connection pool to the CENTRAL global registry database."""
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self._pool_init_lock = asyncio.Lock()

    async def _ensure_pool(self):
        if self.pool is not None:
            return
        async with self._pool_init_lock:
            if self.pool is not None:
                return
            
            print(f"🗄️  Connecting to central DB at: {CENTRAL_DB_URL}", flush=True)
            try:
                self.pool = await asyncpg.create_pool(
                    dsn=CENTRAL_DB_URL,
                    min_size=1,
                    max_size=5,
                    timeout=10.0
                )
                print("✅ Connected to central DB pool!", flush=True)
            except Exception as e:
                print(f"❌ Failed to create central pool: {type(e).__name__}: {e}", flush=True)
                raise

    async def fetch_tenant_db_credentials(self, tenant_id: str) -> Dict[str, Any]:
        await self._ensure_pool()
        assert self.pool is not None
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
                raise LookupError(f"No active tenant DB registration found for suffix identifier: '{tenant_id}'")

            raw_password = row['write_db_password']
            password = decrypt_aes_cbc(raw_password, ENCRYPTION_SECRET) if (raw_password and ENCRYPTION_SECRET) else raw_password

            return {
                "host": os.environ.get("PG_HOST", "localhost"),
                "port": 5432,
                "user": row['write_db_username'],
                "password": password,
                "database": row['suffix']
            }


async def _close_pg_pool_callback(tenant_id: str, pool: asyncpg.Pool):
    """Callback function executed when a Postgres pool is evicted."""
    await pool.close()
    print(f"   ↳ [PG DISCONNECT] Disconnected Postgres pool for tenant: '{tenant_id}'", flush=True)


class PerTenantDBManager:
    """Manages dynamic connection pools to isolated tenant databases using AsyncLRUCache."""
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

            print(f"❄️  [TENANT DB] Cold starting isolated Postgres Pool for Tenant: '{tenant_id}'", flush=True)
            creds = await self._central_registry.fetch_tenant_db_credentials(tenant_id)
            new_pool = await asyncpg.create_pool(
                **creds, 
                min_size=1, 
                max_size=3,
                command_timeout=15.0,
                timeout=5.0
            )
            await self.pools.put(tenant_id, new_pool)
            return new_pool

    async def get_redis_config_for_tenant(self, tenant_id: str) -> Dict[str, Any]:
        tenant_pool = await self._get_tenant_pool(tenant_id)
        async with tenant_pool.acquire() as conn:
            print(f"🔍 [TENANT DB] Querying isolated table 'redis_config_registry' inside DB '{tenant_id}'", flush=True)
            row = await conn.fetchrow(
                """
                SELECT host, port, db, username, password, ssl, cluster_mode, entraid_auth_enabled
                FROM redis_config_registry LIMIT 1;
                """
            )
            if not row:
                raise LookupError(f"No Redis configuration found in database for tenant: '{tenant_id}'")

            return {
                "REDIS_HOST": row['host'],
                "REDIS_PORT": int(row['port']),
                "REDIS_DB": int(row['db']) if row['db'] is not None else 0,
                "REDIS_USERNAME": row['username'],
                "REDIS_PWD": row['password'], 
                "REDIS_SSL": bool(row['ssl']),
                "REDIS_CLUSTER_MODE": bool(row['cluster_mode']),
                "REDIS_ENTRAID_AUTH": bool(row['entraid_auth_enabled'])
            }

    async def close_idle_pools(self):
        """Triggered by background task to reclaim idle pools."""
        await self.pools.evict_idle()

    async def shutdown_all_pools(self):
        """Triggered on server shutdown to clear all connection pools."""
        print(f"🛑 [TENANT DB] Tearing down active database pools...", flush=True)
        await self.pools.clear_all()
        self._creation_locks.clear()
        
        if self._central_registry.pool:
            await self._central_registry.pool.close()
            print("   ↳ Disconnected Global Central Registry pool.", flush=True)
            self._central_registry.pool = None


central_registry = CentralRegistryManager()
# FIXED: Removed trailing comma
tenant_db_manager = PerTenantDBManager(central_registry, idle_timeout=DEFAULT_IDLE_TIMEOUT)






# import os
# import asyncio
# import logging
# import base64
# import hashlib
# from typing import Dict, Any, Optional
# import asyncpg
# from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
# from cryptography.hazmat.primitives import padding

# from src.common.lru_cache import AsyncLRUCache

# logger = logging.getLogger("mcp.tenant_db")

# CENTRAL_DB_URL = os.environ.get("CENTRAL_DATABASE_URL", "postgresql://postgres:Admin12@localhost:5432/global")
# ENCRYPTION_SECRET = os.environ.get("CREDENTIAL_ENCRYPTION_KEY", "MySecure32CharacterEncryptKey!!!")
# DEFAULT_IDLE_TIMEOUT = int(os.environ.get("TENANT_POOL_IDLE_TIMEOUT", "600"))


# def decrypt_aes_cbc(ciphertext_b64: str, key_str: str) -> str:
#     """Tries standard Spring Boot / Java AES decryption strategies."""
#     if not ciphertext_b64 or not key_str:
#         return ciphertext_b64

#     try:
#         data = base64.b64decode(ciphertext_b64)
#     except Exception:
#         return ciphertext_b64

#     raw_key = key_str.encode('utf-8')[:32].ljust(32, b'\x00')
#     sha256_key = hashlib.sha256(key_str.encode('utf-8')).digest()
#     candidate_keys = [raw_key, sha256_key]

#     for key in candidate_keys:
#         # Strategy 1: CBC with Zero IV
#         try:
#             cipher = Cipher(algorithms.AES(key), modes.CBC(b'\x00' * 16))
#             decryptor = cipher.decryptor()
#             padded_plain = decryptor.update(data) + decryptor.finalize()
#             unpadder = padding.PKCS7(128).unpadder()
#             return (unpadder.update(padded_plain) + unpadder.finalize()).decode('utf-8')
#         except Exception:
#             pass

#         # Strategy 2: CBC with Prepended IV
#         if len(data) > 16:
#             try:
#                 iv = data[:16]
#                 ciphertext = data[16:]
#                 cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
#                 decryptor = cipher.decryptor()
#                 padded_plain = decryptor.update(ciphertext) + decryptor.finalize()
#                 unpadder = padding.PKCS7(128).unpadder()
#                 return (unpadder.update(padded_plain) + unpadder.finalize()).decode('utf-8')
#             except Exception:
#                 pass

#         # Strategy 3: ECB
#         try:
#             cipher = Cipher(algorithms.AES(key), modes.ECB())
#             decryptor = cipher.decryptor()
#             padded_plain = decryptor.update(data) + decryptor.finalize()
#             unpadder = padding.PKCS7(128).unpadder()
#             return (unpadder.update(padded_plain) + unpadder.finalize()).decode('utf-8')
#         except Exception:
#             pass

#     logger.warning("Failed to decrypt password with provided secret key. Returning raw payload.")
#     return ciphertext_b64


# class CentralRegistryManager:
#     """Manages an async connection pool to the central registry database."""
#     def __init__(self):
#         self.pool: Optional[asyncpg.Pool] = None
#         self._pool_init_lock = asyncio.Lock()

#     async def _ensure_pool(self):
#         if self.pool is not None:
#             return
#         async with self._pool_init_lock:
#             if self.pool is not None:
#                 return
            
#             logger.info("Initializing central database connection pool")
#             try:
#                 self.pool = await asyncpg.create_pool(
#                     dsn=CENTRAL_DB_URL,
#                     min_size=1,
#                     max_size=5,
#                     timeout=10.0
#                 )
#                 logger.info("Successfully established central database connection pool")
#             except Exception as e:
#                 logger.error(f"Failed to initialize central database pool: {type(e).__name__}: {e}")
#                 raise

#     async def fetch_tenant_db_credentials(self, tenant_id: str) -> Dict[str, Any]:
#         await self._ensure_pool()
#         assert self.pool is not None
#         async with self.pool.acquire() as conn:
#             row = await conn.fetchrow(
#                 """
#                 SELECT suffix, write_db_username, write_db_password 
#                 FROM tenants 
#                 WHERE suffix = $1;
#                 """,
#                 tenant_id
#             )
#             if not row:
#                 raise LookupError(f"No active database registration found for tenant identifier: '{tenant_id}'")

#             raw_password = row['write_db_password']
#             password = decrypt_aes_cbc(raw_password, ENCRYPTION_SECRET) if (raw_password and ENCRYPTION_SECRET) else raw_password

#             return {
#                 "host": os.environ.get("PG_HOST", "localhost"),
#                 "port": 5432,
#                 "user": row['write_db_username'],
#                 "password": password,
#                 "database": row['suffix']
#             }


# async def _close_pg_pool_callback(tenant_id: str, pool: asyncpg.Pool):
#     """Callback function executed when a PostgreSQL pool is evicted."""
#     await pool.close()
#     logger.info(f"Disconnected PostgreSQL pool for evicted tenant: '{tenant_id}'")


# class PerTenantDBManager:
#     """Manages dynamic connection pools to isolated tenant databases using AsyncLRUCache."""
#     def __init__(self, central_registry: CentralRegistryManager, max_pools: int = 20, idle_timeout: int = DEFAULT_IDLE_TIMEOUT):
#         self._central_registry = central_registry
#         self.pools = AsyncLRUCache[asyncpg.Pool](
#             max_size=max_pools,
#             ttl_seconds=idle_timeout,
#             on_evict=_close_pg_pool_callback
#         )
#         self._creation_locks: Dict[str, asyncio.Lock] = {}
#         self._lock = asyncio.Lock()

#     async def _get_tenant_pool(self, tenant_id: str) -> asyncpg.Pool:
#         pool = await self.pools.get(tenant_id)
#         if pool:
#             return pool

#         async with self._lock:
#             if tenant_id not in self._creation_locks:
#                 self._creation_locks[tenant_id] = asyncio.Lock()

#         async with self._creation_locks[tenant_id]:
#             # Double check after acquiring tenant lock
#             pool = await self.pools.get(tenant_id)
#             if pool:
#                 return pool

#             logger.info(f"Initializing isolated PostgreSQL pool for tenant: '{tenant_id}'")
#             creds = await self._central_registry.fetch_tenant_db_credentials(tenant_id)
#             new_pool = await asyncpg.create_pool(
#                 **creds, 
#                 min_size=1, 
#                 max_size=3,
#                 command_timeout=15.0,
#                 timeout=5.0
#             )
#             await self.pools.put(tenant_id, new_pool)
#             return new_pool

#     async def get_redis_config_for_tenant(self, tenant_id: str) -> Dict[str, Any]:
#         tenant_pool = await self._get_tenant_pool(tenant_id)
#         async with tenant_pool.acquire() as conn:
#             logger.debug(f"Fetching Redis configuration registry from database for tenant: '{tenant_id}'")
#             row = await conn.fetchrow(
#                 """
#                 SELECT host, port, db, username, password, ssl, cluster_mode, entraid_auth_enabled
#                 FROM redis_config_registry LIMIT 1;
#                 """
#             )
#             if not row:
#                 raise LookupError(f"No Redis configuration found in database for tenant: '{tenant_id}'")

#             return {
#                 "REDIS_HOST": row['host'],
#                 "REDIS_PORT": int(row['port']),
#                 "REDIS_DB": int(row['db']) if row['db'] is not None else 0,
#                 "REDIS_USERNAME": row['username'],
#                 "REDIS_PWD": row['password'], 
#                 "REDIS_SSL": bool(row['ssl']),
#                 "REDIS_CLUSTER_MODE": bool(row['cluster_mode']),
#                 "REDIS_ENTRAID_AUTH": bool(row['entraid_auth_enabled'])
#             }

#     async def close_idle_pools(self):
#         """Triggered by background task to reclaim idle pools."""
#         await self.pools.evict_idle()

#     async def shutdown_all_pools(self):
#         """Triggered on server shutdown to clear all connection pools."""
#         logger.info("Terminating all active tenant PostgreSQL connection pools")
#         await self.pools.clear_all()
#         self._creation_locks.clear()
        
#         if self._central_registry.pool:
#             await self._central_registry.pool.close()
#             logger.info("Closed central registry database pool")
#             self._central_registry.pool = None


# central_registry = CentralRegistryManager()
# tenant_db_manager = PerTenantDBManager(central_registry, idle_timeout=DEFAULT_IDLE_TIMEOUT)