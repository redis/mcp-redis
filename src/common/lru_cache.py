

# src/common/lru_cache.py
import asyncio
import time
import logging
from typing import Dict, Any, Callable, Awaitable, Optional, TypeVar, Generic, Set

logger = logging.getLogger("mcp.lru_cache")

V = TypeVar('V')

class AsyncLRUCache(Generic[V]):
    """
    Thread-safe, async-friendly LRU Cache with TTL support and async eviction hooks.
    Manages active pool limits and clean teardowns for multi-tenant resources.
    """
    def __init__(
        self, 
        max_size: int = 20, 
        ttl_seconds: int = 600, 
        on_evict: Optional[Callable[[str, V], Awaitable[None]]] = None
    ):
        self._max_size = max_size
        self._ttl_seconds = ttl_seconds
        self._on_evict = on_evict
        
        self._cache: Dict[str, V] = {}
        self._last_accessed: Dict[str, float] = {}
        self._global_lock = asyncio.Lock()
        
        # Track background eviction tasks so GC doesn't drop them
        self._background_tasks: Set[asyncio.Task] = set()

    async def get(self, key: str) -> Optional[V]:
        """Retrieves an item and refreshes its LRU timestamp if valid."""
        async with self._global_lock:
            if key not in self._cache:
                return None

            # TTL check
            if time.time() - self._last_accessed[key] > self._ttl_seconds:
                val = self._cache.pop(key)
                self._last_accessed.pop(key, None)
                if self._on_evict and val:
                    # Safely schedule eviction with strong task reference
                    task = asyncio.create_task(self._safe_evict(key, val, reason="TTL Expired"))
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)
                return None

            # Refresh access timestamp
            self._last_accessed[key] = time.time()
            return self._cache[key]

    async def put(self, key: str, value: V):
        """Inserts or updates an item, triggering LRU eviction if max capacity is hit."""
        async with self._global_lock:
            if key in self._cache:
                self._cache[key] = value
                self._last_accessed[key] = time.time()
                return

            # Capacity eviction (LRU)
            if len(self._cache) >= self._max_size:
                # Safely find min key without crash if cache is unexpectedly empty
                oldest_key = min(self._last_accessed, key=self._last_accessed.get, default=None)
                if oldest_key:
                    evicted_val = self._cache.pop(oldest_key, None)
                    self._last_accessed.pop(oldest_key, None)
                    logger.info(f"[LRU CAPACITY] Max limit ({self._max_size}) reached. Evicting oldest tenant: '{oldest_key}'")
                    
                    if self._on_evict and evicted_val:
                        await self._safe_evict(oldest_key, evicted_val, reason="Capacity Bound Exceeded")

            self._cache[key] = value
            self._last_accessed[key] = time.time()

    async def evict_idle(self):
        """Sweeps cache and evicts items exceeding idle TTL."""
        now = time.time()
        to_evict = []
        
        async with self._global_lock:
            for k, last_time in list(self._last_accessed.items()):
                if now - last_time > self._ttl_seconds:
                    to_evict.append((k, self._cache.pop(k, None)))
                    self._last_accessed.pop(k, None)

        for key, val in to_evict:
            if val and self._on_evict:
                logger.info(f"[TTL EVICTION] Evicting idle pool for tenant: '{key}'")
                await self._safe_evict(key, val, reason="Idle Timeout")

    async def clear_all(self):
        """Cleans up all cached resources on server teardown."""
        async with self._global_lock:
            items = list(self._cache.items())
            self._cache.clear()
            self._last_accessed.clear()

        for key, val in items:
            if val and self._on_evict:
                await self._safe_evict(key, val, reason="Server Shutdown")
                
        # Await remaining background tasks during cleanup
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

    async def _safe_evict(self, key: str, value: V, reason: str):
        try:
            if self._on_evict:
                await self._on_evict(key, value)
        except Exception as e:
            logger.error(f"Error during eviction callback for key '{key}' ({reason}): {e}")

    def __len__(self):
        return len(self._cache)