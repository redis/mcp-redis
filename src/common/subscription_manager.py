import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List

from redis import Redis
from redis.client import PubSub

logger = logging.getLogger(__name__)


class SubscriptionLimitExceededError(Exception):
    """Raised when a new subscription cannot be created due to capacity limits."""


def _decode_message_value(value: Any) -> Any:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    if isinstance(value, list):
        return [_decode_message_value(item) for item in value]
    if isinstance(value, tuple):
        return [_decode_message_value(item) for item in value]
    if isinstance(value, dict):
        return {
            _decode_message_value(key): _decode_message_value(item)
            for key, item in value.items()
        }
    return value


@dataclass
class Subscription:
    subscription_id: str
    pubsub: PubSub
    mode: str
    targets: List[str]
    last_accessed_at: float = field(default_factory=time.time)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


class SubscriptionManager:
    _subscriptions: Dict[str, Subscription] = {}
    _lock = threading.Lock()
    _stale_slots_reserved = 0
    MAX_ACTIVE_SUBSCRIPTIONS = 50
    STALE_SUBSCRIPTION_TTL_SECONDS = 300

    @classmethod
    def subscribe(cls, redis_client: Redis, channel: str) -> Dict[str, Any]:
        pubsub = redis_client.pubsub()
        try:
            pubsub.subscribe(channel)
            return cls._store(pubsub, "channel", [channel])
        except Exception:
            pubsub.close()
            raise

    @classmethod
    def psubscribe(cls, redis_client: Redis, pattern: str) -> Dict[str, Any]:
        pubsub = redis_client.pubsub()
        try:
            pubsub.psubscribe(pattern)
            return cls._store(pubsub, "pattern", [pattern])
        except Exception:
            pubsub.close()
            raise

    @classmethod
    def read_messages(
        cls, subscription_id: str, timeout_ms: int, max_messages: int
    ) -> Dict[str, Any]:
        subscription = cls._get(subscription_id)
        timeout_seconds = timeout_ms / 1000
        messages: List[Dict[str, Any]] = []

        with subscription.lock:
            deadline = time.monotonic() + timeout_seconds
            subscription.last_accessed_at = time.time()
            while len(messages) < max_messages:
                remaining = max(0.0, deadline - time.monotonic())
                message = subscription.pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=remaining,
                )

                if message is None:
                    break

                messages.append(_decode_message_value(message))

        return {
            "subscription_id": subscription.subscription_id,
            "message_count": len(messages),
            "messages": messages,
        }

    @classmethod
    def unsubscribe(cls, subscription_id: str) -> Dict[str, Any]:
        subscription = cls._pop(subscription_id)

        with subscription.lock:
            try:
                if subscription.mode == "pattern":
                    subscription.pubsub.punsubscribe(*subscription.targets)
                else:
                    subscription.pubsub.unsubscribe(*subscription.targets)
            finally:
                subscription.pubsub.close()

        return {
            "status": "success",
            "subscription_id": subscription.subscription_id,
            "mode": subscription.mode,
            "targets": subscription.targets,
        }

    @classmethod
    def reset(cls) -> None:
        with cls._lock:
            subscriptions = list(cls._subscriptions.values())
            cls._subscriptions = {}
            cls._stale_slots_reserved = 0

        for subscription in subscriptions:
            try:
                with subscription.lock:
                    subscription.pubsub.close()
            except Exception as exc:
                logger.warning(
                    "Failed to close pubsub during reset for subscription %s: %s",
                    subscription.subscription_id,
                    exc,
                )

    @classmethod
    def _store(cls, pubsub: PubSub, mode: str, targets: List[str]) -> Dict[str, Any]:
        subscription_id = uuid.uuid4().hex
        subscription = Subscription(
            subscription_id=subscription_id,
            pubsub=pubsub,
            mode=mode,
            targets=targets,
        )

        stale_subscriptions = cls._collect_stale_subscriptions()
        cls._close_stale_subscriptions(stale_subscriptions)

        with cls._lock:
            total_active = len(cls._subscriptions) + cls._stale_slots_reserved
            if total_active >= cls.MAX_ACTIVE_SUBSCRIPTIONS:
                raise SubscriptionLimitExceededError(
                    "Too many active subscriptions. Close unused subscriptions and try again."
                )
            cls._subscriptions[subscription_id] = subscription

        return {
            "status": "success",
            "subscription_id": subscription_id,
            "mode": mode,
            "targets": targets,
        }

    @classmethod
    def _get(cls, subscription_id: str) -> Subscription:
        with cls._lock:
            subscription = cls._subscriptions.get(subscription_id)

        if subscription is None:
            raise KeyError(subscription_id)

        return subscription

    @classmethod
    def _collect_stale_subscriptions(cls) -> List[Subscription]:
        now = time.time()
        with cls._lock:
            stale_ids = [
                subscription_id
                for subscription_id, subscription in cls._subscriptions.items()
                if now - subscription.last_accessed_at
                > cls.STALE_SUBSCRIPTION_TTL_SECONDS
            ]

            stale_subscriptions = [
                cls._subscriptions.pop(subscription_id)
                for subscription_id in stale_ids
                if subscription_id in cls._subscriptions
            ]
            cls._stale_slots_reserved += len(stale_subscriptions)
            return stale_subscriptions

    @classmethod
    def _close_stale_subscriptions(
        cls, stale_subscriptions: List[Subscription]
    ) -> None:
        failed_to_close: List[Subscription] = []
        for stale_subscription in stale_subscriptions:
            try:
                with stale_subscription.lock:
                    stale_subscription.pubsub.close()
            except Exception:
                failed_to_close.append(stale_subscription)

        if failed_to_close:
            with cls._lock:
                for subscription in failed_to_close:
                    cls._subscriptions[subscription.subscription_id] = subscription

        with cls._lock:
            cls._stale_slots_reserved = max(
                0, cls._stale_slots_reserved - len(stale_subscriptions)
            )

    @classmethod
    def _pop(cls, subscription_id: str) -> Subscription:
        with cls._lock:
            subscription = cls._subscriptions.pop(subscription_id, None)

        if subscription is None:
            raise KeyError(subscription_id)

        return subscription
