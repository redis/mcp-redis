import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List

from redis import Redis
from redis.client import PubSub


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
        return {key: _decode_message_value(item) for key, item in value.items()}
    return value


@dataclass
class Subscription:
    subscription_id: str
    pubsub: PubSub
    mode: str
    targets: List[str]
    created_at: float = field(default_factory=time.time)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


class SubscriptionManager:
    _subscriptions: Dict[str, Subscription] = {}
    _lock = threading.Lock()

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
        deadline = time.monotonic() + timeout_seconds
        messages: List[Dict[str, Any]] = []

        with subscription.lock:
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

        for subscription in subscriptions:
            with subscription.lock:
                subscription.pubsub.close()

    @classmethod
    def _store(cls, pubsub: PubSub, mode: str, targets: List[str]) -> Dict[str, Any]:
        subscription_id = uuid.uuid4().hex
        subscription = Subscription(
            subscription_id=subscription_id,
            pubsub=pubsub,
            mode=mode,
            targets=targets,
        )

        with cls._lock:
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
    def _pop(cls, subscription_id: str) -> Subscription:
        with cls._lock:
            subscription = cls._subscriptions.pop(subscription_id, None)

        if subscription is None:
            raise KeyError(subscription_id)

        return subscription
