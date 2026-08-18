"""Small deterministic fakes used by PR1 characterization tests only."""

from __future__ import annotations

import json
from typing import Any


class FakeKafkaProducer:
    """Record producer calls without opening a broker connection."""

    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    def produce(self, *, topic: str, value: str | bytes, key: str | None = None) -> bool:
        self.messages.append({"topic": topic, "value": value, "key": key})
        return True


class FakeKafkaMessage:
    """Minimal confluent-kafka Message surface used by ``process_message``."""

    def __init__(self, payload: dict[str, Any]) -> None:
        self._value = json.dumps(payload, sort_keys=True).encode("utf-8")

    def value(self) -> bytes:
        return self._value


class FakeScaler:
    def transform(self, values: Any) -> Any:
        return values

    def inverse_transform(self, values: Any) -> Any:
        return values
