"""Authentication helpers for external transaction producers."""

from __future__ import annotations

import hashlib
import hmac
import time
from threading import Lock
from typing import Mapping


class ProducerAuthenticationError(ValueError):
    """Raised when a producer request does not meet the HMAC contract."""


class ProducerAuthenticator:
    """Verify signed requests and retain authenticated nonces for a short TTL."""

    def __init__(self, secrets: Mapping[str, str], *, max_age_seconds: int = 300) -> None:
        self._secrets = {producer_id: secret for producer_id, secret in secrets.items() if secret}
        self._max_age_seconds = max_age_seconds
        self._seen_nonces: dict[tuple[str, str], int] = {}
        self._lock = Lock()

    def verify(
        self,
        *,
        producer_id: str | None,
        timestamp: str | None,
        nonce: str | None,
        signature: str | None,
        method: str,
        path: str,
        body: bytes,
        now: int | None = None,
    ) -> str:
        if not producer_id or not timestamp or not nonce or not signature:
            raise ProducerAuthenticationError("producer authentication headers are required")
        if len(nonce) < 16 or len(nonce) > 256:
            raise ProducerAuthenticationError("nonce is invalid")

        try:
            request_time = int(timestamp)
        except ValueError as exc:
            raise ProducerAuthenticationError("timestamp must be unix seconds") from exc

        current_time = int(time.time()) if now is None else now
        if abs(current_time - request_time) > self._max_age_seconds:
            raise ProducerAuthenticationError("request timestamp is outside the allowed window")

        secret = self._secrets.get(producer_id)
        if secret is None:
            raise ProducerAuthenticationError("producer is not recognized")

        body_digest = hashlib.sha256(body).hexdigest()
        canonical = "\n".join((method.upper(), path, producer_id, timestamp, nonce, body_digest))
        expected = hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature.removeprefix("sha256=")):
            raise ProducerAuthenticationError("request signature is invalid")

        nonce_key = (producer_id, nonce)
        with self._lock:
            self._purge_expired_nonces(current_time)
            if nonce_key in self._seen_nonces:
                raise ProducerAuthenticationError("request nonce was already used")
            self._seen_nonces[nonce_key] = request_time + self._max_age_seconds
        return producer_id

    def _purge_expired_nonces(self, now: int) -> None:
        expired = [key for key, expires_at in self._seen_nonces.items() if expires_at < now]
        for key in expired:
            self._seen_nonces.pop(key, None)
