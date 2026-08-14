import hashlib
import hmac
import unittest

from src.api.security import ProducerAuthenticationError, ProducerAuthenticator


def signed_request(*, secret: str, nonce: str, timestamp: str = "1000", body: bytes = b'{"amount":10}') -> str:
    canonical = "\n".join(
        (
            "POST",
            "/api/v1/transaction",
            "taxi-feed",
            timestamp,
            nonce,
            hashlib.sha256(body).hexdigest(),
        )
    )
    return hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).hexdigest()


class ProducerAuthenticatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.secret = "test-secret"
        self.authenticator = ProducerAuthenticator({"taxi-feed": self.secret})
        self.body = b'{"amount":10}'

    def test_accepts_a_valid_fresh_signature(self) -> None:
        nonce = "nonce-which-is-long-enough"
        producer = self.authenticator.verify(
            producer_id="taxi-feed",
            timestamp="1000",
            nonce=nonce,
            signature=signed_request(secret=self.secret, nonce=nonce),
            method="POST",
            path="/api/v1/transaction",
            body=self.body,
            now=1000,
        )
        self.assertEqual("taxi-feed", producer)

    def test_rejects_a_tampered_body_and_replayed_nonce(self) -> None:
        nonce = "nonce-which-is-long-enough"
        signature = signed_request(secret=self.secret, nonce=nonce)
        with self.assertRaises(ProducerAuthenticationError):
            self.authenticator.verify(
                producer_id="taxi-feed", timestamp="1000", nonce=nonce, signature=signature,
                method="POST", path="/api/v1/transaction", body=b'{"amount":11}', now=1000,
            )
        self.authenticator.verify(
            producer_id="taxi-feed", timestamp="1000", nonce=nonce, signature=signature,
            method="POST", path="/api/v1/transaction", body=self.body, now=1000,
        )
        with self.assertRaisesRegex(ProducerAuthenticationError, "already used"):
            self.authenticator.verify(
                producer_id="taxi-feed", timestamp="1000", nonce=nonce, signature=signature,
                method="POST", path="/api/v1/transaction", body=self.body, now=1000,
            )

    def test_rejects_expired_or_unknown_producers(self) -> None:
        nonce = "nonce-which-is-long-enough"
        with self.assertRaisesRegex(ProducerAuthenticationError, "outside"):
            self.authenticator.verify(
                producer_id="taxi-feed", timestamp="1", nonce=nonce,
                signature=signed_request(secret=self.secret, nonce=nonce, timestamp="1"),
                method="POST", path="/api/v1/transaction", body=self.body, now=1000,
            )
        with self.assertRaisesRegex(ProducerAuthenticationError, "not recognized"):
            self.authenticator.verify(
                producer_id="unknown", timestamp="1000", nonce=nonce,
                signature=signed_request(secret=self.secret, nonce=nonce),
                method="POST", path="/api/v1/transaction", body=self.body, now=1000,
            )
