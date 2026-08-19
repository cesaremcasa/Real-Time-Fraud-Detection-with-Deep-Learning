from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.e2e_redpanda import _signed_body


def test_e2e_fixture_signature_headers_are_deterministic_shape():
    body, transaction_id, headers_json = _signed_body()
    headers = json.loads(headers_json)
    payload = json.loads(body)
    assert payload["transaction_id"] == transaction_id
    assert set(headers) == {
        "Content-Type",
        "X-Producer-Id",
        "X-Request-Timestamp",
        "X-Request-Nonce",
        "X-Request-Signature",
    }
    assert len(headers["X-Request-Signature"]) == 64
