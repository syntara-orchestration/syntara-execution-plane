"""Tests for encryption at the EP WorkItem persistence boundary."""

import base64

import pytest

from execution_plane.models.encrypted_payload import EncryptedWorkItemPayload


def test_payload_type_encrypts_then_decrypts_json_objects(monkeypatch: pytest.MonkeyPatch) -> None:
    key = base64.urlsafe_b64encode(b"x" * 32).decode("ascii")
    monkeypatch.setenv("EP_CREDENTIAL_ENCRYPTION_KEY", key)
    payload = {"invocation": {"inputs": {"secret": "must not persist in cleartext"}}}
    payload_type = EncryptedWorkItemPayload()

    stored = payload_type.process_bind_param(payload, None)

    assert stored is not None
    assert "must not persist in cleartext" not in str(stored)
    assert payload_type.process_result_value(stored, None) == payload


def test_payload_type_passes_legacy_plain_json_through() -> None:
    payload = {"legacy": "row"}

    assert EncryptedWorkItemPayload().process_result_value(payload, None) == payload


def test_payload_type_requires_key_for_new_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EP_CREDENTIAL_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("EP_CREDENTIAL_ENCRYPTION_KEY_PATH", raising=False)

    with pytest.raises(RuntimeError, match="requires EP_CREDENTIAL_ENCRYPTION_KEY"):
        EncryptedWorkItemPayload().process_bind_param({"work": "payload"}, None)
