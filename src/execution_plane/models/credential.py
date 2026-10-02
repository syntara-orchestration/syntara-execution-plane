"""EP-owned encryption for management credentials persisted in the EP database."""

from __future__ import annotations

import base64
import secrets

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sqlalchemy import String
from sqlalchemy.types import TypeDecorator

from execution_plane.config import get_ep_settings

_PREFIX = "epv1:"
_ASSOCIATED_DATA = b"syntara-execution-plane-credential-v1"


class EncryptedCredential(TypeDecorator[str]):
    """Encrypt credential strings at the SQLAlchemy persistence boundary."""

    impl = String
    cache_ok = True

    def process_bind_param(self, value: str | None, _dialect: object) -> str | None:
        """Encrypt each value with an EP-only AES-GCM key before it reaches PostgreSQL."""
        if value is None:
            return None
        key = base64.urlsafe_b64decode(get_ep_settings().credential_encryption_key or "")
        nonce = secrets.token_bytes(12)
        ciphertext = AESGCM(key).encrypt(nonce, value.encode("utf-8"), _ASSOCIATED_DATA)
        return _PREFIX + base64.urlsafe_b64encode(nonce + ciphertext).decode("ascii")

    def process_result_value(self, value: str | None, _dialect: object) -> str | None:
        """Decrypt an EP credential after reading it from PostgreSQL."""
        if value is None:
            return None
        if not value.startswith(_PREFIX):
            msg = "An unencrypted credential is present in the Execution Plane database"
            raise ValueError(msg)
        encrypted = base64.urlsafe_b64decode(value.removeprefix(_PREFIX))
        key = base64.urlsafe_b64decode(get_ep_settings().credential_encryption_key or "")
        return AESGCM(key).decrypt(encrypted[:12], encrypted[12:], _ASSOCIATED_DATA).decode("utf-8")
