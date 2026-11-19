"""Credential encryption settings."""

from __future__ import annotations

import base64
from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings

_AES_256_KEY_BYTES = 32


class CredentialEncryptionSettings(BaseSettings):
    """EP-owned data encryption key settings."""

    credential_encryption_key: str | None = Field(default=None, validation_alias="EP_CREDENTIAL_ENCRYPTION_KEY")
    credential_encryption_key_path: str | None = Field(
        default=None,
        validation_alias="EP_CREDENTIAL_ENCRYPTION_KEY_PATH",
    )

    @model_validator(mode="after")
    def load_credential_encryption_key(self) -> CredentialEncryptionSettings:
        """Load EP's data key from a secret file and validate its AES-256 size."""
        if self.credential_encryption_key is None and self.credential_encryption_key_path:
            self.credential_encryption_key = (
                Path(self.credential_encryption_key_path).read_text(encoding="ascii").strip()
            )
        if self.credential_encryption_key is None:
            msg = "EP_CREDENTIAL_ENCRYPTION_KEY or EP_CREDENTIAL_ENCRYPTION_KEY_PATH must be configured"
            raise ValueError(msg)
        decoded_key = base64.urlsafe_b64decode(self.credential_encryption_key)
        if len(decoded_key) != _AES_256_KEY_BYTES:
            msg = "The Execution Plane credential encryption key must decode to 32 bytes"
            raise ValueError(msg)
        return self
