"""Authentication for trusted EP service clients."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from execution_plane.config import EPSettings, get_ep_settings

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

_bearer = HTTPBearer(auto_error=False)


class ServiceIdentity(BaseModel):
    """Verified calling service identity from an AO-signed token."""

    client_id: str
    scopes: frozenset[str]


@lru_cache(maxsize=8)
def _load_public_key(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


async def get_service_identity(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    settings: Annotated[EPSettings, Depends(get_ep_settings)],
) -> ServiceIdentity:
    """Validate signature, issuer, audience, client, and signed project grant."""
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Service bearer token required")
    if not settings.ao_jwt_public_key_path or not settings.ao_jwt_issuer:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service authentication is not configured",
        )
    try:
        claims = jwt.decode(
            credentials.credentials,
            _load_public_key(settings.ao_jwt_public_key_path),
            algorithms=["ES256"],
            audience=settings.service_jwt_audience,
            issuer=settings.ao_jwt_issuer,
            options={"require": ["exp", "iat", "iss", "aud", "client_id"]},
        )
        client_id = str(claims["client_id"])
        scopes = frozenset(str(claims.get("scope", "")).split())
    except (jwt.InvalidTokenError, KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid service token") from exc
    if client_id != settings.ao_client_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Service client is not enabled")
    return ServiceIdentity(client_id=client_id, scopes=scopes)


def require_scope(scope: str) -> Callable[..., Awaitable[ServiceIdentity]]:
    """Build a dependency that checks the signed service permission set."""

    async def check_scope(identity: Annotated[ServiceIdentity, Depends(get_service_identity)]) -> ServiceIdentity:
        if scope not in identity.scopes:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Service token lacks the required scope")
        return identity

    return check_scope
