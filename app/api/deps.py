"""FastAPI dependencies: identity and service access."""

import re
from typing import Annotated

from fastapi import Depends, Header, Request

from app.container import Container
from app.core.errors import UnauthenticatedError
from app.schemas.documents import USER_ID_PATTERN
from app.services.documents import DocumentService

_USER_ID_RE = re.compile(USER_ID_PATTERN)


def get_container(request: Request) -> Container:
    return request.app.state.container


def get_document_service(container: Annotated[Container, Depends(get_container)]) -> DocumentService:
    return container.documents


def get_current_user(
    x_user_id: Annotated[
        str | None,
        Header(
            alias="X-User-Id",
            description="Caller identity. Stands in for an authenticated principal (e.g. a JWT `sub`) "
            "that an upstream gateway would normally verify.",
        ),
    ] = None,
) -> str:
    if not x_user_id or not _USER_ID_RE.fullmatch(x_user_id):
        raise UnauthenticatedError("Missing or malformed X-User-Id header")
    return x_user_id


CurrentUser = Annotated[str, Depends(get_current_user)]
Documents = Annotated[DocumentService, Depends(get_document_service)]
