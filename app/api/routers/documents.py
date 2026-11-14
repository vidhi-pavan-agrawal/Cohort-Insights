"""Document endpoints. Thin: validate -> call service -> map to response model."""

from typing import Annotated

from fastapi import APIRouter, Path, Query, Response, status

from app.api.deps import CurrentUser, Documents
from app.core.errors import ForbiddenError
from app.domain import DocumentStatus
from app.schemas.documents import (
    REF_PATTERN,
    USER_ID_PATTERN,
    DocumentCreate,
    DocumentDetailOut,
    DocumentPage,
    DocumentPatch,
    SubmissionOutcome,
    SubmitOut,
    to_document_out,
)

router = APIRouter(tags=["documents"])

_ERRORS = {
    401: {"description": "Missing/malformed X-User-Id"},
    404: {"description": "Not found (or not owned by the caller — indistinguishable by design)"},
}


@router.post(
    "/documents",
    response_model=SubmitOut,
    status_code=status.HTTP_201_CREATED,
    responses={
        200: {"model": SubmitOut, "description": "Existing client_doc_ref: duplicate replay or new version"},
        403: {"description": "Body user_id differs from the authenticated caller"},
        409: {"description": "client_doc_ref owned by another user, or stale ref_version"},
        429: {"description": "Too many active documents for this user"},
        **_ERRORS,
    },
)
async def submit_document(
    body: DocumentCreate, user: CurrentUser, service: Documents, response: Response
) -> SubmitOut:
    if body.user_id != user:
        raise ForbiddenError("user_id in body must match the authenticated caller")
    result = await service.submit(
        user_id=body.user_id,
        title=body.title,
        content=body.content,
        client_doc_ref=body.client_doc_ref,
        ref_version=body.ref_version,
    )
    if result.outcome is not SubmissionOutcome.CREATED:
        response.status_code = status.HTTP_200_OK
    return to_document_out(result.document, SubmitOut, submission=result.outcome)


# Declared before /documents/{document_id} so "by-ref" is never parsed as a document id.
@router.get("/documents/by-ref/{client_doc_ref:path}", response_model=DocumentDetailOut, responses=_ERRORS)
async def get_document_by_ref(
    client_doc_ref: Annotated[str, Path(pattern=REF_PATTERN)], user: CurrentUser, service: Documents
) -> DocumentDetailOut:
    return to_document_out(await service.get_by_ref(client_doc_ref, user), DocumentDetailOut)


@router.get("/documents/{document_id}", response_model=DocumentDetailOut, responses=_ERRORS)
async def get_document(document_id: str, user: CurrentUser, service: Documents) -> DocumentDetailOut:
    return to_document_out(await service.get(document_id, user), DocumentDetailOut)


@router.patch(
    "/documents/{document_id}",
    response_model=DocumentDetailOut,
    responses={
        409: {"description": "expected_version mismatch"},
        429: {"description": "Rate limited"},
        **_ERRORS,
    },
)
async def patch_document(
    document_id: str, body: DocumentPatch, user: CurrentUser, service: Documents
) -> DocumentDetailOut:
    doc = await service.patch(document_id, user, content=body.content, expected_version=body.expected_version)
    return to_document_out(doc, DocumentDetailOut)


@router.post(
    "/documents/{document_id}/retry",
    response_model=DocumentDetailOut,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        409: {"description": "Document is not in failed state"},
        429: {"description": "Rate limited"},
        **_ERRORS,
    },
)
async def retry_document(document_id: str, user: CurrentUser, service: Documents) -> DocumentDetailOut:
    return to_document_out(await service.retry(document_id, user), DocumentDetailOut)


@router.get(
    "/users/{user_id}/documents",
    response_model=DocumentPage,
    responses={403: {"description": "Path user_id differs from the authenticated caller"}, **_ERRORS},
)
async def list_user_documents(
    user_id: Annotated[str, Path(pattern=USER_ID_PATTERN)],
    user: CurrentUser,
    service: Documents,
    page: Annotated[int, Query(ge=1, le=10_000)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    status_filter: Annotated[DocumentStatus | None, Query(alias="status")] = None,
) -> DocumentPage:
    if user_id != user:
        raise ForbiddenError("You can only list your own documents")
    items, total = await service.list_for_user(user_id, status=status_filter, page=page, page_size=page_size)
    return DocumentPage(
        items=[to_document_out(d) for d in items],
        page=page,
        page_size=page_size,
        total=total,
        has_next=page * page_size < total,
    )
