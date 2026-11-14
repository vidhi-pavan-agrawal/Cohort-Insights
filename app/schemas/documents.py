"""Pydantic request/response models (the HTTP contract). Mongo documents never leak out directly."""

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator, model_validator

from app.domain import DocumentStatus, Stage, StageState

USER_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.@\-]{0,63}$"
REF_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.:/\-]{0,199}$"

UserId = Annotated[str, StringConstraints(pattern=USER_ID_PATTERN)]
ClientDocRef = Annotated[str, StringConstraints(pattern=REF_PATTERN)]
Title = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=300)]
Content = Annotated[str, StringConstraints(min_length=1, max_length=100_000)]


def _reject_blank(value: str) -> str:
    if not value.strip():
        raise ValueError("must contain non-whitespace characters")
    if "\x00" in value:
        raise ValueError("must not contain NUL characters")
    return value


# ---------------------------------------------------------------- requests
class DocumentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: UserId
    title: Title
    content: Content
    client_doc_ref: ClientDocRef | None = Field(
        default=None, description="Partner CMS identifier. Optional; globally unique when present."
    )
    ref_version: int | None = Field(
        default=None,
        ge=0,
        description="Partner-side monotonically increasing version of `client_doc_ref`. Lets the API reject "
        "out-of-order/stale resubmissions with 409. Requires client_doc_ref.",
    )

    _content_not_blank = field_validator("content")(_reject_blank)

    @model_validator(mode="after")
    def _ref_version_requires_ref(self) -> "DocumentCreate":
        if self.ref_version is not None and self.client_doc_ref is None:
            raise ValueError("ref_version requires client_doc_ref")
        return self


class DocumentPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    content: Content
    expected_version: int | None = Field(
        default=None,
        ge=1,
        description="Optimistic concurrency: the content_version you last read. 409 if it changed since.",
    )

    _content_not_blank = field_validator("content")(_reject_blank)


# --------------------------------------------------------------- responses
class StageOut(BaseModel):
    state: StageState
    attempts: int
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_error: str | None = None


class PipelineOut(BaseModel):
    version: int = Field(description="The content_version this pipeline run is computing.")
    failed_stage: Stage | None = None
    cache_hit: bool = False
    stages: dict[Stage, StageOut]


class ResultOut(BaseModel):
    summary: str
    tags: list[str]
    content_version: int = Field(description="content_version this summary+tags were computed from.")
    content_hash: str
    completed_at: datetime
    is_current: bool = Field(
        description="True iff content_version == the document's current content_version."
    )


class DocumentOut(BaseModel):
    document_id: str
    user_id: str
    title: str
    client_doc_ref: str | None = None
    ref_version: int | None = None
    status: DocumentStatus
    content_version: int
    content_hash: str
    result_is_current: bool = Field(
        description="True only when `result` was produced from the current content. While a new version is "
        "being processed the previous result is still returned, but with is_current=false."
    )
    result: ResultOut | None = None
    pipeline: PipelineOut
    created_at: datetime
    updated_at: datetime


class DocumentDetailOut(DocumentOut):
    content: str


class SubmissionOutcome(StrEnum):
    CREATED = "created"  # new document (201)
    DUPLICATE = "duplicate"  # same client_doc_ref + same content: idempotent replay (200)
    NEW_VERSION = "new_version"  # same client_doc_ref + different content: content replaced (200)


class SubmitOut(DocumentDetailOut):
    submission: SubmissionOutcome


class DocumentPage(BaseModel):
    items: list[DocumentOut]
    page: int
    page_size: int
    total: int
    has_next: bool


class HealthOut(BaseModel):
    status: str
    checks: dict[str, Any]


# ------------------------------------------------------------- mapping
def to_document_out(doc: dict[str, Any], model: type[DocumentOut] = DocumentOut, **extra: Any) -> Any:
    version = int(doc["content_version"])
    raw_result = doc.get("result")
    result = None
    if raw_result:
        result = ResultOut(
            **{
                k: raw_result[k]
                for k in ("summary", "tags", "content_version", "content_hash", "completed_at")
            },
            is_current=int(raw_result["content_version"]) == version,
        )
    pipeline = doc["pipeline"]
    fields: dict[str, Any] = {
        "document_id": str(doc["_id"]),
        "user_id": doc["user_id"],
        "title": doc["title"],
        "client_doc_ref": doc.get("client_doc_ref"),
        "ref_version": doc.get("ref_version"),
        "status": doc["status"],
        "content_version": version,
        "content_hash": doc["content_hash"],
        "result_is_current": bool(result and result.is_current),
        "result": result,
        "pipeline": PipelineOut(
            version=pipeline["version"],
            failed_stage=pipeline.get("failed_stage"),
            cache_hit=pipeline.get("cache_hit", False),
            stages=pipeline["stages"],
        ),
        "created_at": doc["created_at"],
        "updated_at": doc["updated_at"],
        **extra,
    }
    if issubclass(model, DocumentDetailOut):
        fields["content"] = doc["content"]
    return model(**fields)
