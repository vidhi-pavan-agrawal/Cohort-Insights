"""Document use-cases. Framework-agnostic: raises domain errors, returns raw Mongo documents."""

from dataclasses import dataclass
from typing import Any

from bson import ObjectId
from pymongo.errors import DuplicateKeyError

from app.core.config import Settings
from app.core.errors import (
    ConflictError,
    InvalidStateError,
    NotFoundError,
    RateLimitExceededError,
    RefConflictError,
    VersionConflictError,
)
from app.core.logging import get_logger
from app.domain import ACTIVE_STATUSES, DocumentStatus, Stage, content_hash
from app.pipeline.runner import PipelineRunner
from app.repositories.documents import (
    Doc,
    DocumentRepository,
    build_result,
    cached_pipeline,
    new_pipeline,
    parse_object_id,
    utcnow,
)
from app.schemas.documents import SubmissionOutcome
from app.services.content_cache import ContentCache
from app.services.rate_limiter import ActiveJobRateLimiter, SlotReservation

log = get_logger(__name__)

# How many times we re-read and retry when a compare-and-set loses a race without the caller
# having asked for strict optimistic concurrency.
_CAS_RETRIES = 3


class _CasMiss(Exception):
    """The guarded write matched nothing because the document changed after we read it."""


@dataclass(frozen=True, slots=True)
class SubmitResult:
    document: Doc
    outcome: SubmissionOutcome


class DocumentService:
    def __init__(
        self,
        *,
        repo: DocumentRepository,
        rate_limiter: ActiveJobRateLimiter,
        cache: ContentCache,
        runner: PipelineRunner,
        settings: Settings,
    ) -> None:
        self._repo = repo
        self._limiter = rate_limiter
        self._cache = cache
        self._runner = runner
        self._settings = settings

    # ------------------------------------------------------------------ reads
    async def get(self, document_id: str, user_id: str) -> Doc:
        oid = parse_object_id(document_id)
        doc = await self._repo.get_owned(oid, user_id) if oid else None
        if doc is None:
            raise NotFoundError("Document not found")
        return doc

    async def get_by_ref(self, client_doc_ref: str, user_id: str) -> Doc:
        doc = await self._repo.get_by_ref(client_doc_ref)
        if doc is None or doc["user_id"] != user_id:
            raise NotFoundError("Document not found")
        return doc

    async def list_for_user(
        self, user_id: str, *, status: DocumentStatus | None, page: int, page_size: int
    ) -> tuple[list[Doc], int]:
        return await self._repo.list_for_user(
            user_id, status=status, skip=(page - 1) * page_size, limit=page_size
        )

    # ----------------------------------------------------------------- submit
    async def submit(
        self,
        *,
        user_id: str,
        title: str,
        content: str,
        client_doc_ref: str | None,
        ref_version: int | None,
    ) -> SubmitResult:
        chash = content_hash(content)
        if client_doc_ref is None:
            doc = await self._create(user_id, title, content, chash, None, None)
            return SubmitResult(doc, SubmissionOutcome.CREATED)

        # Crosswalk path. Loop because two partner retries may race on the same ref: one insert wins
        # the unique index, the other gets DuplicateKeyError / a CAS miss and re-resolves.
        for _ in range(_CAS_RETRIES):
            existing = await self._repo.get_by_ref(client_doc_ref)
            try:
                if existing is None:
                    doc = await self._create(user_id, title, content, chash, client_doc_ref, ref_version)
                    return SubmitResult(doc, SubmissionOutcome.CREATED)
                return await self._resubmit_ref(existing, user_id, title, content, chash, ref_version)
            except (DuplicateKeyError, _CasMiss):
                log.info("client_doc_ref_race_retry", client_doc_ref=client_doc_ref)
        raise ConflictError("client_doc_ref is being modified concurrently; retry the request")

    async def _resubmit_ref(
        self,
        existing: Doc,
        user_id: str,
        title: str,
        content: str,
        chash: str,
        ref_version: int | None,
    ) -> SubmitResult:
        if existing["user_id"] != user_id:
            # Refs are a global partner namespace; one ref can map to exactly one document.
            raise RefConflictError("client_doc_ref is already associated with another document")

        stored_rv: int | None = existing.get("ref_version")

        if existing["content_hash"] == chash:
            # Idempotent replay (partner retry). Never reprocess; just remember the highest ref_version.
            if ref_version is not None and (stored_rv is None or ref_version > stored_rv):
                existing = await self._repo.bump_ref_version(existing["_id"], ref_version) or existing
            return SubmitResult(existing, SubmissionOutcome.DUPLICATE)

        if ref_version is not None and stored_rv is not None and ref_version <= stored_rv:
            raise VersionConflictError(
                "Stale client_doc_ref submission: a newer or equal ref_version with different content "
                "has already been accepted",
                details={"current_ref_version": stored_rv, "received_ref_version": ref_version},
            )

        doc = await self._apply_new_content(
            existing,
            content=content,
            chash=chash,
            title=title,
            ref_version=ref_version,
            # Fence on the ref_version we compared against, so an out-of-order pair racing in
            # parallel cannot both pass the check above.
            extra_filter={"ref_version": stored_rv},
        )
        return SubmitResult(doc, SubmissionOutcome.NEW_VERSION)

    async def _create(
        self,
        user_id: str,
        title: str,
        content: str,
        chash: str,
        client_doc_ref: str | None,
        ref_version: int | None,
    ) -> Doc:
        now = utcnow()
        doc_id = ObjectId()
        doc: Doc = {
            "_id": doc_id,
            "user_id": user_id,
            "title": title,
            "content": content,
            "content_hash": chash,
            "content_version": 1,
            "stage1": None,
            "result": None,
            "created_at": now,
            "updated_at": now,
        }
        if client_doc_ref is not None:
            doc["client_doc_ref"] = client_doc_ref
            doc["ref_version"] = ref_version

        cached = await self._cache.get(chash)
        if cached is not None:
            # Served from cache: never enters an active state, so it does not consume a rate-limit slot.
            doc.update(
                status=DocumentStatus.COMPLETED.value,
                pipeline=cached_pipeline(1, now),
                result=build_result(
                    version=1, chash=chash, summary=cached.summary, tags=cached.tags, now=now
                ),
            )
            await self._repo.insert(doc)
            log.info("document_created_from_cache", doc_id=str(doc_id), cache_source=cached.source)
            return doc

        reservation = await self._reserve(user_id, str(doc_id), 1)
        doc.update(status=DocumentStatus.QUEUED.value, pipeline=new_pipeline(1))
        try:
            await self._repo.insert(doc)
        except Exception:
            await self._limiter.rollback(reservation)
            raise
        self._runner.enqueue(doc_id)
        log.info("document_created", doc_id=str(doc_id))
        return doc

    # ------------------------------------------------------------------ patch
    async def patch(
        self, document_id: str, user_id: str, *, content: str, expected_version: int | None
    ) -> Doc:
        oid = parse_object_id(document_id)
        if oid is None:
            raise NotFoundError("Document not found")
        chash = content_hash(content)

        for _ in range(_CAS_RETRIES):
            existing = await self._repo.get_owned(oid, user_id)
            if existing is None:
                raise NotFoundError("Document not found")
            current = int(existing["content_version"])
            if expected_version is not None and current != expected_version:
                raise VersionConflictError(
                    "Document was modified since you read it",
                    details={"current_version": current, "expected_version": expected_version},
                )
            if existing["content_hash"] == chash:
                return existing  # identical content: no new version, nothing to reprocess
            try:
                return await self._apply_new_content(existing, content=content, chash=chash)
            except _CasMiss:
                if expected_version is not None:
                    raise VersionConflictError(
                        "Document was modified concurrently", details={"expected_version": expected_version}
                    ) from None
                log.info("patch_cas_retry", doc_id=document_id)
        raise VersionConflictError("Document is being modified concurrently; retry the request")

    async def _apply_new_content(
        self,
        existing: Doc,
        *,
        content: str,
        chash: str,
        title: str | None = None,
        ref_version: int | None = None,
        extra_filter: dict[str, Any] | None = None,
    ) -> Doc:
        """Replace content as a new content_version, in ONE compare-and-set write.

        The same write (a) swaps content+hash, (b) bumps content_version, (c) resets status and
        pipeline bookkeeping, (d) drops the lease (fencing out any in-flight worker) and (e) clears
        stage-1 partial output. The previous `result` is intentionally kept: it is stamped with the
        old content_version, so readers see it as non-current and can never mistake it for current.
        """
        doc_id: ObjectId = existing["_id"]
        user_id: str = existing["user_id"]
        old_version = int(existing["content_version"])
        new_version = old_version + 1
        now = utcnow()

        fields: Doc = {"content": content, "content_hash": chash, "stage1": None}
        if title is not None:
            fields["title"] = title
        if ref_version is not None:
            fields["ref_version"] = ref_version

        cached = await self._cache.get(chash)
        if cached is not None:
            fields.update(
                status=DocumentStatus.COMPLETED.value,
                pipeline=cached_pipeline(new_version, now),
                result=build_result(
                    version=new_version, chash=chash, summary=cached.summary, tags=cached.tags, now=now
                ),
            )
            doc = await self._repo.replace_content(
                doc_id,
                user_id=user_id,
                expected_version=old_version,
                set_fields=fields,
                extra_filter=extra_filter,
            )
            if doc is None:
                raise _CasMiss
            if existing["status"] in ACTIVE_STATUSES:
                await self._limiter.release(user_id, str(doc_id), old_version)
            log.info("document_new_version_from_cache", doc_id=str(doc_id), content_version=new_version)
            return doc

        reservation = await self._reserve(user_id, str(doc_id), new_version)
        fields.update(status=DocumentStatus.QUEUED.value, pipeline=new_pipeline(new_version))
        try:
            doc = await self._repo.replace_content(
                doc_id,
                user_id=user_id,
                expected_version=old_version,
                set_fields=fields,
                extra_filter=extra_filter,
            )
        except Exception:
            await self._limiter.rollback(reservation)
            raise
        if doc is None:
            await self._limiter.rollback(reservation)
            raise _CasMiss
        self._runner.enqueue(doc_id)
        log.info("document_new_version", doc_id=str(doc_id), content_version=new_version)
        return doc

    # ------------------------------------------------------------------ retry
    async def retry(self, document_id: str, user_id: str) -> Doc:
        """Re-run a failed document from the stage that failed (stage-1 output is reused)."""
        existing = await self.get(document_id, user_id)
        if existing["status"] != DocumentStatus.FAILED.value:
            raise InvalidStateError(
                "Only failed documents can be retried", details={"status": existing["status"]}
            )
        version = int(existing["content_version"])
        stage = Stage(existing["pipeline"]["failed_stage"])
        reservation = await self._reserve(user_id, str(existing["_id"]), version)
        try:
            doc = await self._repo.prepare_retry(
                existing["_id"], user_id=user_id, version=version, stage=stage
            )
        except Exception:
            await self._limiter.rollback(reservation)
            raise
        if doc is None:
            await self._limiter.rollback(reservation)
            raise VersionConflictError("Document changed while scheduling the retry; re-read and try again")
        self._runner.enqueue(existing["_id"])
        log.info("document_retry_scheduled", doc_id=document_id, from_stage=stage.value)
        return doc

    # ---------------------------------------------------------------- helpers
    async def _reserve(self, user_id: str, doc_id: str, version: int) -> SlotReservation:
        reservation = await self._limiter.acquire(user_id, doc_id, version)
        if not reservation.allowed:
            raise RateLimitExceededError(
                f"At most {self._limiter.max_active} documents may be queued/processing/enriching at once",
                details={"max_active": self._limiter.max_active},
            )
        return reservation
