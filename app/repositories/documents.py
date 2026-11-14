"""Data access for the `documents` collection.

Every write that the pipeline performs is *version-guarded*: the filter pins both the
`content_version` the run was started for and the run's lease token. If a PATCH bumped the version
(or another worker took over the lease) the filter matches nothing and the write is a no-op.
That single-document compare-and-set is what makes stale writes impossible rather than unlikely.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId
from pymongo import DESCENDING, ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection

from app.db.mongo import DOCUMENTS, Mongo
from app.domain import ACTIVE_STATUSES, STAGE_ORDER, DocumentStatus, Stage, StageState

Doc = dict[str, Any]

_ACTIVE = [s.value for s in ACTIVE_STATUSES]


def utcnow() -> datetime:
    return datetime.now(UTC)


def parse_object_id(value: str) -> ObjectId | None:
    """Malformed ids are treated exactly like unknown ids (-> 404), never as a 422 that leaks shape."""
    try:
        return ObjectId(value)
    except (InvalidId, TypeError):
        return None


def ref_filter(client_doc_ref: str) -> Doc:
    """Filter for a by-ref lookup.

    The `$type` clause is not redundant: `client_doc_ref_unique` is a *partial* index, and MongoDB's
    planner only uses a partial index when the query provably implies its partialFilterExpression.
    A bare equality does not, and silently falls back to a COLLSCAN (asserted in the tests).
    """
    return {"client_doc_ref": {"$eq": client_doc_ref, "$type": "string"}}


def new_stage_state() -> Doc:
    return {
        "state": StageState.PENDING.value,
        "attempts": 0,
        "started_at": None,
        "finished_at": None,
        "last_error": None,
    }


def new_pipeline(version: int) -> Doc:
    """Fresh pipeline bookkeeping bound to one content_version."""
    return {
        "version": version,
        "failed_stage": None,
        "cache_hit": False,
        "stages": {stage.value: new_stage_state() for stage in STAGE_ORDER},
        "lease": None,
    }


def cached_pipeline(version: int, now: datetime) -> Doc:
    """Pipeline bookkeeping for a document satisfied straight from the content cache."""
    pipeline = new_pipeline(version)
    pipeline["cache_hit"] = True
    for stage in STAGE_ORDER:
        pipeline["stages"][stage.value].update(
            state=StageState.SUCCEEDED.value, started_at=now, finished_at=now
        )
    return pipeline


def build_result(*, version: int, chash: str, summary: str, tags: list[str], now: datetime) -> Doc:
    """The ONLY place reader-visible output lives. One sub-document, one version stamp."""
    return {
        "content_version": version,
        "content_hash": chash,
        "summary": summary,
        "tags": tags,
        "completed_at": now,
    }


class DocumentRepository:
    def __init__(self, mongo: Mongo) -> None:
        self.col: AsyncCollection = mongo.db[DOCUMENTS]

    # ------------------------------------------------------------------ reads
    async def get(self, doc_id: ObjectId) -> Doc | None:
        return await self.col.find_one({"_id": doc_id})

    async def get_owned(self, doc_id: ObjectId, user_id: str) -> Doc | None:
        # Ownership is part of the query itself, so "not yours" and "doesn't exist" are the same miss.
        return await self.col.find_one({"_id": doc_id, "user_id": user_id})

    async def get_by_ref(self, client_doc_ref: str) -> Doc | None:
        return await self.col.find_one(ref_filter(client_doc_ref))

    async def list_for_user(
        self, user_id: str, *, status: DocumentStatus | None, skip: int, limit: int
    ) -> tuple[list[Doc], int]:
        query: Doc = {"user_id": user_id}
        if status is not None:
            query["status"] = status.value
        cursor = (
            self.col.find(query, projection={"content": 0})
            .sort([("created_at", DESCENDING), ("_id", DESCENDING)])
            .skip(skip)
            .limit(limit)
        )
        items = await cursor.to_list(length=limit)
        total = await self.col.count_documents(query)
        return items, total

    async def count_active(self, user_id: str) -> int:
        return await self.col.count_documents({"user_id": user_id, "status": {"$in": _ACTIVE}})

    async def find_result_by_hash(self, chash: str) -> Doc | None:
        """Any stored result computed from content with this hash (Redis-down cache fallback).

        Results are a deterministic function of the content hash, so any match is equally valid.
        The `$type` clause lets the planner use the partial `result_content_hash` index.
        """
        doc = await self.col.find_one(
            {"result": {"$type": "object"}, "result.content_hash": chash}, projection={"result": 1}
        )
        return doc["result"] if doc else None

    async def list_active(self) -> list[Doc]:
        cursor = self.col.find({"status": {"$in": _ACTIVE}}, projection={"user_id": 1, "content_version": 1})
        return await cursor.to_list(length=None)

    async def terminal_version(self, doc_id: str) -> int | None:
        """content_version of a document if it is in a terminal state, else None."""
        oid = parse_object_id(doc_id)
        if oid is None:
            return None
        doc = await self.col.find_one(
            {"_id": oid, "status": {"$nin": _ACTIVE}}, projection={"content_version": 1}
        )
        return int(doc["content_version"]) if doc else None

    async def find_orphans(self, now: datetime, limit: int = 500) -> list[ObjectId]:
        """Active docs nobody currently holds a live lease on (crashed worker, lost in-memory queue)."""
        cursor = self.col.find(
            {
                "status": {"$in": _ACTIVE},
                "$or": [{"pipeline.lease": None}, {"pipeline.lease.expires_at": {"$lte": now}}],
            },
            projection={"_id": 1},
        ).limit(limit)
        return [d["_id"] async for d in cursor]

    # ----------------------------------------------------------------- writes
    async def insert(self, doc: Doc) -> None:
        await self.col.insert_one(doc)

    async def replace_content(
        self,
        doc_id: ObjectId,
        *,
        user_id: str,
        expected_version: int,
        set_fields: Doc,
        extra_filter: Doc | None = None,
    ) -> Doc | None:
        """Atomically swap content and bump content_version (compare-and-set on the old version).

        The same update resets pipeline bookkeeping and drops the lease, which instantly fences out
        any worker still running against the previous version.
        """
        query: Doc = {"_id": doc_id, "user_id": user_id, "content_version": expected_version}
        if extra_filter:
            query.update(extra_filter)
        return await self.col.find_one_and_update(
            query,
            {"$set": {**set_fields, "updated_at": utcnow()}, "$inc": {"content_version": 1}},
            return_document=ReturnDocument.AFTER,
        )

    async def bump_ref_version(self, doc_id: ObjectId, ref_version: int) -> Doc | None:
        return await self.col.find_one_and_update(
            {"_id": doc_id},
            {"$max": {"ref_version": ref_version}},
            return_document=ReturnDocument.AFTER,
        )

    async def prepare_retry(
        self, doc_id: ObjectId, *, user_id: str, version: int, stage: Stage
    ) -> Doc | None:
        """failed -> queued, resetting only the failed stage. Stage-1 output (if any) is kept."""
        return await self.col.find_one_and_update(
            {
                "_id": doc_id,
                "user_id": user_id,
                "content_version": version,
                "status": DocumentStatus.FAILED.value,
            },
            {
                "$set": {
                    "status": DocumentStatus.QUEUED.value,
                    "pipeline.failed_stage": None,
                    "pipeline.lease": None,
                    f"pipeline.stages.{stage.value}": new_stage_state(),
                    "updated_at": utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )

    # ------------------------------------------------------- pipeline writes
    async def claim(self, doc_id: ObjectId, *, token: str, owner: str, lease_seconds: float) -> Doc | None:
        """Atomically take the lease on an active doc. At most one live lease can exist per doc."""
        now = utcnow()
        return await self.col.find_one_and_update(
            {
                "_id": doc_id,
                "status": {"$in": _ACTIVE},
                "$or": [{"pipeline.lease": None}, {"pipeline.lease.expires_at": {"$lte": now}}],
            },
            {
                "$set": {
                    "pipeline.lease": {
                        "token": token,
                        "owner": owner,
                        "expires_at": now + timedelta(seconds=lease_seconds),
                    }
                }
            },
            return_document=ReturnDocument.AFTER,
        )

    async def guarded_update(
        self,
        doc_id: ObjectId,
        *,
        version: int,
        token: str,
        set_fields: Doc,
        inc_fields: Doc | None = None,
        lease_seconds: float | None = None,
        release_lease: bool = False,
    ) -> bool:
        """Compare-and-set on (content_version, lease token). Returns False if the run was superseded."""
        now = utcnow()
        set_doc: Doc = {**set_fields, "updated_at": now}
        if release_lease:
            set_doc["pipeline.lease"] = None
        elif lease_seconds is not None:
            set_doc["pipeline.lease.expires_at"] = now + timedelta(seconds=lease_seconds)
        update: Doc = {"$set": set_doc}
        if inc_fields:
            update["$inc"] = inc_fields
        res = await self.col.update_one(
            {"_id": doc_id, "content_version": version, "pipeline.lease.token": token}, update
        )
        return res.matched_count == 1
