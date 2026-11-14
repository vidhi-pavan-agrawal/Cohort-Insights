"""MongoDB client lifecycle and index management."""

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient, IndexModel
from pymongo.asynchronous.database import AsyncDatabase

from app.core.config import Settings
from app.core.logging import get_logger
from app.domain import ACTIVE_STATUSES

log = get_logger(__name__)

DOCUMENTS = "documents"

# Every index is named so it can be referenced in docs/explain plans and so changes are explicit.
DOCUMENT_INDEXES: list[IndexModel] = [
    # GET /users/{id}/documents (newest first, stable tiebreak on _id for equal timestamps).
    IndexModel(
        [("user_id", ASCENDING), ("created_at", DESCENDING), ("_id", DESCENDING)],
        name="user_created_desc",
    ),
    # GET /users/{id}/documents?status=... and the Mongo fallback for rate limiting.
    IndexModel(
        [("user_id", ASCENDING), ("status", ASCENDING), ("created_at", DESCENDING), ("_id", DESCENDING)],
        name="user_status_created_desc",
    ),
    # GET /documents/by-ref/{ref}: point lookup. Unique only when present (ref is optional).
    IndexModel(
        [("client_doc_ref", ASCENDING)],
        name="client_doc_ref_unique",
        unique=True,
        partialFilterExpression={"client_doc_ref": {"$type": "string"}},
    ),
    # Content cache fallback when Redis is unavailable: find any completed result for a hash.
    IndexModel(
        [("result.content_hash", ASCENDING)],
        name="result_content_hash",
        partialFilterExpression={"result": {"$type": "object"}},
    ),
    # Recovery sweeper: only active docs are indexed, so this stays small regardless of history.
    IndexModel(
        [("status", ASCENDING), ("pipeline.lease.expires_at", ASCENDING)],
        name="active_lease",
        partialFilterExpression={"status": {"$in": [s.value for s in ACTIVE_STATUSES]}},
    ),
]


class Mongo:
    def __init__(self, settings: Settings) -> None:
        self.client: AsyncMongoClient = AsyncMongoClient(
            settings.mongo_uri,
            serverSelectionTimeoutMS=settings.mongo_server_selection_timeout_ms,
            tz_aware=True,
            appname=settings.app_name,
        )
        self.db: AsyncDatabase = self.client[settings.mongo_db]

    async def ensure_indexes(self) -> None:
        await self.db[DOCUMENTS].create_indexes(DOCUMENT_INDEXES)
        log.info("mongo_indexes_ensured", count=len(DOCUMENT_INDEXES))

    async def ping(self) -> bool:
        await self.client.admin.command("ping")
        return True

    async def close(self) -> None:
        await self.client.close()
