"""Content-addressed result cache.

Key: `cache:result:v1:{sha256(normalized content)}` -> JSON {"summary", "tags"}.

The key is a pure function of content, never of document_id, so PATCH cannot make an entry for
old content answer for new content: new content has a different hash and therefore a different key.
Entries are immutable (the mock pipeline is deterministic per content), so no invalidation is needed;
TTL only bounds memory. The `v1` segment lets us invalidate everything if the pipeline logic changes.
"""

import json
from dataclasses import dataclass

from redis import asyncio as aioredis

from app.core.config import Settings
from app.core.logging import get_logger
from app.db.redis import REDIS_ERRORS
from app.repositories.documents import DocumentRepository

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class CachedResult:
    summary: str
    tags: list[str]
    source: str  # "redis" | "mongo"


class ContentCache:
    KEY_PREFIX = "cache:result:v1:"

    def __init__(self, redis: aioredis.Redis, repo: DocumentRepository, settings: Settings) -> None:
        self._redis = redis
        self._repo = repo
        self._ttl = settings.cache_ttl_seconds

    def key(self, chash: str) -> str:
        return f"{self.KEY_PREFIX}{chash}"

    async def get(self, chash: str) -> CachedResult | None:
        try:
            raw = await self._redis.get(self.key(chash))
            if raw is not None:
                data = json.loads(raw)
                return CachedResult(summary=data["summary"], tags=list(data["tags"]), source="redis")
        except REDIS_ERRORS as exc:
            log.warning("cache_degraded", error=str(exc))
        except (ValueError, KeyError, TypeError) as exc:
            log.warning("cache_entry_corrupt", key=self.key(chash), error=str(exc))

        # Redis miss or outage: MongoDB already holds every result stamped with its content hash.
        result = await self._repo.find_result_by_hash(chash)
        if result is None:
            return None
        cached = CachedResult(summary=result["summary"], tags=list(result["tags"]), source="mongo")
        await self.set(chash, cached.summary, cached.tags)  # warm Redis for next time
        return cached

    async def set(self, chash: str, summary: str, tags: list[str]) -> None:
        try:
            await self._redis.set(
                self.key(chash), json.dumps({"summary": summary, "tags": tags}), ex=self._ttl
            )
        except REDIS_ERRORS as exc:
            log.warning("cache_write_failed", error=str(exc))
