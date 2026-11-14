"""Per-user active-job limiter.

Redis layout: one HASH per user, `rl:active:{user_id}` -> {document_id: content_version}.

Why a hash of members instead of an INCR/DECR counter:
* Idempotent: acquiring for a document that is already active (e.g. PATCH mid-run) does not
  double count; releasing twice does not under count. Counters drift on every retry/crash.
* Versioned release: the value is the content_version that holds the slot. A worker that finishes
  an *old* version can only release if the slot still belongs to that version (compare-and-delete),
  so a PATCH racing a completion can never free the slot of the new run.
* Self-describing: reconciliation against MongoDB (the source of truth) can compare members.

All mutations are optimistic WATCH/MULTI/EXEC transactions (pure redis-py, no server-side scripts):
the per-user key is watched and the transaction is retried if it changes before EXEC, so
check-and-set is atomic under concurrent submits.
If Redis is unavailable we fall back to an indexed MongoDB count (see `acquire`).
"""

from collections import defaultdict
from dataclasses import dataclass

from redis import asyncio as aioredis
from redis.asyncio.client import Pipeline

from app.core.config import Settings
from app.core.logging import get_logger
from app.db.redis import REDIS_ERRORS
from app.repositories.documents import DocumentRepository

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class SlotReservation:
    allowed: bool
    user_id: str
    doc_id: str
    version: int
    newly_added: bool = False
    previous_version: int | None = None
    degraded: bool = False  # True -> decided by the MongoDB fallback, nothing to roll back in Redis


class ActiveJobRateLimiter:
    KEY_PREFIX = "rl:active:"

    def __init__(self, redis: aioredis.Redis, repo: DocumentRepository, settings: Settings) -> None:
        self._redis = redis
        self._repo = repo
        self._max = settings.rate_limit_max_active
        self._ttl = settings.rate_limit_key_ttl_seconds

    async def _txn(self, key: str, fn):
        # `fn(pipe)` reads through the watched pipe, then calls pipe.multi() and queues its writes.
        # If `key` changes before EXEC, redis-py retries the whole function.
        return await self._redis.transaction(fn, key, value_from_callable=True)

    async def _acquire(self, key: str, doc_id: str, version: int) -> list[int]:
        """Returns [1] newly added | [2, previous_version] already active | [0] rejected."""

        async def fn(pipe: Pipeline) -> list[int]:
            cur = await pipe.hget(key, doc_id)
            if cur is None and await pipe.hlen(key) >= self._max:
                return [0]
            pipe.multi()
            pipe.hset(key, doc_id, version)
            pipe.expire(key, self._ttl)
            return [1] if cur is None else [2, int(cur)]

        return await self._txn(key, fn)

    async def _release(self, key: str, doc_id: str, max_version: int) -> int:
        """Compare-and-delete: only the version holding the slot (or an older one) may free it."""

        async def fn(pipe: Pipeline) -> int:
            cur = await pipe.hget(key, doc_id)
            if cur is None or int(cur) > max_version:
                return 0
            pipe.multi()
            pipe.hdel(key, doc_id)
            return 1

        return await self._txn(key, fn)

    async def _restore(self, key: str, doc_id: str, expected: int, previous: int) -> int:
        """Compare-and-swap back to the previous holder (rolls back a failed PATCH)."""

        async def fn(pipe: Pipeline) -> int:
            cur = await pipe.hget(key, doc_id)
            if cur is None or int(cur) != expected:
                return 0
            pipe.multi()
            pipe.hset(key, doc_id, previous)
            return 1

        return await self._txn(key, fn)

    async def _ensure(self, key: str, doc_id: str, version: int) -> None:
        """Track an active doc at (at least) its current version. Never lowers a version."""

        async def fn(pipe: Pipeline) -> None:
            cur = await pipe.hget(key, doc_id)
            pipe.multi()
            if cur is None or int(cur) < version:
                pipe.hset(key, doc_id, version)
            pipe.expire(key, self._ttl)

        await self._txn(key, fn)

    @property
    def max_active(self) -> int:
        return self._max

    def key(self, user_id: str) -> str:
        return f"{self.KEY_PREFIX}{user_id}"

    async def acquire(self, user_id: str, doc_id: str, version: int) -> SlotReservation:
        try:
            res = await self._acquire(self.key(user_id), doc_id, version)
        except REDIS_ERRORS as exc:
            # Degraded mode: MongoDB is the source of truth for "active", so an indexed count on
            # (user_id, status) gives the right answer, minus atomicity (documented trade-off).
            log.warning("rate_limiter_degraded", error=str(exc), user_id=user_id)
            active = await self._repo.count_active(user_id)
            return SlotReservation(
                allowed=active < self._max, user_id=user_id, doc_id=doc_id, version=version, degraded=True
            )
        code = int(res[0])
        if code == 0:
            return SlotReservation(allowed=False, user_id=user_id, doc_id=doc_id, version=version)
        return SlotReservation(
            allowed=True,
            user_id=user_id,
            doc_id=doc_id,
            version=version,
            newly_added=code == 1,
            previous_version=int(res[1]) if code == 2 else None,
        )

    async def release(self, user_id: str, doc_id: str, version: int) -> None:
        """Free the slot if it is held by `version` or older. Safe to call repeatedly."""
        try:
            await self._release(self.key(user_id), doc_id, version)
        except REDIS_ERRORS as exc:
            # Not fatal: the periodic reconciliation removes slots of terminal documents.
            log.warning("rate_limiter_release_failed", error=str(exc), user_id=user_id, doc_id=doc_id)

    async def rollback(self, reservation: SlotReservation) -> None:
        """Undo an `acquire` whose MongoDB write did not happen."""
        if not reservation.allowed or reservation.degraded:
            return
        key = self.key(reservation.user_id)
        try:
            if reservation.newly_added:
                await self._release(key, reservation.doc_id, reservation.version)
            elif reservation.previous_version is not None:
                await self._restore(
                    key, reservation.doc_id, reservation.version, reservation.previous_version
                )
        except REDIS_ERRORS as exc:
            log.warning("rate_limiter_rollback_failed", error=str(exc), doc_id=reservation.doc_id)

    async def active_count(self, user_id: str) -> int:
        try:
            return int(await self._redis.hlen(self.key(user_id)))
        except REDIS_ERRORS:
            return await self._repo.count_active(user_id)

    async def reconcile(self) -> None:
        """Heal drift between Redis and MongoDB (crash between a DB write and a Redis call, Redis
        restart without persistence, writes made while degraded).

        Only *safe* corrections are applied, so this can run concurrently with live traffic:
        * active-in-Mongo but missing/older in Redis  -> add (never lowers a version)
        * present in Redis but Mongo says terminal at a version >= the slot's -> compare-and-delete
        Members whose document does not exist yet are left alone: that is a submit in flight
        (acquire happens before insert); the key TTL is the backstop for truly leaked entries.
        """
        try:
            active = await self._repo.list_active()
            by_user: dict[str, dict[str, int]] = defaultdict(dict)
            for doc in active:
                by_user[doc["user_id"]][str(doc["_id"])] = int(doc["content_version"])
            for user_id, docs in by_user.items():
                for doc_id, version in docs.items():
                    await self._ensure(self.key(user_id), doc_id, version)

            removed = 0
            async for key in self._redis.scan_iter(match=f"{self.KEY_PREFIX}*", count=500):
                user_id = key[len(self.KEY_PREFIX) :]
                members = await self._redis.hgetall(key)
                stale = {d: int(v) for d, v in members.items() if d not in by_user.get(user_id, {})}
                for doc_id, slot_version in stale.items():
                    doc = await self._repo.terminal_version(doc_id)
                    if doc is not None and doc >= slot_version:
                        removed += await self._release(key, doc_id, slot_version)
            if removed:
                log.info("rate_limiter_reconciled", removed=removed)
        except REDIS_ERRORS as exc:
            log.warning("rate_limiter_reconcile_skipped", error=str(exc))
