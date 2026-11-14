"""In-process, multi-replica-safe pipeline runner.

Queueing is in-process (asyncio.Queue) for simplicity, but *correctness never depends on it*:
MongoDB is the source of truth. A job is only executed by whoever wins the atomic `claim` (lease),
and every subsequent write is fenced by (content_version, lease token). Lost queue entries (process
crash/restart) are found again by the recovery sweeper, which re-enqueues active documents whose
lease is absent or expired. Running several API replicas is therefore safe: they may race to claim
the same job, but exactly one wins.

Run lifecycle for one claim of document D at content_version v:

    claim lease ─▶ [stage1 output for v exists?] ─yes─▶ skip to enriching
                        │ no
                        ▼
                    processing (attempts with backoff) ─fail─▶ status=failed, failed_stage=processing
                        │ ok: stage1={v, summary}, status=enriching
                        ▼
                    enriching (attempts with backoff)  ─fail─▶ status=failed, failed_stage=enriching
                        │ ok: result={v, summary, tags}, status=completed     (stage1 kept: resumable)
                        ▼
                    release lease, release rate-limit slot, write content cache
"""

import asyncio
import os
import random
import socket
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from bson import ObjectId

from app.core.config import Settings
from app.core.logging import get_logger
from app.domain import DocumentStatus, Stage, StageState
from app.pipeline.stages import StageError, StageSimulator, extract_tags, summarize
from app.repositories.documents import DocumentRepository, build_result, utcnow
from app.services.content_cache import ContentCache
from app.services.rate_limiter import ActiveJobRateLimiter

log = get_logger(__name__)

Sleep = Callable[[float], Awaitable[None]]


class _Superseded(Exception):
    """Our fenced write matched nothing: content changed (PATCH) or the lease was taken over."""


class _StageFailed(Exception):
    """All attempts of a stage failed; the document has been marked failed."""


class PipelineRunner:
    def __init__(
        self,
        *,
        repo: DocumentRepository,
        rate_limiter: ActiveJobRateLimiter,
        cache: ContentCache,
        settings: Settings,
        simulator: StageSimulator,
        sleep: Sleep = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self._repo = repo
        self._limiter = rate_limiter
        self._cache = cache
        self._settings = settings
        self._simulate = simulator
        self._sleep = sleep
        self._rng = rng or random.Random()
        self.instance_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self._queue: asyncio.Queue[ObjectId] = asyncio.Queue()
        self._queued: set[ObjectId] = set()
        self._tasks: list[asyncio.Task[None]] = []
        self._stopping = False

    # ------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        self._stopping = False
        await self.recover_once()
        for i in range(self._settings.pipeline_worker_concurrency):
            self._tasks.append(asyncio.create_task(self._worker_loop(i), name=f"pipeline-worker-{i}"))
        self._tasks.append(asyncio.create_task(self._recovery_loop(), name="pipeline-recovery"))
        log.info(
            "pipeline_started", instance=self.instance_id, workers=self._settings.pipeline_worker_concurrency
        )

    async def stop(self, grace_seconds: float = 10.0) -> None:
        # The flag matters, not just cancel(): a cancellation that lands inside a driver call can be
        # surfaced as an ordinary exception (pymongo does this), which the per-job guard would
        # otherwise log and keep looping on.
        self._stopping = True
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            _, pending = await asyncio.wait(self._tasks, timeout=grace_seconds)
            if pending:
                log.warning("pipeline_stop_timeout", pending=[t.get_name() for t in pending])
        self._tasks.clear()
        # In-flight leases simply expire; another replica (or our next start) resumes the work.
        log.info("pipeline_stopped", instance=self.instance_id)

    def enqueue(self, doc_id: ObjectId) -> None:
        if doc_id in self._queued:
            return
        self._queued.add(doc_id)
        self._queue.put_nowait(doc_id)

    async def drain(self) -> None:
        """Wait until every enqueued job has been handled (used by tests and graceful tooling)."""
        await self._queue.join()

    # -------------------------------------------------------------- loops
    async def _worker_loop(self, index: int) -> None:
        while not self._stopping:
            doc_id = await self._queue.get()
            self._queued.discard(doc_id)
            try:
                await self.process(doc_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                if self._stopping:
                    log.info("pipeline_job_interrupted_by_shutdown", doc_id=str(doc_id))
                else:
                    log.exception("pipeline_job_crashed", doc_id=str(doc_id), worker=index)
            finally:
                self._queue.task_done()

    async def _recovery_loop(self) -> None:
        while not self._stopping:
            await asyncio.sleep(self._settings.recovery_interval_seconds)
            try:
                await self.recover_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("pipeline_recovery_failed")

    async def recover_once(self) -> int:
        orphans = await self._repo.find_orphans(utcnow())
        for doc_id in orphans:
            self.enqueue(doc_id)
        await self._limiter.reconcile()
        if orphans:
            log.info("pipeline_recovered_orphans", count=len(orphans))
        return len(orphans)

    # ----------------------------------------------------------- one job
    async def process(self, doc_id: ObjectId) -> None:
        token = uuid.uuid4().hex
        doc = await self._repo.claim(
            doc_id, token=token, owner=self.instance_id, lease_seconds=self._settings.lease_seconds
        )
        if doc is None:
            log.debug("pipeline_claim_skipped", doc_id=str(doc_id))
            return

        version = int(doc["content_version"])
        with structlog.contextvars.bound_contextvars(
            doc_id=str(doc_id), content_version=version, run=token[:8]
        ):
            log.info("pipeline_run_started", status=doc["status"])
            try:
                await self._run(doc, token, version)
            except _Superseded:
                # Not an error: a PATCH bumped the version (its own run will produce the result) or
                # our lease expired and someone else owns the job now. Our slot was not released.
                log.info("pipeline_run_superseded")
            except _StageFailed:
                log.info("pipeline_run_failed")

    async def _run(self, doc: dict[str, Any], token: str, version: int) -> None:
        doc_id: ObjectId = doc["_id"]
        stage1 = doc.get("stage1")
        if stage1 and stage1.get("content_version") == version:
            summary: str = stage1["summary"]
            log.info("pipeline_stage_resumed", skipped_stage=Stage.PROCESSING.value)
        else:
            summary = await self._run_stage(
                doc, token, version, Stage.PROCESSING, lambda: summarize(doc["content"])
            )
            await self._fenced(
                doc_id,
                version,
                token,
                {
                    "stage1": {
                        "content_version": version,
                        "content_hash": doc["content_hash"],
                        "summary": summary,
                    },
                    "status": DocumentStatus.ENRICHING.value,
                    "pipeline.stages.processing.state": StageState.SUCCEEDED.value,
                    "pipeline.stages.processing.finished_at": utcnow(),
                },
            )

        tags: list[str] = await self._run_stage(
            doc, token, version, Stage.ENRICHING, lambda: extract_tags(summary)
        )

        now = utcnow()
        # THE commit point: summary + tags + version stamp + status land in one atomic write.
        await self._fenced(
            doc_id,
            version,
            token,
            {
                "result": build_result(
                    version=version, chash=doc["content_hash"], summary=summary, tags=tags, now=now
                ),
                "status": DocumentStatus.COMPLETED.value,
                "pipeline.stages.enriching.state": StageState.SUCCEEDED.value,
                "pipeline.stages.enriching.finished_at": now,
            },
            release_lease=True,
        )
        log.info("pipeline_run_completed", tags=len(tags))
        await self._cache.set(doc["content_hash"], summary, tags)
        await self._limiter.release(doc["user_id"], str(doc_id), version)

    async def _run_stage(
        self, doc: dict[str, Any], token: str, version: int, stage: Stage, compute: Callable[[], Any]
    ) -> Any:
        doc_id: ObjectId = doc["_id"]
        prefix = f"pipeline.stages.{stage.value}"
        max_attempts = self._settings.stage_max_attempts
        for attempt in range(1, max_attempts + 1):
            start_fields: dict[str, Any] = {
                "status": stage.value,
                f"{prefix}.state": StageState.RUNNING.value,
            }
            if attempt == 1:
                start_fields[f"{prefix}.started_at"] = utcnow()
            # Starting each attempt is itself fenced: it renews the lease and detects a PATCH early.
            await self._fenced(doc_id, version, token, start_fields, inc={f"{prefix}.attempts": 1})
            try:
                await self._simulate(stage, str(doc_id), attempt)
                return compute()
            except StageError as exc:
                if attempt == max_attempts:
                    await self._fenced(
                        doc_id,
                        version,
                        token,
                        {
                            "status": DocumentStatus.FAILED.value,
                            "pipeline.failed_stage": stage.value,
                            f"{prefix}.state": StageState.FAILED.value,
                            f"{prefix}.finished_at": utcnow(),
                            f"{prefix}.last_error": str(exc),
                        },
                        release_lease=True,
                    )
                    log.warning("pipeline_stage_failed", stage=stage.value, attempts=attempt, error=str(exc))
                    await self._limiter.release(doc["user_id"], str(doc_id), version)
                    raise _StageFailed from exc
                delay = self._backoff(attempt)
                log.info(
                    "pipeline_stage_retrying", stage=stage.value, attempt=attempt, delay_s=round(delay, 2)
                )
                await self._fenced(doc_id, version, token, {f"{prefix}.last_error": str(exc)})
                await self._sleep(delay)
        raise AssertionError("unreachable")  # pragma: no cover

    def _backoff(self, attempt: int) -> float:
        """Exponential backoff with 'equal jitter': half fixed, half random. Avoids retry stampedes."""
        cap = min(
            self._settings.retry_backoff_max_seconds,
            self._settings.retry_backoff_base_seconds * 2 ** (attempt - 1),
        )
        return cap / 2 + self._rng.uniform(0, cap / 2)

    async def _fenced(
        self,
        doc_id: ObjectId,
        version: int,
        token: str,
        set_fields: dict[str, Any],
        *,
        inc: dict[str, Any] | None = None,
        release_lease: bool = False,
    ) -> None:
        ok = await self._repo.guarded_update(
            doc_id,
            version=version,
            token=token,
            set_fields=set_fields,
            inc_fields=inc,
            lease_seconds=None if release_lease else self._settings.lease_seconds,
            release_lease=release_lease,
        )
        if not ok:
            raise _Superseded
