"""Composition root: builds every long-lived object once and wires dependencies explicitly."""

from dataclasses import dataclass

from redis import asyncio as aioredis

from app.core.config import Settings
from app.db.mongo import Mongo
from app.db.redis import create_redis
from app.pipeline.runner import PipelineRunner
from app.pipeline.stages import RandomStageSimulator, StageSimulator
from app.repositories.documents import DocumentRepository
from app.services.content_cache import ContentCache
from app.services.documents import DocumentService
from app.services.rate_limiter import ActiveJobRateLimiter


@dataclass
class Container:
    settings: Settings
    mongo: Mongo
    redis: aioredis.Redis
    repo: DocumentRepository
    rate_limiter: ActiveJobRateLimiter
    cache: ContentCache
    runner: PipelineRunner
    documents: DocumentService

    @classmethod
    def build(cls, settings: Settings, simulator: StageSimulator | None = None) -> "Container":
        mongo = Mongo(settings)
        redis = create_redis(settings)
        repo = DocumentRepository(mongo)
        rate_limiter = ActiveJobRateLimiter(redis, repo, settings)
        cache = ContentCache(redis, repo, settings)
        runner = PipelineRunner(
            repo=repo,
            rate_limiter=rate_limiter,
            cache=cache,
            settings=settings,
            simulator=simulator or RandomStageSimulator(settings),
        )
        documents = DocumentService(
            repo=repo, rate_limiter=rate_limiter, cache=cache, runner=runner, settings=settings
        )
        return cls(settings, mongo, redis, repo, rate_limiter, cache, runner, documents)

    async def close(self) -> None:
        await self.redis.aclose()
        await self.mongo.close()
