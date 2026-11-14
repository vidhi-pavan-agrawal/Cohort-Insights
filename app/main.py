"""Application factory + lifespan (startup/shutdown of clients and pipeline workers)."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routers import documents, health
from app.container import Container
from app.core.config import Settings, get_settings
from app.core.errors import register_exception_handlers
from app.core.logging import configure_logging, get_logger
from app.core.middleware import RequestContextMiddleware
from app.pipeline.stages import StageSimulator

log = get_logger(__name__)


def create_app(settings: Settings | None = None, *, simulator: StageSimulator | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level, settings.log_json)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        container = Container.build(settings, simulator)
        app.state.container = container
        await container.mongo.ensure_indexes()
        if settings.pipeline_enabled:
            await container.runner.start()
        log.info("app_started", environment=settings.environment)
        try:
            yield
        finally:
            await container.runner.stop()
            await container.close()
            log.info("app_stopped")

    app = FastAPI(
        title="Cohort Insights API",
        version="1.0.0",
        description="Ingests documents, runs a two-stage simulated pipeline, and serves versioned results.",
        lifespan=lifespan,
    )
    app.add_middleware(RequestContextMiddleware)
    register_exception_handlers(app)
    app.include_router(health.router)
    app.include_router(documents.router)
    return app


app = create_app()
