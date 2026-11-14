"""Liveness and readiness probes."""

import asyncio
from typing import Annotated, Any

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from pymongo.errors import PyMongoError

from app.api.deps import get_container
from app.container import Container
from app.db.redis import REDIS_ERRORS
from app.schemas.documents import HealthOut

router = APIRouter(tags=["health"])

_CHECK_TIMEOUT_S = 2.0


@router.get("/health/live", response_model=HealthOut)
async def live() -> HealthOut:
    """Process is up. Does not touch dependencies (so orchestrators don't restart us for a DB blip)."""
    return HealthOut(status="ok", checks={})


@router.get("/health", response_model=HealthOut, responses={503: {"model": HealthOut}})
async def health(container: Annotated[Container, Depends(get_container)]) -> Any:
    """Readiness: MongoDB is required (503 if down); Redis is optional (we degrade, report 'degraded')."""
    checks: dict[str, Any] = {}
    try:
        await asyncio.wait_for(container.mongo.ping(), _CHECK_TIMEOUT_S)
        checks["mongodb"] = {"status": "ok"}
    except (PyMongoError, TimeoutError) as exc:
        checks["mongodb"] = {"status": "error", "error": type(exc).__name__}
    try:
        await asyncio.wait_for(container.redis.ping(), _CHECK_TIMEOUT_S)
        checks["redis"] = {"status": "ok"}
    except REDIS_ERRORS as exc:
        checks["redis"] = {"status": "error", "error": type(exc).__name__}

    if checks["mongodb"]["status"] != "ok":
        body = HealthOut(status="unavailable", checks=checks)
        return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content=body.model_dump())
    overall = "ok" if checks["redis"]["status"] == "ok" else "degraded"
    return HealthOut(status=overall, checks=checks)
