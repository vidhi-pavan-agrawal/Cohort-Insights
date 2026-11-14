"""Shared fixtures.

Integration tests run against real MongoDB + Redis (docker compose provides them). Each test gets
its own Mongo database and a flushed Redis DB, plus a controllable StageSimulator so timing and
failures are deterministic instead of random sleeps.
"""

import asyncio
import os
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from redis import asyncio as aioredis

from app.container import Container
from app.core.config import Settings
from app.domain import Stage
from app.main import create_app
from app.pipeline.stages import StageError

TEST_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/15")


class ScriptedSimulator:
    """Deterministic stand-in for RandomStageSimulator.

    * gates[stage]: an Event the stage waits on (closed -> the stage blocks until the test opens it)
    * entered[stage]: set whenever any attempt of that stage starts
    * fail_always / fail_times: scripted failures per stage
    """

    def __init__(self) -> None:
        self.gates: dict[Stage, asyncio.Event] = {}
        self.entered: dict[Stage, asyncio.Event] = defaultdict(asyncio.Event)
        self.calls: list[tuple[Stage, str, int]] = []
        self.fail_always: set[Stage] = set()
        self.fail_times: dict[Stage, int] = {}

    def close_gate(self, stage: Stage) -> None:
        self.gates[stage] = asyncio.Event()

    def open_gate(self, stage: Stage) -> None:
        if stage in self.gates:
            self.gates[stage].set()

    def count(self, stage: Stage, doc_id: str | None = None) -> int:
        return sum(1 for s, d, _ in self.calls if s == stage and (doc_id is None or d == doc_id))

    async def __call__(self, stage: Stage, doc_id: str, attempt: int) -> None:
        self.calls.append((stage, doc_id, attempt))
        self.entered[stage].set()
        gate = self.gates.get(stage)
        if gate is not None:
            await gate.wait()
        if stage in self.fail_always:
            raise StageError(f"scripted {stage.value} failure")
        if self.fail_times.get(stage, 0) > 0:
            self.fail_times[stage] -= 1
            raise StageError(f"scripted transient {stage.value} failure")


def make_settings(**overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "environment": "test",
        "log_level": "WARNING",
        "log_json": False,
        "mongo_db": f"test_{uuid.uuid4().hex[:12]}",
        "redis_url": TEST_REDIS_URL,
        "processing_min_seconds": 0,
        "processing_max_seconds": 0,
        "enriching_min_seconds": 0,
        "enriching_max_seconds": 0,
        "stage_failure_rate": 0,
        "stage_max_attempts": 3,
        "retry_backoff_base_seconds": 0,
        "retry_backoff_max_seconds": 0,
        "lease_seconds": 5,
        "recovery_interval_seconds": 3600,  # tests trigger recovery explicitly
        "pipeline_worker_concurrency": 4,
    }
    base.update(overrides)
    return Settings(**base)


@pytest.fixture
def simulator() -> ScriptedSimulator:
    return ScriptedSimulator()


@pytest.fixture
def settings() -> Settings:
    return make_settings()


@pytest.fixture
async def app(settings: Settings, simulator: ScriptedSimulator) -> AsyncIterator[FastAPI]:
    redis = aioredis.from_url(settings.redis_url)
    try:
        await redis.flushdb()
    finally:
        await redis.aclose()

    application = create_app(settings, simulator=simulator)
    async with LifespanManager(application, startup_timeout=15, shutdown_timeout=15):
        yield application
        # Unblock anything still waiting so shutdown is clean.
        for stage in Stage:
            simulator.open_gate(stage)
        container: Container = application.state.container
        await container.mongo.client.drop_database(settings.mongo_db)


@pytest.fixture
def container(app: FastAPI) -> Container:
    return app.state.container


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


# ------------------------------------------------------------------ helpers
def hdr(user: str) -> dict[str, str]:
    return {"X-User-Id": user}


async def submit(
    client: AsyncClient, user: str = "alice", content: str = "Hello world. Second line.", **extra: Any
):
    body = {"user_id": user, "title": "A title", "content": content, **extra}
    return await client.post("/documents", json=body, headers=hdr(user))


async def get_doc(client: AsyncClient, doc_id: str, user: str = "alice") -> dict[str, Any]:
    r = await client.get(f"/documents/{doc_id}", headers=hdr(user))
    assert r.status_code == 200, r.text
    return r.json()


async def wait_until(
    predicate: Callable[[], Awaitable[bool]], within: float = 5.0, interval: float = 0.01
) -> None:
    deadline = asyncio.get_running_loop().time() + within
    while True:
        if await predicate():
            return
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met within timeout")
        await asyncio.sleep(interval)


async def wait_status(
    client: AsyncClient, doc_id: str, status: str, user: str = "alice", within: float = 5.0
):
    snapshot: dict[str, Any] = {}

    async def _check() -> bool:
        snapshot.update(await get_doc(client, doc_id, user))
        return snapshot["status"] == status

    await wait_until(_check, within)
    return snapshot


def assert_consistent(doc: dict[str, Any]) -> None:
    """The reader-facing staleness invariant. Must hold for EVERY observable snapshot."""
    result = doc["result"]
    if result is None:
        assert doc["result_is_current"] is False
        assert doc["status"] != "completed"
        return
    is_current = result["content_version"] == doc["content_version"]
    assert result["is_current"] is is_current
    assert doc["result_is_current"] is is_current
    if is_current:
        assert result["content_hash"] == doc["content_hash"]
    if doc["status"] == "completed":
        assert is_current, "completed must always mean the result reflects the current content"
