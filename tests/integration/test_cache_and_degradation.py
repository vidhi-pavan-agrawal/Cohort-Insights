"""Content-hash cache and graceful degradation when Redis is down."""

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from app.domain import Stage, content_hash
from app.main import create_app
from tests.conftest import ScriptedSimulator, hdr, make_settings, submit, wait_status

pytestmark = pytest.mark.integration


async def test_identical_content_is_served_from_cache(client, container, simulator):
    first = (await submit(client, content="Cache me. Please.")).json()["document_id"]
    await container.runner.drain()
    original = await wait_status(client, first, "completed")
    calls = len(simulator.calls)

    r = await submit(client, user="bob", content="Cache me. Please.")
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "completed" and body["pipeline"]["cache_hit"] is True
    assert body["result"]["summary"] == original["result"]["summary"]
    assert body["result"]["tags"] == original["result"]["tags"]
    assert body["result_is_current"] is True
    assert len(simulator.calls) == calls


async def test_cache_is_keyed_by_content_not_document(client, container):
    a = (await submit(client, content="Content X. About apples.")).json()["document_id"]
    await container.runner.drain()
    await wait_status(client, a, "completed")
    assert await container.redis.exists(container.cache.key(content_hash("Content X. About apples.")))
    assert not [k async for k in container.redis.scan_iter(match=f"*{a}*")], (
        "no cache key may contain a doc id"
    )

    b = (await submit(client, content="Content Y. About bananas.")).json()["document_id"]
    await container.runner.drain()
    await wait_status(client, b, "completed")

    # PATCH b to X's content: must get X's result immediately, stamped with b's NEW version.
    r = await client.patch(
        f"/documents/{b}", json={"content": "Content X. About apples."}, headers=hdr("alice")
    )
    patched = r.json()
    assert patched["status"] == "completed" and patched["content_version"] == 2
    assert patched["result"]["content_version"] == 2 and patched["result_is_current"] is True
    assert "apples" in patched["result"]["tags"]

    # PATCH b to brand-new content: the old (X) result must not be presented as current.
    r = await client.patch(
        f"/documents/{b}", json={"content": "Content Z. About cherries."}, headers=hdr("alice")
    )
    fresh = r.json()
    assert fresh["status"] == "queued" and fresh["result_is_current"] is False
    await container.runner.drain()
    done = await wait_status(client, b, "completed")
    assert "cherries" in done["result"]["tags"]


async def test_cache_falls_back_to_mongo_when_redis_entry_missing(client, container, simulator):
    first = (await submit(client, content="Evicted entry.")).json()["document_id"]
    await container.runner.drain()
    await wait_status(client, first, "completed")
    await container.redis.flushdb()  # TTL eviction / Redis restart without persistence
    r = await submit(client, user="bob", content="Evicted entry.")
    assert r.json()["status"] == "completed"
    assert await container.redis.exists(container.cache.key(content_hash("Evicted entry.")))  # re-warmed


async def test_service_degrades_gracefully_without_redis():
    settings = make_settings(redis_url="redis://127.0.0.1:1/0", redis_socket_timeout_seconds=0.2)
    simulator = ScriptedSimulator()
    app = create_app(settings, simulator=simulator)
    async with LifespanManager(app, startup_timeout=15, shutdown_timeout=15):
        try:
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
                health = await client.get("/health")
                assert health.status_code == 200 and health.json()["status"] == "degraded"
                assert health.json()["checks"]["redis"]["status"] == "error"

                simulator.close_gate(Stage.PROCESSING)
                for i in range(3):
                    assert (await submit(client, content=f"No redis {i}.")).status_code == 201
                # Limit still enforced, via the indexed MongoDB count.
                assert (await submit(client, content="No redis 4.")).status_code == 429

                simulator.open_gate(Stage.PROCESSING)
                await app.state.container.runner.drain()
                page = (await client.get("/users/alice/documents", headers=hdr("alice"))).json()
                assert {d["status"] for d in page["items"]} == {"completed"}
        finally:
            simulator.open_gate(Stage.PROCESSING)
            await app.state.container.mongo.client.drop_database(settings.mongo_db)
