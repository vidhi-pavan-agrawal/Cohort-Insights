"""Per-user active-job limit (Redis) and optimistic concurrency on PATCH."""

import asyncio

import pytest

from app.domain import Stage
from tests.conftest import hdr, submit, wait_status

pytestmark = pytest.mark.integration


async def _fill(client, simulator, user="alice", n=3):
    simulator.close_gate(Stage.PROCESSING)
    ids = []
    for i in range(n):
        r = await submit(client, user=user, content=f"Active document {user} {i}.")
        assert r.status_code == 201, r.text
        ids.append(r.json()["document_id"])
    return ids


async def test_fourth_active_document_is_rejected_with_429(client, container, simulator):
    await _fill(client, simulator)
    r = await submit(client, content="One too many.")
    assert r.status_code == 429
    assert r.json()["error"]["code"] == "rate_limit_exceeded"
    assert "Retry-After" in r.headers
    assert (await submit(client, user="bob", content="Bob is unaffected.")).status_code == 201


async def test_slot_frees_when_pipeline_finishes_including_failures(client, container, simulator):
    ids = await _fill(client, simulator)
    simulator.fail_always.add(Stage.ENRICHING)  # some finish as failed: still frees the slot
    simulator.open_gate(Stage.PROCESSING)
    await container.runner.drain()
    for doc_id in ids:
        await wait_status(client, doc_id, "failed")
    assert await container.rate_limiter.active_count("alice") == 0
    assert (await submit(client, content="Now allowed.")).status_code == 201


async def test_limit_counts_all_active_stages(client, container, simulator):
    simulator.close_gate(Stage.ENRICHING)
    for i in range(3):
        assert (await submit(client, content=f"Enriching {i}.")).status_code == 201
    # all three reach 'enriching' and stay there
    for _ in range(200):
        page = (
            await client.get("/users/alice/documents", params={"status": "enriching"}, headers=hdr("alice"))
        ).json()
        if page["total"] == 3:
            break
        await asyncio.sleep(0.01)
    assert page["total"] == 3
    assert (await submit(client, content="Blocked while enriching.")).status_code == 429


async def test_concurrent_submits_admit_exactly_the_limit(client, simulator):
    simulator.close_gate(Stage.PROCESSING)
    results = await asyncio.gather(*(submit(client, content=f"Burst {i}.") for i in range(12)))
    codes = [r.status_code for r in results]
    assert codes.count(201) == 3 and codes.count(429) == 9


async def test_cache_hit_does_not_consume_a_slot(client, container, simulator):
    first = (await submit(client, content="Popular content.")).json()["document_id"]
    await container.runner.drain()
    await wait_status(client, first, "completed")
    await _fill(client, simulator)
    r = await submit(client, content="Popular content.")
    assert r.status_code == 201 and r.json()["status"] == "completed"


async def test_patch_of_terminal_doc_needs_a_slot_but_active_doc_does_not(client, container, simulator):
    done = (await submit(client, content="Finished doc.")).json()["document_id"]
    await container.runner.drain()
    await wait_status(client, done, "completed")
    active = await _fill(client, simulator)

    r = await client.patch(f"/documents/{done}", json={"content": "Reactivate me."}, headers=hdr("alice"))
    assert r.status_code == 429
    r = await client.patch(
        f"/documents/{active[0]}", json={"content": "Edit while active."}, headers=hdr("alice")
    )
    assert r.status_code == 200
    assert await container.rate_limiter.active_count("alice") == 3


async def test_concurrent_patches_with_same_expected_version_one_wins(client, container):
    doc_id = (await submit(client, content="Base.")).json()["document_id"]
    await container.runner.drain()
    results = await asyncio.gather(
        *(
            client.patch(
                f"/documents/{doc_id}",
                json={"content": f"Writer {i}.", "expected_version": 1},
                headers=hdr("alice"),
            )
            for i in range(5)
        )
    )
    codes = sorted(r.status_code for r in results)
    assert codes == [200, 409, 409, 409, 409]
    winner = next(r.json() for r in results if r.status_code == 200)
    assert winner["content_version"] == 2
    loser = next(r.json() for r in results if r.status_code == 409)
    assert loser["error"]["code"] == "version_conflict"


async def test_concurrent_patches_without_expected_version_all_apply_sequentially(client, container):
    doc_id = (await submit(client, content="Base.")).json()["document_id"]
    await container.runner.drain()
    results = await asyncio.gather(
        *(
            client.patch(f"/documents/{doc_id}", json={"content": f"LWW {i}."}, headers=hdr("alice"))
            for i in range(3)
        )
    )
    assert all(r.status_code == 200 for r in results)
    versions = sorted(r.json()["content_version"] for r in results)
    assert versions == [2, 3, 4], "each PATCH must get its own version (no lost update)"
