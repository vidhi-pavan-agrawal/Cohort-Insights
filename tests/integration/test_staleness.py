"""Staleness on update: a reader can never observe a summary/tags/content version mismatch."""

import asyncio
import random

import pytest
from bson import ObjectId

from app.domain import Stage, content_hash
from app.pipeline.stages import extract_tags, summarize
from tests.conftest import assert_consistent, get_doc, hdr, submit, wait_status, wait_until

pytestmark = pytest.mark.integration

V1 = "Alpha particles decay slowly. Alpha sources need shielding."
V2 = "Kubernetes schedules containers. Kubernetes restarts failed pods."


async def test_patch_mid_processing_never_exposes_old_run(client, container, simulator):
    simulator.close_gate(Stage.PROCESSING)
    doc_id = (await submit(client, content=V1)).json()["document_id"]
    await asyncio.wait_for(simulator.entered[Stage.PROCESSING].wait(), 5)  # v1 run is mid-stage-1

    r = await client.patch(f"/documents/{doc_id}", json={"content": V2}, headers=hdr("alice"))
    assert r.status_code == 200
    patched = r.json()
    assert patched["content_version"] == 2 and patched["status"] == "queued"
    assert patched["pipeline"]["version"] == 2
    assert_consistent(patched)

    simulator.open_gate(Stage.PROCESSING)  # v1 run and v2 run both resume
    await container.runner.drain()
    doc = await wait_status(client, doc_id, "completed")
    assert_consistent(doc)
    assert doc["result"]["content_version"] == 2
    assert doc["result"]["summary"] == summarize(V2)
    assert doc["result"]["tags"] == extract_tags(summarize(V2))

    raw = await container.repo.get(ObjectId(doc_id))
    assert raw["stage1"]["content_version"] == 2  # v1's stage-1 write was fenced out


async def test_patch_mid_enriching_cannot_mix_old_summary_with_new_tags(client, container, simulator):
    simulator.close_gate(Stage.ENRICHING)
    doc_id = (await submit(client, content=V1)).json()["document_id"]
    await asyncio.wait_for(simulator.entered[Stage.ENRICHING].wait(), 5)
    raw = await container.repo.get(ObjectId(doc_id))
    assert raw["stage1"]["summary"] == summarize(V1)  # v1 summary exists, tags not yet

    await client.patch(f"/documents/{doc_id}", json={"content": V2}, headers=hdr("alice"))
    raw = await container.repo.get(ObjectId(doc_id))
    # PATCH discards v1's stage-1 output; any stage-1 present now must belong to v2.
    assert raw["stage1"] is None or raw["stage1"]["content_version"] == 2

    simulator.open_gate(Stage.ENRICHING)
    await container.runner.drain()
    doc = await wait_status(client, doc_id, "completed")
    assert_consistent(doc)
    assert doc["result"]["summary"] == summarize(V2)
    assert doc["result"]["tags"] == extract_tags(summarize(V2))
    assert not set(doc["result"]["tags"]) & set(extract_tags(summarize(V1)))


async def test_previous_result_is_visible_but_explicitly_not_current(client, container, simulator):
    doc_id = (await submit(client, content=V1)).json()["document_id"]
    await container.runner.drain()
    v1 = await wait_status(client, doc_id, "completed")
    assert v1["result_is_current"] is True

    simulator.close_gate(Stage.PROCESSING)
    await client.patch(f"/documents/{doc_id}", json={"content": V2}, headers=hdr("alice"))
    await wait_status(client, doc_id, "processing")
    mid = await get_doc(client, doc_id)
    assert_consistent(mid)
    assert mid["content_version"] == 2
    assert mid["result"]["content_version"] == 1 and mid["result"]["is_current"] is False
    assert mid["result_is_current"] is False
    assert mid["result"]["content_hash"] != mid["content_hash"]

    simulator.open_gate(Stage.PROCESSING)
    await container.runner.drain()
    done = await wait_status(client, doc_id, "completed")
    assert done["result_is_current"] is True and done["result"]["content_version"] == 2


async def test_superseded_worker_write_is_a_no_op(client, container, simulator):
    """Directly exercise the fence: a write pinned to an old version/lease matches nothing."""
    simulator.close_gate(Stage.PROCESSING)
    doc_id = ObjectId((await submit(client, content=V1)).json()["document_id"])
    await asyncio.wait_for(simulator.entered[Stage.PROCESSING].wait(), 5)
    token = (await container.repo.get(doc_id))["pipeline"]["lease"]["token"]

    await client.patch(f"/documents/{doc_id}", json={"content": V2}, headers=hdr("alice"))
    ok = await container.repo.guarded_update(
        doc_id, version=1, token=token, set_fields={"result": {"summary": "evil", "content_version": 1}}
    )
    assert ok is False
    assert (await container.repo.get(doc_id))["result"] is None


async def test_invariant_holds_under_random_patch_storm(client, container, simulator):
    """Hammer one document with PATCHes while polling; every snapshot must be self-consistent and
    every completed snapshot's output must be exactly the function of that snapshot's content."""
    doc_id = (await submit(client, content="seed content. second sentence.")).json()["document_id"]
    rng = random.Random(42)
    stop = asyncio.Event()
    observed = 0

    async def poller() -> None:
        nonlocal observed
        while not stop.is_set():
            doc = await get_doc(client, doc_id)
            assert_consistent(doc)
            if doc["status"] == "completed":
                assert doc["result"]["content_hash"] == content_hash(doc["content"])
                assert doc["result"]["summary"] == summarize(doc["content"])
                assert doc["result"]["tags"] == extract_tags(summarize(doc["content"]))
            observed += 1
            await asyncio.sleep(0)

    async def patcher() -> None:
        for i in range(25):
            words = " ".join(
                rng.choice(["apple", "banana", "cherry", "delta", "echo", "fox"]) for _ in range(6)
            )
            await client.patch(
                f"/documents/{doc_id}", json={"content": f"Edit {i} {words}."}, headers=hdr("alice")
            )
            await asyncio.sleep(rng.uniform(0, 0.01))

    poll_task = asyncio.create_task(poller())
    await patcher()
    await container.runner.drain()
    await wait_until(lambda: _completed(client, doc_id))
    stop.set()
    await poll_task
    final = await get_doc(client, doc_id)
    assert_consistent(final)
    assert final["content_version"] == 26 and final["result_is_current"] is True
    assert observed > 10


async def _completed(client, doc_id) -> bool:
    return (await get_doc(client, doc_id))["status"] == "completed"


async def test_identical_patch_is_a_no_op(client, container):
    doc_id = (await submit(client, content=V1)).json()["document_id"]
    await container.runner.drain()
    r = await client.patch(f"/documents/{doc_id}", json={"content": f"  {V1}\n"}, headers=hdr("alice"))
    assert r.status_code == 200
    assert r.json()["content_version"] == 1 and r.json()["status"] == "completed"
