"""Two-stage pipeline: failures, partial progress, retries, claims and leases."""

import asyncio

import pytest
from bson import ObjectId

from app.domain import Stage
from app.repositories.documents import utcnow
from tests.conftest import hdr, submit, wait_status

pytestmark = pytest.mark.integration


async def test_processing_failure_reports_failed_stage(client, container, simulator, settings):
    simulator.fail_always.add(Stage.PROCESSING)
    doc_id = (await submit(client)).json()["document_id"]
    await container.runner.drain()
    doc = await wait_status(client, doc_id, "failed")
    assert doc["pipeline"]["failed_stage"] == "processing"
    proc = doc["pipeline"]["stages"]["processing"]
    assert proc["state"] == "failed" and proc["attempts"] == settings.stage_max_attempts
    assert "scripted" in proc["last_error"]
    assert doc["pipeline"]["stages"]["enriching"]["state"] == "pending"
    assert doc["result"] is None


async def test_enriching_failure_preserves_stage1_and_retry_resumes(client, container, simulator):
    simulator.fail_always.add(Stage.ENRICHING)
    doc_id = (await submit(client, content="Resumable work. Keep stage one.")).json()["document_id"]
    await container.runner.drain()
    failed = await wait_status(client, doc_id, "failed")
    assert failed["pipeline"]["failed_stage"] == "enriching"
    assert failed["pipeline"]["stages"]["processing"]["state"] == "succeeded"
    raw = await container.repo.get(ObjectId(doc_id))
    assert raw["stage1"]["content_version"] == 1
    processing_runs = simulator.count(Stage.PROCESSING, doc_id)

    simulator.fail_always.clear()
    r = await client.post(f"/documents/{doc_id}/retry", headers=hdr("alice"))
    assert r.status_code == 202 and r.json()["status"] == "queued"
    await container.runner.drain()
    done = await wait_status(client, doc_id, "completed")
    assert simulator.count(Stage.PROCESSING, doc_id) == processing_runs, "stage 1 must not be redone"
    assert done["pipeline"]["stages"]["processing"]["attempts"] == 1
    assert done["result"]["summary"] == "Resumable work. Keep stage one."


async def test_transient_failure_is_retried_with_backoff(client, container, simulator):
    simulator.fail_times[Stage.PROCESSING] = 1
    simulator.fail_times[Stage.ENRICHING] = 2
    doc_id = (await submit(client)).json()["document_id"]
    await container.runner.drain()
    doc = await wait_status(client, doc_id, "completed")
    assert doc["pipeline"]["stages"]["processing"]["attempts"] == 2
    assert doc["pipeline"]["stages"]["enriching"]["attempts"] == 3
    assert doc["pipeline"]["stages"]["enriching"]["last_error"] is not None


async def test_retry_only_allowed_for_failed(client, container):
    doc_id = (await submit(client)).json()["document_id"]
    await container.runner.drain()
    await wait_status(client, doc_id, "completed")
    r = await client.post(f"/documents/{doc_id}/retry", headers=hdr("alice"))
    assert r.status_code == 409 and r.json()["error"]["code"] == "invalid_state"


async def test_two_workers_cannot_claim_the_same_job(client, container, simulator):
    simulator.close_gate(Stage.PROCESSING)
    doc_id = ObjectId((await submit(client)).json()["document_id"])
    await asyncio.wait_for(simulator.entered[Stage.PROCESSING].wait(), 5)
    # The queue worker holds the lease; extra direct attempts (another replica, a duplicate enqueue)
    # must lose the atomic claim and do nothing.
    await asyncio.gather(*(container.runner.process(doc_id) for _ in range(5)))
    assert simulator.count(Stage.PROCESSING, str(doc_id)) == 1
    simulator.open_gate(Stage.PROCESSING)
    await container.runner.drain()
    await wait_status(client, str(doc_id), "completed")


async def test_expired_lease_is_taken_over_and_old_holder_is_fenced(client, container, simulator):
    simulator.close_gate(Stage.PROCESSING)
    doc_id = ObjectId((await submit(client)).json()["document_id"])
    await asyncio.wait_for(simulator.entered[Stage.PROCESSING].wait(), 5)
    old_token = (await container.repo.get(doc_id))["pipeline"]["lease"]["token"]

    # Simulate the holder hanging past its lease (e.g. GC pause / partitioned replica).
    await container.mongo.db["documents"].update_one(
        {"_id": doc_id}, {"$set": {"pipeline.lease.expires_at": utcnow()}}
    )
    simulator.open_gate(Stage.PROCESSING)
    await container.runner.process(doc_id)  # a "second replica" takes over and finishes
    doc = await wait_status(client, str(doc_id), "completed")
    assert doc["result_is_current"] is True
    assert not await container.repo.guarded_update(doc_id, version=1, token=old_token, set_fields={"x": 1})


async def test_recovery_sweeper_requeues_orphaned_jobs(client, container, settings):
    """A doc left 'processing' with an expired lease (crashed worker) is picked up again."""
    from app.repositories.documents import new_pipeline

    now = utcnow()
    doc_id = ObjectId()
    await container.mongo.db["documents"].insert_one(
        {
            "_id": doc_id,
            "user_id": "alice",
            "title": "orphan",
            "content": "Orphaned by a crash.",
            "content_hash": "h",
            "content_version": 1,
            "status": "processing",
            "pipeline": {**new_pipeline(1), "lease": {"token": "dead", "owner": "dead", "expires_at": now}},
            "stage1": None,
            "result": None,
            "created_at": now,
            "updated_at": now,
        }
    )
    assert await container.runner.recover_once() >= 1
    await container.runner.drain()
    await wait_status(client, str(doc_id), "completed")
