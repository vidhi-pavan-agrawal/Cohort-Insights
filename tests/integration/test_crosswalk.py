"""Crosswalk: client_doc_ref lookup + defined behaviour on repeat submissions."""

import asyncio

import pytest

from app.db.mongo import DOCUMENTS
from tests.conftest import hdr, submit, wait_status

pytestmark = pytest.mark.integration


async def test_lookup_by_ref(client, container):
    doc_id = (await submit(client, content="Partner doc.", client_doc_ref="cms:42")).json()["document_id"]
    r = await client.get("/documents/by-ref/cms:42", headers=hdr("alice"))
    assert r.status_code == 200 and r.json()["document_id"] == doc_id
    assert (await client.get("/documents/by-ref/unknown", headers=hdr("alice"))).status_code == 404


async def test_ref_lookup_is_owner_scoped(client):
    await submit(client, user="alice", content="Partner doc.", client_doc_ref="cms-1")
    assert (await client.get("/documents/by-ref/cms-1", headers=hdr("bob"))).status_code == 404


async def test_ref_with_slashes(client):
    await submit(client, content="Path-like ref.", client_doc_ref="tenant/a/doc-1")
    assert (await client.get("/documents/by-ref/tenant/a/doc-1", headers=hdr("alice"))).status_code == 200


async def test_repeat_same_ref_same_content_is_idempotent(client, container, simulator):
    first = await submit(client, content="Same body.", client_doc_ref="r1")
    await container.runner.drain()
    calls_before = len(simulator.calls)

    again = await submit(client, content="  Same body.\n", client_doc_ref="r1")  # normalizes to same hash
    assert again.status_code == 200
    assert again.json()["submission"] == "duplicate"
    assert again.json()["document_id"] == first.json()["document_id"]
    assert again.json()["content_version"] == 1
    await container.runner.drain()
    assert len(simulator.calls) == calls_before, "a replay must not trigger reprocessing"
    assert await container.mongo.db[DOCUMENTS].count_documents({"client_doc_ref": "r1"}) == 1


async def test_repeat_same_ref_different_content_creates_new_version(client, container):
    doc_id = (await submit(client, content="Version one.", client_doc_ref="r2")).json()["document_id"]
    await container.runner.drain()

    r = await submit(client, content="Version two text.", client_doc_ref="r2")
    assert r.status_code == 200
    body = r.json()
    assert body["submission"] == "new_version"
    assert body["document_id"] == doc_id
    assert body["content_version"] == 2 and body["status"] == "queued"
    assert body["result"]["is_current"] is False

    await container.runner.drain()
    doc = await wait_status(client, doc_id, "completed")
    assert doc["result"]["summary"] == "Version two text."


async def test_out_of_order_ref_versions_are_rejected(client, container):
    doc_id = (await submit(client, content="Rev 2 content.", client_doc_ref="r3", ref_version=2)).json()[
        "document_id"
    ]
    await container.runner.drain()

    stale = await submit(client, content="Rev 1 content (late).", client_doc_ref="r3", ref_version=1)
    assert stale.status_code == 409
    err = stale.json()["error"]
    assert err["code"] == "version_conflict"
    assert err["details"] == {"current_ref_version": 2, "received_ref_version": 1}

    same_version_diff_content = await submit(client, content="Different.", client_doc_ref="r3", ref_version=2)
    assert same_version_diff_content.status_code == 409

    late_retry_of_current = await submit(client, content="Rev 2 content.", client_doc_ref="r3", ref_version=1)
    assert late_retry_of_current.status_code == 200
    assert late_retry_of_current.json()["submission"] == "duplicate"

    newer = await submit(client, content="Rev 3 content.", client_doc_ref="r3", ref_version=3)
    assert newer.status_code == 200 and newer.json()["submission"] == "new_version"
    assert newer.json()["document_id"] == doc_id
    assert newer.json()["ref_version"] == 3


async def test_idempotent_replay_records_higher_ref_version(client, container):
    await submit(client, content="Body.", client_doc_ref="r4", ref_version=1)
    r = await submit(client, content="Body.", client_doc_ref="r4", ref_version=5)
    assert r.status_code == 200 and r.json()["ref_version"] == 5
    stale = await submit(client, content="Other body.", client_doc_ref="r4", ref_version=4)
    assert stale.status_code == 409


async def test_ref_owned_by_another_user_conflicts(client):
    await submit(client, user="alice", content="Alice's.", client_doc_ref="shared")
    r = await submit(client, user="bob", content="Bob's.", client_doc_ref="shared")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "client_doc_ref_conflict"


async def test_concurrent_first_submissions_of_same_ref_yield_one_document(client, container):
    results = await asyncio.gather(
        *(submit(client, content=f"Racing content {i}.", client_doc_ref="race") for i in range(6))
    )
    codes = sorted(r.status_code for r in results)
    assert codes.count(201) == 1, codes
    assert set(codes) <= {200, 201, 409, 429}
    assert await container.mongo.db[DOCUMENTS].count_documents({"client_doc_ref": "race"}) == 1


async def test_concurrent_out_of_order_ref_versions_converge_to_highest(client, container):
    await submit(client, content="v0 content.", client_doc_ref="ooo", ref_version=0)
    await container.runner.drain()
    results = await asyncio.gather(
        *(submit(client, content=f"v{i} content.", client_doc_ref="ooo", ref_version=i) for i in (3, 1, 2))
    )
    assert all(r.status_code in (200, 409) for r in results)
    doc = (await client.get("/documents/by-ref/ooo", headers=hdr("alice"))).json()
    # Whatever the interleaving, a lower ref_version can never overwrite a higher one.
    accepted = [r.json()["ref_version"] for r in results if r.status_code == 200]
    assert doc["ref_version"] == max(accepted) == 3


async def test_by_ref_lookup_uses_the_unique_index(container):
    from app.repositories.documents import ref_filter

    plan = await container.mongo.db.command(
        "explain", {"find": DOCUMENTS, "filter": ref_filter("x"), "limit": 1}, verbosity="queryPlanner"
    )
    winning = str(plan["queryPlanner"]["winningPlan"])
    assert "IXSCAN" in winning and "client_doc_ref_unique" in winning
    assert "COLLSCAN" not in winning
