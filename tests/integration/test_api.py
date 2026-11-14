"""Core API behaviour: submit/poll/list, validation, ownership, health."""

import pytest

from tests.conftest import get_doc, hdr, submit, wait_status

pytestmark = pytest.mark.integration


async def test_submit_then_poll_until_completed(client, container):
    r = await submit(client, content="FastAPI serves requests. MongoDB stores documents. Redis caches.")
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "queued" and body["submission"] == "created"
    assert body["content_version"] == 1 and body["result"] is None

    await container.runner.drain()
    doc = await wait_status(client, body["document_id"], "completed")
    assert doc["result_is_current"] is True
    assert doc["result"]["summary"] == "FastAPI serves requests. MongoDB stores documents."
    assert doc["result"]["tags"][:2] == ["fastapi", "serves"]
    assert doc["pipeline"]["stages"]["processing"]["state"] == "succeeded"
    assert doc["pipeline"]["stages"]["enriching"]["state"] == "succeeded"
    assert doc["pipeline"]["failed_stage"] is None


async def test_validation_errors_are_422_with_envelope(client):
    r = await client.post(
        "/documents", json={"user_id": "alice", "title": "", "content": "x"}, headers=hdr("alice")
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation_error"


async def test_missing_identity_is_401(client):
    r = await client.post("/documents", json={"user_id": "alice", "title": "t", "content": "x"})
    assert r.status_code == 401
    assert (await client.get("/users/alice/documents")).status_code == 401


async def test_cannot_submit_on_behalf_of_another_user(client):
    r = await client.post(
        "/documents", json={"user_id": "alice", "title": "t", "content": "x"}, headers=hdr("mallory")
    )
    assert r.status_code == 403


async def test_non_owner_gets_indistinguishable_404(client):
    doc_id = (await submit(client, user="alice")).json()["document_id"]
    other = await client.get(f"/documents/{doc_id}", headers=hdr("bob"))
    missing = await client.get("/documents/0123456789abcdef01234567", headers=hdr("bob"))
    malformed = await client.get("/documents/not-an-object-id", headers=hdr("bob"))
    assert other.status_code == missing.status_code == malformed.status_code == 404
    strip = lambda r: {k: v for k, v in r.json()["error"].items() if k != "request_id"}  # noqa: E731
    assert strip(other) == strip(missing) == strip(malformed)

    patch = await client.patch(f"/documents/{doc_id}", json={"content": "hijack"}, headers=hdr("bob"))
    assert patch.status_code == 404
    assert (await client.post(f"/documents/{doc_id}/retry", headers=hdr("bob"))).status_code == 404


async def test_listing_is_scoped_paginated_newest_first(client, container):
    ids = []
    for i in range(5):
        r = await submit(client, user="alice", content=f"Document number {i}.")
        ids.append(r.json()["document_id"])
        await container.runner.drain()  # stay under the active limit
    await submit(client, user="bob", content="Bob's document.")

    r = await client.get("/users/alice/documents", params={"page": 1, "page_size": 2}, headers=hdr("alice"))
    assert r.status_code == 200
    page = r.json()
    assert page["total"] == 5 and page["has_next"] is True
    assert [d["document_id"] for d in page["items"]] == ids[::-1][:2]
    assert all(d["user_id"] == "alice" for d in page["items"])
    assert "content" not in page["items"][0]

    last = (
        await client.get("/users/alice/documents", params={"page": 3, "page_size": 2}, headers=hdr("alice"))
    ).json()
    assert [d["document_id"] for d in last["items"]] == [ids[0]] and last["has_next"] is False

    forbidden = await client.get("/users/alice/documents", headers=hdr("bob"))
    assert forbidden.status_code == 403


async def test_listing_status_filter(client, container, simulator):
    from app.domain import Stage

    done = (await submit(client, content="Completes quickly.")).json()["document_id"]
    await container.runner.drain()
    await wait_status(client, done, "completed")

    simulator.close_gate(Stage.PROCESSING)
    pending = (await submit(client, content="Will be stuck in processing.")).json()["document_id"]
    await wait_status(client, pending, "processing")

    r = await client.get("/users/alice/documents", params={"status": "completed"}, headers=hdr("alice"))
    assert [d["document_id"] for d in r.json()["items"]] == [done]
    r = await client.get("/users/alice/documents", params={"status": "processing"}, headers=hdr("alice"))
    assert [d["document_id"] for d in r.json()["items"]] == [pending]
    bad = await client.get("/users/alice/documents", params={"status": "bogus"}, headers=hdr("alice"))
    assert bad.status_code == 422


async def test_health_reports_dependencies(client):
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "checks": {"mongodb": {"status": "ok"}, "redis": {"status": "ok"}}}
    assert (await client.get("/health/live")).status_code == 200


async def test_request_id_is_propagated(client):
    r = await client.get("/health/live", headers={"X-Request-ID": "abc123"})
    assert r.headers["X-Request-ID"] == "abc123"


async def test_get_returns_content_and_versions(client):
    doc_id = (await submit(client, content="Some content.")).json()["document_id"]
    doc = await get_doc(client, doc_id)
    assert doc["content"] == "Some content."
    assert len(doc["content_hash"]) == 64
