# Cohort Insights API

A FastAPI service that takes in documents and runs them through a simulated two-stage pipeline (`processing` → `enriching`). It serves versioned results to the submitting user and to a partner system that refers to documents by its own `client_doc_ref`.

**Stack:** Python 3.12 (3.11+ supported), FastAPI, MongoDB 7 (async PyMongo), Redis 7, Docker Compose.

**Code map:** HTTP routes are in [`app/api/routers/`](app/api/routers/), request/response models in [`app/schemas/documents.py`](app/schemas/documents.py), persistence and indexes in [`app/repositories/documents.py`](app/repositories/documents.py) and [`app/db/mongo.py`](app/db/mongo.py), and the worker state machine in [`app/pipeline/runner.py`](app/pipeline/runner.py).

---

## Quick start

```bash
cp .env.example .env            # optional – every setting has a default
docker compose up --build       # API on http://localhost:8000, docs at /docs
make test                       # full test suite inside compose, against real MongoDB + Redis
```

| Command | What it does |
|---|---|
| `make up` / `make down` | Start the stack detached / stop it and remove volumes |
| `make logs` | Tail the API's JSON logs |
| `make test` | `docker compose --profile test run --rm --build tests` |
| `make lint` / `make fmt` | ruff check + format |

To run the tests locally instead, start only the backing services with `docker compose up -d mongo redis`, then:

```bash
python3.11 -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/pytest
```

> Real stage durations are 10–20 s and 5–15 s. For a quicker demo, set e.g. `PROCESSING_MIN_SECONDS=1 PROCESSING_MAX_SECONDS=2 ENRICHING_MIN_SECONDS=1 ENRICHING_MAX_SECONDS=2 LEASE_SECONDS=20` in `.env`.

### Example session

```bash
H='-H Content-Type:application/json -H X-User-Id:alice'

# submit
curl -s $H localhost:8000/documents -d '{"user_id":"alice","title":"Q3","content":"Retention rose 4%. Churn fell.","client_doc_ref":"cms:42","ref_version":1}'
# -> 201 {"document_id":"665f…","status":"queued","content_version":1,"result":null,"result_is_current":false,…}

curl -s $H localhost:8000/documents/665f…                   # poll
curl -s $H localhost:8000/documents/by-ref/cms:42           # partner lookup
curl -s $H -X PATCH localhost:8000/documents/665f… -d '{"content":"New text.","expected_version":1}'
curl -s $H 'localhost:8000/users/alice/documents?page=1&page_size=20&status=completed'
curl -s $H -X POST localhost:8000/documents/665f…/retry     # only for failed documents
curl -s localhost:8000/health
```

---

## API

Identity comes from the **`X-User-Id`** header. It stands in for the verified principal (e.g. a JWT `sub`) that an API gateway would normally inject; see *Assumptions*. A missing or malformed header returns `401`.

| Method & path | Success | Errors |
|---|---|---|
| `POST /documents` | `201` created. `200` for a repeat `client_doc_ref` (`submission: duplicate \| new_version`) | `403` body `user_id` ≠ caller · `409` ref owned by another user / stale `ref_version` · `429` limit · `422` validation |
| `PATCH /documents/{id}` `{content, expected_version?}` | `200` | `404` · `409` `expected_version` mismatch · `429` |
| `GET /documents/{id}` | `200` | `404` (missing, malformed id **or not yours**: indistinguishable) |
| `GET /documents/by-ref/{client_doc_ref}` | `200` | `404` (missing or not yours) |
| `POST /documents/{id}/retry` | `202` (resumes at the failed stage) | `404` · `409 invalid_state` if not `failed` · `429` |
| `GET /users/{user_id}/documents?page&page_size&status` | `200` `{items,page,page_size,total,has_next}` newest first | `403` if `user_id` ≠ caller |
| `GET /health` | `200` `ok` / `degraded` (Redis down) | `503` if MongoDB is down |
| `GET /health/live` | `200` (no dependency checks) | – |

Every error uses one envelope: `{"error": {"code", "message", "request_id", "details"?}}`. Every response carries `X-Request-ID`, which is also bound into every log line.

### What a document looks like

```jsonc
{
  "document_id": "665f…", "user_id": "alice", "title": "Q3", "content": "…",
  "client_doc_ref": "cms:42", "ref_version": 2,
  "status": "enriching",                 // queued | processing | enriching | completed | failed
  "content_version": 2,                  // increments on every content change
  "content_hash": "9b1c…",               // sha256 of the normalized current content
  "result_is_current": false,
  "result": {                            // last committed output (may belong to an older version)
    "summary": "…", "tags": ["…"],
    "content_version": 1, "content_hash": "e3a0…", "completed_at": "…",
    "is_current": false                  // result.content_version == content_version
  },
  "pipeline": {
    "version": 2, "failed_stage": null, "cache_hit": false,
    "stages": {
      "processing": {"state": "succeeded", "attempts": 1, "started_at": "…", "finished_at": "…", "last_error": null},
      "enriching":  {"state": "running",   "attempts": 2, "started_at": "…", "finished_at": null, "last_error": "simulated enriching failure (attempt 1)"}
    }
  }
}
```

`status` gives the coarse position. `pipeline.stages.*` is a structured per-stage state machine (`pending | running | succeeded | failed`, with attempt counts and the last error), and `pipeline.failed_stage` names exactly which stage failed. Ad-hoc string fields aren't needed.

---

## Schema & Staleness Design

**Goal:** a reader must never be able to observe (a) an old summary presented as current, or (b) a summary and tags from different content versions. Both must be *impossible by construction*, not just unlikely under normal timing.

### The schema (one document per `documents` row)

```text
_id, user_id, title, client_doc_ref?, ref_version?, created_at, updated_at
content            current text
content_hash       sha256(NFC(content).strip())
content_version    int, +1 on every content change               ← the authority
status             queued|processing|enriching|completed|failed
pipeline           {version, failed_stage, cache_hit, stages{processing,enriching}, lease{token,owner,expires_at}}
stage1             {content_version, content_hash, summary} | null    internal; never returned by the API
result             {content_version, content_hash, summary, tags, completed_at} | null   ← the ONLY output readers see
```

### Why a mismatch is unobservable

1. **Summary and tags live in one sub-document and are written by one statement.** The enriching stage commits `result = {content_version: v, content_hash, summary, tags}` together with `status: "completed"` in a single `$set`. MongoDB applies a single-document update atomically, so no read can see the new tags with an old summary, or vice versa. They are never stored as separate top-level fields that could be updated independently.
2. **Every derived value carries the version it was computed from.** `result.content_version` (and `content_hash`) is stamped inside the same sub-document. A reader compares it with the document's top-level `content_version` from *the same read*, so the comparison can never be torn. The API exposes the comparison as `result.is_current` / `result_is_current`. This is not a free-floating boolean: the API computes it at read time from two values that come from a single atomic snapshot.
3. **Stale workers cannot write: every pipeline write is a compare-and-set.** Every write a worker makes has the filter `{_id, content_version: v, "pipeline.lease.token": token}`. A `PATCH` is itself a compare-and-set on the old version. In **one** update it swaps content and hash, `$inc`s `content_version`, resets `status`/`pipeline`, **drops the lease** and clears `stage1`. From that instant, any in-flight run for version *v* matches nothing. Its next write, including the final `result` commit, has no effect; the worker sees `matched_count == 0` and abandons the run. So a result for version *v* can only be committed while the document is still at version *v*.
4. **Stage 1's output is tied to a version too.** `stage1.content_version` must equal the version being processed for enriching to reuse it (the "resume after enriching failed" path). Old partial progress can therefore never be combined with new content.
5. **The content cache is keyed by content, not by document.** The key is `cache:result:v1:{sha256(content)}`. New content has a different hash, so an old entry can't answer for it. The cached value is a deterministic function of exactly that content.

Together, these give the invariant that the tests enforce on every observed response:
`status == "completed" ⇒ result.content_version == content_version`, and `result.summary`/`result.tags` always come from the same `result.content_version`. While a new version is processing, the previous result **is** still returned, because it is useful, but always with `is_current: false` and its own version and hash. A caller can therefore tell exactly how stale it is. See `tests/integration/test_staleness.py`: a PATCH mid-processing, a PATCH mid-enriching, a superseded worker's write being a no-op, and a randomized PATCH storm that checks the invariant on every poll.

**Two workers, one job.** `claim` is an atomic `find_one_and_update` that only succeeds if there is no lease or the lease has expired, so at most one live lease exists per document. Every attempt start renews the lease. If a worker stalls past `LEASE_SECONDS`, another can take over, and the original holder is fenced by the token check in (3). This is tested in `test_pipeline.py`.

**Two PATCHes racing.** With `expected_version`, exactly one wins and the other gets `409 version_conflict` (optimistic concurrency). Without it, each PATCH re-reads and retries its compare-and-set, so both apply in sequence and each gets its own version; there are no lost updates (`test_rate_limit_and_concurrency.py`).

---

## Crosswalk (`client_doc_ref`)

`client_doc_ref` is optional. It is **globally unique when present**, enforced by the unique partial index `client_doc_ref_unique`, because it is a partner namespace and must map to exactly one document. The partner may also send an optional, monotonically increasing `ref_version` to order its submissions.

| Repeat submission of an existing ref | Outcome |
|---|---|
| Same user, **same content** (by hash) | `200`, `submission: "duplicate"`. Idempotent replay: no reprocessing, no new version. A higher `ref_version` is recorded. |
| Same user, **different content**, no `ref_version` or a newer one | `200`, `submission: "new_version"`. Same `document_id`, content replaced exactly like a PATCH (version +1, full reprocess, old result kept but marked not current). |
| Same user, different content, `ref_version` ≤ stored | `409 version_conflict` with `current_ref_version`/`received_ref_version`. An out-of-order retry can't roll the document back. |
| **Different user** | `409 client_doc_ref_conflict`. We never merge into or reveal another user's document. |
| Two first submissions racing | The unique index lets exactly one insert win. The loser re-resolves and follows the rules above. |

**Rationale.** "Same ref = same logical document" matches what a CMS means by an id. Idempotent replay makes partner retries safe. `ref_version` gives a defined answer to out-of-order delivery, where wall-clock timing would be a race. The by-ref fence (`ref_version` is part of the compare-and-set filter) means two out-of-order requests in flight together still converge to the highest version (tested). Every branch is covered in `tests/integration/test_crosswalk.py`.

`GET /documents/by-ref/{ref}` returns the document for that ref if the caller owns it, otherwise `404`.

---

## Indexes

| Name | Keys | Serves |
|---|---|---|
| `client_doc_ref_unique` | `{client_doc_ref: 1}` unique, partial `client_doc_ref: {$type: "string"}` | **`GET /documents/by-ref/{ref}`** (point lookup) + uniqueness |
| `user_created_desc` | `{user_id: 1, created_at: -1, _id: -1}` | listing, newest first, stable tiebreak |
| `user_status_created_desc` | `{user_id: 1, status: 1, created_at: -1, _id: -1}` | listing with `?status=`; Mongo fallback count for the rate limiter |
| `result_content_hash` | `{result.content_hash: 1}` partial `result: {$type: "object"}` | content-cache fallback when Redis is unavailable |
| `active_lease` | `{status: 1, pipeline.lease.expires_at: 1}` partial on active statuses | recovery sweeper (index size tracks in-flight work, not history) |

**A gotcha I hit and tested.** MongoDB only uses a *partial* index if the query provably implies the index's `partialFilterExpression`. A plain `{client_doc_ref: "x"}` does **not** imply it, and silently falls back to a COLLSCAN. So by-ref lookups query `{client_doc_ref: {$eq: ref, $type: "string"}}`, and the hash lookup adds `result: {$type: "object"}`. `tests/integration/test_indexes.py` checks the `explain()` plans: `IXSCAN` on the named index and no `COLLSCAN`.

---

## Redis: two separate concerns

**1. Active-job rate limiter** (`app/services/rate_limiter.py`)
- One hash per user: `rl:active:{user_id}` → `{document_id: content_version}`.
- `ACQUIRE` is a Lua script that does an atomic check and insert (HLEN < 3). It is idempotent: a PATCH of an already-active document replaces its member instead of taking a second slot.
- `RELEASE` is a compare-and-delete: only the version holding the slot can free it. A stale worker finishing an old version therefore can't free the new run's slot.
- The slot counts all of `queued`, `processing` and `enriching`. It is freed on `completed` **and** `failed`. A retry re-acquires it.
- A set of members was chosen over INCR/DECR because counters drift on every crash or retry.
- The key has a TTL as a leak backstop, and a reconciler compares the hash against MongoDB (the source of truth) on startup and on every sweep.

**2. Content cache** (`app/services/content_cache.py`)
- `cache:result:v1:{sha256}` → `{summary, tags}`, with TTL `CACHE_TTL_SECONDS` (7 days).
- It is global across users, because the output is a pure function of the content.
- A hit completes the document immediately (`pipeline.cache_hit: true`) and **does not consume a rate-limit slot**. It works for both submit and PATCH.

**Graceful degradation.** Redis uses short socket timeouts (0.5 s). If Redis is down:
- The limiter falls back to an indexed MongoDB count over active statuses. This is correct but not atomic under simultaneous bursts; that trade-off is documented.
- The cache falls back to `result_content_hash` in MongoDB and re-warms Redis when it returns.
- `/health` reports `degraded`, not `unavailable`.
- Requests keep succeeding (`test_service_degrades_gracefully_without_redis`).

---

## Pipeline

- Workers are in-process asyncio tasks (`PIPELINE_WORKER_CONCURRENCY` per replica), fed by an in-memory queue. **Correctness never depends on the queue.** MongoDB holds all state, the atomic lease decides who runs a job, and a recovery sweeper (`RECOVERY_INTERVAL_SECONDS`) re-enqueues active documents whose lease is missing or expired, e.g. after a crash or restart. Running several API replicas is therefore safe.
- Each stage gets `STAGE_MAX_ATTEMPTS` attempts, separated by **exponential backoff with equal jitter** (base 1 s, cap 8 s). Only once they are all used up does the document become `failed`, with `failed_stage` set.
- **Partial progress:** stage 1's summary is persisted in `stage1` (stamped with its version) *before* enriching starts. `POST /documents/{id}/retry` after an enriching failure skips processing entirely (`test_enriching_failure_preserves_stage1_and_retry_resumes`).
- Stage **outputs** are deterministic mocks: summary = first two sentences, capped at 280 chars; tags = the most frequent non-stopwords of the summary. Latency and failures (~10 % per stage attempt) are injected separately by a `StageSimulator`, so tests can script them precisely.
- Shutdown is bounded. Workers are cancelled with a grace period, and any half-done job is simply picked up again once its lease expires.

---

## Ownership: why 404 and not 403

For a document id or ref that isn't yours, the API returns **404**, the same response body as for an id that doesn't exist or is malformed. Ownership is part of the Mongo query (`{_id, user_id}`), so the two cases are literally the same code path, with no timing or body difference. A 403 would confirm that the id exists, which leaks information about other tenants (e.g. lets someone enumerate partner refs). The exception is the **listing** route: there the `user_id` is in the path and comes from the caller themselves, so `403` ("you can only list your own documents") reveals nothing and is clearer.

---

## Input validation (beyond Pydantic defaults)

- `extra="forbid"` on request bodies.
- `user_id`/`X-User-Id` must match `^[A-Za-z0-9][A-Za-z0-9_.@-]{0,63}$`. `client_doc_ref` must match `^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,199}$`, so `/` is allowed and the route uses `{ref:path}`.
- `title` is 1–300 characters after trimming. `content` is 1–100 000 characters, must not be blank, and must contain no NUL bytes.
- `ref_version ≥ 0` and requires `client_doc_ref`. `expected_version ≥ 1`. `page ≥ 1`. `1 ≤ page_size ≤ 100`. `status` is an enum.
- Settings are validated at startup. For example, `LEASE_SECONDS` must exceed the longest stage attempt plus the maximum backoff; otherwise healthy workers would lose their leases.

---

## Production readiness

- **Structure:** routers → services → repository, wired in a composition root (`app/container.py`), with FastAPI DI on top. Services raise domain errors and never import FastAPI.
- **Logging:** structlog JSON logs with request-id and job context (`doc_id`, `content_version`, `run`) carried through contextvars.
- **Error handling:** one error envelope. There is no `except Exception: pass`: every broad catch logs with a traceback and has a comment saying why it exists.
- **Config:** 12-factor environment config (`app/core/config.py`, documented in `.env.example`).
- **Docker:** multi-stage Dockerfile running as a non-root user. Compose healthchecks plus `depends_on: service_healthy`. Liveness and readiness probes are separate.
- **Tests:** 71 tests. Unit tests cover hashing, mocks, backoff, mapping and settings. Integration tests use `httpx.AsyncClient` against **real** MongoDB and Redis, with a scripted stage simulator so race windows are opened deterministically rather than with sleeps.

## Assumptions

- Authentication is out of scope. `X-User-Id` is trusted, as if set by a gateway, and the body's `user_id` must match it.
- "Replaces content in place" means the same `document_id` with a new `content_version`. The previous result stays readable but is marked not current, rather than deleted.
- Content identity is SHA-256 of NFC-normalized content with outer whitespace trimmed. Inner whitespace is kept, because it can carry meaning.
- An identical PATCH (same hash) is a no-op: no new version and no reprocessing.
- PATCHing a document that is already active does not need an extra rate-limit slot; its existing slot is handed over to the new version. PATCHing a terminal document needs one (429 if the user is at the limit).
- The cache is shared across users. Results are derived only from the content, which the submitter already has, so sharing leaks nothing.

## What I'd do with more time

- **A real queue and a separate worker deployment** (e.g. Redis Streams/Arq, or a Mongo change-stream outbox), so the API and workers scale and deploy independently. The lease and fencing design carries over unchanged.
- **Transactional outbox** for the Redis side effects, instead of best-effort release plus reconciliation.
- **Keyset pagination** and an approximate `total` (see *At 100×*). OpenTelemetry tracing and Prometheus metrics (queue depth, stage latency, 429 rate, cache hit ratio).
- Real auth (JWT/OIDC) and partner credentials scoped per `client_doc_ref` namespace. Request-rate limiting in addition to the active-job limit.
- Property-based tests (Hypothesis) for the PATCH/worker interleavings. Load tests. A history collection of past results if consumers need more than the latest.

---

## At 100×

**A user with 500K documents.** `user_created_desc` and `user_status_created_desc` keep working for the first page: equality on `user_id`, then an index walk already in `created_at` order. Two things degrade. First, `skip` walks and throws away `(page−1)·page_size` index keys, so page 20 000 reads ~400K keys. Second, `count_documents` for `total` scans all 500K keys on every request. The rate-limit fallback count is fine because it is bounded by active statuses. The deeper issue is that one tenant becomes one contiguous key range, so its writes all land on the same index leaf pages, and later on the same shard.

**Shard key.** I'd use the compound **`{user_id: 1, _id: "hashed"}`**. Listing, ownership checks and rate-limit counts include `user_id`, so mongos targets only that user's chunks rather than the whole cluster. Hashing `_id` gives a large tenant enough key range to split across shards and avoids routing its sequential inserts to one ever-growing range. Plain `user_id` would make all 500K documents one indivisible shard-key value (a jumbo chunk); `{user_id: 1, _id: 1}` is splittable but concentrates a tenant's newest inserts at the range's hot end. The trade-off is that a user's listing fans out across that user's chunks and merges sorted results; it is not a single-shard query. By-ref and content-hash fallback lookups don't contain this shard key, so I'd move them to side collections: `refs{_id: client_doc_ref → doc_id, user_id}` sharded by `client_doc_ref` (which also permits globally unique refs), and a hash-result collection sharded by content hash. Both remain point lookups rather than cluster-wide document scans.

**Redis limiter at 100× QPS.** The per-user hash plus Lua design still holds. Each script touches exactly one key, so it is Redis Cluster–safe, and load spreads across slots by user. Three things change. First, reconciliation currently `SCAN`s every key and lists every active Mongo document; at scale I'd make it event-driven (a change stream emitting releases) or sample per user. Second, release-on-completion is best effort; I'd move it into an outbox so leaks stop depending on TTL. Third, very hot tenants get a local token bucket in front of the cluster, so a misbehaving partner can't saturate one slot. In degraded mode the Mongo-count fallback isn't atomic; at that scale I'd fail closed (429) for users near the limit rather than over-admit.

**Pagination.** Skip/limit no longer works, for two reasons: the cost is linear in depth, and results drift (inserts shift pages, so items repeat or go missing). I'd use **keyset (cursor) pagination** on the existing index: `?cursor=<opaque base64(created_at,_id)>` with `{user_id, $or: [{created_at < c}, {created_at = c, _id < id}]}`, sorted `created_at -1, _id -1`. That is an O(page_size) index range scan on any page. Exact `total` would become optional or approximate (cached per user, or `has_next` from fetching `limit+1`).
