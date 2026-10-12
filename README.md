# Cohort Insights API

A FastAPI backend service that ingests content, processes it through a two-stage simulated pipeline, and serves the results to multiple consumers.

---

## Quick Start

**Prerequisites:** Docker, Docker Compose, Python 3.11+

### 1 — Start the full stack with Docker Compose

```bash
docker compose up --build
```

This starts the API on `http://localhost:8000`, MongoDB on `27017`, and Redis on `6379`.
The API creates all MongoDB indexes and recovers any in-flight pipeline jobs automatically on startup.

### 2 — Interactive docs

```
http://localhost:8000/docs     # Swagger UI
http://localhost:8000/redoc    # ReDoc
```

### 3 — Health check

```bash
curl http://localhost:8000/health
# {"status":"healthy","mongodb":"healthy","redis":"healthy"}
```

---

## Running Tests

Tests require running MongoDB and Redis (the test suite uses a separate `cohort_insights_test` database).

```bash
# Start infrastructure only
docker compose up -d mongodb redis

# Install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run the full test suite
pytest -v

# Run a specific test file
pytest tests/test_staleness.py -v
```

Pipeline delays are automatically shortened to ~0.1–0.2 s and failure rate set to 0 for the test run (configured via `conftest.py` environment overrides before any app imports).

---

## Environment Variables

Copy `.env.example` to `.env` and adjust as needed:

| Variable | Default | Description |
|---|---|---|
| `MONGODB_URL` | `mongodb://localhost:27017` | MongoDB connection string |
| `MONGODB_DATABASE` | `cohort_insights` | Database name |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection string |
| `PIPELINE_STAGE1_MIN_DELAY` | `10` | Stage 1 minimum simulated delay (s) |
| `PIPELINE_STAGE1_MAX_DELAY` | `20` | Stage 1 maximum simulated delay (s) |
| `PIPELINE_STAGE2_MIN_DELAY` | `5` | Stage 2 minimum simulated delay (s) |
| `PIPELINE_STAGE2_MAX_DELAY` | `15` | Stage 2 maximum simulated delay (s) |
| `PIPELINE_FAILURE_RATE` | `0.1` | Per-attempt failure probability (0–1) |
| `PIPELINE_MAX_RETRIES` | `2` | Maximum retries per stage |
| `PIPELINE_RETRY_BASE_DELAY` | `1.0` | Base delay for exponential backoff (s) |
| `RATE_LIMIT_MAX_CONCURRENT` | `3` | Max concurrent active documents per user |
| `CACHE_TTL` | `3600` | Content cache TTL in Redis (s) |
| `LOG_LEVEL` | `INFO` | Logging level |

---

## API Reference

All endpoints that identify a caller use the `X-User-Id` header (returns 401 if missing).

### POST /documents

Submit a document for processing.

```json
{
  "user_id": "alice",
  "title": "My Report",
  "content": "Full text content...",
  "client_doc_ref": "cms-doc-42"   // optional, unique per user
}
```

Returns `201` with `{ document_id, status, cached }`.

Returns `429` if the user has 3 or more documents already in the active pipeline.
Returns `409` if `client_doc_ref` is already owned by a different user.

**Repeat ref behaviour:** If the same `client_doc_ref` is re-submitted by the same user with *different* content, the existing document is updated in-place (new version, reprocessed). With *identical* content, the cached result is returned immediately (`cached: true`). This is documented in the "Crosswalk" section below.

---

### PATCH /documents/{document_id}

Replace the document's content and reprocess from scratch.

```bash
curl -X PATCH http://localhost:8000/documents/<id> \
  -H "X-User-Id: alice" \
  -H "Content-Type: application/json" \
  -d '{"content": "Updated content..."}'
```

Returns `200 { document_id, status, cached }`, `404` if not found / not owner, `409` on concurrent PATCH race.

---

### GET /documents/{document_id}

Poll the document's status and results.

```bash
curl http://localhost:8000/documents/<id> -H "X-User-Id: alice"
```

Response:
```json
{
  "document_id": "...",
  "user_id": "alice",
  "title": "My Report",
  "status": "completed",
  "stage1": { "status": "completed", "error": null, "attempt": 1 },
  "stage2": { "status": "completed", "error": null, "attempt": 1 },
  "summary": "Summary: ...",
  "tags": ["keyword1", "keyword2"],
  "content_hash": "abc123...",
  "summary_content_hash": "abc123...",
  "tags_content_hash": "abc123...",
  "version": 1,
  "created_at": "...",
  "updated_at": "..."
}
```

---

### GET /documents/by-ref/{client_doc_ref}

Resolve a document by external reference (uses the `client_doc_ref` index).

```bash
curl http://localhost:8000/documents/by-ref/cms-doc-42 -H "X-User-Id: alice"
```

---

### GET /users/{user_id}/documents

List a user's documents, newest first, with optional pagination and status filter.

```bash
curl "http://localhost:8000/users/alice/documents?page=1&page_size=10&status=completed" \
  -H "X-User-Id: alice"
```

The `X-User-Id` header must match the path `user_id` — otherwise 404.

---

### GET /health

Returns connectivity status of MongoDB and Redis. Returns `200` when both are reachable, `503` otherwise.

---

## Design Decisions

### Two-Stage Async Pipeline

The pipeline runs as a fire-and-forget `asyncio.Task`. Stage 1 (processing) produces a summary; Stage 2 (enriching) reads that summary and produces tags. The stages are sequential within a single document but fully concurrent across documents.

Each stage has an independent ~10% failure chance and retries with exponential backoff (up to `PIPELINE_MAX_RETRIES` retries). A stage 2 failure does **not** re-run stage 1 — partial progress is preserved and the `stage1`/`stage2` sub-documents independently reflect which stage failed.

The `pipeline_run_id` field is a UUID generated fresh on every submission or PATCH. Every MongoDB write from the pipeline is conditional on this ID matching the current document — if a PATCH has fired in the meantime, the old run's writes match zero documents and are silently discarded. This is the core race-condition guard.

### Startup Recovery

On startup, the app scans for documents left in `{queued, processing, enriching}` states (from a previous crash or restart), resets them to `queued`, assigns new `pipeline_run_id` values, and re-starts their pipelines. Stale Redis rate-limiter entries from the previous process are flushed first.

### Ownership

Only the submitting user can read, update, or list their documents. Non-owner or nonexistent documents both return `404` — this avoids confirming a document's existence to a caller who doesn't own it. The listing endpoint (`GET /users/{user_id}/documents`) enforces this by requiring `X-User-Id == path user_id`.

### Crosswalk / External Reference

`client_doc_ref` is stored with a **unique sparse index** — MongoDB enforces uniqueness only when the field is present (nulls are excluded). The `GET /documents/by-ref/{ref}` endpoint always resolves via this index, never via a collection scan.

**Repeat submission behaviour** (documented design choice):

| Condition | Behaviour |
|---|---|
| Same ref, same user, same content | Cached result returned immediately |
| Same ref, same user, different content | Existing document updated (new version, reprocessed) |
| Same ref, different user | HTTP 409 Conflict |

This treats `client_doc_ref` as a stable identifier owned by a user, not a nonce. The partner CMS can re-send the same ref safely — if the content hasn't changed, no reprocessing occurs; if the content has changed, the document is updated in-place and the ref continues to point to it.

---

## Schema & Staleness Design

### The Problem

A PATCH changes the document's content. The pipeline runs asynchronously. A reader polling during reprocessing must never see a mix of old summary with new tags, or vice versa. A boolean `is_stale` flag with nothing to compare against doesn't solve this — two fields could be updated at different moments, creating a brief window where the flag is `false` but the results are mixed.

### The Mechanism

Every document stores three hashes:

```
content_hash           — SHA-256 of the current content field
summary.content_hash   — SHA-256 of the content that produced the summary
tags.content_hash      — SHA-256 of the content that produced the tags
```

A reader interprets these as follows:

- `content_hash == summary_content_hash == tags_content_hash` → results are current and consistent.
- Any mismatch → reprocessing is in progress; the reader should poll again.
- `summary_content_hash` or `tags_content_hash` is `null` → that stage hasn't completed yet.

### Why Mixed Versions Are Impossible

The guarantee comes from two independent mechanisms working together:

**1. `pipeline_run_id` conditional writes.**
When a PATCH fires, it writes a new `pipeline_run_id` to the document. Every MongoDB update from the running pipeline includes a filter clause `{ pipeline_run_id: <run_id captured at start> }`. When the run ID no longer matches (because a PATCH superseded it), the update matches zero documents. The old summary can never overwrite the new content's summary, because the write is gated on a version token that was invalidated the moment PATCH ran.

**2. Field-level content_hash stored with each derived result.**
`summary.content_hash` is written atomically in the same `$set` as `summary.text`. `tags.content_hash` is written atomically with `tags.keywords`. There is no sequence of events where `summary.text` and `tags.keywords` could both be present but belong to different content versions, because each field carries its own provenance hash — a reader can independently verify each one.

Together: the run_id mechanism prevents stale results from being written at all; the per-field content hashes give callers a direct, unambiguous way to verify what they observe without trusting any boolean flag.

### Optimistic Concurrency on PATCH

`PATCH /documents/{id}` uses a `find_one_and_update` with `{ version: <current_version> }` in the filter. If two concurrent PATCHes race, the second one finds a different version and returns HTTP 409. The client can re-read and retry. This prevents the scenario where two PATCHes both succeed and one's pipeline_run_id overwrites the other silently.

---

## MongoDB Schema

```json
{
  "_id": ObjectId,
  "user_id": "string",
  "title": "string",
  "content": "string",
  "content_hash": "sha256hex",
  "version": 1,
  "client_doc_ref": "string | null",
  "pipeline_run_id": "uuid_hex",
  "status": "queued | processing | enriching | completed | failed",
  "stage1": {
    "status": "queued | processing | completed | failed",
    "error": "string | null",
    "attempt": 0,
    "started_at": "datetime | null",
    "completed_at": "datetime | null"
  },
  "stage2": { "...same structure..." },
  "summary": {
    "text": "string | null",
    "content_hash": "sha256hex | null"
  },
  "tags": {
    "keywords": ["string"] | null,
    "content_hash": "sha256hex | null"
  },
  "created_at": "datetime",
  "updated_at": "datetime"
}
```

### Indexes

| Index | Fields | Options | Purpose |
|---|---|---|---|
| 1 | `(user_id, created_at desc)` | — | `GET /users/{id}/documents`, newest first |
| 2 | `(user_id, status, created_at desc)` | — | Same endpoint with status filter |
| 3 | `client_doc_ref` | unique, sparse | `GET /documents/by-ref/{ref}` — no collection scan |

---

## Redis Usage

Two distinct concerns, separate key namespaces:

**Rate limiter** — `rate_limit:{user_id}` → Redis Set of active document_ids.
Check-and-add is atomic via a Lua script (no TOCTOU race). The set carries a 1-hour safety-net TTL, refreshed on every write. Fails open if Redis is unavailable.

**Content cache** — `cache:{user_id}:{content_hash}` → JSON `{ summary, tags }`.
Keyed by content hash, not document_id. A PATCH to new content computes a new hash → new cache key → the old entry is irrelevant. TTL defaults to 1 hour, refreshed on every read. Fails gracefully (cache miss) if Redis is unavailable.

---

## At 100×

### What happens to user_id-based indexes at 500K documents per user?

The compound index `(user_id, created_at desc)` becomes a hot-partition index — a single B-tree leaf range covers all 500K documents for that user. Index scans are still fast for individual users (they're range scans on a sorted structure), but the index itself becomes unbalanced if a small number of users hold the vast majority of documents. The real problem is `count_documents({"user_id": user_id})` for the pagination total, which requires scanning the entire user partition; at 500K documents, this becomes noticeably slow (hundreds of milliseconds).

Skip/limit pagination (`skip(N).limit(M)`) is the deeper issue: MongoDB must traverse all N preceding documents before returning M results. At page 1000 with page_size 50, MongoDB skips 50K documents. At 500K documents this degrades to O(N) per query. The fix is **cursor-based (keysearch) pagination**: instead of `skip`, pass the `_id` or `created_at` of the last document seen, and use `{ user_id: x, created_at: { $lt: last_seen } }` as the filter. This is O(log N) regardless of page depth.

### Sharding / partition key

I would shard on **`user_id`**. Each user's documents are always co-located on the same shard, so `GET /users/{id}/documents` never requires a scatter-gather across shards. Cross-user operations don't exist in this API. The risk is uneven distribution if a small number of users account for most writes (the hot-user problem from above) — this can be mitigated by hashing the shard key (`hashed` shard key in MongoDB) or by pre-splitting and using a composite key like `(user_id, document_id)`.

I would **not** shard on `document_id` alone, even though it distributes writes evenly. A listing query would become a scatter-gather across all shards (a fan-out to N shards, merge, re-sort), and since the listing endpoint is the high-read path this would be worse than the hot-user problem.

### Redis rate-limiter at 100× QPS

The current design uses a Redis Lua script (atomic SADD + SCARD). At 100× submit QPS, the Lua script itself is not the bottleneck — Redis single-threaded command execution handles hundreds of thousands of ops/sec. The bottleneck is the single Redis instance. I would switch to a Redis Cluster with the rate-limit key hashed to the same slot as the user (using hash tags: `{user_id}:rate_limit`) so all operations for a user land on one shard. If Redis Cluster is not available, a sliding window approach using `INCR` with a short TTL (rather than a set) reduces per-operation overhead. I would also add a local (in-process) L1 counter with a short TTL (50–100 ms) to absorb burst traffic without hitting Redis on every request.

### Skip/limit pagination at 100× scale

As discussed above, skip/limit pagination breaks at scale because each deep-page query requires MongoDB to skip N documents. The replacement is **keyset (cursor) pagination**: the client receives an opaque `next_cursor` token (encoding `_id` or `created_at` of the last item seen) and the next request filters `{ user_id, created_at: { $lt: cursor_value } }`. This is an indexed range scan — O(log N) regardless of page depth. I would keep skip/limit as a fallback for small result sets (total < 1000) and switch automatically for larger ones, or require all callers to use cursor-based pagination from the start.

---

## What I Would Do With More Time

- **Dedicated worker pool**: Move the pipeline from in-process asyncio tasks to a proper task queue (Celery + Redis, or a MongoDB-backed queue with a dedicated worker process). This decouples pipeline throughput from API throughput and allows horizontal scaling of workers independently.
- **Idempotent pipeline steps**: Add a MongoDB distributed lock (using `findOneAndUpdate` with a TTL) per pipeline stage to prevent two worker instances from running the same stage concurrently after a crash and recovery.
- **Pagination cursors**: Replace skip/limit with keyset pagination as described in "At 100×".
- **Metrics & tracing**: Add Prometheus counters for pipeline stage durations, failure rates, cache hit rates, and rate-limit rejections. Instrument with OpenTelemetry for distributed tracing.
- **Authentication**: Replace the `X-User-Id` header with a real JWT/OAuth token. The header is acceptable for a take-home but not for production.
- **Rate limiter with sliding window**: Replace the set-based limiter with a sorted-set sliding-window approach for finer-grained control over burst behaviour.
- **More granular error responses**: Add machine-readable error codes alongside human-readable messages so callers can programmatically distinguish rate-limit errors from conflict errors from not-found errors without parsing strings.
- **Document deletion endpoint**: A `DELETE /documents/{id}` that releases the rate-limit slot and removes the cache entry.
