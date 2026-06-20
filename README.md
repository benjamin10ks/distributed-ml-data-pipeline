# Distributed ML Data Pipeline

A distributed, production-grade ML data pipeline covering ingestion, streaming, feature engineering, storage, and model serving. Built as a summer learning project in **Go**, touching systems design, distributed computing, data engineering, and ML infrastructure.

> **Language note**: The pipeline services (ingestion workers, Kafka consumers/producers, inference API, orchestration) are written in Go. External infrastructure that runs as its own process — MLflow, Feast — is language-agnostic and communicates over HTTP/gRPC. The distributed compute layer uses Go-native workers rather than Spark/Ray. See [Go-specific considerations](#go-specific-considerations) below.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          INGESTION LAYER                            │
│                                                                     │
│   HTTP Push Adapter        S3/MinIO Adapter       (CDC — stretch)  │
│   POST /ingest/events      POST /minio/events      Runner iface     │
│          │                        │                                 │
│          └────────────┬───────────┘                                 │
│                       ▼                                             │
│               worker.process()  ← in-process, sequential today     │
│               ┌───────────────────────────────────────┐            │
│               │  1. idempotency check (manifest DB)   │            │
│               │  2. manifest insert → "processing"    │            │
│               │  3. validate: magic bytes, size,      │            │
│               │              checksum sidecar         │            │
│               │  4. parse: CSV / NDJSON / Parquet     │            │
│               │            → []Record                 │            │
│               │  5. store: Snappy Parquet →           │            │
│               │            processed bucket           │            │
│               │  6. manifest → "done"                 │            │
│               │  7. publish → files.processed (Kafka) │            │
│               └───────────────────────────────────────┘            │
│                       │                                             │
│               Bad files → quarantine bucket + manifest "quarantined"│
└───────────────────────┬─────────────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────────────┐
│                       STREAMING LAYER                               │
│                       Kafka (franz-go)                              │
│             Partitioned, durable, replayable event bus              │
│                                                                     │
│   Topic: files.processed  (path + metadata, not file contents)      │
└───────┬───────────────────────┬────────────────────────┬────────────┘
        │                       │                        │
        │  Phase 5              │  Phase 7               │  Phase 10
┌───────▼──────────┐   ┌────────▼────────┐   ┌──────────▼──────────┐
│ Feature          │   │ Lakehouse       │   │ Drift / Monitoring  │
│ Engineering      │   │ Writer          │   │ (Prometheus)        │
│                  │   │                 │   │                     │
│ type coercion    │   │ append Parquet  │   │ track feature       │
│ normalization    │   │ to Delta/Iceberg│   │ distributions,      │
│ encoding         │   │ on MinIO        │   │ alert on KL         │
│ null/range checks│   │                 │   │ divergence          │
└───────┬──────────┘   └─────────────────┘   └─────────────────────┘
        │
┌───────▼──────────────────────────────────────────────────────────────┐
│                         STORAGE LAYER                                │
│   Feature Store (Redis + Feast)    Data Lakehouse    Model Registry  │
│                                   (Parquet, Delta)   (MLflow)        │
└───────┬──────────────────────────────────────────────────────────────┘
        │
┌───────▼──────────────────────────────────────────────────────────────┐
│                         SERVING LAYER                                │
│   Real-time Inference (ONNX)          Batch Prediction               │
│   POST /predict → Redis → model       reads lakehouse → worker pool  │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Build Checklist

### Phase 1 — Foundation & Infrastructure ✅

> Get the skeleton standing. Nothing works end-to-end yet, but the core services are running locally.

- [x] Initialize Go workspace: `go work init`, one `go.mod` per service under each top-level directory
- [x] Set up monorepo structure (`ingestion/`, `streaming/`, `processing/`, `storage/`, `serving/`, `infra/`)
- [x] Write `docker-compose.yml` with Kafka, Zookeeper, Postgres, and MinIO (local S3)
- [x] Configure Kafka topics with appropriate partition counts and retention settings
- [x] Stand up Confluent Schema Registry (or use the Redpanda bundled one)
- [x] Write a `Makefile` with `up`, `down`, `logs`, `reset`, and `build` targets (`go build ./...`)
- [x] Add `.env.example` with all required environment variables documented
- [x] Set up a shared `internal/config/` package using `os.Getenv` + a typed config struct
- [x] Set up structured logging with `slog` (stdlib since Go 1.21) — JSON output, `trace_id` field from context

---

### Phase 2 — File-Based Ingestion 🔧 In Progress

> Implement the file ingestion pipeline: landing zone → manifest → validate → parse → processed storage.

**Ingestion adapter layer**

- [x] Define `IngestionsAdapter` interface (`Register(mux)` + `Events() <-chan RawEvent`) in `ingestion/file/adapter.go`
- [x] Define `RawEvent` envelope type (`Source`, `Payload`, `Format`, `Path`, `ContentHash`, `Size`, `ReceivedAt`, `Metadata`)
- [x] Define optional `Runner` interface for adapters that need a background goroutine (CDC would use this if introduced)
- [x] Build S3 adapter (`ingestion/file/s3.go`) — registers `POST /minio/events`, downloads object, hashes with SHA-256 via `TeeReader`, emits `RawEvent`
- [x] Wire MinIO webhook notifications → shared `http.Server` → S3 adapter handler — `handleNotificaion` in `s3.go` now calls `url.QueryUnescape` on the notification key before using it (MinIO, like AWS S3, sends keys URL-encoded, e.g. `source%3Dsmoketest%2Fdate%3D...`); fixed and verified live via a real MinIO upload → webhook → ingestion round trip
- [x] Build HTTP adapter (`ingestion/file/http.go`) — registers `POST /ingest/events/{source}`, writes payload to landing bucket, emits `RawEvent`
- [x] Share a single `http.Server` and `ServeMux` across all adapters — instantiated in `main.go`, adapters call `Register(mux)`

**Landing zone**

- [x] Create MinIO buckets: `landing`, `processed`, `quarantine` via `make init` (`mc` CLI)
- [x] Key structure: `source={name}/date={YYYY-MM-DD}/{filename}` — constructed by HTTP adapter on write, read from notification key by S3 adapter
- [x] Configure MinIO webhook notification on `s3:ObjectCreated` events → `POST /minio/events`

**File manifest**

- [x] Create `file_manifest` table (`path`, `content_hash`, `source`, `status`, `created_at`, `processed_at`)
- [x] Partial index on `status` for `pending` and `processing` rows only
- [x] `Manifest` struct in `ingestion/file/manifest.go` with `Insert`, `GetByHash`, `UpdateStatus`, `GetStuck` — `GetByHash` returns `(nil, nil)` for a not-found hash instead of surfacing `pgx.ErrNoRows`, so new files aren't mistaken for an error
- [x] Status constants: `pending → processing → done | failed | quarantined`
- [x] `GetStuck` queries entries in `processing` older than a threshold — used on restart to recover crashed workers
- [x] DB connection pool (`pgx/v5`) opened in `main.go`, injected into `Manifest` and `Worker`

**Worker**

- [x] `Worker` in `ingestion/file/worker.go` merges all adapter channels into one stream via a fan-in goroutine per adapter (each goroutine loops on its adapter's channel for the life of the process)
- [x] Sequential `process()` loop for now — Phase 6 replaces with bounded `errgroup` worker pool
- [x] `process()` sequence: idempotency check → manifest insert → `processing` → validate → parse → write processed → `done` → publish Kafka event
- [x] Kafka publish failure does not fail the operation — file is safely stored, error logged
- [ ] Stretch: retry failed Kafka publishes by tracking publish state on the manifest (not implemented — a failed publish is currently logged and dropped, not retried)
- [x] Bad files log and continue — one quarantined file does not stop the pipeline

**Parsers**

- [x] `Parser` interface (`Parse(io.Reader) ([]Record, error)`) in `ingestion/file/parser/parser.go`
- [x] `Record` type: `map[string]any` — preserves native types, coercion deferred to feature engineering
- [x] `For(format string) (Parser, error)` factory — dispatches on `"csv"`, `"parquet"`, `"ndjson"`
- [x] CSV parser (`ingestion/file/parser/csv.go`) — `encoding/csv` stdlib, `ReuseRecord` for reduced allocations, copies header row
- [x] NDJSON parser (`ingestion/file/parser/ndjson.go`) — `json.NewDecoder` streaming loop
- [x] Parquet parser (`ingestion/file/parser/parquet.go`) — `parquet-go`, reads in 128-row chunks via `GenericReader`
- [x] `parse.go` in `ingestion/file/` — seam between `RawEvent` and parser package; only file that knows both types
- [x] Format detection (`detectFormat`) — magic bytes take precedence over file extension

- [x] `validator.go` — size bounds, magic byte verification, recomputes SHA-256 and compares against the adapter-computed `ContentHash` (no external checksum sidecar file today)
- [x] `store.go` — write parsed records to processed bucket as Snappy-compressed Parquet, 128–256MB target file size, multi-part + manifest JSON for larger record sets
- [x] `kafka.go` — publish lightweight processed event (path + metadata, not file contents) to `files.processed` topic
- [x] `publishProcessed` and `writeProcessed` implementations called by worker
- [x] `mustOpenDB` implementation in `main.go`
- [x] `mustOpenKafka` implementation in `main.go` — connects with retry/backoff and hands the client to `Worker`, so `publishProcessed` actually publishes (franz-go enables idempotent produces by default)
- [x] `WorkerConfig.S3Client` wired in `main.go` — added `S3Adapter.Client()` accessor so `main.go` reuses the adapter's already-configured `s3.Client` instead of constructing a second one; fixed and verified live end-to-end (upload → webhook → manifest `done` → parquet in `processed` bucket → Kafka consumer receives it)
- [x] Quarantine: copy original file to quarantine bucket, write reason metadata, log alert
- [x] `GetStuck` recovery: on startup query manifest for stuck `processing` entries, requeue them
- [ ] Stretch: replace buffered channel with ElasticMQ for production-style durability
- [x] Integration test: upload a file end-to-end, assert it appears in processed bucket with correct hash and manifest status `done` (lives at `ingestion/test/integration_test.go`)

---

### Phase 3 — CDC Ingestion ⏳ Deferred Stretch Goal

> **Why deferred**: CDC (Change Data Capture) captures row-level changes from an upstream operational database and streams them as events. This pipeline's Postgres instance holds only internal manifest metadata — pipeline bookkeeping, not business data. Capturing changes to manifest rows would be circular and serve no purpose. CDC becomes relevant only if an upstream operational database (e.g., an orders DB or users DB) is introduced whose row-level changes are themselves the data to be streamed. The `Runner` interface in `adapter.go` already accommodates a CDC adapter when that time comes; the Debezium connector config lives in `infra/debezium/` ready to be picked up.

If an upstream operational DB is introduced, the implementation path would be:

- [ ] Enable logical replication on the upstream Postgres (`wal_level = logical`)
- [ ] Create a replication slot: `SELECT pg_create_logical_replication_slot('debezium', 'pgoutput')`
- [ ] Configure and run Debezium Postgres connector (via Kafka Connect)
- [ ] Verify event envelope shape: `before`, `after`, `op`, `ts_ms`, `source` fields present
- [ ] Register Avro schemas for each source table in Schema Registry
- [ ] Configure topic naming convention: `{server}.{schema}.{table}`
- [ ] Verify partition-by-primary-key routing (all changes for a given row go to same partition)
- [ ] Implement dead-letter queue topic for parse/schema failures
- [ ] Add replication slot lag monitoring (`pg_replication_slots` → alert if lag > threshold)
- [ ] Run initial snapshot test: connect Debezium to a table with existing rows, verify read events
- [ ] Write consumer that reads CDC events and prints before/after diffs (smoke test)
- [ ] Add `cdc.go` to `ingestion/file/` implementing the `Runner` interface (no HTTP routes — background goroutine only)

---

### Phase 4 — Streaming Layer (Kafka) 🔧 In Progress

> Harden the event bus: producers, consumers, schema enforcement, and observability.

- [x] Kafka client chosen: `github.com/twmb/franz-go` (pure Go, no CGO)
- [x] Topics created via `infra/kafka/create-topics.sh`: `files.raw` (6 partitions), `files.processed` (6 partitions), `cdc.events` (3 partitions), `dead-letter` (1 partition)
- [x] Set retention policies per topic (raw events: 7 days, processed: 30 days — configured in `create-topics.sh`, overridable via env)
- [ ] Define topic partitioning strategy per source — producer currently keys by `content_hash` (`kafka.go`), not by `source`; revisit if a downstream consumer needs per-source ordering
- [ ] Enforce schema definition via Protobuf serialization (`proto/events/`) — `ProcessedEvent` is currently a JSON-serialized Go struct
- [x] Kafka producer wired: `mustOpenKafka` in `main.go` connects with retry/backoff; `publishProcessed` in `kafka.go` publishes to `files.processed`; idempotent writes + all-ISR acks are franz-go defaults (left as-is, not explicitly configured)
- [x] Implement a generic Kafka consumer with manual offset commits (no auto-commit) — `Consumer` in `streaming/kafka/consumer.go`; `kgo.DisableAutoCommit()` + `CommitRecords` per record after its handler succeeds, `OnPartitionsRevoked` commits in-flight work before a rebalance takes the partition away
- [x] Use goroutines for concurrent partition consumption — one goroutine per partition is idiomatic — `Consumer.Run` starts one worker goroutine per `(topic, partition)` it's assigned, fed by per-partition channels so order is preserved within a partition
- [x] Smoke-test consumer wired to a real topic: `streaming/kafka/cmd/processed-consumer/main.go` reads `files.processed`, decodes `file.ProcessedEvent`, and logs it — verified live: produced a message via `kafka-console-producer`, confirmed the consumer logged it, and confirmed via `kafka-consumer-groups --describe` that the offset was actually committed (not just processed in-memory)
- [ ] Add consumer group lag monitoring (expose as Prometheus metrics via `prometheus/client_golang`)
- [ ] Test at-least-once delivery: kill a consumer mid-batch, verify no events are lost on restart
- [ ] Test exactly-once processing: verify idempotent writes don't produce duplicates
- [ ] Add schema evolution test: add a nullable column, verify existing consumers don't break

---

### Phase 5 — Feature Engineering

> Build the transformation layer that turns raw events into ML-ready features.

- [ ] Define a `Feature` interface with `Name() string`, `Transform(v any) (any, error)`, `InverseTransform(v any) (any, error)`
- [ ] Implement numerical features: normalization, standardization, bucketization (pure Go, no external deps)
- [ ] Implement categorical features: one-hot encoding, target encoding, frequency encoding
- [ ] Implement temporal features: hour-of-day, day-of-week, days-since-event, rolling windows
- [ ] Implement text features: tokenization, TF-IDF (Go-native); for embeddings, call an external model server over gRPC
- [ ] Build a `FeaturePipeline` struct that chains transforms — serialize pipeline config to JSON for reproducibility
- [ ] **Critical**: verify training-time and serving-time transforms produce identical output for the same input (training-serving skew test)
- [ ] Add data type validation at pipeline boundaries (reject unexpected nulls, out-of-range values)
- [ ] Note: CSV records arrive as `map[string]string` — type coercion from `Record` (`map[string]any`) happens here, not in the parser
- [ ] Write a backfill job that recomputes features over historical Parquet files using a worker pool (`errgroup` from `golang.org/x/sync`)
- [ ] Benchmark transform throughput (target: process 1M rows/minute on a single node)

---

### Phase 6 — Distributed Compute

> Scale processing beyond a single node using Go-native worker pools and Kafka partitioning.

> **Go note**: Rather than Spark or Ray (JVM/Python ecosystems), Go-native distribution uses Kafka partitions as the work distribution mechanism — each worker consumes one or more partitions concurrently. This is a cleaner mental model and performs well for moderate data volumes (< 10TB/day).

- [ ] Replace sequential `process()` loop in worker with bounded `errgroup` pool — configurable via `WORKER_COUNT` env var
- [ ] Use `errgroup` (`golang.org/x/sync/errgroup`) for fan-out with coordinated error propagation
- [ ] Port the CSV chunking parser to fan out chunks across the worker pool — track chunk offsets for resumability
- [ ] Port feature engineering pipeline to process Parquet row groups in parallel (one goroutine per row group)
- [ ] Implement data partitioning by date: each worker claims a date partition via a Postgres advisory lock (prevents double-processing)
- [ ] Add job checkpointing: write progress to Postgres after each chunk so failed jobs resume from last good offset
- [ ] Write a job that joins CDC events with file-ingested data — use an in-memory hash join for datasets that fit in RAM, spill to disk via temp Parquet files for larger sets
- [ ] Profile with `go tool pprof` — identify CPU vs I/O bottlenecks; check goroutine count isn't unbounded

---

### Phase 7 — Storage Layer

> Build the three storage tiers: data lakehouse, feature store, and model registry.

**Data Lakehouse**

- [ ] Set up Delta Lake (or Apache Iceberg) on MinIO
- [ ] Implement schema-on-write with enforced column types
- [ ] Enable time-travel: verify you can query the table as of a past timestamp
- [ ] Set up table partitioning: `date` at top level, `source` at second level
- [ ] Implement compaction job (merge small files into 256MB target size)
- [ ] Add data retention policy (raw: 90 days, processed: 1 year)

**Feature Store**

- [ ] Set up Feast with a local Redis online store and Parquet offline store
- [ ] Define `FeatureView` for each feature group, with `Entity` and `ttl`
- [ ] Write a materialization job that pushes offline features to online store
- [ ] Verify point-in-time correct feature retrieval for training (no future leakage)
- [ ] Benchmark online feature retrieval latency (target: < 10ms p99)

**Model Registry**

- [ ] Add `mlflow` bucket to MinIO via `make init`
- [ ] Add MLflow tracking server to `docker-compose.yml` (Postgres backend + MinIO artifact store)
- [ ] Log a dummy model run: params, metrics, and a serialized model artifact
- [ ] Implement model versioning: promote a model from `Staging` to `Production`
- [ ] Write Go model loader using MLflow REST API — fetches current `Production` version by name, downloads ONNX artifact from MinIO

---

### Phase 8 — Serving Layer

> Stand up inference endpoints and batch prediction jobs.

- [ ] Build inference service using Go's `net/http` stdlib (or `github.com/go-chi/chi` for routing)
- [ ] Add a `POST /predict` endpoint: fetches features from feature store (Redis via `go-redis/v9`), runs inference, returns result
- [ ] For model execution: load an ONNX model using `github.com/onnxruntime-go` — avoids a Python sidecar for most model types
- [ ] Alternatively: call a Python model server (TF Serving, Triton) over gRPC from Go — idiomatic for large neural nets
- [ ] Add a `GET /health` endpoint that checks model loaded, Redis reachable, and current model version
- [ ] Containerize the service: write `Dockerfile` using a distroless or scratch base image — Go binaries produce small, fast images
- [ ] Implement a batch prediction job: reads Parquet from lakehouse, fans out predictions across worker pool, writes results back
- [ ] Load test the inference endpoint using `hey` or `k6` (target: 100 req/s at < 50ms p99 latency on a single container)
- [ ] Stretch - Add a canary deployment pattern: route 5% of traffic to a new model version via a weighted round-robin in the handler

---

### Phase 9 — Orchestration - This might not be implemented by the end of the summer, but the design will be in place

> Wire the pipeline together with a scheduler that handles retries, dependencies, and backfills.

- [ ] Choose orchestrator: **Temporal** (Go-native, strongly typed workflows — best fit for a Go project) or **Airflow** (Python-based but widely used, interact via REST API from Go)
- [ ] Define a Workflow for the nightly file ingestion run (Temporal: a Go function decorated with `workflow.ExecuteActivity`)
- [ ] Define a Workflow for feature materialization (depends on ingestion workflow completing — use `workflow.GetVersion` for safe rollout)
- [ ] Define a Workflow for model retraining trigger (signals the Python training job, waits for completion signal)
- [ ] Implement retry policy: 3 retries with exponential backoff, alert on final failure
- [ ] Implement backfill: re-run the ingestion workflow for a specific date range using a `for` loop over dates in a parent workflow
- [ ] Add SLA monitoring: use Temporal's workflow timeout + a heartbeat activity to detect stalled runs

---

### Phase 10 — Monitoring & Observability

> You can't fix what you can't see. Add observability before you call the project done.

- [ ] Expose Prometheus metrics from every service (Kafka lag, file processing rate, inference latency)
- [ ] Set up Grafana with dashboards for: pipeline throughput, error rates, Kafka consumer lag, inference p50/p99
- [ ] Implement **data drift detection**: track input feature distributions, alert when KL divergence exceeds threshold
- [ ] Implement **model performance monitoring**: log predictions and actuals, compute rolling accuracy/RMSE
- [ ] Add structured logging (JSON logs with `trace_id`, `source`, `pipeline_stage`, `duration_ms`)
- [ ] Set up alerting rules: replication slot lag, quarantine bucket non-empty, inference error rate > 1%
- [ ] Write a runbook for each alert: what it means, how to diagnose, how to resolve

---

## Key Design Decisions

Recorded in `ADR/` as decisions are made.

| Decision                     | Choice                                                                     | Status     |
| ---------------------------- | -------------------------------------------------------------------------- | ---------- |
| Kafka client                 | `franz-go` (pure Go, no CGO)                                               | ✅ decided |
| Postgres client              | `pgx/v5`                                                                   | ✅ decided |
| S3 client                    | `aws-sdk-go-v2`                                                            | ✅ decided |
| Parquet library              | `parquet-go`                                                               | ✅ decided |
| HTTP routing                 | stdlib `net/http` + `ServeMux`                                             | ✅ decided |
| Adapter pattern              | `Register(mux)` + shared server                                            | ✅ decided |
| Runner interface             | Defined in adapter.go; CDC would use it if an upstream DB is introduced    | ✅ decided |
| Ingestion package layout     | `ingestion/file/` package (room for `ingestion/cdc/` etc. per source type) | ✅ decided |
| Infra config (Debezium etc.) | `infra/debezium/` not `ingestion/` — ready if CDC is introduced            | ✅ decided |
| Compute framework            | Go-native worker pools, not Spark/Ray                                      | ✅ decided |
| CDC / Debezium               | Deferred — no upstream operational DB; manifest DB is internal only        | ⏳ stretch |
| Orchestrator                 | Temporal (pending Phase 9)                                                 | ⏳ pending |
| Inference runtime            | ONNX in-process vs gRPC sidecar                                            | ⏳ pending |
| Feature store                | Custom Redis                                                               | ✅ decided |
| Message format               | Protobuf                                                                   | ⏳ pending |
| Exactly-once semantics       | At-least-once + idempotent manifest                                        | ✅ decided |
| Training-serving skew        | Feature pipeline serialized to JSON, same code path for train and serve    | ⏳ pending |

---

## Go-Specific Considerations

### Key libraries

| Concern                 | Library                               |
| ----------------------- | ------------------------------------- |
| Kafka producer/consumer | `github.com/twmb/franz-go`            |
| Postgres                | `github.com/jackc/pgx/v5`             |
| S3 / SQS                | `github.com/aws/aws-sdk-go-v2`        |
| Parquet                 | `github.com/parquet-go/parquet-go`    |
| Redis (feature store)   | `github.com/redis/go-redis/v9`        |
| Prometheus metrics      | `github.com/prometheus/client_golang` |
| HTTP routing            | stdlib `net/http`                     |
| ONNX inference          | `github.com/yalue/onnxruntime_go`     |
| Concurrency             | `golang.org/x/sync/errgroup`          |
| Workflow orchestration  | `go.temporal.io/sdk`                  |

### Where Go fits naturally

The ingestion workers, Kafka consumers, the inference API, and the orchestration layer are all excellent Go. Goroutines make concurrent file processing and multi-partition Kafka consumption clean and explicit. The compiled binary + distroless Docker image story is far simpler than Python for deployment.

### Where to plan carefully

For **model training**, Go has no scikit-learn or PyTorch equivalent. The practical pattern is to keep training in Python and use Go only for the pipeline that feeds training data and serves trained models. Your Go inference service loads an ONNX-exported model — a format that most Python training frameworks (scikit-learn via `skl2onnx`, PyTorch, TensorFlow) can export to.

For **the feature store**, Feast is Python-native. Options: run Feast's materialization jobs as a Docker sidecar and interact with the online store (Redis) directly from Go, or implement a lightweight custom feature store backed by Redis yourself — a reasonable choice for a learning project.

### Concurrency patterns you'll use repeatedly

```go
// Fan-out: process N files concurrently, collect errors
g, ctx := errgroup.WithContext(ctx)
for _, file := range files {
    file := file
    g.Go(func() error {
        return processFile(ctx, file)
    })
}
if err := g.Wait(); err != nil {
    return err
}

// Worker pool: bounded concurrency via buffered channel
jobs := make(chan File, 100)
for i := 0; i < workerCount; i++ {
    go func() {
        for f := range jobs {
            processFile(ctx, f)
        }
    }()
}
```

---

## Non-Functional Requirements

| Concern                  | Target                                         |
| ------------------------ | ---------------------------------------------- |
| File processing latency  | < 5 min from landing to processed storage      |
| Online feature retrieval | < 10ms p99                                     |
| Inference endpoint       | < 50ms p99 at 100 req/s                        |
| Pipeline availability    | Zero data loss on any single component failure |
| Backfill speed           | Re-process 30 days of data in < 2 hours        |

---

## Local Dev Setup

```bash
# Set environment variables
set -a && source .env && set +a

# Start all infrastructure
make up

# Initialize MinIO buckets and notifications
make init

# Build all Go services — this is a compile check only. There are two
# independent main packages in this repo (ingestion/file/cmd and
# streaming/kafka/cmd/processed-consumer), so `go build ./...` discards the
# resulting binaries instead of writing them to disk (that's documented Go
# behavior whenever a build pattern matches more than one package, not a
# bug). See "Run the services" below to actually get something running.
go build ./...

# Verify Kafka is healthy (container name is <compose-project>-kafka-1, e.g. infra-kafka-1)
docker exec -it infra-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list

# Run the services. `go run ./...` will NOT work here either — go run
# refuses to run a pattern that matches multiple main packages. Run each
# service in its own terminal (or background with &):

# terminal 1 — the ingestion service (S3/HTTP adapters, worker, Kafka producer)
go run ./ingestion/file/cmd

# terminal 2 — smoke-test consumer that logs files.processed events
go run ./streaming/kafka/cmd/processed-consumer

# Or build real binaries once and run those instead of `go run`:
#   go build -o bin/ingestion-file ./ingestion/file/cmd
#   go build -o bin/processed-consumer ./streaming/kafka/cmd/processed-consumer
#   ./bin/ingestion-file &
#   ./bin/processed-consumer &

# Upload a test file to MinIO landing zone (via the minio container's mc client —
# no aws/mc CLI required on the host) — do this after the ingestion service
# above is running, otherwise the file just sits in the landing bucket unread
docker cp tests/fixtures/sample.csv infra-minio-1:/tmp/sample.csv
docker exec infra-minio-1 mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec infra-minio-1 mc cp /tmp/sample.csv \
  "local/landing/source=test/date=$(date -u +%Y-%m-%d)/sample.csv"

# Watch the file manifest
psql -U postgres -c \
  "SELECT path, status, content_hash FROM file_manifest ORDER BY created_at DESC LIMIT 10;"

# Run all tests
go test ./... -race -timeout 120s

# Profile the ingestion worker
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
```

---

## Repository Structure

```
.
├── ingestion/
│   ├── file/                     # File-based ingestion package (not a flat ingestion/ package)
│   │   ├── cmd/
│   │   │   └── main.go           # Wires config, adapters, shared mux, DB pool, Kafka client, worker
│   │   ├── adapter.go            # IngestionsAdapter interface, Runner interface, RawEvent
│   │   ├── s3.go                 # S3/MinIO adapter (Register + webhook handler)
│   │   ├── http.go                # HTTP push adapter (Register + POST /ingest/events/{source})
│   │   ├── worker.go              # Fan-in merge, sequential process() loop, stuck-entry recovery
│   │   ├── parse.go               # Seam: translates RawEvent → parser.Input → []Record
│   │   ├── validator.go           # Size bounds, magic bytes, content-hash check
│   │   ├── detector.go            # Unused Runner-shaped stub, not wired into main.go
│   │   ├── store.go               # Write Parquet (Snappy) to processed bucket, multi-part chunking
│   │   ├── kafka.go               # Publish processed event to Kafka
│   │   ├── manifest.go            # ManifestEntry, Manifest struct, DB operations
│   │   ├── runmigrations.go       # Runs SQL migrations from ingestion/migrations/ on startup
│   │   └── parser/
│   │       ├── parser.go          # Parser interface, Record type, For() factory
│   │       ├── csv.go
│   │       ├── parquet.go
│   │       └── ndjson.go
│   ├── migrations/
│   │   └── 001_create_file_manifest.up.sql
│   └── test/
│       └── integration_test.go   # Real E2E test: upload → MinIO notify → manifest done → processed object exists
├── internal/
│   ├── config/                   # ConfigFromEnv, requireEnv, getEnv — shared typed config struct
│   └── logging/                  # slog setup (JSON output, service name, log level)
├── streaming/
│   └── kafka/
│       ├── consumer.go            # Generic Consumer: manual offset commits, one goroutine per partition
│       └── cmd/
│           └── processed-consumer/
│               └── main.go        # Smoke-test consumer: logs files.processed events
├── processing/
│   ├── features/                 # empty — Feature interface + transforms (Phase 5)
│   └── compute/                  # empty — worker pool jobs (Phase 6)
├── storage/
│   ├── lakehouse/                 # empty (Phase 7)
│   ├── feature_store/             # empty (Phase 7)
│   └── registry/                  # empty (Phase 7)
├── serving/
│   ├── inference/                 # empty (Phase 8)
│   └── batch/                     # empty (Phase 8)
├── monitoring/
│   ├── dashboards/                 # empty (Phase 10)
│   └── alerts/                     # empty (Phase 10)
├── infra/
│   ├── docker-compose.yml
│   ├── Makefile
│   ├── kafka/
│   │   └── create-topics.sh        # Creates files.raw, files.processed, cdc.events, dead-letter
│   └── debezium/                    # empty — Debezium connector config, stretch goal (Phase 3)
├── tests/
│   ├── fixtures/
│   │   └── sample.csv                # Sample fixture for manual dev testing (id, name, score, category)
│   └── integration/                 # empty placeholder — real integration test lives at ingestion/test/, which uses its own inline payload rather than this fixture
├── ADR/                              # empty — no decision records written yet
└── README.md
```

Note: there is no `orchestration/` directory yet (Phase 9 hasn't started). `ingestion/` is not flat — it's a parent directory holding one subpackage per source type (`file/` today, `cdc/` if Phase 3 is ever picked up).

---

## Resources

**Go-specific**

- [franz-go Kafka client examples](https://github.com/twmb/franz-go/tree/master/examples)
- [pgx Postgres driver docs](https://pkg.go.dev/github.com/jackc/pgx/v5)
- [Temporal Go SDK quickstart](https://docs.temporal.io/develop/go)
- [ONNX Runtime Go bindings](https://github.com/yalue/onnxruntime_go)
- [Go pprof profiling guide](https://pkg.go.dev/net/http/pprof)

**Distributed systems fundamentals**

- [Designing Data-Intensive Applications — Kleppmann](https://dataintensive.net/) ← read this first if you haven't
- [Kafka: The Definitive Guide (free PDF)](https://www.confluent.io/resources/kafka-the-definitive-guide/)
- [Debezium Postgres Connector Docs](https://debezium.io/documentation/reference/connectors/postgresql.html)

**Storage**

- [Delta Lake Getting Started](https://docs.delta.io/latest/quick-start.html)
- [Apache Iceberg spec](https://iceberg.apache.org/spec/)

**ML infrastructure**

- [Feast Feature Store Docs](https://docs.feast.dev/)
- [MLflow Tracking Guide](https://mlflow.org/docs/latest/tracking.html)
- [ONNX model export guides](https://onnx.ai/sklearn-onnx/)
