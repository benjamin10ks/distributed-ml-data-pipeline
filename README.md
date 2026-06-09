# Distributed ML Data Pipeline

A distributed, production-grade ML data pipeline covering ingestion, streaming, feature engineering, storage, and model serving. Built as a summer learning project in **Go**, touching systems design, distributed computing, data engineering, and ML infrastructure.

> **Language note**: The pipeline services (ingestion workers, Kafka consumers/producers, inference API, orchestration) are written in Go. External infrastructure that runs as its own process — MLflow, Feast — is language-agnostic and communicates over HTTP/gRPC. The distributed compute layer uses Go-native workers rather than Spark/Ray. See [Go-specific considerations](#go-specific-considerations) below.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                      │
│  HTTP Push         File Drops        Kafka Consumer         │
│  (POST /ingest)    (MinIO/S3)        (upstream topics)      │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                    STREAMING LAYER                          │
│                    Kafka (franz-go)                         │
│          Partitioned, durable, replayable event bus         │
└──────────┬──────────────┬──────────────┬────────────────────┘
           │              │              │
┌──────────▼──┐   ┌───────▼──────┐  ┌───▼─────────────────────┐
│  Validate   │   │   Feature    │  │  Distributed Compute    │
│  + Filter   │   │  Engineering │  │  (Go worker pools)      │
└──────────┬──┘   └───────┬──────┘  └───┬─────────────────────┘
           └──────────────┼─────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                      STORAGE LAYER                          │
│   Feature Store        Data Lakehouse      Model Registry   │
│   (Redis + Feast)    (Parquet, Delta)      (MLflow)         │
└──────────┬──────────────┬──────────────┬────────────────────┘
           │              │              │
┌──────────▼──┐   ┌───────▼──────┐  ┌───▼─────────────────────┐
│  Real-time  │   │    Batch     │  │      Monitoring         │
│  Inference  │   │  Prediction  │  │   (Drift, Prometheus)   │
│  (ONNX)     │   │              │  │                         │
└─────────────┘   └──────────────┘  └─────────────────────────┘
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

- [x] Define `IngestionAdapter` interface (`Register(mux)` + `Events() <-chan RawEvent`) in `ingestion/adapter.go`
- [x] Define `RawEvent` envelope type (`Source`, `Payload`, `Format`, `Path`, `ContentHash`, `Size`, `ReceivedAt`, `Metadata`)
- [x] Define optional `Runner` interface for adapters that need a background goroutine (CDC would use this if introduced)
- [x] Build S3 adapter (`ingestion/s3.go`) — registers `POST /minio/events`, downloads object, hashes with SHA-256 via `TeeReader`, emits `RawEvent`
- [x] Wire MinIO webhook notifications → shared `http.Server` → S3 adapter handler
- [x] Build HTTP adapter (`ingestion/http.go`) — registers `POST /ingest/events/{source}`, writes payload to landing bucket, emits `RawEvent`
- [x] Share a single `http.Server` and `ServeMux` across all adapters — instantiated in `main.go`, adapters call `Register(mux)`

**Landing zone**

- [x] Create MinIO buckets: `landing`, `processed`, `quarantine` via `make init` (`mc` CLI)
- [x] Key structure: `source={name}/date={YYYY-MM-DD}/{filename}` — constructed by HTTP adapter on write, read from notification key by S3 adapter
- [x] Configure MinIO webhook notification on `s3:ObjectCreated` events → `POST /minio/events`

**File manifest**

- [x] Create `file_manifest` table (`path`, `content_hash`, `source`, `status`, `created_at`, `processed_at`)
- [x] Partial index on `status` for `pending` and `processing` rows only
- [x] `Manifest` struct in `ingestion/manifest.go` with `Insert`, `GetByHash`, `UpdateStatus`, `GetStuck`
- [x] Status constants: `pending → processing → done | failed | quarantined`
- [x] `GetStuck` queries entries in `processing` older than a threshold — used on restart to recover crashed workers
- [x] DB connection pool (`pgx/v5`) opened in `main.go`, injected into `Manifest` and `Worker`

**Worker**

- [x] `Worker` in `ingestion/worker.go` merges all adapter channels into one stream via a fan-in goroutine per adapter
- [x] Sequential `process()` loop for now — Phase 6 replaces with bounded `errgroup` worker pool
- [x] `process()` sequence: idempotency check → manifest insert → `processing` → validate → parse → write processed → `done` → publish Kafka event
- [x] Kafka publish failure does not fail the operation — file is safely stored, publish retried via manifest query
- [x] Bad files log and continue — one quarantined file does not stop the pipeline

**Parsers**

- [x] `Parser` interface (`Parse(io.Reader) ([]Record, error)`) in `ingestion/parser/parser.go`
- [x] `Record` type: `map[string]any` — preserves native types, coercion deferred to feature engineering
- [x] `For(format string) (Parser, error)` factory — dispatches on `"csv"`, `"parquet"`, `"ndjson"`
- [x] CSV parser (`ingestion/parser/csv.go`) — `encoding/csv` stdlib, `ReuseRecord` for reduced allocations, copies header row
- [x] NDJSON parser (`ingestion/parser/ndjson.go`) — `json.NewDecoder` streaming loop
- [x] Parquet parser (`ingestion/parser/parquet.go`) — `parquet-go`, reads in 128-row chunks via `GenericReader`
- [x] `parse.go` in `ingestion/` — seam between `RawEvent` and parser package; only file that knows both types
- [x] Format detection (`detectFormat`) — magic bytes take precedence over file extension

**Still to do**

- [x] `validate.go` — size bounds, magic byte verification, optional checksum sidecar check
- [x] `store.go` — write parsed records to processed bucket as Snappy-compressed Parquet, 128–256MB target file size
- [x] `kafka.go` — publish lightweight processed event (path + metadata, not file contents) to `files.processed` topic
- [x] `publishProcessed` and `writeProcessed` implementations called by worker
- [x] `mustOpenDB` implementation in `main.go`
- [x] Quarantine: copy original file to quarantine bucket, write reason metadata, log alert
- [x] `GetStuck` recovery: on startup query manifest for stuck `processing` entries, requeue them
- [ ] Stretch: replace buffered channel with ElasticMQ for production-style durability
- [x] Integration test: upload a file end-to-end, assert it appears in processed bucket with correct hash and manifest status `done`

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
- [ ] Add `cdc.go` to `ingestion/` implementing the `Runner` interface (no HTTP routes — background goroutine only)

---

### Phase 4 — Streaming Layer (Kafka)

> Harden the event bus: producers, consumers, schema enforcement, and observability.

- [x] Kafka client chosen: `github.com/twmb/franz-go` (pure Go, no CGO)
- [ ] Define topic partitioning strategy per source (CDC: by PK, files: by source name)
- [ ] Set retention policies per topic (raw events: 7 days, processed: 30 days)
- [ ] Implement a generic Kafka producer with retry logic and idempotent writes enabled
- [ ] Implement a generic Kafka consumer with manual offset commits (no auto-commit)
- [ ] Use goroutines for concurrent partition consumption — one goroutine per partition is idiomatic
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
- [ ] Add a canary deployment pattern: route 5% of traffic to a new model version via a weighted round-robin in the handler

---

### Phase 9 — Orchestration

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

| Decision                     | Choice                                                                  | Status     |
| ---------------------------- | ----------------------------------------------------------------------- | ---------- |
| Kafka client                 | `franz-go` (pure Go, no CGO)                                            | ✅ decided |
| Postgres client              | `pgx/v5`                                                                | ✅ decided |
| S3 client                    | `aws-sdk-go-v2`                                                         | ✅ decided |
| Parquet library              | `parquet-go`                                                            | ✅ decided |
| HTTP routing                 | stdlib `net/http` + `ServeMux`                                          | ✅ decided |
| Adapter pattern              | `Register(mux)` + shared server                                         | ✅ decided |
| Runner interface             | Defined in adapter.go; CDC would use it if an upstream DB is introduced | ✅ decided |
| Ingestion package layout     | Flat `ingestion/` package                                               | ✅ decided |
| Infra config (Debezium etc.) | `infra/debezium/` not `ingestion/` — ready if CDC is introduced         | ✅ decided |
| Compute framework            | Go-native worker pools, not Spark/Ray                                   | ✅ decided |
| CDC / Debezium               | Deferred — no upstream operational DB; manifest DB is internal only     | ⏳ stretch |
| Orchestrator                 | Temporal (pending Phase 9)                                              | ⏳ pending |
| Inference runtime            | ONNX in-process vs gRPC sidecar                                         | ⏳ pending |
| Feature store                | Custom Redis                                                            | ✅ decided |
| Message format               | Avro vs Protobuf vs JSON                                                | ⏳ pending |
| Exactly-once semantics       | At-least-once + idempotent manifest                                     | ✅ decided |
| Training-serving skew        | Feature pipeline serialized to JSON, same code path for train and serve | ⏳ pending |

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

# Build all Go services
go build ./...

# Verify Kafka is healthy
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Upload a test file to MinIO landing zone
aws s3 cp tests/fixtures/sample.csv \
  s3://landing/source=test/date=2026-05-17/sample.csv \
  --endpoint-url http://localhost:9000

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
├── ingestion/                  # Flat package — all ingestion code lives here
│   ├── cmd/
│   │   └── main.go             # Wires config, adapters, shared mux, worker
│   ├── adapter.go              # IngestionAdapter interface, Runner interface, RawEvent
│   ├── s3.go                   # S3/MinIO adapter (Register + webhook handler)
│   ├── http.go                 # HTTP push adapter (Register + POST /ingest/events/{source})
│   ├── cdc.go                  # CDC adapter stub — stretch goal (implements Runner, no HTTP routes)
│   ├── worker.go               # Fan-in merge, sequential process() loop
│   ├── parse.go                # Seam: translates RawEvent → parser.Input → []Record
│   ├── validate.go             # Size bounds, magic bytes, checksum — TODO
│   ├── store.go                # Write Parquet to processed bucket — TODO
│   ├── kafka.go                # Publish processed event to Kafka — TODO
│   ├── manifest.go             # ManifestEntry, Manifest struct, DB operations
│   ├── config.go               # ConfigFromEnv, requireEnv, getEnv
│   ├── migrations/
│   │   └── 001_file_manifest.sql
│   └── parser/
│       ├── parser.go           # Parser interface, Record type, For() factory
│       ├── csv.go
│       ├── parquet.go
│       └── ndjson.go
├── streaming/
│   └── kafka/                  # Generic producer/consumer base types (Phase 4)
├── processing/
│   ├── features/               # Feature interface + transforms (Phase 5)
│   └── compute/                # Worker pool jobs (Phase 6)
├── storage/
│   ├── lakehouse/              # Parquet/Delta write helpers (Phase 7)
│   ├── feature_store/          # Redis client + Feast config (Phase 7)
│   └── registry/               # MLflow REST client (Phase 7)
├── serving/
│   ├── inference/              # net/http + ONNX inference service (Phase 8)
│   └── batch/                  # Batch prediction worker pool (Phase 8)
├── orchestration/
│   └── workflows/              # Temporal workflow + activity definitions (Phase 9)
├── monitoring/
│   ├── dashboards/             # Grafana JSON exports (Phase 10)
│   └── alerts/                 # Prometheus alerting rules (Phase 10)
├── infra/
│   ├── docker-compose.yml
│   ├── Makefile
│   └── debezium/               # Debezium connector config — stretch goal (Phase 3)
├── tests/
│   ├── fixtures/               # sample.csv, sample.parquet, sample.ndjson
│   └── integration/            # End-to-end pipeline tests
├── ADR/                        # Architecture Decision Records
└── README.md
```

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
