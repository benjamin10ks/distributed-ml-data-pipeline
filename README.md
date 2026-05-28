# Distributed ML Data Pipeline

A distributed, production-grade ML data pipeline covering ingestion, streaming, feature engineering, storage, and model serving. Built as a summer learning project in **Go**, touching systems design, distributed computing, data engineering, and ML infrastructure.

> **Language note**: The pipeline services (ingestion workers, Kafka consumers/producers, inference API, orchestration) are written in Go. External infrastructure that runs as its own process — Debezium, MLflow, Feast — is language-agnostic and communicates over HTTP/gRPC. The distributed compute layer uses Go-native workers rather than Spark/Ray. See [Go-specific considerations](#go-specific-considerations) below.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                      │
│  APIs / Webhooks   File Drops    DB Change Streams   IoT    │
│  (REST, gRPC)      (S3, GCS)     (CDC, Debezium)   (MQTT)  │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                    STREAMING LAYER                          │
│          Kafka · Pulsar · Kinesis                           │
│     Partitioned, durable, replayable event bus             │
└──────────┬──────────────┬──────────────┬────────────────────┘
           │              │              │
┌──────────▼──┐   ┌───────▼──────┐  ┌───▼─────────────────────┐
│  Validate   │   │   Feature    │  │  Distributed Compute    │
│  + Filter   │   │  Engineering │  │  (Spark, Ray, Dask)     │
└──────────┬──┘   └───────┬──────┘  └───┬─────────────────────┘
           └──────────────┼─────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                      STORAGE LAYER                          │
│   Feature Store        Data Lakehouse      Model Registry   │
│   (Feast, Tecton)    (Parquet, Delta)    (MLflow, DVC)      │
└──────────┬──────────────┬──────────────┬────────────────────┘
           │              │              │
┌──────────▼──┐   ┌───────▼──────┐  ┌───▼─────────────────────┐
│  Real-time  │   │    Batch     │  │      Monitoring         │
│  Inference  │   │  Prediction  │  │   (Drift, Data Quality) │
└─────────────┘   └──────────────┘  └─────────────────────────┘
```

---

## Build Checklist

### Phase 1 — Foundation & Infrastructure

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

### Phase 2 — File-Based Ingestion

> Implement the file ingestion pipeline: landing zone → manifest → validate → parse → processed storage.

- [x] Create landing zone bucket structure: `source={name}/date={YYYY-MM-DD}/`
- [x] Implement **file manifest** table in Postgres (columns: `path`, `content_hash`, `source`, `status`, `created_at`, `processed_at`)
- [x] MinIO event notifications → webhook → buffered channel (implemented)
- [ ] Stretch: replace buffered channel with ElasticMQ for production-style durability
- [ ] Build arrival detection worker (polls queue, deduplicates via manifest hash check) — use `aws-sdk-go-v2` for S3/SQS
- [ ] Implement validation checks: file size bounds, format sniffing, checksum verification (`crypto/sha256` stdlib)
- [ ] Build CSV parser with chunking (target 128MB chunks) — use `encoding/csv` stdlib, handle quoted fields
- [ ] Build Parquet parser (read row groups independently for parallelism) — use `github.com/parquet-go/parquet-go`
- [ ] Add NDJSON support using `encoding/json` with a streaming decoder (`json.NewDecoder` + loop)
- [ ] Write parsed output to processed storage as Snappy-compressed Parquet, 128–256MB files
- [ ] Implement quarantine bucket + alerting for validation failures
- [ ] Update manifest status on success / failure / quarantine — use `pgx/v5` for Postgres
- [ ] Write integration test: upload a file, assert it appears in processed storage with correct hash

---

### Phase 3 — CDC Ingestion

> Stream database changes into Kafka using Debezium and logical replication.

- [ ] Enable logical replication on Postgres (`wal_level = logical`)
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

---

### Phase 4 — Streaming Layer (Kafka)

> Harden the event bus: producers, consumers, schema enforcement, and observability.

- [ ] Choose Go Kafka client: `github.com/twmb/franz-go` (recommended, full-featured) or `confluent-kafka-go` (CGO, closer to librdkafka)
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
- [ ] Write a backfill job that recomputes features over historical Parquet files using a worker pool (use `errgroup` from `golang.org/x/sync`)
- [ ] Benchmark transform throughput (target: process 1M rows/minute on a single node)

---

### Phase 6 — Distributed Compute

> Scale processing beyond a single node using Go-native worker pools and Kafka partitioning.

> **Go note**: Rather than Spark or Ray (JVM/Python ecosystems), Go-native distribution uses Kafka partitions as the work distribution mechanism — each worker consumes one or more partitions concurrently. This is a cleaner mental model and performs well for moderate data volumes (< 10TB/day).

- [ ] Implement a `WorkerPool` using goroutines + a buffered channel as the job queue — configurable concurrency via `WORKER_COUNT` env var
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

- [ ] Set up MLflow tracking server (backed by Postgres for metadata, MinIO for artifacts)
- [ ] Log a dummy model run: params, metrics, and a serialized model artifact
- [ ] Implement model versioning: promote a model from `Staging` to `Production`
- [ ] Write a model loader that fetches the current `Production` model by name at startup

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

## Key Design Decisions to Document

As you build, record why you made each of these choices in an `ADR/` (Architecture Decision Records) folder:

- [ ] **Message format**: Avro vs Protobuf vs JSON — and why
- [ ] **Compute approach**: Kafka-partition-based workers vs external Spark cluster — and why
- [ ] **Feature store**: Feast vs Tecton vs custom Redis-backed store — and why
- [ ] **Orchestrator**: Temporal vs Airflow — and why
- [ ] **Inference runtime**: ONNX Runtime in-process vs external model server (TF Serving/Triton) — and why
- [ ] **Exactly-once semantics**: how you achieve it (or why you settled for at-least-once + idempotency)
- [ ] **Training-serving skew**: how your architecture prevents it

---

## Go-Specific Considerations

### Key libraries

| Concern                 | Library                               |
| ----------------------- | ------------------------------------- |
| Kafka producer/consumer | `github.com/twmb/franz-go`            |
| Postgres                | `github.com/jackc/pgx`                |
| S3 / SQS                | `github.com/aws/aws-sdk-go-v2`        |
| Parquet                 | `github.com/parquet-go/parquet-go`    |
| Redis (feature store)   | `github.com/redis/go-redis/v9`        |
| Prometheus metrics      | `github.com/prometheus/client_golang` |
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
    file := file // capture loop variable
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
| CDC event latency        | < 1 second end-to-end                          |
| Online feature retrieval | < 10ms p99                                     |
| Inference endpoint       | < 50ms p99 at 100 req/s                        |
| Pipeline availability    | Zero data loss on any single component failure |
| Backfill speed           | Re-process 30 days of data in < 2 hours        |

---

## Local Dev Setup

```bash

# Set environment variables from .env file
set -a
source .env
set +a

# Start all infrastructure
make up

# Initialize MinIO buckets
make init

# Build all Go services
go build ./...

# Verify Kafka is healthy
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Verify Postgres replication slot (for CDC)
psql -U postgres -c "SELECT slot_name, active FROM pg_replication_slots;"

# Upload a test file to MinIO landing zone
aws s3 cp tests/fixtures/sample.csv s3://landing/source=test/date=2026-05-17/ \
  --endpoint-url http://localhost:9000

# Watch the file manifest
psql -U postgres -c "SELECT path, status, content_hash FROM file_manifest ORDER BY created_at DESC LIMIT 10;"

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
│   ├── file/           # File-based ingestion worker (Go)
│   │   ├── cmd/main.go
│   │   ├── detector.go
│   │   ├── manifest.go
│   │   ├── parser/
│   │   │   ├── csv.go
│   │   │   ├── parquet.go
│   │   │   └── ndjson.go
│   │   └── validator.go
│   └── cdc/            # Debezium config + CDC event consumer (Go)
├── streaming/
│   └── kafka/          # Producer/consumer base types, topic configs (Go)
├── processing/
│   ├── features/       # Feature interface + transform implementations (Go)
│   └── compute/        # Worker pool jobs (Go)
├── storage/
│   ├── lakehouse/      # Parquet/Delta write helpers (Go)
│   ├── feature_store/  # Redis online store client (Go) + Feast config (Python)
│   └── registry/       # MLflow REST client (Go)
├── serving/
│   ├── inference/      # net/http inference service + ONNX loader (Go)
│   └── batch/          # Batch prediction worker pool (Go)
├── orchestration/
│   └── workflows/      # Temporal workflow and activity definitions (Go)
├── monitoring/
│   ├── dashboards/     # Grafana dashboard JSON exports
│   └── alerts/         # Prometheus alerting rules
├── infra/
│   ├── docker-compose.yml
│   └── Makefile
├── tests/
│   ├── fixtures/       # Sample CSV, Parquet, NDJSON files
│   └── integration/    # End-to-end pipeline tests (Go)
├── ADR/                # Architecture Decision Records
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
