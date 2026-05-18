# Distributed ML Data Pipeline

A distributed, production-grade ML data pipeline covering ingestion, streaming, feature engineering, storage, and model serving. Built as a summer learning project touching systems design, distributed computing, data engineering, and ML infrastructure.

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

- [ ] Set up monorepo structure (`ingestion/`, `streaming/`, `processing/`, `storage/`, `serving/`, `infra/`)
- [ ] Write `docker-compose.yml` with Kafka, Zookeeper, Postgres, and MinIO (local S3)
- [ ] Configure Kafka topics with appropriate partition counts and retention settings
- [ ] Stand up Confluent Schema Registry (or use the Redpanda bundled one)
- [ ] Write a `Makefile` with `up`, `down`, `logs`, `reset` targets
- [ ] Add `.env.example` with all required environment variables documented
- [ ] Set up a shared `config/` module that all services read from

---

### Phase 2 — File-Based Ingestion

> Implement the file ingestion pipeline: landing zone → manifest → validate → parse → processed storage.

- [ ] Create landing zone bucket structure: `source={name}/date={YYYY-MM-DD}/`
- [ ] Implement **file manifest** table in Postgres (columns: `path`, `content_hash`, `source`, `status`, `created_at`, `processed_at`)
- [ ] Wire S3/MinIO event notifications → SQS-compatible queue on file upload
- [ ] Build arrival detection worker (polls queue, deduplicates via manifest hash check)
- [ ] Implement validation checks: file size bounds, format sniffing, checksum verification
- [ ] Build CSV parser with chunking (target 128MB chunks, handle quoted fields)
- [ ] Build Parquet parser (read row groups independently for parallelism)
- [ ] Add NDJSON support (reject standard JSON arrays, enforce newline-delimited)
- [ ] Write parsed output to processed storage as Snappy-compressed Parquet, 128–256MB files
- [ ] Implement quarantine bucket + alerting for validation failures
- [ ] Update manifest status on success / failure / quarantine
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

- [ ] Define topic partitioning strategy per source (CDC: by PK, files: by source name)
- [ ] Set retention policies per topic (raw events: 7 days, processed: 30 days)
- [ ] Implement a generic Kafka producer with retry logic and idempotent writes enabled
- [ ] Implement a generic Kafka consumer with manual offset commits (no auto-commit)
- [ ] Add consumer group lag monitoring (expose as Prometheus metrics)
- [ ] Test at-least-once delivery: kill a consumer mid-batch, verify no events are lost on restart
- [ ] Test exactly-once processing: verify idempotent writes don't produce duplicates
- [ ] Add schema evolution test: add a nullable column, verify existing consumers don't break

---

### Phase 5 — Feature Engineering

> Build the transformation layer that turns raw events into ML-ready features.

- [ ] Define a `Feature` interface / base class with `name`, `dtype`, `transform()`, `inverse_transform()`
- [ ] Implement numerical features: normalization, standardization, bucketization
- [ ] Implement categorical features: one-hot encoding, target encoding, frequency encoding
- [ ] Implement temporal features: hour-of-day, day-of-week, days-since-event, rolling windows
- [ ] Implement text features: tokenization, TF-IDF, embedding lookup
- [ ] Build a `FeaturePipeline` that chains transforms and is serializable (pickle / ONNX)
- [ ] **Critical**: verify training-time and serving-time transforms produce identical output for the same input (training-serving skew test)
- [ ] Add data type validation at pipeline boundaries (reject unexpected nulls, out-of-range values)
- [ ] Write a backfill job that recomputes features over historical data using Spark or Ray
- [ ] Benchmark transform throughput (target: process 1M rows/minute on a single node)

---

### Phase 6 — Distributed Compute

> Scale processing beyond a single node using a distributed compute framework.

- [ ] Choose framework: **Spark** (batch-first, mature) or **Ray** (Python-native, good for ML)
- [ ] Set up local cluster (Spark standalone or Ray local mode for dev)
- [ ] Port the CSV chunking parser to run as a Spark job (`spark.read.csv` with schema)
- [ ] Port feature engineering pipeline to run as a Spark DataFrame transform
- [ ] Implement data partitioning strategy for compute jobs (partition by date, repartition before shuffle-heavy ops)
- [ ] Add job checkpointing so failed jobs can resume from last good partition
- [ ] Write a job that joins CDC events with file-ingested data (tests distributed join correctness)
- [ ] Profile and eliminate data skew (check partition size distribution after any `groupBy`)

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

- [ ] Build a FastAPI inference service that loads a model from the registry at startup
- [ ] Add a `/predict` endpoint: fetches features from feature store, runs inference, returns result
- [ ] Add a `/health` endpoint that checks model loaded, feature store reachable, and model version
- [ ] Containerize the service: write `Dockerfile`, verify cold-start time < 5s
- [ ] Implement a batch prediction job: reads from lakehouse, writes predictions back to a results table
- [ ] Load test the inference endpoint (target: 100 req/s at < 50ms p99 latency on a single container)
- [ ] Add a canary deployment pattern: route 5% of traffic to a new model version, compare metrics

---

### Phase 9 — Orchestration

> Wire the pipeline together with a scheduler that handles retries, dependencies, and backfills.

- [ ] Choose orchestrator: **Airflow** (mature, large ecosystem) or **Prefect** (simpler Python API)
- [ ] Define a DAG / Flow for the nightly file ingestion run
- [ ] Define a DAG for feature materialization (depends on ingestion DAG completing)
- [ ] Define a DAG for model retraining (depends on feature materialization)
- [ ] Implement retry policy: 3 retries with exponential backoff, alert on final failure
- [ ] Implement backfill: re-run the ingestion DAG for a specific date range
- [ ] Add SLA monitoring: alert if a DAG hasn't completed by a deadline time

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
- [ ] **Compute framework**: Spark vs Ray vs Dask — and why
- [ ] **Feature store**: Feast vs Tecton vs custom — and why
- [ ] **Orchestrator**: Airflow vs Prefect vs Dagster — and why
- [ ] **Exactly-once semantics**: how you achieve it (or why you settled for at-least-once + idempotency)
- [ ] **Training-serving skew**: how your architecture prevents it

---

## Non-Functional Requirements

| Concern | Target |
|---|---|
| File processing latency | < 5 min from landing to processed storage |
| CDC event latency | < 1 second end-to-end |
| Online feature retrieval | < 10ms p99 |
| Inference endpoint | < 50ms p99 at 100 req/s |
| Pipeline availability | Zero data loss on any single component failure |
| Backfill speed | Re-process 30 days of data in < 2 hours |

---

## Local Dev Setup

```bash
# Start all infrastructure
make up

# Verify Kafka is healthy
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Verify Postgres replication slot (for CDC)
psql -U postgres -c "SELECT slot_name, active FROM pg_replication_slots;"

# Upload a test file to MinIO landing zone
aws s3 cp tests/fixtures/sample.csv s3://landing/source=test/date=2026-05-17/ \
  --endpoint-url http://localhost:9000

# Watch the file manifest
psql -U postgres -c "SELECT path, status, content_hash FROM file_manifest ORDER BY created_at DESC LIMIT 10;"
```

---

## Repository Structure

```
.
├── ingestion/
│   ├── file/           # File-based ingestion workers
│   └── cdc/            # Debezium config and CDC consumers
├── streaming/
│   └── kafka/          # Producer/consumer base classes, topic configs
├── processing/
│   ├── features/       # Feature definitions and transform pipelines
│   └── compute/        # Spark / Ray jobs
├── storage/
│   ├── lakehouse/      # Delta Lake table definitions and compaction jobs
│   ├── feature_store/  # Feast feature views and materialization jobs
│   └── registry/       # MLflow experiment and model management
├── serving/
│   ├── inference/      # FastAPI inference service
│   └── batch/          # Batch prediction jobs
├── orchestration/
│   └── dags/           # Airflow DAGs or Prefect flows
├── monitoring/
│   ├── dashboards/     # Grafana dashboard JSON exports
│   └── alerts/         # Prometheus alerting rules
├── infra/
│   ├── docker-compose.yml
│   └── Makefile
├── tests/
│   ├── fixtures/       # Sample files for integration tests
│   └── integration/    # End-to-end pipeline tests
├── ADR/                # Architecture Decision Records
└── README.md
```

---

## Resources

- [Debezium Postgres Connector Docs](https://debezium.io/documentation/reference/connectors/postgresql.html)
- [Delta Lake Getting Started](https://docs.delta.io/latest/quick-start.html)
- [Feast Feature Store Docs](https://docs.feast.dev/)
- [MLflow Tracking Guide](https://mlflow.org/docs/latest/tracking.html)
- [Kafka: The Definitive Guide (free PDF)](https://www.confluent.io/resources/kafka-the-definitive-guide/)
- [Designing Data-Intensive Applications — Kleppmann](https://dataintensive.net/) ← read this first if you haven't
