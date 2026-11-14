# NY Taxi DTC - Implementation Plan

## Project Overview

**Goal**: Predict taxi fare and dynamic surge multiplier based on real-time and historical NYC trip data
**Tech Stack**: PySpark (Batch & Streaming), Kafka, Redis, PostgreSQL, MongoDB, FastAPI, MLflow, Airflow, GCS, Terraform, H3 Spatial Grid

---

## 1. Data Architecture

```
[Real-Time Source] -> Python Producer -> Kafka -> PySpark Streaming (H3 Aggregation) -> Redis -> FastAPI
[Historical Source] -> GCS (Raw) -> PySpark Batch -> PostgreSQL (DW) / MongoDB (Feature Store) -> MLflow / FastAPI
```

### Component Responsibilities

| Component      | Role                                                                  |
| -------------- | --------------------------------------------------------------------- |
| **GCS**        | Raw data storage (Data Lake)                                          |
| **Kafka**      | Message broker for simulated real-time trip streams                   |
| **PySpark**    | Batch processing & Structured Streaming (spatial/window aggregations) |
| **Redis**      | In-memory cache for real-time demand/supply states                    |
| **PostgreSQL** | Data warehouse for historical reporting and BI                        |
| **MongoDB**    | Feature Store for historical grid baseline features                   |
| **FastAPI**    | Real-time serving API for dynamic fare & surge prediction             |
| **MLflow**     | Model registry and experiment tracking                                |
| **Airflow**    | Workflow orchestration for batch ETL and model retraining             |
| **Terraform**  | Infrastructure as Code                                                |

---

## 2. Implementation Phases

### Phase 1: Infrastructure (Terraform)

**Goal**: Provision GCS, Kafka, PostgreSQL, MongoDB, and Redis instances

**Tasks**:

- [ ] Configure GCS bucket & service account permissions
- [ ] Setup Kafka (local docker-compose or Cloud-based)
- [ ] Spin up PostgreSQL, MongoDB, and Redis
- [ ] Configure networking and security groups

**Files**:

- `terraform/main.tf`
- `terraform/variables.tf`
- `terraform/outputs.tf`

---

### Phase 2: Data Ingestion & Streaming Simulation

**Goal**: Support batch ingestion to GCS and real-time Kafka streaming

**Tasks**:

- [ ] Refactor batch ingestion to pull historical NYC Taxi Parquet files to GCS
- [ ] Implement `src/pipeline/kafka_producer.py` to stream historical trips into Kafka topic `nyc-taxi-trips` sorted by pickup timestamp (simulating real-time)

**New files**:

- `src/pipeline/ingestion.py`
- `src/pipeline/kafka_producer.py`

---

### Phase 3: Processing Layer (PySpark Batch & Streaming)

**Goal**: Clean, transform, and aggregate data using spatial grids (H3)

**Tasks**:

- [ ] PySpark Batch: Clean historical data, map coordinates to H3 spatial grid, calculate route baselines (rush hour, weather factors)
- [ ] PySpark Streaming: Read from Kafka, use sliding windows (e.g., 10 mins window, 1 min slide) to calculate real-time Demand and Supply per `h3_index`

**New files**:

- `src/pipeline/processing_batch.py`
- `src/pipeline/processing_stream.py`

---

### Phase 4: Data Loading & Storage

**Goal**: Save real-time and batch outputs to appropriate storage engines

**Tasks**:

- [ ] Stream writer: Write real-time demand/supply metrics per H3 grid directly to Redis with TTL
- [ ] Batch writer: Load processed historical features into PostgreSQL and MongoDB (Feature Store)

**New files**:

- `src/pipeline/loading.py`

---

### Phase 5: ML Pipeline & Serving (FastAPI & MLflow)

**Goal**: Build a dynamic pricing model and serve it via a real-time API

**Tasks**:

- [ ] Train XGBoost/LightGBM model using H3 grid features and surge multipliers
- [ ] Register model in MLflow Model Registry
- [ ] Build FastAPI serving layer:
  - Input: `(latitude, longitude, datetime)`
  - Resolve coordinates to H3 cell ID
  - Query Redis for real-time demand/supply
  - Query MongoDB/Postgres for historical features
  - Feed features to ML model and return calculated fare + surge multiplier

**New files**:

- `src/ml/training.py`
- `src/ml/serving_api.py`

---

### Phase 6: Airflow DAGs

**Goal**: Orchestrate daily batch updates and model retraining

**Tasks**:

- [ ] Create batch ETL DAG (GCS -> PySpark -> Postgres/Mongo)
- [ ] Create ML training DAG (retrain model weekly/daily, register to MLflow)

**New files**:

- `dags/batch_etl_dag.py`
- `dags/ml_retraining_dag.py`

---

### Phase 7: Testing, Monitoring & CI/CD

**Goal**: Ensure quality, reliability, and deployment safety

**Tasks**:

- [ ] Unit tests for coordinate-to-H3 conversion and pricing logic
- [ ] Integration tests for end-to-end flow (Kafka -> Spark -> Redis -> FastAPI)
- [ ] Setup Grafana dashboard monitoring Kafka lag, Redis memory, and FastAPI response time

---

## 3. Spatial and Validation Details

### H3 Grid Resolution

- Target resolution: **Resolution 7** (approx. 1.2 km edge length) or **Resolution 8** (approx. 460 m edge length) for urban NYC areas.

### Dynamic Pricing Surge Formula (Conceptual)

```
Surge Multiplier = 1.0 + Max(0, (Demand - Supply) / (Demand + Supply)) * Alpha
```

_Where Alpha is a sensitivity coefficient (e.g., 0.5)._

---

## 4. Directory Structure

```
ny-taxi-dtc/
├── src/
│   ├── pipeline/
│   │   ├── __init__.py
│   │   ├── ingestion.py
│   │   ├── kafka_producer.py
│   │   ├── processing_batch.py
│   │   ├── processing_stream.py
│   │   └── loading.py
│   ├── ml/
│   │   ├── __init__.py
│   │   ├── training.py
│   │   └── serving_api.py
│   └── utils/
├── terraform/
├── dags/
├── tests/
├── scripts/
├── main.py
├── plan.md
└── pyproject.toml
```
