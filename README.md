# StreamAgent

**Agentic observability for real-time data pipelines**

StreamAgent monitors streaming data pipelines for anomalies and autonomously reasons through their root causes. Unlike conventional monitoring tools that raise alerts, StreamAgent uses a LangGraph-orchestrated AI agent to classify anomalies, generate root cause hypotheses via a local LLM, and produce structured incident reports — all without human intervention or paid APIs.

---

## Demo

> Producer injecting null anomaly → PySpark detecting → LangGraph agent reasoning → incident report

```json
{
  "incident_id": "cb3454ec-b8a2-498b-b628-8f83b124aff4",
  "timestamp": "2026-05-07T04:11:02.680946",
  "anomaly_type": "null_spike",
  "severity": "high",
  "affected_columns": ["pickup_location_id", "dropoff_location_id"],
  "observed_value": 0.43,
  "expected_range": [0.0, 0.02],
  "probable_cause": "Sudden surge in null values suggests a missing or corrupt upstream ETL pipeline configuration, resulting in incomplete records being ingested.",
  "recommended_action": "Halt downstream jobs. Inspect upstream producer for schema changes or ETL failures."
}
```

---

## Architecture

```
[ Redpanda ]  →  [ PySpark Structured Streaming ]  →  [ LangGraph Agent ]
                         ↓ (on anomaly)                       ↓
               [ Statistical Monitors ]            [ Ollama / LLaMA 3 ]
               · Null rate                                    ↓
               · Z-score / distribution shift      [ Incident Report ]
               · Volume anomaly                              ↓
               · Schema drift               [ MinIO ] + [ Grafana Dashboard ]
```

### Agent reasoning flow

```
Anomaly payload → classify_node → [if medium/high] → reason_node → recommend_node → report_node → END
                                → [if low]         ↗
```

---

## Tech stack

| Layer | Tool |
|---|---|
| Message broker | Redpanda (Kafka-compatible) |
| Stream processing | PySpark Structured Streaming |
| Agent orchestration | LangGraph |
| LLM inference | Ollama + LLaMA 3 8B (local, free) |
| Object storage | MinIO (S3-compatible) |
| Metrics | Prometheus + Grafana |
| API | FastAPI |
| Demo UI | Streamlit |
| Containerisation | Docker Compose |

---

## Anomaly types detected

| Type | Detection method | Example |
|---|---|---|
| `null_spike` | Null fraction > threshold per column | Location ID nulls jump from 1% to 43% |
| `distribution_shift` | Z-score > 3 on numeric columns | Fare amount 3× historical mean |
| `volume` | Record count outside expected range | Batch size drops to zero mid-day |
| `schema_drift` | Column fingerprint vs registry | New column appears in stream |

---

## Prerequisites

- Python 3.10+
- Docker + Docker Compose
- Ollama installed locally → [ollama.com](https://ollama.com)
- 8GB RAM minimum (for LLaMA 3 8B)

---

## Quickstart

### 1. Clone the repo

```bash
git clone https://github.com/yourusername/streamagent.git
cd streamagent
```

### 2. Install Python dependencies

```bash
pip install langgraph langchain-core ollama confluent-kafka pyspark fastapi uvicorn streamlit prometheus-client
```

### 3. Pull the LLM

```bash
ollama pull llama3
ollama serve
```

### 4. Start infrastructure

```bash
docker-compose up -d
```

This starts Redpanda, MinIO, Prometheus, and Grafana.

### 5. Create the Kafka topic

```bash
docker exec -it redpanda rpk topic create nyc-taxi --partitions 1 --replicas 1
```

### 6. Run the agent (standalone test)

```bash
python src/agent/agent.py
```

### 7. Run the full pipeline

Terminal 1 — start the producer:
```bash
python src/producer/producer.py
```

Terminal 2 — start the consumer + agent:
```bash
python src/consumer/consumer.py
```

---

## Project structure

```
streamagent/
├── docker-compose.yml
├── README.md
├── src/
│   ├── agent/
│   │   └── agent.py          # LangGraph agent — classify, reason, recommend, report
│   ├── producer/
│   │   └── producer.py       # Simulates NYC taxi stream with injected anomalies
│   ├── consumer/
│   │   └── consumer.py       # Plain Python Kafka consumer → calls agent
│   ├── pyspark/
│   │   └── detector.py       # PySpark Structured Streaming with 4 statistical checks
│   └── api/
│       └── main.py           # FastAPI server exposing incident reports
├── dashboard/
│   └── streamlit_app.py      # Live demo UI
└── grafana/
    └── dashboard.json        # Importable Grafana dashboard
```

---

## How the agent works

The LangGraph agent receives an anomaly payload and executes a four-node reasoning graph:

**`classify_node`** — computes deviation magnitude from the expected range using a z-score formula and sets severity to `low`, `medium`, or `high`.

**`reason_node`** — invoked only for medium and high severity. Calls LLaMA 3 locally via Ollama with a structured prompt containing the anomaly type, affected columns, observed value, and historical baseline. Returns a 2–3 sentence root cause hypothesis.

**`recommend_node`** — looks up a hardcoded remediation playbook keyed by `anomaly_type × severity` and returns a concrete action for the on-call engineer.

**`report_node`** — assembles all accumulated state into a structured incident report JSON with a UUID, timestamp, and all fields above.

---

## AgentState fields

| Field | Type | Set by |
|---|---|---|
| `anomaly_type` | `str` | You (input) |
| `affected_columns` | `list[str]` | You (input) |
| `observed_value` | `float` | You (input) |
| `expected_range` | `list[float]` | You (input) |
| `severity` | `str` | `classify_node` |
| `probable_cause` | `str` | `reason_node` |
| `recommended_action` | `str` | `recommend_node` |
| `incident_report` | `dict` | `report_node` |

---

## Docker Compose services

```yaml
services:
  redpanda:     # Kafka-compatible message broker  → localhost:9092
  minio:        # S3-compatible object storage     → localhost:9000
  prometheus:   # Metrics scraping                 → localhost:9090
  grafana:      # Observability dashboard          → localhost:3000
```

Grafana default credentials: `admin / admin`

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Redpanda broker address |
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `OLLAMA_HOST` | `http://localhost:11434` | Ollama server address |
| `LLM_MODEL` | `llama3` | Ollama model name |
| `NULL_THRESHOLD` | `0.05` | Max acceptable null rate (5%) |
| `ZSCORE_THRESHOLD` | `3.0` | Z-score threshold for distribution shift |
| `VOLUME_TOLERANCE` | `0.4` | Allowed volume deviation (±40%) |

---

## Dataset

Uses the [NYC Taxi and Limousine Commission Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — a public domain dataset with realistic structured records. The producer replays historical records at a configurable rate with injected anomalies at regular intervals.

---

## Roadmap

- [x] LangGraph agent with four-node reasoning graph
- [x] Kafka producer with injected anomaly simulation
- [x] Plain Python consumer invoking agent
- [ ] PySpark Structured Streaming detector
- [ ] MinIO incident report persistence
- [ ] FastAPI incident report endpoint
- [ ] Prometheus metrics + Grafana dashboard
- [ ] Streamlit demo UI
- [ ] Hugging Face Spaces deployment

---

## Why this project

Data observability is a real, expensive problem — companies like Monte Carlo and Acceldata have raised hundreds of millions solving it. StreamAgent's differentiator is the **agentic reasoning layer**: existing tools alert, StreamAgent reasons. It combines real data engineering infrastructure (Kafka, PySpark, Spark) with modern AI tooling (LangGraph, local LLMs) in a way that mirrors production systems at companies like Salesforce, Databricks, and Confluent.

---

## Author

**Aanchal Sikarwar**
[LinkedIn](https://linkedin.com/in/aanchal-sikarwar) · [GitHub](https://github.com/yourusername)
