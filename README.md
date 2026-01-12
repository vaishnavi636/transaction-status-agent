# Transaction Status Agent

An explainable transaction status detection agent built using **FastAPI** and **DuckDB**.  
The system ingests transaction data from multiple file formats and returns **deterministic, auditable status decisions** at scale.

This project is designed for correctness, transparency, and performance — suitable for transaction-processing and financial pipelines.

---

## Key Features

- Supports **CSV, JSON, NDJSON, Parquet**
- Flexible schema handling via config-driven field mapping
- Deterministic status classification:
  - SUCCESS
  - FAILED
  - PENDING
  - PROCESSING
  - STUCK
- Confidence score, reason, evidence, and rules fired for every decision
- Scales to **hundreds of thousands of transactions**
- Batch status lookup for large request volumes
- File upload support via Swagger UI
- Fully automated tests

---

## What “Agent” Means Here

This is not a black-box ML model.

The agent is an **explainable decision engine** that:
1. Normalizes raw transaction states using configurable synonyms
2. Applies deterministic business rules (including stuck detection)
3. Produces:
   - final status
   - confidence
   - reason
   - evidence
   - rules fired

Every decision is transparent and auditable.


## Project Structure

transaction-status-agent/
├── app/
│ ├── api/
│ ├── core/
│ ├── connectors/
│ └── main.py
├── configs/
│ ├── mappings.yaml
│ ├── status_synonyms.yaml
│ └── rules.yaml
├── scripts/
│ ├── generate_synthetic.py
│ └── build_index.py
├── sample_data/
│ ├── sample_transactions.csv
│ ├── sample_transactions.json
│ ├── sample_transactions.ndjson
│ └── HOW_TO_TEST_FILES.txt
├── tests/
│ ├── test_ingest_formats.py
│ ├── test_classifier.py
│ ├── test_mapper.py
│ └── test_store.py
└── README.md

## Setup

### Create virtual environment
```
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
Run tests

pytest -q
Expected output:

..... [100%]

Run the API

uvicorn app.main:app --reload
Open Swagger UI:

http://127.0.0.1:8000/docs
Ingest Transaction Data

Option 1: Upload a file (Recommended)
Supported formats:

CSV

JSON

NDJSON

Parquet

Using Swagger UI:

POST /ingest/upload
Using curl:

curl -X POST http://127.0.0.1:8000/ingest/upload ^
  -F "file=@sample_data\sample_transactions.csv"

Option 2: Ingest from local path

curl -X POST "http://127.0.0.1:8000/ingest?data_path=sample_data/sample_transactions.csv"
Query Transaction Status
Single transaction
http
GET /transaction/{transaction_id}
Example:

/transaction/tx_1001
Batch transaction status
http

POST /transactions/status
Request body:

json

{
  "transaction_ids": ["tx_1001", "tx_1002", "UNKNOWN_ID"]
}

Response includes:

requested count
found count
missing IDs
detailed results

Automated Test Coverage
The test suite verifies:

CSV ingestion

JSON ingestion

NDJSON ingestion

Parquet ingestion

Store lookup correctness

Agent classification rules

Batch querying behavior

Run:

pytest -q

Configuration
mappings.yaml
Maps varying column names to canonical fields
(e.g. tx_id → transaction_id, state → status)

status_synonyms.yaml
Defines which raw statuses map to final states

rules.yaml
Controls:

stuck detection thresholds
confidence scoring
rule behavior
All behavior can be changed without code changes.