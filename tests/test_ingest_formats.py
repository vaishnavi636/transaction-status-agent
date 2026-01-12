import json
import duckdb
from pathlib import Path

from app.core.store import TransactionStore


def _create_csv(path: Path):
    path.write_text(
        "transaction_id,status,created_at,updated_at\n"
        "t1,success,2026-01-10T10:00:00Z,2026-01-10T10:01:00Z\n"
        "t2,failed,2026-01-10T10:05:00Z,2026-01-10T10:05:30Z\n",
        encoding="utf-8",
    )


def _create_json(path: Path):
    data = [
        {
            "transaction_id": "t3",
            "status": "pending",
            "created_at": "2026-01-11T09:00:00Z",
            "updated_at": "2026-01-11T09:05:00Z",
        },
        {
            "transaction_id": "t4",
            "status": "success",
            "created_at": "2026-01-11T10:00:00Z",
            "updated_at": "2026-01-11T10:01:00Z",
        },
    ]
    path.write_text(json.dumps(data), encoding="utf-8")


def _create_ndjson(path: Path):
    lines = [
        {"transaction_id": "t5", "status": "processing"},
        {"transaction_id": "t6", "status": "declined"},
    ]
    with path.open("w", encoding="utf-8") as f:
        for row in lines:
            f.write(json.dumps(row) + "\n")


def _create_parquet(path: Path):
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE t AS
        SELECT 't7' AS transaction_id, 'success' AS status
        UNION ALL
        SELECT 't8' AS transaction_id, 'failed' AS status
    """)
    con.execute(f"COPY t TO '{path.as_posix()}' (FORMAT PARQUET)")
    con.close()


def test_ingest_csv_json_ndjson_parquet(tmp_path):
    db_path = tmp_path / "test.duckdb"
    store = TransactionStore(str(db_path))

    csv_path = tmp_path / "data.csv"
    json_path = tmp_path / "data.json"
    ndjson_path = tmp_path / "data.ndjson"
    parquet_path = tmp_path / "data.parquet"

    _create_csv(csv_path)
    _create_json(json_path)
    _create_ndjson(ndjson_path)
    _create_parquet(parquet_path)

    # CSV
    store.ingest(str(csv_path))
    assert store.lookup("t1")["transaction_id"] == "t1"

    # JSON
    store.ingest(str(json_path))
    assert store.lookup("t3")["transaction_id"] == "t3"

    # NDJSON
    store.ingest(str(ndjson_path))
    assert store.lookup("t5")["transaction_id"] == "t5"

    # Parquet
    store.ingest(str(parquet_path))
    assert store.lookup("t7")["transaction_id"] == "t7"

    store.close()
