import duckdb
from app.core.store import TransactionStore

def test_store_ingest_and_lookup(tmp_path):
    db_path = tmp_path / "test.duckdb"

    con = duckdb.connect()
    con.execute("CREATE TABLE t(transaction_id VARCHAR, status VARCHAR) AS VALUES ('a1','success'),('a2','failed')")
    parquet_path = tmp_path / "t.parquet"
    con.execute(f"COPY t TO '{parquet_path.as_posix()}' (FORMAT PARQUET)")
    con.close()

    store = TransactionStore(str(db_path))
    store.ingest(str(parquet_path))
    row = store.lookup("a1")
    assert row["transaction_id"] == "a1"
    store.close()
