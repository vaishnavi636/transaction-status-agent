from pathlib import Path
from typing import Dict, Any
import duckdb
from .errors import NotFoundError
from ..connectors.loader import validate_path, duckdb_relation

class TransactionStore:
    def __init__(self, duckdb_path: str):
        self.duckdb_path = duckdb_path
        Path(self.duckdb_path).parent.mkdir(exist_ok=True, parents=True)
        self.con = duckdb.connect(self.duckdb_path)

    def close(self):
        try:
            self.con.close()
        except Exception:
            pass

    def ingest(self, data_path: str, table: str = "txns"):
        validate_path(data_path)
        rel = duckdb_relation(self.con, data_path)
        self.con.execute(f"DROP TABLE IF EXISTS {table}")
        self.con.execute(f"CREATE TABLE {table} AS SELECT * FROM rel")

        # Best-effort: if no transaction_id column exists, index will fail (and that’s fine).
        try:
            self.con.execute(f"CREATE INDEX IF NOT EXISTS idx_txid ON {table}(transaction_id)")
        except Exception:
            pass

    def lookup(self, txid: str, table: str = "txns") -> Dict[str, Any]:
        q = f"SELECT * FROM {table} WHERE CAST(transaction_id AS VARCHAR) = ? LIMIT 1"
        df = self.con.execute(q, [str(txid)]).fetchdf()
        if df is None or df.empty:
            raise NotFoundError(f"transaction_id {txid} not found")
        return df.iloc[0].to_dict()

    def stats(self, table: str = "txns") -> Dict[str, Any]:
        try:
            cnt = self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except Exception:
            cnt = None
        return {"table": table, "count": cnt}
