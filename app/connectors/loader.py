from pathlib import Path
import duckdb
from ..core.errors import DataSourceError

SUPPORTED = {".csv", ".json", ".ndjson", ".parquet"}

def _ext(path: str) -> str:
    return Path(path).suffix.lower()

def validate_path(path: str):
    p = Path(path)
    if not p.exists():
        raise DataSourceError(f"Data file not found: {path}")
    if _ext(path) not in SUPPORTED:
        raise DataSourceError(f"Unsupported file type {_ext(path)}. Supported: {sorted(SUPPORTED)}")

def duckdb_relation(con: duckdb.DuckDBPyConnection, path: str):
    ext = _ext(path)
    if ext == ".csv":
        return con.from_query(f"SELECT * FROM read_csv_auto('{path}', HEADER=TRUE)")
    if ext == ".parquet":
        return con.from_query(f"SELECT * FROM read_parquet('{path}')")
    if ext == ".ndjson":
        return con.from_query(f"SELECT * FROM read_json_auto('{path}', format='newline_delimited')")
    if ext == ".json":
        return con.from_query(f"SELECT * FROM read_json_auto('{path}')")
    raise DataSourceError(f"Unsupported file type: {ext}")
