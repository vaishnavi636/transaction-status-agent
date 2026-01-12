from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Any, Dict

from ..core.config import settings
from ..core.store import TransactionStore
from ..core.classifier import Agent
from ..core.errors import NotFoundError, DataSourceError

router = APIRouter()

# Ensure data dir exists
settings.ensure_dirs()

agent = Agent(settings.MAPPINGS_PATH, settings.SYNONYMS_PATH, settings.RULES_PATH)
store = TransactionStore(settings.DUCKDB_PATH)


class BatchRequest(BaseModel):
    transaction_ids: List[str]


@router.get("/health")
def health():
    return {"ok": True}


@router.post("/ingest")
def ingest(data_path: str = Query(..., description="Path to CSV/JSON/NDJSON/Parquet")):
    try:
        store.ingest(data_path)
        return {"ok": True, "stats": store.stats()}
    except DataSourceError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingest failed: {e}")


@router.get("/transaction/{txid}")
def get_transaction(txid: str):
    try:
        row = store.lookup(txid)
        decision = agent.evaluate(row)
        return decision.model_dump()
    except NotFoundError:
        raise HTTPException(status_code=404, detail="Transaction not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/transactions/status")
def batch_status(req: BatchRequest):
    """
    Batch status lookup for many transaction IDs.
    Uses one SQL query (fast) and then classifies each returned row.
    """
    ids = [str(x).strip() for x in req.transaction_ids if str(x).strip()]
    if not ids:
        raise HTTPException(status_code=400, detail="transaction_ids is empty")

    try:
        # placeholders = ",".join(["?"] * len(ids))
        # q = f"SELECT * FROM txns WHERE CAST(transaction_id AS VARCHAR) IN ({placeholders})"

        # cur = store.con.execute(q, ids)
        # rows = cur.fetchall()

        # cols = [d[0] for d in cur.description]
        store.con.execute("CREATE TEMP TABLE req_ids(txid VARCHAR)")
        store.con.executemany("INSERT INTO req_ids VALUES (?)",[(x,) for x in ids])
        cur = store.con.execute("""
            SELECT t.*
            FROM txns t
            JOIN req_ids r
              ON CAST(t.transaction_id AS VARCHAR) = r.txid
        """)

        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

# Cleanup
        store.con.execute("DROP TABLE req_ids")
        


        results: List[Dict[str, Any]] = []
        found_ids = set()

        for r in rows:
            row_dict = dict(zip(cols, r))
            d = agent.evaluate(row_dict)
            results.append(d.model_dump())
            found_ids.add(d.transaction_id)

        missing = [txid for txid in ids if txid not in found_ids]

        return {
            "requested": len(ids),
            "found": len(results),
            "missing": missing[:1000],  # safety limit to avoid huge responses
            "results": results
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
