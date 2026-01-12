from fastapi import APIRouter, HTTPException, Query
from ..core.config import settings
from ..core.store import TransactionStore
from ..core.classifier import Agent
from ..core.errors import NotFoundError, DataSourceError

router = APIRouter()
agent = Agent(settings.MAPPINGS_PATH, settings.SYNONYMS_PATH, settings.RULES_PATH)
store = TransactionStore(settings.DUCKDB_PATH)

@router.get("/health")
def health():
    return {"ok": True}

@router.post("/ingest")
def ingest(data_path: str = Query(..., description="Path to CSV/JSON/NDJSON/Parquet")):
    try:
        settings.ensure_dirs()
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
