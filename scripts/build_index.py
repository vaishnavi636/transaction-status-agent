import sys
from pathlib import Path

# Ensure project root is on PYTHONPATH
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.core.config import settings
from app.core.store import TransactionStore

def main():
    settings.ensure_dirs()
    store = TransactionStore(settings.DUCKDB_PATH)
    store.ingest(settings.DATA_PATH)
    print("Built store:", store.stats())
    store.close()

if __name__ == "__main__":
    main()
