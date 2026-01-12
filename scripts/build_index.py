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
