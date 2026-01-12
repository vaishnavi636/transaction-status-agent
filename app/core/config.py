from pydantic_settings import BaseSettings
from pathlib import Path

class Settings(BaseSettings):
    DATA_PATH: str = "sample_data/transactions.parquet"
    DUCKDB_PATH: str = "data/txn.duckdb"

    MAPPINGS_PATH: str = "configs/mappings.yaml"
    SYNONYMS_PATH: str = "configs/status_synonyms.yaml"
    RULES_PATH: str = "configs/rules.yaml"

    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000

    def ensure_dirs(self):
        Path("data").mkdir(exist_ok=True)

settings = Settings()
