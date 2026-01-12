import csv
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
import duckdb

STATUSES = [
    "success", "failed", "pending", "processing",
    "queued", "declined", "settled", "in_progress",
    "authorizing", "capturing"
]

def row_generator(n: int):
    now = datetime.now(timezone.utc)
    for _ in range(n):
        txid = str(uuid.uuid4())
        status = random.choice(STATUSES)

        created = now - timedelta(minutes=random.randint(0, 5000))
        updated = created + timedelta(minutes=random.randint(0, 200))

        if random.random() < 0.05:
            updated = now - timedelta(minutes=random.randint(60, 600))

        err_code = ""
        err_msg = ""
        if status in {"failed", "declined"} or random.random() < 0.02:
            err_code = random.choice(["E401", "E403", "DO_NOT_HONOR", "X999"])
            err_msg = random.choice(["insufficient funds", "invalid account", "unknown error"])

        yield [
            txid,
            status,
            created.isoformat(),
            updated.isoformat(),
            err_code,
            err_msg,
            round(random.random() * 10000, 2),
            random.choice(["INR", "USD", "EUR"]),
            random.choice(["stripe", "adyen", "paypal", "razorpay"]),
        ]

def main():
    repo_root = Path(__file__).resolve().parents[1]
    out_dir = repo_root / "sample_data"
    out_dir.mkdir(exist_ok=True)

    csv_path = out_dir / "transactions.csv"
    parquet_path = out_dir / "transactions.parquet"

    n = 200_000          # start with 20_000 if you want quick test
    log_every = 50_000

    headers = [
        "transaction_id", "status", "created_at", "updated_at",
        "error_code", "error_message", "amount", "currency", "provider"
    ]

    print(f"[start] generating {n} rows -> {csv_path}")
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for i, row in enumerate(row_generator(n), start=1):
            w.writerow(row)
            if i % log_every == 0:
                print(f"[progress] wrote {i}/{n} rows to CSV")

    print("[start] converting CSV -> Parquet using DuckDB")
    con = duckdb.connect()
    con.execute(f"""
        CREATE OR REPLACE TABLE t AS
        SELECT * FROM read_csv_auto('{csv_path.as_posix()}', HEADER=TRUE)
    """)
    con.execute(f"COPY t TO '{parquet_path.as_posix()}' (FORMAT PARQUET)")
    con.close()

    print(f"[done] Parquet created: {parquet_path}")

    # optional cleanup:
    # try: csv_path.unlink()
    # except Exception: pass

if __name__ == "__main__":
    main()
