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

def generate_rows(n: int):
    now = datetime.now(timezone.utc)
    for _ in range(n):
        txid = str(uuid.uuid4())
        status = random.choice(STATUSES)

        created = now - timedelta(minutes=random.randint(0, 5000))
        updated = created + timedelta(minutes=random.randint(0, 200))

        # 5% chance: simulate stuck (updated long ago)
        if random.random() < 0.05:
            updated = now - timedelta(minutes=random.randint(60, 600))

        err_code = None
        err_msg = None

        if status in {"failed", "declined"} or random.random() < 0.02:
            err_code = random.choice(["E401", "E403", "DO_NOT_HONOR", "X999"])
            err_msg = random.choice(["insufficient funds", "invalid account", "unknown error"])

        yield {
            "transaction_id": txid,
            "status": status,
            "created_at": created.isoformat(),
            "updated_at": updated.isoformat(),
            "error_code": err_code,
            "error_message": err_msg,
            "amount": round(random.random() * 10000, 2),
            "currency": random.choice(["INR", "USD", "EUR"]),
            "provider": random.choice(["stripe", "adyen", "paypal", "razorpay"]),
        }

def main():
    out_dir = Path("sample_data")
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "transactions.parquet"

    n = 200_000  # increase later (500_000 / 1_000_000) for performance test
    rows = list(generate_rows(n))

    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT * FROM rows", {"rows": rows})
    con.execute(f"COPY t TO '{out_path.as_posix()}' (FORMAT PARQUET)")
    con.close()

    print(f"Wrote {n} rows to {out_path}")

if __name__ == "__main__":
    main()
