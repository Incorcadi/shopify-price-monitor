import csv
from pathlib import Path

BASE = Path("docs/demo")

def slim_csv(src_name: str, dst_name: str, keep_cols: list[str]) -> None:
    src = BASE / src_name
    dst = BASE / dst_name

    if not src.exists():
        raise FileNotFoundError(f"Missing file: {src}")

    with src.open("r", encoding="utf-8", newline="") as f_in:
        reader = csv.DictReader(f_in)
        with dst.open("w", encoding="utf-8", newline="") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=keep_cols)
            writer.writeheader()
            for row in reader:
                writer.writerow({k: row.get(k, "") for k in keep_cols})

def main() -> None:
    slim_csv(
        "current_prices.csv",
        "current_prices_portfolio.csv",
        ["product_title", "product_handle", "sku", "price", "currency", "availability"]
    )
    slim_csv(
        "price_changes.csv",
        "price_changes_portfolio.csv",
        ["kind", "product_title", "sku", "before_price", "after_price", "before_availability", "after_availability"]
    )
    print("OK: generated docs/demo/*_portfolio.csv")

if __name__ == "__main__":
    main()