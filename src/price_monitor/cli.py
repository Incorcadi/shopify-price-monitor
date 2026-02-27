from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

from .diff import diff_snapshots
from .export_csv import write_changes_csv, write_current_csv
from .http_client import HttpClient
from .normalize import variants_from_product
from .notify_slack import send_slack
from .shopify import iter_collection_products_json, iter_products_from_targets_csv, iter_products_json
from .storage_sqlite import SqliteStore


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _setup_logging(level: str) -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


log = logging.getLogger("price_monitor")


def cmd_run(args: argparse.Namespace) -> int:
    _setup_logging(args.log_level)

    client = HttpClient(timeout=args.timeout, min_interval=args.min_interval)
    store = SqliteStore(args.db)

    started_at = _utc_now()
    run_id = store.start_run(store_domain=args.store, started_at=started_at)

    prev = store.load_latest_map()

    products: list[dict] = []
    if args.targets_csv:
        log.info("targets mode: csv=%s", args.targets_csv)
        for p in iter_products_from_targets_csv(client, store_domain=args.store, csv_path=args.targets_csv):
            products.append(p)
            if args.max_products and len(products) >= args.max_products:
                break
    elif args.collection:
        log.info("collection mode: /collections/%s/products.json", args.collection)
        for p in iter_collection_products_json(
            client,
            store_domain=args.store,
            collection_handle=args.collection,
            limit=min(args.limit, 250),
            max_pages=args.max_pages,
        ):
            products.append(p)
            if args.max_products and len(products) >= args.max_products:
                break
    else:
        log.info("catalog mode: /products.json pagination")
        for p in iter_products_json(
            client,
            store_domain=args.store,
            limit=min(args.limit, 250),
            max_pages=args.max_pages,
        ):
            products.append(p)
            if args.max_products and len(products) >= args.max_products:
                break

    snapshots = []
    for p in products:
        snapshots.extend(list(variants_from_product(p, store_domain=args.store, currency=args.currency)))

    store.upsert_snapshots(run_id, snapshots)

    curr = store.load_latest_map()
    changes = diff_snapshots(prev, curr)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    write_current_csv(str(out_dir / "current_prices.csv"), store.load_latest())
    write_changes_csv(str(out_dir / "price_changes.csv"), changes)

    if args.slack_webhook:
        err = send_slack(args.slack_webhook, changes, title=f"Price monitor: {args.store}")
        if err:
            log.warning("slack error: %s", err)

    log.info(
        "ok: store=%s products=%s variants=%s changes=%s db=%s out=%s",
        args.store,
        len(products),
        len(snapshots),
        len(changes),
        args.db,
        args.out,
    )
    store.close()
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    _setup_logging(args.log_level)

    store = SqliteStore(args.db)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    write_current_csv(str(out_dir / "current_prices.csv"), store.load_latest())

    prev = store.load_prev_latest_map()
    curr = store.load_latest_map()
    changes = diff_snapshots(prev, curr)
    write_changes_csv(str(out_dir / "price_changes.csv"), changes)

    log.info("exported: db=%s out=%s current=%s changes=%s", args.db, args.out, len(curr), len(changes))
    store.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="price-monitor",
        description="Competitor price & stock monitor (Shopify storefront)",
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="scrape -> store -> diff -> export -> (optional) slack")
    run.add_argument("--store", required=True, help="Shopify store domain (e.g. example.myshopify.com or store.com)")
    run.add_argument("--currency", default="USD", help="Currency label (USD/EUR/GBP...)")
    run.add_argument("--db", required=True, help="SQLite db path")
    run.add_argument("--out", default="outputs", help="Output directory for CSV")

    run.add_argument("--targets-csv", default="", help="Optional CSV with product URLs/handles (columns: url,handle)")
    run.add_argument("--collection", default="", help="Optional collection handle to monitor (e.g. flour-setting-powder)")

    run.add_argument("--timeout", type=float, default=20.0)
    run.add_argument("--min-interval", type=float, default=0.25, help="Polite delay between requests (seconds)")

    run.add_argument("--limit", type=int, default=250, help="per-page limit for /products.json (<=250)")
    run.add_argument("--max-pages", type=int, default=20)
    run.add_argument("--max-products", type=int, default=0, help="0 = no limit")

    run.add_argument("--slack-webhook", default="", help="Slack incoming webhook URL (optional)")
    run.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARNING/ERROR")

    run.set_defaults(fn=cmd_run)

    exp = sub.add_parser("export", help="export CSV from an existing db")
    exp.add_argument("--db", required=True)
    exp.add_argument("--out", default="outputs")
    exp.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARNING/ERROR")
    exp.set_defaults(fn=cmd_export)

    return ap


def main() -> int:
    ap = build_parser()
    args = ap.parse_args()

    # normalize empty strings
    if getattr(args, "targets_csv", None) == "":
        args.targets_csv = ""
    if getattr(args, "collection", None) == "":
        args.collection = ""

    return int(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(main())
