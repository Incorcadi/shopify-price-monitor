from __future__ import annotations

import json
from pathlib import Path

from price_monitor.normalize import variants_from_product
from price_monitor.diff import diff_snapshots
from price_monitor.storage_sqlite import SqliteStore


FIX = Path(__file__).parent / "fixtures"


def test_variants_from_product():
    data = json.loads((FIX / "products_page1.json").read_text(encoding="utf-8"))
    p = data["products"][0]
    snaps = list(variants_from_product(p, store_domain="example.myshopify.com", currency="USD"))
    assert len(snaps) == 2
    assert snaps[0].product_title == "Coffee Beans"
    assert snaps[0].variant_id == 101
    assert snaps[0].availability == "in_stock"
    assert snaps[1].availability == "out_of_stock"
    assert snaps[0].compare_at_price == 15.0


def test_diff_snapshots_price_and_stock():
    data = json.loads((FIX / "products_page1.json").read_text(encoding="utf-8"))
    p = data["products"][0]
    s1 = list(variants_from_product(p, store_domain="example.myshopify.com", currency="USD"))
    # modify price/stock for variant 101
    p2 = json.loads(json.dumps(p))
    p2["variants"][0]["price"] = "10.00"
    p2["variants"][0]["available"] = False
    s2 = list(variants_from_product(p2, store_domain="example.myshopify.com", currency="USD"))

    prev = {f"{it.store_domain}::variant::{it.variant_id}": it for it in s1}
    curr = {f"{it.store_domain}::variant::{it.variant_id}": it for it in s2}
    changes = diff_snapshots(prev, curr)

    kinds = sorted({c.kind for c in changes})
    assert "price" in kinds
    assert "availability" in kinds


def test_sqlite_store_roundtrip(tmp_path: Path):
    db = str(tmp_path / "m.db")
    store = SqliteStore(db)
    run_id = store.start_run(store_domain="example.myshopify.com", started_at="2026-02-12T00:00:00Z")

    data = json.loads((FIX / "products_page1.json").read_text(encoding="utf-8"))
    snaps = []
    for p in data["products"]:
        snaps.extend(list(variants_from_product(p, store_domain="example.myshopify.com", currency="USD")))

    n = store.upsert_snapshots(run_id, snaps)
    assert n == 3

    latest = store.load_latest()
    assert len(latest) == 3
    store.close()
