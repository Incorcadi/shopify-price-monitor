from __future__ import annotations

import csv
from dataclasses import asdict
from pathlib import Path
from typing import Iterable, List

from .diff import Change


FIELDS = [
    "store_domain",
    "product_title",
    "product_handle",
    "product_url",
    "variant_id",
    "variant_title",
    "sku",
    "price",
    "compare_at_price",
    "currency",
    "availability",
    "image_url",
    "scraped_at",
]


def write_current_csv(path: str, items: Iterable) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for it in items:
            row = asdict(it)
            w.writerow({k: row.get(k) for k in FIELDS})


def write_changes_csv(path: str, changes: List[Change]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "kind",
        "store_domain",
        "product_title",
        "product_url",
        "variant_id",
        "variant_title",
        "sku",
        "before_price",
        "after_price",
        "before_availability",
        "after_availability",
        "scraped_at",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for ch in changes:
            b = ch.before
            a = ch.after
            w.writerow({
                "kind": ch.kind,
                "store_domain": a.store_domain,
                "product_title": a.product_title,
                "product_url": a.product_url,
                "variant_id": a.variant_id,
                "variant_title": a.variant_title,
                "sku": a.sku,
                "before_price": (b.price if b else None),
                "after_price": a.price,
                "before_availability": (b.availability if b else None),
                "after_availability": a.availability,
                "scraped_at": a.scraped_at,
            })
