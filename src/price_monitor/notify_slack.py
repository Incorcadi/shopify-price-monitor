from __future__ import annotations

import json
from typing import List, Optional

import requests

from .diff import Change


def _fmt_change(ch: Change) -> str:
    a = ch.after
    if ch.kind == "new":
        return f"NEW: {a.product_title} ({a.variant_title}) — {a.price} {a.currency} — {a.product_url}"
    if ch.kind == "price":
        bp = ch.before.price if ch.before else None
        return f"PRICE: {a.product_title} ({a.variant_title}) {bp} → {a.price} {a.currency} — {a.product_url}"
    if ch.kind == "availability":
        ba = ch.before.availability if ch.before else None
        return f"STOCK: {a.product_title} ({a.variant_title}) {ba} → {a.availability} — {a.product_url}"
    if ch.kind == "compare_at":
        b = ch.before.compare_at_price if ch.before else None
        return f"SALE: {a.product_title} ({a.variant_title}) compare_at {b} → {a.compare_at_price} — {a.product_url}"
    return f"{ch.kind}: {a.product_title} — {a.product_url}"


def send_slack(webhook_url: str, changes: List[Change], *, title: str = "Price monitor update") -> Optional[str]:
    """Отправляет алерт в Slack Incoming Webhook. Возвращает текст ошибки или None."""
    if not changes:
        return None

    lines = [_fmt_change(ch) for ch in changes[:30]]
    text = title + "\n" + "\n".join(lines)

    payload = {"text": text}

    try:
        r = requests.post(webhook_url, data=json.dumps(payload), headers={"Content-Type": "application/json"}, timeout=15)
        if r.status_code >= 300:
            return f"Slack webhook HTTP {r.status_code}: {r.text[:200]}"
        return None
    except requests.RequestException as e:
        return f"{type(e).__name__}: {e}"
