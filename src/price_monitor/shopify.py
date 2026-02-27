from __future__ import annotations

import csv
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

from .http_client import HttpClient

log = logging.getLogger(__name__)


def normalize_store_domain(domain: str) -> str:
    domain = (domain or "").strip()
    domain = domain.replace("https://", "").replace("http://", "").strip("/")
    return domain


def _store_url(domain: str) -> str:
    return f"https://{normalize_store_domain(domain)}"


def product_url(store_domain: str, handle: str) -> str:
    base = _store_url(store_domain)
    return f"{base}/products/{handle}"


_HANDLE_RE = re.compile(r"/products/([^/?#]+)")


def extract_handle(url_or_handle: str) -> str:
    """Accepts a handle or a product URL and returns the handle (best-effort)."""
    s = (url_or_handle or "").strip()
    if not s:
        return ""
    m = _HANDLE_RE.search(s)
    if m:
        return m.group(1).strip()
    # If user pasted only a handle
    return s.strip().strip("/")


def iter_products_json(
    client: HttpClient,
    *,
    store_domain: str,
    limit: int = 250,
    max_pages: int = 20,
) -> Iterator[Dict[str, Any]]:
    """Iterates products via the public storefront endpoint `/products.json`.

    Important:
    - Some stores disable/limit this endpoint.
    - `limit` is usually <= 250.

    This implementation uses the classic `page=` pagination (portfolio-friendly).
    """
    base = _store_url(store_domain)
    page = 1
    while page <= max_pages:
        url = f"{base}/products.json?limit={limit}&page={page}"
        res, err = client.get(url)
        if err or res is None:
            log.warning("request failed: %s (page=%s) err=%s", url, page, err)
            break
        if res.status_code != 200:
            log.warning("non-200 from products.json: %s status=%s", url, res.status_code)
            break

        try:
            data = json.loads(res.text)
        except json.JSONDecodeError:
            log.warning("invalid JSON from products.json: %s", url)
            break

        products = data.get("products") or []
        if not products:
            break

        for p in products:
            if isinstance(p, dict):
                yield p

        if len(products) < limit:
            break

        page += 1

def iter_collection_products_json(
    client: HttpClient,
    *,
    store_domain: str,
    collection_handle: str,
    limit: int = 250,
    max_pages: int = 20,
) -> Iterator[Dict[str, Any]]:
    """Iterates products for a specific collection via `/collections/<handle>/products.json`.

    Useful when you want to monitor a subset of the catalog.

    Notes:
    - Not all stores expose this endpoint.
    - `limit` is usually <= 250.
    """
    base = _store_url(store_domain)
    ch = (collection_handle or "").strip().strip("/")
    if not ch:
        return
        yield  # pragma: no cover

    page = 1
    while page <= max_pages:
        url = f"{base}/collections/{ch}/products.json?limit={limit}&page={page}"
        res, err = client.get(url)
        if err or res is None:
            log.warning("request failed: %s (page=%s) err=%s", url, page, err)
            break
        if res.status_code != 200:
            log.warning("non-200 from collection products.json: %s status=%s", url, res.status_code)
            break

        try:
            data = json.loads(res.text)
        except json.JSONDecodeError:
            log.warning("invalid JSON from collection products.json: %s", url)
            break

        products = data.get("products") or []
        if not products:
            break

        for p in products:
            if isinstance(p, dict):
                yield p

        if len(products) < limit:
            break

        page += 1

def fetch_product_by_handle(
    client: HttpClient,
    *,
    store_domain: str,
    handle: str,
) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Fetches a single product via `/products/<handle>.json`.

    This is a good fallback when `/products.json` is blocked.
    """
    handle = extract_handle(handle)
    if not handle:
        return None, "empty handle"

    base = _store_url(store_domain)
    url = f"{base}/products/{handle}.json"
    res, err = client.get(url)
    if err or res is None:
        return None, err or "request failed"
    if res.status_code != 200:
        return None, f"HTTP {res.status_code}"

    try:
        data = json.loads(res.text)
    except json.JSONDecodeError:
        return None, "invalid JSON"

    p = data.get("product")
    if isinstance(p, dict):
        return p, None
    return None, "missing 'product' field"


def iter_products_from_targets_csv(
    client: HttpClient,
    *,
    store_domain: str,
    csv_path: str,
) -> Iterator[Dict[str, Any]]:
    """Reads a CSV with targets (product URLs or handles) and yields product JSON dicts.

    Supported columns:
    - `url`  (full product URL)
    - `handle`

    Any other columns are ignored.
    """
    p = Path(csv_path)
    if not p.exists():
        raise FileNotFoundError(str(p))

    with p.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            url = (row.get("url") or "").strip()
            handle = (row.get("handle") or "").strip()
            h = extract_handle(handle or url)
            if not h:
                log.warning("skip row with no handle/url: %s", row)
                continue
            prod, err = fetch_product_by_handle(client, store_domain=store_domain, handle=h)
            if err or prod is None:
                log.warning("failed to fetch product handle=%s err=%s", h, err)
                continue
            yield prod
