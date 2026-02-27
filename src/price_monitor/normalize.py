from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional

from .models import VariantSnapshot
from .shopify import product_url


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip().replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _avail(variant: Dict[str, Any]) -> str:
    # Shopify storefront JSON обычно содержит boolean available
    av = variant.get("available")
    if av is True:
        return "in_stock"
    if av is False:
        return "out_of_stock"
    # fallback
    qty = variant.get("inventory_quantity")
    if isinstance(qty, int):
        return "in_stock" if qty > 0 else "out_of_stock"
    return "in_stock"


def variants_from_product(product: Dict[str, Any], *, store_domain: str, currency: str) -> Iterator[VariantSnapshot]:
    """Разворачивает Shopify product JSON в набор VariantSnapshot (по каждому variant)."""
    handle = str(product.get("handle") or "").strip()
    title = str(product.get("title") or "").strip()
    url = product_url(store_domain, handle) if handle else f"https://{store_domain}"

    images = product.get("images") or []
    image_url = None
    if isinstance(images, list) and images:
        first = images[0]
        if isinstance(first, dict):
            image_url = first.get("src") or first.get("url")

    for v in (product.get("variants") or []):
        if not isinstance(v, dict):
            continue
        vid = v.get("id")
        if not isinstance(vid, int):
            continue

        vtitle = str(v.get("title") or "").strip()
        sku = v.get("sku")
        sku = str(sku).strip() if sku else None

        price = _to_float(v.get("price"))
        compare_at = _to_float(v.get("compare_at_price"))

        yield VariantSnapshot(
            store_domain=store_domain,
            product_handle=handle,
            product_title=title,
            product_url=url,
            variant_id=vid,
            variant_title=vtitle,
            sku=sku,
            price=price,
            compare_at_price=compare_at,
            currency=currency,
            availability=_avail(v),
            image_url=image_url,
            scraped_at=_utc_now(),
        )
