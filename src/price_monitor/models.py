from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class VariantSnapshot:
    """Снимок цены/наличия по конкретному варианту товара."""

    store_domain: str
    product_handle: str
    product_title: str
    product_url: str

    variant_id: int
    variant_title: str
    sku: Optional[str]

    price: Optional[float]
    compare_at_price: Optional[float]
    currency: str

    availability: str  # in_stock / out_of_stock
    image_url: Optional[str]

    scraped_at: str  # ISO datetime
