from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from .models import VariantSnapshot


@dataclass(frozen=True)
class Change:
    key: str
    kind: str  # new / price / availability / compare_at
    before: Optional[VariantSnapshot]
    after: VariantSnapshot


def diff_snapshots(prev: Dict[str, VariantSnapshot], curr: Dict[str, VariantSnapshot]) -> List[Change]:
    out: List[Change] = []

    for key, after in curr.items():
        before = prev.get(key)
        if before is None:
            out.append(Change(key=key, kind="new", before=None, after=after))
            continue

        if before.availability != after.availability:
            out.append(Change(key=key, kind="availability", before=before, after=after))

        if before.price != after.price:
            out.append(Change(key=key, kind="price", before=before, after=after))

        if before.compare_at_price != after.compare_at_price:
            out.append(Change(key=key, kind="compare_at", before=before, after=after))

    return out
