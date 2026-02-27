from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Mapping, Tuple

import pandas as pd
import streamlit as st


def connect(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path, check_same_thread=False)


def _show_df(df: pd.DataFrame, *, height: int) -> None:
    """Render a dataframe; hide the index if the installed Streamlit supports it."""
    try:
        st.dataframe(df, use_container_width=True, height=height, hide_index=True)  # type: ignore[call-arg]
    except TypeError:
        st.dataframe(df, use_container_width=True, height=height)


def _db_mtime(db_path: str) -> float:
    try:
        return Path(db_path).stat().st_mtime
    except FileNotFoundError:
        return 0.0


@st.cache_data(show_spinner=False)
def load_latest_snapshot(db_path: str, mtime: float) -> pd.DataFrame:
    _ = mtime  # used for cache invalidation when DB changes
    con = connect(db_path)
    try:
        rows = con.execute("SELECT payload_json, scraped_at FROM items_latest").fetchall()
    finally:
        con.close()

    items: List[Dict[str, Any]] = []
    for payload_json, scraped_at in rows:
        d: Dict[str, Any] = json.loads(payload_json)
        d["scraped_at"] = scraped_at
        d["key"] = f'{d.get("store_domain")}::variant::{d.get("variant_id")}'
        items.append(d)

    df = pd.DataFrame(items)
    if df.empty:
        return df

    # Keep only useful columns for the dashboard
    cols = [
        "product_title",
        "product_handle",
        "sku",
        "price",
        "currency",
        "availability",
    ]
    cols = [c for c in cols if c in df.columns]
    df = df.loc[:, cols].copy()

    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    return df


@st.cache_data(show_spinner=False)
def load_run_ids(db_path: str, mtime: float) -> List[Tuple[int, str, str]]:
    _ = mtime
    con = connect(db_path)
    try:
        rows = con.execute(
            "SELECT run_id, store_domain, started_at FROM runs ORDER BY run_id DESC LIMIT 5"
        ).fetchall()
    finally:
        con.close()
    out: List[Tuple[int, str, str]] = []
    for run_id, store_domain, started_at in rows:
        out.append((int(run_id), str(store_domain), str(started_at)))
    return out


@st.cache_data(show_spinner=False)
def load_run_items(db_path: str, run_id: int, mtime: float) -> Dict[str, Dict[str, Any]]:
    _ = mtime
    con = connect(db_path)
    try:
        rows = con.execute(
            "SELECT key, payload_json FROM items_raw WHERE run_id = ?",
            (run_id,),
        ).fetchall()
    finally:
        con.close()

    out: Dict[str, Dict[str, Any]] = {}
    for key, payload_json in rows:
        out[str(key)] = json.loads(payload_json)
    return out


def diff_two_runs(
    prev: Mapping[str, Mapping[str, Any]],
    curr: Mapping[str, Mapping[str, Any]],
) -> pd.DataFrame:
    keys = set(prev.keys()) | set(curr.keys())
    changes: List[Dict[str, Any]] = []

    for k in keys:
        a = prev.get(k)
        b = curr.get(k)

        if a is None and b is not None:
            changes.append(
                {
                    "kind": "new",
                    "title": b.get("product_title"),
                    "sku": b.get("sku"),
                    "price_before": None,
                    "price_after": b.get("price"),
                    "stock_before": None,
                    "stock_after": b.get("availability"),
                }
            )
            continue

        if a is not None and b is None:
            changes.append(
                {
                    "kind": "removed",
                    "title": a.get("product_title"),
                    "sku": a.get("sku"),
                    "price_before": a.get("price"),
                    "price_after": None,
                    "stock_before": a.get("availability"),
                    "stock_after": None,
                }
            )
            continue

        if a is None or b is None:
            continue

        if a.get("price") != b.get("price"):
            changes.append(
                {
                    "kind": "price",
                    "title": b.get("product_title"),
                    "sku": b.get("sku"),
                    "price_before": a.get("price"),
                    "price_after": b.get("price"),
                    "stock_before": a.get("availability"),
                    "stock_after": b.get("availability"),
                }
            )

        if a.get("availability") != b.get("availability"):
            changes.append(
                {
                    "kind": "availability",
                    "title": b.get("product_title"),
                    "sku": b.get("sku"),
                    "price_before": a.get("price"),
                    "price_after": b.get("price"),
                    "stock_before": a.get("availability"),
                    "stock_after": b.get("availability"),
                }
            )

    df = pd.DataFrame(changes)
    if not df.empty:
        for c in ("price_before", "price_after"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# ---------- UI ----------
st.set_page_config(page_title="Shopify Price Monitor", layout="wide")
st.title("Shopify Price Monitor — Demo Dashboard")

default_db = "outputs/demo_diff2.db"
db_path = st.sidebar.text_input("SQLite DB path", value=default_db)
mtime = _db_mtime(db_path)

if not Path(db_path).exists():
    st.error(f"DB not found: {db_path}")
    st.stop()

df = load_latest_snapshot(db_path, mtime)
runs = load_run_ids(db_path, mtime)

if df.empty:
    st.warning("No data in items_latest yet. Run `price-monitor run ...` first.")
    st.stop()

# KPI
col1, col2, col3 = st.columns(3)
total = len(df)
in_stock = int((df["availability"] == "in_stock").sum()) if "availability" in df.columns else 0
out_stock = int((df["availability"] == "out_of_stock").sum()) if "availability" in df.columns else 0

col1.metric("Products/variants", total)
col2.metric("In stock", in_stock)
col3.metric("Out of stock", out_stock)

st.divider()

# Filters
st.subheader("Latest snapshot")
q = st.text_input("Search (title / sku / handle)", "")
stock_filter = st.selectbox("Stock filter", ["all", "in_stock", "out_of_stock"])
sort_by = st.selectbox("Sort by", ["title", "price", "stock"])

view: pd.DataFrame = df.copy()

if q.strip():
    ql = q.strip().lower()
    mask = pd.Series(False, index=view.index)
    for c in ("product_title", "sku", "product_handle"):
        if c in view.columns:
            mask = mask | view[c].fillna("").astype(str).str.lower().str.contains(ql)
    view = view.loc[mask]

if stock_filter != "all" and "availability" in view.columns:
    view = view.loc[view["availability"] == stock_filter]

if sort_by == "title" and "product_title" in view.columns:
    view = view.sort_values(by="product_title")
elif sort_by == "price" and "price" in view.columns:
    view = view.sort_values(by="price", ascending=False)
elif sort_by == "stock" and "availability" in view.columns:
    view = view.sort_values(by="availability")

# Client-friendly columns (no long URLs; hide Handle by default)
display = view.loc[:, [c for c in ("product_title", "sku", "price", "currency", "availability") if c in view.columns]].copy()
display = display.rename(
    columns={
        "product_title": "Title",
        "sku": "SKU",
        "price": "Price",
        "currency": "Cur",
        "availability": "Stock",
    }
)
_show_df(display, height=420)

st.divider()

st.subheader("Changes between last two runs")
if len(runs) < 2:
    st.info("Need at least 2 runs saved in DB to show a diff.")
else:
    run_curr = runs[0][0]
    run_prev = runs[1][0]

    curr_items = load_run_items(db_path, run_curr, mtime)
    prev_items = load_run_items(db_path, run_prev, mtime)
    diff_df = diff_two_runs(prev_items, curr_items)

    if diff_df.empty:
        st.success("No changes detected between the last two runs.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("New", int((diff_df["kind"] == "new").sum()))
        c2.metric("Removed", int((diff_df["kind"] == "removed").sum()))
        c3.metric("Price changes", int((diff_df["kind"] == "price").sum()))
        c4.metric("Stock changes", int((diff_df["kind"] == "availability").sum()))

        kind = st.selectbox("Filter kind", ["all", "new", "removed", "price", "availability"])
        show = diff_df.copy()
        if kind != "all":
            show = show.loc[show["kind"] == kind]

        # For portfolio: show only meaningful columns for each kind
        if kind == "new":
            cols = ["kind", "title", "sku", "price_after", "stock_after"]
        elif kind == "removed":
            cols = ["kind", "title", "sku", "price_before", "stock_before"]
        elif kind == "price":
            cols = ["kind", "title", "sku", "price_before", "price_after", "stock_after"]
        elif kind == "availability":
            cols = ["kind", "title", "sku", "stock_before", "stock_after", "price_after"]
        else:
            cols = ["kind", "title", "sku", "price_before", "price_after", "stock_before", "stock_after"]

        pretty = {
            "kind": "Kind",
            "title": "Title",
            "sku": "SKU",
            "price_before": "Price (before)",
            "price_after": "Price (after)",
            "stock_before": "Stock (before)",
            "stock_after": "Stock (after)",
        }
        show = show.loc[:, cols].rename(columns=pretty)
        _show_df(show, height=360)
