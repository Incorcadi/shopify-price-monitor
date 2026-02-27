from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from .models import VariantSnapshot


SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  run_id INTEGER PRIMARY KEY AUTOINCREMENT,
  store_domain TEXT NOT NULL,
  started_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS items_raw (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  key TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  scraped_at TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS items_latest (
  key TEXT PRIMARY KEY,
  payload_json TEXT NOT NULL,
  scraped_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  key TEXT NOT NULL,
  price REAL,
  compare_at_price REAL,
  availability TEXT,
  scraped_at TEXT NOT NULL
);
"""


def _key(s: VariantSnapshot) -> str:
    # самый стабильный ключ для мониторинга — variant_id
    return f"{s.store_domain}::variant::{s.variant_id}"


class SqliteStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.cx = sqlite3.connect(db_path)
        self.cx.execute("PRAGMA journal_mode=WAL;")
        self.cx.executescript(SCHEMA)
        self.cx.commit()

    def close(self) -> None:
        self.cx.close()

    def start_run(self, *, store_domain: str, started_at: str) -> int:
        cur = self.cx.execute("INSERT INTO runs(store_domain, started_at) VALUES(?, ?)", (store_domain, started_at))
        self.cx.commit()
        return int(cur.lastrowid)

    def upsert_snapshots(self, run_id: int, items: Iterable[VariantSnapshot]) -> int:
        n = 0
        for it in items:
            key = _key(it)
            payload = json.dumps(asdict(it), ensure_ascii=False)
            self.cx.execute(
                "INSERT INTO items_raw(run_id, key, payload_json, scraped_at) VALUES(?,?,?,?)",
                (run_id, key, payload, it.scraped_at),
            )
            self.cx.execute(
                "INSERT INTO items_latest(key, payload_json, scraped_at) VALUES(?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET payload_json=excluded.payload_json, scraped_at=excluded.scraped_at",
                (key, payload, it.scraped_at),
            )
            self.cx.execute(
                "INSERT INTO history(key, price, compare_at_price, availability, scraped_at) VALUES(?,?,?,?,?)",
                (key, it.price, it.compare_at_price, it.availability, it.scraped_at),
            )
            n += 1
        self.cx.commit()
        return n

    def load_latest(self) -> List[VariantSnapshot]:
        rows = self.cx.execute("SELECT payload_json FROM items_latest").fetchall()
        out: List[VariantSnapshot] = []
        for (js,) in rows:
            d = json.loads(js)
            out.append(VariantSnapshot(**d))
        return out

    def load_latest_map(self) -> dict[str, VariantSnapshot]:
        rows = self.cx.execute("SELECT key, payload_json FROM items_latest").fetchall()
        out: dict[str, VariantSnapshot] = {}
        for key, js in rows:
            d = json.loads(js)
            out[str(key)] = VariantSnapshot(**d)
        return out

    def load_prev_latest_map(self) -> dict[str, VariantSnapshot]:
        # предыдущий снимок: берём второй по "scraped_at" в items_raw по каждому key (грубый, но работает для портфолио)
        # Для "взрослого" варианта лучше хранить "latest_per_run".
        rows = self.cx.execute(
            "SELECT key, payload_json FROM items_raw ORDER BY id DESC"
        ).fetchall()

        # берём первое встреченное значение как latest, второе как prev
        latest_seen: set[str] = set()
        prev: dict[str, VariantSnapshot] = {}
        for key, js in rows:
            key = str(key)
            if key not in latest_seen:
                latest_seen.add(key)
                continue
            if key not in prev:
                d = json.loads(js)
                prev[key] = VariantSnapshot(**d)
        return prev
