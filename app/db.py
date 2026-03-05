from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def db_session(db_path: Path):
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with db_session(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS clients (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              slug TEXT NOT NULL UNIQUE,
              name TEXT NOT NULL,
              economic_group TEXT NOT NULL DEFAULT 'default',
              root_dir TEXT NOT NULL,
              active INTEGER NOT NULL DEFAULT 1,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS client_store_maps (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              client_id INTEGER NOT NULL,
              cnpj TEXT NOT NULL,
              store_name TEXT NOT NULL,
              UNIQUE (client_id, cnpj),
              FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS store_runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              store_map_id INTEGER NOT NULL,
              period TEXT NOT NULL,
              started_at TEXT NOT NULL,
              finished_at TEXT,
              status TEXT NOT NULL,
              message TEXT,
              pending_skus INTEGER NOT NULL DEFAULT 0,
              alerts_count INTEGER NOT NULL DEFAULT 0,
              FOREIGN KEY (store_map_id) REFERENCES client_store_maps(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              client_id INTEGER NOT NULL,
              started_at TEXT NOT NULL,
              finished_at TEXT,
              status TEXT NOT NULL,
              message TEXT,
              xml_count INTEGER NOT NULL DEFAULT 0,
              pending_review_count INTEGER NOT NULL DEFAULT 0,
              alerts_diario_count INTEGER NOT NULL DEFAULT 0,
              alerts_semanal_count INTEGER NOT NULL DEFAULT 0,
              alerts_semanal_atual_count INTEGER NOT NULL DEFAULT 0,
              alerts_mensal_count INTEGER NOT NULL DEFAULT 0,
              output_dir TEXT,
              FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS global_catalog (
              sku_id TEXT PRIMARY KEY,
              sku_name_canonical TEXT,
              brand TEXT,
              category TEXT,
              base_measure TEXT,
              payload_json TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS global_mappings (
              fingerprint TEXT PRIMARY KEY,
              sku_id TEXT,
              base_measure_override TEXT,
              base_qty_per_purchase_unit_override TEXT,
              status TEXT,
              payload_json TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS global_review (
              fingerprint TEXT PRIMARY KEY,
              status TEXT,
              chosen_sku_id TEXT,
              notes TEXT,
              payload_json TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            """
        )
        _ensure_clients_schema(conn)
        _ensure_store_maps_schema(conn)


def _ensure_clients_schema(conn: sqlite3.Connection) -> None:
    cols = {
        str(row["name"])
        for row in fetch_all(conn, "PRAGMA table_info(clients)")
    }
    if "economic_group" not in cols:
        conn.execute(
            "ALTER TABLE clients ADD COLUMN economic_group TEXT NOT NULL DEFAULT 'default'"
        )
    if "contact_name" not in cols:
        conn.execute(
            "ALTER TABLE clients ADD COLUMN contact_name TEXT NOT NULL DEFAULT ''"
        )
    if "whatsapp" not in cols:
        conn.execute(
            "ALTER TABLE clients ADD COLUMN whatsapp TEXT NOT NULL DEFAULT ''"
        )
    if "client_type" not in cols:
        conn.execute(
            "ALTER TABLE clients ADD COLUMN client_type TEXT NOT NULL DEFAULT 'grupo'"
        )


def _ensure_store_maps_schema(conn: sqlite3.Connection) -> None:
    cols = {
        str(row["name"])
        for row in fetch_all(conn, "PRAGMA table_info(client_store_maps)")
    }
    if "whatsapp" not in cols:
        conn.execute(
            "ALTER TABLE client_store_maps ADD COLUMN whatsapp TEXT NOT NULL DEFAULT ''"
        )
    if "nfstock_token_encrypted" not in cols:
        conn.execute(
            "ALTER TABLE client_store_maps ADD COLUMN nfstock_token_encrypted TEXT NOT NULL DEFAULT ''"
        )
    if "active" not in cols:
        conn.execute(
            "ALTER TABLE client_store_maps ADD COLUMN active INTEGER NOT NULL DEFAULT 1"
        )


def fetch_all(conn: sqlite3.Connection, query: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    cur = conn.execute(query, tuple(params))
    return list(cur.fetchall())


def fetch_one(conn: sqlite3.Connection, query: str, params: Iterable[Any] = ()) -> sqlite3.Row | None:
    cur = conn.execute(query, tuple(params))
    return cur.fetchone()
