from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .db import db_session, fetch_all, fetch_one, utc_now_iso


@dataclass
class Client:
    id: int
    slug: str
    name: str
    economic_group: str
    root_dir: Path
    active: bool


@dataclass
class RunSummary:
    id: int
    client_id: int
    started_at: str
    finished_at: str | None
    status: str
    message: str | None
    xml_count: int
    pending_review_count: int
    alerts_diario_count: int
    alerts_semanal_count: int
    alerts_semanal_atual_count: int
    alerts_mensal_count: int
    output_dir: str | None


def _row_to_client(row) -> Client:
    return Client(
        id=int(row["id"]),
        slug=str(row["slug"]),
        name=str(row["name"]),
        economic_group=str(row["economic_group"] or "default"),
        root_dir=Path(str(row["root_dir"])),
        active=bool(row["active"]),
    )


def _row_to_run(row) -> RunSummary:
    return RunSummary(
        id=int(row["id"]),
        client_id=int(row["client_id"]),
        started_at=str(row["started_at"]),
        finished_at=row["finished_at"],
        status=str(row["status"]),
        message=row["message"],
        xml_count=int(row["xml_count"] or 0),
        pending_review_count=int(row["pending_review_count"] or 0),
        alerts_diario_count=int(row["alerts_diario_count"] or 0),
        alerts_semanal_count=int(row["alerts_semanal_count"] or 0),
        alerts_semanal_atual_count=int(row["alerts_semanal_atual_count"] or 0),
        alerts_mensal_count=int(row["alerts_mensal_count"] or 0),
        output_dir=row["output_dir"],
    )


def create_or_update_client(
    db_path: Path,
    *,
    slug: str,
    name: str,
    economic_group: str = "default",
    root_dir: Path,
    active: bool = True,
) -> Client:
    now = utc_now_iso()
    group_key = economic_group.strip() or "default"
    with db_session(db_path) as conn:
        current = fetch_one(conn, "SELECT * FROM clients WHERE slug = ?", (slug,))
        if current:
            conn.execute(
                """
                UPDATE clients
                   SET name = ?, economic_group = ?, root_dir = ?, active = ?, updated_at = ?
                 WHERE slug = ?
                """,
                (name, group_key, str(root_dir), int(active), now, slug),
            )
        else:
            conn.execute(
                """
                INSERT INTO clients (slug, name, economic_group, root_dir, active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (slug, name, group_key, str(root_dir), int(active), now, now),
            )
        row = fetch_one(conn, "SELECT * FROM clients WHERE slug = ?", (slug,))
        return _row_to_client(row)


def set_client_stores(db_path: Path, client_id: int, stores: dict[str, str]) -> None:
    with db_session(db_path) as conn:
        conn.execute("DELETE FROM client_store_maps WHERE client_id = ?", (client_id,))
        for cnpj, store_name in stores.items():
            digits = "".join(ch for ch in str(cnpj) if ch.isdigit())
            if not digits:
                continue
            conn.execute(
                """
                INSERT INTO client_store_maps (client_id, cnpj, store_name)
                VALUES (?, ?, ?)
                """,
                (client_id, digits, store_name.strip()),
            )


def get_client_stores(db_path: Path, client_id: int) -> dict[str, str]:
    with db_session(db_path) as conn:
        rows = fetch_all(
            conn,
            "SELECT cnpj, store_name FROM client_store_maps WHERE client_id = ? ORDER BY cnpj",
            (client_id,),
        )
    return {str(r["cnpj"]): str(r["store_name"]) for r in rows}


def list_clients(db_path: Path, *, only_active: bool = False) -> list[Client]:
    query = "SELECT * FROM clients"
    params: tuple = ()
    if only_active:
        query += " WHERE active = 1"
    query += " ORDER BY name"

    with db_session(db_path) as conn:
        rows = fetch_all(conn, query, params)
    return [_row_to_client(r) for r in rows]


def get_client_by_slug(db_path: Path, slug: str) -> Client | None:
    with db_session(db_path) as conn:
        row = fetch_one(conn, "SELECT * FROM clients WHERE slug = ?", (slug,))
    return _row_to_client(row) if row else None


def get_client_by_id(db_path: Path, client_id: int) -> Client | None:
    with db_session(db_path) as conn:
        row = fetch_one(conn, "SELECT * FROM clients WHERE id = ?", (client_id,))
    return _row_to_client(row) if row else None


def set_client_active(db_path: Path, client_id: int, active: bool) -> None:
    with db_session(db_path) as conn:
        conn.execute(
            "UPDATE clients SET active = ?, updated_at = ? WHERE id = ?",
            (int(active), utc_now_iso(), client_id),
        )


def create_run(db_path: Path, client_id: int) -> int:
    with db_session(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO runs (client_id, started_at, status)
            VALUES (?, ?, 'running')
            """,
            (client_id, utc_now_iso()),
        )
        return int(cur.lastrowid)


def finish_run(
    db_path: Path,
    run_id: int,
    *,
    status: str,
    message: str | None = None,
    xml_count: int = 0,
    pending_review_count: int = 0,
    alerts_diario_count: int = 0,
    alerts_semanal_count: int = 0,
    alerts_semanal_atual_count: int = 0,
    alerts_mensal_count: int = 0,
    output_dir: str | None = None,
) -> None:
    with db_session(db_path) as conn:
        conn.execute(
            """
            UPDATE runs
               SET finished_at = ?,
                   status = ?,
                   message = ?,
                   xml_count = ?,
                   pending_review_count = ?,
                   alerts_diario_count = ?,
                   alerts_semanal_count = ?,
                   alerts_semanal_atual_count = ?,
                   alerts_mensal_count = ?,
                   output_dir = ?
             WHERE id = ?
            """,
            (
                utc_now_iso(),
                status,
                message,
                xml_count,
                pending_review_count,
                alerts_diario_count,
                alerts_semanal_count,
                alerts_semanal_atual_count,
                alerts_mensal_count,
                output_dir,
                run_id,
            ),
        )


def get_last_run_for_client(db_path: Path, client_id: int) -> RunSummary | None:
    with db_session(db_path) as conn:
        row = fetch_one(
            conn,
            "SELECT * FROM runs WHERE client_id = ? ORDER BY id DESC LIMIT 1",
            (client_id,),
        )
    return _row_to_run(row) if row else None


def list_recent_runs(db_path: Path, client_id: int, limit: int = 20) -> list[RunSummary]:
    with db_session(db_path) as conn:
        rows = fetch_all(
            conn,
            "SELECT * FROM runs WHERE client_id = ? ORDER BY id DESC LIMIT ?",
            (client_id, limit),
        )
    return [_row_to_run(r) for r in rows]
