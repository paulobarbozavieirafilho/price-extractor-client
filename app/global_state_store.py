from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .db import db_session, fetch_all, fetch_one, utc_now_iso


CATALOG_COLS = [
    "sku_id",
    "sku_name_canonical",
    "brand",
    "category",
    "base_measure",
]

MAPPINGS_COLS = [
    "fingerprint",
    "sku_id",
    "base_measure_override",
    "base_qty_per_purchase_unit_override",
    "status",
    "updated_at",
]

REVIEW_COLS = [
    "fingerprint",
    "emit_cnpj",
    "emit_xNome",
    "dest_cnpj",
    "dest_xNome",
    "xProd",
    "xProd_norm",
    "uCom",
    "qCom",
    "vUnCom",
    "suggested_sku_id",
    "suggested_sku_name",
    "suggested_sku_name_canonical",
    "suggested_base_measure",
    "suggested_base_qty_per_purchase_unit",
    "confidence",
    "rationale",
    "status",
    "chosen_sku_id",
    "base_measure_override",
    "base_qty_per_purchase_unit_override",
    "notes",
    "conversion_issue",
    "created_at",
    "updated_at",
]

PENDING_REVIEW_STATUSES = {"", "PENDING", "REVIEW"}

ALERT_SHEETS = [
    "Compras",
    "Alertas_Diario",
    "Alertas_Semanal",
    "Alertas_Semanal_Atual",
    "Alertas_Mensal",
]


def _normalize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if pd.isna(value):
            return None
        return float(value)
    if isinstance(value, (datetime, pd.Timestamp)):
        return pd.to_datetime(value).strftime("%Y-%m-%d %H:%M:%S")
    if pd.isna(value):
        return None
    return str(value)


def _row_to_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {k: _normalize_value(v) for k, v in row.items()}


def _loads_payload(payload_json: str) -> dict[str, Any]:
    try:
        obj = json.loads(payload_json)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return {}


def _read_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_excel(path, sheet_name=sheet_name)
    except Exception:
        return pd.DataFrame()


def _reorder_columns(df: pd.DataFrame, preferred: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=preferred)
    for col in preferred:
        if col not in df.columns:
            df[col] = ""
    lead = [c for c in preferred if c in df.columns]
    tail = [c for c in df.columns if c not in lead]
    return df[lead + tail]


def import_global_state_from_excel(db_path: Path, editor_path: Path) -> dict[str, int]:
    review_df = _read_sheet(editor_path, "Review")
    catalog_df = _read_sheet(editor_path, "Catalog")
    mappings_df = _read_sheet(editor_path, "Mappings")

    inserted = {"review": 0, "catalog": 0, "mappings": 0}
    now = utc_now_iso()

    with db_session(db_path) as conn:
        for _, row in catalog_df.iterrows():
            payload = _row_to_payload(dict(row))
            sku_id = str(payload.get("sku_id") or "").strip()
            if not sku_id:
                continue
            conn.execute(
                """
                INSERT INTO global_catalog (
                  sku_id, sku_name_canonical, brand, category, base_measure, payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sku_id) DO UPDATE SET
                  sku_name_canonical=excluded.sku_name_canonical,
                  brand=excluded.brand,
                  category=excluded.category,
                  base_measure=excluded.base_measure,
                  payload_json=excluded.payload_json,
                  updated_at=excluded.updated_at
                """,
                (
                    sku_id,
                    str(payload.get("sku_name_canonical") or ""),
                    str(payload.get("brand") or ""),
                    str(payload.get("category") or ""),
                    str(payload.get("base_measure") or ""),
                    json.dumps(payload, ensure_ascii=False),
                    now,
                ),
            )
            inserted["catalog"] += 1

        for _, row in mappings_df.iterrows():
            payload = _row_to_payload(dict(row))
            fingerprint = str(payload.get("fingerprint") or "").strip()
            if not fingerprint:
                continue
            conn.execute(
                """
                INSERT INTO global_mappings (
                  fingerprint, sku_id, base_measure_override, base_qty_per_purchase_unit_override,
                  status, payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fingerprint) DO UPDATE SET
                  sku_id=excluded.sku_id,
                  base_measure_override=excluded.base_measure_override,
                  base_qty_per_purchase_unit_override=excluded.base_qty_per_purchase_unit_override,
                  status=excluded.status,
                  payload_json=excluded.payload_json,
                  updated_at=excluded.updated_at
                """,
                (
                    fingerprint,
                    str(payload.get("sku_id") or ""),
                    str(payload.get("base_measure_override") or ""),
                    str(payload.get("base_qty_per_purchase_unit_override") or ""),
                    str(payload.get("status") or ""),
                    json.dumps(payload, ensure_ascii=False),
                    now,
                ),
            )
            inserted["mappings"] += 1

        for _, row in review_df.iterrows():
            payload = _row_to_payload(dict(row))
            fingerprint = str(payload.get("fingerprint") or "").strip()
            if not fingerprint:
                continue
            conn.execute(
                """
                INSERT INTO global_review (
                  fingerprint, status, chosen_sku_id, notes, payload_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fingerprint) DO UPDATE SET
                  status=excluded.status,
                  chosen_sku_id=excluded.chosen_sku_id,
                  notes=excluded.notes,
                  payload_json=excluded.payload_json,
                  updated_at=excluded.updated_at
                """,
                (
                    fingerprint,
                    str(payload.get("status") or ""),
                    str(payload.get("chosen_sku_id") or ""),
                    str(payload.get("notes") or ""),
                    json.dumps(payload, ensure_ascii=False),
                    now,
                    now,
                ),
            )
            inserted["review"] += 1

    return inserted


def _rows_to_dataframe(rows: list[Any], key_col: str, preferred_cols: list[str]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=preferred_cols)
    data: list[dict[str, Any]] = []
    for row in rows:
        payload = _loads_payload(str(row["payload_json"]))
        payload[key_col] = str(row[key_col] or payload.get(key_col) or "")
        data.append(payload)
    df = pd.DataFrame(data)
    return _reorder_columns(df, preferred_cols)


def export_global_state_to_excel(db_path: Path, editor_path: Path) -> None:
    editor_path.parent.mkdir(parents=True, exist_ok=True)
    preserved: dict[str, pd.DataFrame] = {
        name: _read_sheet(editor_path, name) for name in ALERT_SHEETS
    }

    with db_session(db_path) as conn:
        catalog_rows = fetch_all(conn, "SELECT * FROM global_catalog ORDER BY sku_id")
        mappings_rows = fetch_all(conn, "SELECT * FROM global_mappings ORDER BY fingerprint")
        review_rows = fetch_all(conn, "SELECT * FROM global_review ORDER BY updated_at DESC")

    catalog_df = _rows_to_dataframe(catalog_rows, "sku_id", CATALOG_COLS)
    mappings_df = _rows_to_dataframe(mappings_rows, "fingerprint", MAPPINGS_COLS)
    review_df = _rows_to_dataframe(review_rows, "fingerprint", REVIEW_COLS)

    with pd.ExcelWriter(editor_path, engine="openpyxl") as writer:
        review_df.to_excel(writer, index=False, sheet_name="Review")
        catalog_df.to_excel(writer, index=False, sheet_name="Catalog")
        mappings_df.to_excel(writer, index=False, sheet_name="Mappings")
        for sheet_name in ALERT_SHEETS:
            preserved[sheet_name].to_excel(writer, index=False, sheet_name=sheet_name)


def global_state_counts(db_path: Path) -> dict[str, int]:
    with db_session(db_path) as conn:
        review_total = int(fetch_one(conn, "SELECT COUNT(*) AS n FROM global_review")["n"])
        catalog_total = int(fetch_one(conn, "SELECT COUNT(*) AS n FROM global_catalog")["n"])
        mappings_total = int(fetch_one(conn, "SELECT COUNT(*) AS n FROM global_mappings")["n"])
        review_pending = int(
            fetch_one(
                conn,
                """
                SELECT COUNT(*) AS n
                  FROM global_review
                 WHERE UPPER(TRIM(COALESCE(status, ''))) IN ('', 'PENDING', 'REVIEW')
                """,
            )["n"]
        )
    return {
        "review_total": review_total,
        "review_pending": review_pending,
        "catalog_total": catalog_total,
        "mappings_total": mappings_total,
    }


def _review_row_payload(row: Any) -> dict[str, Any]:
    payload = _loads_payload(str(row["payload_json"]))
    payload["fingerprint"] = str(row["fingerprint"] or payload.get("fingerprint") or "")
    payload["status"] = str(row["status"] or payload.get("status") or "")
    payload["chosen_sku_id"] = str(row["chosen_sku_id"] or payload.get("chosen_sku_id") or "")
    payload["notes"] = str(row["notes"] or payload.get("notes") or "")
    payload["created_at"] = str(payload.get("created_at") or row["created_at"] or "")
    payload["updated_at"] = str(payload.get("updated_at") or row["updated_at"] or "")
    for col in REVIEW_COLS:
        payload.setdefault(col, "")
    return payload


def _match_review_status(status: str, status_filter: str) -> bool:
    normalized = status.strip().upper()
    key = status_filter.strip().lower() or "pending"
    if key == "all":
        return True
    if key == "pending":
        return normalized in PENDING_REVIEW_STATUSES
    if key == "approved":
        return normalized == "APPROVED"
    if key in {"ignore", "ignored"}:
        return normalized in {"IGNORE", "IGNORED"}
    if key == "review":
        return normalized == "REVIEW"
    if key == "empty":
        return normalized == ""
    return normalized == key.upper()


def list_global_review_rows(
    db_path: Path,
    q: str = "",
    limit: int = 300,
    status_filter: str = "pending",
) -> list[dict[str, Any]]:
    qn = q.strip().lower()
    out: list[dict[str, Any]] = []
    with db_session(db_path) as conn:
        rows = fetch_all(conn, "SELECT * FROM global_review ORDER BY updated_at DESC")

    for row in rows:
        payload = _review_row_payload(row)
        if not _match_review_status(str(payload.get("status", "")), status_filter):
            continue

        if qn:
            hay = " ".join(
                str(payload.get(k, ""))
                for k in [
                    "fingerprint",
                    "emit_cnpj",
                    "emit_xNome",
                    "dest_cnpj",
                    "dest_xNome",
                    "xProd",
                    "xProd_norm",
                    "uCom",
                    "qCom",
                    "vUnCom",
                    "suggested_sku_id",
                    "suggested_sku_name",
                    "suggested_sku_name_canonical",
                    "suggested_base_measure",
                    "suggested_base_qty_per_purchase_unit",
                    "confidence",
                    "rationale",
                    "status",
                    "chosen_sku_id",
                    "base_measure_override",
                    "base_qty_per_purchase_unit_override",
                    "notes",
                    "conversion_issue",
                ]
            ).lower()
            if qn not in hay:
                continue

        out.append(payload)
        if len(out) >= limit:
            break
    return out


def global_review_status_counts(db_path: Path) -> dict[str, int]:
    counts = {
        "all": 0,
        "pending": 0,
        "approved": 0,
        "ignore": 0,
        "review": 0,
        "empty": 0,
    }
    with db_session(db_path) as conn:
        rows = fetch_all(
            conn,
            """
            SELECT UPPER(TRIM(COALESCE(status, ''))) AS status_norm, COUNT(*) AS n
              FROM global_review
             GROUP BY UPPER(TRIM(COALESCE(status, '')))
            """,
        )
    for row in rows:
        status_norm = str(row["status_norm"] or "")
        n = int(row["n"] or 0)
        counts["all"] += n
        if status_norm in PENDING_REVIEW_STATUSES:
            counts["pending"] += n
        if status_norm == "APPROVED":
            counts["approved"] += n
        if status_norm in {"IGNORE", "IGNORED"}:
            counts["ignore"] += n
        if status_norm == "REVIEW":
            counts["review"] += n
        if status_norm == "":
            counts["empty"] += n
    return counts


def list_global_pending_review(db_path: Path, q: str = "", limit: int = 300) -> list[dict[str, Any]]:
    return list_global_review_rows(db_path, q=q, limit=limit, status_filter="pending")


def apply_global_review_decision(
    db_path: Path,
    *,
    fingerprint: str,
    action: str,
    chosen_sku_id: str | None = None,
    base_measure_override: str | None = None,
    base_qty_per_purchase_unit_override: str | None = None,
    notes: str | None = None,
) -> bool:
    fp = str(fingerprint).strip()
    if not fp:
        return False

    act = action.strip().lower()
    if act not in {"approve", "ignore"}:
        return False

    with db_session(db_path) as conn:
        row = fetch_one(conn, "SELECT * FROM global_review WHERE fingerprint = ?", (fp,))
        if not row:
            return False
        payload = _loads_payload(str(row["payload_json"]))

        new_status = "APPROVED" if act == "approve" else "IGNORE"
        payload["status"] = new_status
        if act == "approve" and (chosen_sku_id or "").strip():
            payload["chosen_sku_id"] = (chosen_sku_id or "").strip()
        if base_measure_override is not None:
            payload["base_measure_override"] = base_measure_override.strip()
        if base_qty_per_purchase_unit_override is not None:
            payload["base_qty_per_purchase_unit_override"] = (
                base_qty_per_purchase_unit_override.strip()
            )
        if notes is not None:
            payload["notes"] = notes.strip()
        payload["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        conn.execute(
            """
            UPDATE global_review
               SET status = ?, chosen_sku_id = ?, notes = ?, payload_json = ?, updated_at = ?
             WHERE fingerprint = ?
            """,
            (
                new_status,
                str(payload.get("chosen_sku_id", "") or ""),
                str(payload.get("notes", "") or ""),
                json.dumps(payload, ensure_ascii=False),
                utc_now_iso(),
                fp,
            ),
        )
    return True


def update_global_review_suggestion(
    db_path: Path,
    *,
    fingerprint: str,
    suggested_sku_id: str | None = None,
    suggested_sku_name: str | None = None,
    suggested_sku_name_canonical: str | None = None,
    suggested_base_measure: str | None = None,
    suggested_base_qty_per_purchase_unit: str | None = None,
    confidence: str | None = None,
    rationale: str | None = None,
) -> bool:
    fp = str(fingerprint or "").strip()
    if not fp:
        return False

    with db_session(db_path) as conn:
        row = fetch_one(conn, "SELECT * FROM global_review WHERE fingerprint = ?", (fp,))
        if not row:
            return False

        payload = _review_row_payload(row)
        if suggested_sku_id is not None:
            payload["suggested_sku_id"] = str(suggested_sku_id).strip()
        if suggested_sku_name is not None:
            payload["suggested_sku_name"] = str(suggested_sku_name).strip()
        if suggested_sku_name_canonical is not None:
            payload["suggested_sku_name_canonical"] = str(suggested_sku_name_canonical).strip()
        if suggested_base_measure is not None:
            payload["suggested_base_measure"] = str(suggested_base_measure).strip()
        if suggested_base_qty_per_purchase_unit is not None:
            payload["suggested_base_qty_per_purchase_unit"] = str(
                suggested_base_qty_per_purchase_unit
            ).strip()
        if confidence is not None:
            payload["confidence"] = str(confidence).strip()
        if rationale is not None:
            payload["rationale"] = str(rationale).strip()

        payload["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        conn.execute(
            """
            UPDATE global_review
               SET payload_json = ?, updated_at = ?
             WHERE fingerprint = ?
            """,
            (
                json.dumps(payload, ensure_ascii=False),
                utc_now_iso(),
                fp,
            ),
        )
    return True


def list_global_mappings(db_path: Path, q: str = "", limit: int = 500) -> list[dict[str, Any]]:
    qn = q.strip().lower()
    rows_out: list[dict[str, Any]] = []
    with db_session(db_path) as conn:
        rows = fetch_all(conn, "SELECT * FROM global_mappings ORDER BY updated_at DESC")

    for row in rows:
        payload = _loads_payload(str(row["payload_json"]))
        payload["fingerprint"] = str(row["fingerprint"] or payload.get("fingerprint") or "")
        payload["sku_id"] = str(row["sku_id"] or payload.get("sku_id") or "")
        payload["status"] = str(row["status"] or payload.get("status") or "")
        payload["base_measure_override"] = str(
            row["base_measure_override"] or payload.get("base_measure_override") or ""
        )
        payload["base_qty_per_purchase_unit_override"] = str(
            row["base_qty_per_purchase_unit_override"]
            or payload.get("base_qty_per_purchase_unit_override")
            or ""
        )

        if qn:
            hay = " ".join(
                str(payload.get(k, "")) for k in ["fingerprint", "sku_id", "status", "base_measure_override"]
            ).lower()
            if qn not in hay:
                continue

        rows_out.append(payload)
        if len(rows_out) >= limit:
            break
    return rows_out


def upsert_global_mapping(
    db_path: Path,
    *,
    fingerprint: str,
    sku_id: str,
    status: str,
    base_measure_override: str = "",
    base_qty_per_purchase_unit_override: str = "",
) -> bool:
    fp = str(fingerprint).strip()
    if not fp:
        return False
    now = utc_now_iso()
    with db_session(db_path) as conn:
        current = fetch_one(conn, "SELECT * FROM global_mappings WHERE fingerprint = ?", (fp,))
        payload = _loads_payload(str(current["payload_json"])) if current else {}
        payload["fingerprint"] = fp
        payload["sku_id"] = sku_id.strip()
        payload["status"] = status.strip()
        payload["base_measure_override"] = base_measure_override.strip()
        payload["base_qty_per_purchase_unit_override"] = base_qty_per_purchase_unit_override.strip()
        payload["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        conn.execute(
            """
            INSERT INTO global_mappings (
              fingerprint, sku_id, base_measure_override, base_qty_per_purchase_unit_override,
              status, payload_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fingerprint) DO UPDATE SET
              sku_id=excluded.sku_id,
              base_measure_override=excluded.base_measure_override,
              base_qty_per_purchase_unit_override=excluded.base_qty_per_purchase_unit_override,
              status=excluded.status,
              payload_json=excluded.payload_json,
              updated_at=excluded.updated_at
            """,
            (
                fp,
                sku_id.strip(),
                base_measure_override.strip(),
                base_qty_per_purchase_unit_override.strip(),
                status.strip(),
                json.dumps(payload, ensure_ascii=False),
                now,
            ),
        )
    return True


def list_global_catalog(db_path: Path, q: str = "", limit: int = 500) -> list[dict[str, Any]]:
    qn = q.strip().lower()
    rows_out: list[dict[str, Any]] = []
    with db_session(db_path) as conn:
        rows = fetch_all(conn, "SELECT * FROM global_catalog ORDER BY sku_id")
    for row in rows:
        payload = _loads_payload(str(row["payload_json"]))
        payload["sku_id"] = str(row["sku_id"] or payload.get("sku_id") or "")
        payload["sku_name_canonical"] = str(
            row["sku_name_canonical"] or payload.get("sku_name_canonical") or ""
        )
        payload["brand"] = str(row["brand"] or payload.get("brand") or "")
        payload["category"] = str(row["category"] or payload.get("category") or "")
        payload["base_measure"] = str(row["base_measure"] or payload.get("base_measure") or "")

        if qn:
            hay = " ".join(
                str(payload.get(k, ""))
                for k in ["sku_id", "sku_name_canonical", "brand", "category", "base_measure"]
            ).lower()
            if qn not in hay:
                continue
        rows_out.append(payload)
        if len(rows_out) >= limit:
            break
    return rows_out


def upsert_global_catalog(
    db_path: Path,
    *,
    sku_id: str,
    sku_name_canonical: str,
    brand: str = "",
    category: str = "",
    base_measure: str = "",
) -> bool:
    sku = str(sku_id).strip()
    if not sku:
        return False
    now = utc_now_iso()
    with db_session(db_path) as conn:
        current = fetch_one(conn, "SELECT * FROM global_catalog WHERE sku_id = ?", (sku,))
        payload = _loads_payload(str(current["payload_json"])) if current else {}
        payload["sku_id"] = sku
        payload["sku_name_canonical"] = sku_name_canonical.strip()
        payload["brand"] = brand.strip()
        payload["category"] = category.strip()
        payload["base_measure"] = base_measure.strip()
        payload["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        conn.execute(
            """
            INSERT INTO global_catalog (
              sku_id, sku_name_canonical, brand, category, base_measure, payload_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sku_id) DO UPDATE SET
              sku_name_canonical=excluded.sku_name_canonical,
              brand=excluded.brand,
              category=excluded.category,
              base_measure=excluded.base_measure,
              payload_json=excluded.payload_json,
              updated_at=excluded.updated_at
            """,
            (
                sku,
                sku_name_canonical.strip(),
                brand.strip(),
                category.strip(),
                base_measure.strip(),
                json.dumps(payload, ensure_ascii=False),
                now,
            ),
        )
    return True
