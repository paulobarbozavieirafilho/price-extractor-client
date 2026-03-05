from __future__ import annotations

import base64
import hashlib
import io
import traceback
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd
from pydantic import BaseModel

from .db import db_session, fetch_all, fetch_one, utc_now_iso
from .global_state_store import (
    export_global_state_to_excel,
    global_state_counts,
    import_global_state_from_excel,
)
from .notebook_bridge import LegacyNotebookBridge, PipelineRunResult
from .repository import get_client_by_id
from .runner import ensure_client_layout, get_shared_editor_path, run_client_pipeline, slugify
from .settings import Settings


PERIOD_LABELS = {
    "DIARIO": "Diario D-1",
    "SEMANAL": "Semanal fechado",
    "SEMANAL_ATUAL": "Semana atual",
    "MENSAL": "Mensal fechado",
}

PERIOD_FILE_SLUGS = {
    "DIARIO": "diario",
    "SEMANAL": "semanal",
    "SEMANAL_ATUAL": "semanal_atual",
    "MENSAL": "mensal",
}


class GroupPayload(BaseModel):
    nome: str
    contato: str = ""
    whatsapp: str = ""
    ativo: bool = True


class StorePayload(BaseModel):
    grupo_id: int
    nome: str
    cnpj: str = ""
    whatsapp: str = ""
    nfstock_token: str = ""
    ativo: bool = True


class StoreUpdatePayload(StorePayload):
    id: int


class PipelineRunPayload(BaseModel):
    grupo_id: int | None = None
    loja_id: int | None = None
    periodo: str = "DIARIO"


def create_cockpit_router(settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/api", tags=["cockpit"])

    def _normalize_period(periodo: str) -> str:
        value = str(periodo or "").strip().upper() or "DIARIO"
        if value not in PERIOD_LABELS:
            raise HTTPException(status_code=422, detail=f"Periodo invalido: {periodo}")
        return value

    def _only_digits(value: str) -> str:
        return "".join(ch for ch in str(value or "") if ch.isdigit())

    def _storage_cnpj(value: str) -> str:
        digits = _only_digits(value)
        if digits:
            return digits
        suffix = utc_now_iso().replace("-", "").replace(":", "").replace("+", "").replace("T", "")
        return f"NO-CNPJ-{suffix}"

    def _display_cnpj(value: str) -> str:
        raw = str(value or "")
        if raw.startswith("NO-CNPJ-"):
            return ""
        return raw

    def _token_key() -> bytes:
        raw = f"{settings.workspace_dir}|price-extractor-client|nfstock".encode("utf-8")
        return hashlib.sha256(raw).digest()

    def _encrypt_token(value: str) -> str:
        token = str(value or "").strip()
        if not token:
            return ""
        key = _token_key()
        payload = token.encode("utf-8")
        encrypted = bytes(payload[idx] ^ key[idx % len(key)] for idx in range(len(payload)))
        return base64.urlsafe_b64encode(encrypted).decode("ascii")

    def _row_value(row, key: str, default=""):
        try:
            value = row[key]
        except Exception:
            return default
        return default if value is None else value

    def _to_group_row(row) -> dict:
        tipo = str(_row_value(row, "client_type", "grupo") or "grupo").strip().lower()
        if tipo not in {"grupo", "independente"}:
            tipo = "grupo"
        run_ts = str(
            _row_value(
                row,
                "run_finished_at",
                _row_value(row, "run_started_at", ""),
            )
            or ""
        )
        return {
            "id": int(_row_value(row, "id", 0)),
            "nome": str(_row_value(row, "name", "")),
            "contato": str(_row_value(row, "contact_name", "")),
            "whatsapp": _only_digits(str(_row_value(row, "whatsapp", ""))),
            "tipo": tipo,
            "ativo": bool(_row_value(row, "active", 0)),
            "ultimo_run": _fmt_last_run(run_ts),
            "status_run": _status_to_ui(str(_row_value(row, "run_status", ""))),
            "skus_pendentes": int(_row_value(row, "pending_review_count", 0) or 0),
            "alerts_diario_count": int(_row_value(row, "alerts_diario_count", 0) or 0),
            "alerts_semanal_count": int(_row_value(row, "alerts_semanal_count", 0) or 0),
            "alerts_semanal_atual_count": int(_row_value(row, "alerts_semanal_atual_count", 0) or 0),
            "alerts_mensal_count": int(_row_value(row, "alerts_mensal_count", 0) or 0),
            "output_dir": str(_row_value(row, "run_output_dir", "")),
        }

    def _fmt_last_run(ts: str) -> str:
        value = str(ts or "").strip()
        if not value:
            return "-"
        try:
            if value.endswith("Z"):
                value = value.replace("Z", "+00:00")
            dt = datetime.fromisoformat(value)
            return dt.strftime("%d/%m %H:%M")
        except Exception:
            return value

    def _status_to_ui(status: str) -> str:
        s = str(status or "").strip().lower()
        if s == "running":
            return "running"
        if s in {"failed", "erro", "error"}:
            return "erro"
        return "ok"

    def _list_groups() -> list[dict]:
        with db_session(settings.db_path) as conn:
            rows = fetch_all(
                conn,
                """
                SELECT c.id,
                       c.name,
                       c.contact_name,
                       c.whatsapp,
                       c.client_type,
                       c.active,
                       lr.status AS run_status,
                       lr.started_at AS run_started_at,
                       lr.finished_at AS run_finished_at,
                       lr.pending_review_count AS pending_review_count,
                       lr.alerts_diario_count AS alerts_diario_count,
                       lr.alerts_semanal_count AS alerts_semanal_count,
                       lr.alerts_semanal_atual_count AS alerts_semanal_atual_count,
                       lr.alerts_mensal_count AS alerts_mensal_count,
                       lr.output_dir AS run_output_dir
                  FROM clients c
                  LEFT JOIN (
                        SELECT r1.*
                          FROM runs r1
                          JOIN (
                                SELECT client_id, MAX(id) AS max_id
                                  FROM runs
                                 GROUP BY client_id
                          ) latest ON latest.max_id = r1.id
                  ) lr ON lr.client_id = c.id
                 ORDER BY c.name
                """,
            )
        return [_to_group_row(row) for row in rows]

    def _resolve_group_output_dir(group_id: int) -> Path:
        with db_session(settings.db_path) as conn:
            group_row = fetch_one(conn, "SELECT root_dir FROM clients WHERE id = ?", (group_id,))
            if not group_row:
                raise HTTPException(status_code=404, detail="Grupo nao encontrado")
            run_row = fetch_one(
                conn,
                """
                SELECT output_dir
                  FROM runs
                 WHERE client_id = ? AND COALESCE(output_dir, '') <> ''
                 ORDER BY id DESC
                 LIMIT 1
                """,
                (group_id,),
            )

        output_dir = str(run_row["output_dir"] or "").strip() if run_row else ""
        if output_dir:
            return Path(output_dir)
        return Path(str(group_row["root_dir"])) / "outputs"

    def _resolve_group_editor_path(group_id: int) -> Path:
        return _resolve_group_output_dir(group_id) / "pipeline_editor.xlsx"

    def _latest_whatsapp_file(group_id: int, period_key: str) -> Path | None:
        output_dir = _resolve_group_output_dir(group_id)
        alerts_dir = output_dir / "whatsapp_alertas"
        if not alerts_dir.exists():
            return None
        period_slug = PERIOD_FILE_SLUGS[period_key]
        files = sorted(
            alerts_dir.glob(f"whatsapp_{period_slug}_*.txt"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        return files[0] if files else None

    def _read_text_file(path: Path) -> str:
        if not path.exists():
            return ""
        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            return path.read_text(encoding="utf-8", errors="replace")

    def _list_stores(*, group_id: int | None = None) -> list[dict]:
        query = """
            SELECT s.id,
                   s.client_id AS grupo_id,
                   s.store_name AS nome,
                   s.cnpj,
                   s.whatsapp,
                   s.nfstock_token_encrypted,
                   s.active,
                   c.name AS grupo_nome,
                   lr.status AS run_status,
                   lr.started_at AS run_started_at,
                   lr.finished_at AS run_finished_at,
                   lr.pending_skus AS run_pending_skus
              FROM client_store_maps s
              JOIN clients c ON c.id = s.client_id
              LEFT JOIN (
                    SELECT sr1.*
                      FROM store_runs sr1
                      JOIN (
                            SELECT store_map_id, MAX(id) AS max_id
                              FROM store_runs
                             GROUP BY store_map_id
                      ) latest ON latest.max_id = sr1.id
              ) lr ON lr.store_map_id = s.id
        """
        params: list[object] = []
        if group_id is not None:
            query += " WHERE s.client_id = ?"
            params.append(int(group_id))
        query += " ORDER BY c.name, s.store_name"

        out: list[dict] = []
        with db_session(settings.db_path) as conn:
            rows = fetch_all(conn, query, params)
        for row in rows:
            run_ts = str(row["run_finished_at"] or row["run_started_at"] or "")
            token_encrypted = str(row["nfstock_token_encrypted"] or "")
            out.append(
                {
                    "id": int(row["id"]),
                    "grupo_id": int(row["grupo_id"]),
                    "grupo_nome": str(row["grupo_nome"] or ""),
                    "nome": str(row["nome"] or ""),
                    "cnpj": _display_cnpj(str(row["cnpj"] or "")),
                    "whatsapp": _only_digits(str(row["whatsapp"] or "")),
                    "ativo": bool(row["active"]),
                    "nfstock_token_masked": "••••••••" if token_encrypted else "",
                    "nfstock_token_configurado": bool(token_encrypted),
                    "ultimo_run": _fmt_last_run(run_ts),
                    "status_run": _status_to_ui(str(row["run_status"] or "")),
                    "skus_pendentes": int(row["run_pending_skus"] or 0),
                }
            )
        return out

    def _get_group_or_404(group_id: int) -> dict:
        with db_session(settings.db_path) as conn:
            row = fetch_one(
                conn,
                """
                SELECT c.id,
                       c.name,
                       c.contact_name,
                       c.whatsapp,
                       c.client_type,
                       c.active,
                       lr.status AS run_status,
                       lr.started_at AS run_started_at,
                       lr.finished_at AS run_finished_at,
                       lr.pending_review_count AS pending_review_count,
                       lr.alerts_diario_count AS alerts_diario_count,
                       lr.alerts_semanal_count AS alerts_semanal_count,
                       lr.alerts_semanal_atual_count AS alerts_semanal_atual_count,
                       lr.alerts_mensal_count AS alerts_mensal_count,
                       lr.output_dir AS run_output_dir
                  FROM clients c
                  LEFT JOIN (
                        SELECT r1.*
                          FROM runs r1
                          JOIN (
                                SELECT client_id, MAX(id) AS max_id
                                  FROM runs
                                 GROUP BY client_id
                          ) latest ON latest.max_id = r1.id
                  ) lr ON lr.client_id = c.id
                 WHERE c.id = ?
                """,
                (group_id,),
            )
        if not row:
            raise HTTPException(status_code=404, detail="Grupo nao encontrado")
        return _to_group_row(row)

    def _new_unique_slug(conn, nome: str) -> str:
        base = slugify(nome)
        slug = base
        seq = 2
        while fetch_one(conn, "SELECT id FROM clients WHERE slug = ?", (slug,)):
            slug = f"{base}-{seq}"
            seq += 1
        return slug

    def _period_alert_count(result: PipelineRunResult | None, periodo: str) -> int:
        if not result:
            return 0
        key = _normalize_period(periodo)
        if key == "DIARIO":
            return int(result.alerts_diario_count)
        if key == "SEMANAL":
            return int(result.alerts_semanal_count)
        if key == "SEMANAL_ATUAL":
            return int(result.alerts_semanal_atual_count)
        return int(result.alerts_mensal_count)

    def _group_period_alert_count(group_row: dict, period_key: str) -> int:
        if period_key == "DIARIO":
            return int(group_row.get("alerts_diario_count") or 0)
        if period_key == "SEMANAL":
            return int(group_row.get("alerts_semanal_count") or 0)
        if period_key == "SEMANAL_ATUAL":
            return int(group_row.get("alerts_semanal_atual_count") or 0)
        return int(group_row.get("alerts_mensal_count") or 0)

    def _finish_store_runs(
        run_ids: list[int],
        *,
        status: str,
        message: str,
        pending_skus: int = 0,
        alerts_count: int = 0,
    ) -> None:
        if not run_ids:
            return
        now = utc_now_iso()
        with db_session(settings.db_path) as conn:
            for run_id in run_ids:
                conn.execute(
                    """
                    UPDATE store_runs
                       SET finished_at = ?, status = ?, message = ?, pending_skus = ?, alerts_count = ?
                     WHERE id = ?
                    """,
                    (now, status, message, pending_skus, alerts_count, run_id),
                )

    def _run_group_task(group_id: int, periodo: str, run_ids: list[int]) -> None:
        client = get_client_by_id(settings.db_path, group_id)
        if not client:
            _finish_store_runs(run_ids, status="failed", message="Grupo nao encontrado.")
            return
        try:
            bridge = LegacyNotebookBridge(settings.notebook_path)
            outcome = run_client_pipeline(settings, client, bridge)
            if outcome.ok:
                _finish_store_runs(
                    run_ids,
                    status="success",
                    message="Processamento concluido.",
                    pending_skus=int(outcome.result.pending_review_count if outcome.result else 0),
                    alerts_count=_period_alert_count(outcome.result, periodo),
                )
            else:
                _finish_store_runs(
                    run_ids,
                    status="failed",
                    message=str(outcome.message or "Falha no processamento."),
                )
        except Exception as exc:
            _finish_store_runs(
                run_ids,
                status="failed",
                message=f"{type(exc).__name__}: {exc}",
            )

    def _run_store_task(store_id: int, periodo: str, run_id: int) -> None:
        with db_session(settings.db_path) as conn:
            row = fetch_one(
                conn,
                """
                SELECT s.id, s.client_id, s.store_name, s.cnpj, c.name AS client_name, c.root_dir
                  FROM client_store_maps s
                  JOIN clients c ON c.id = s.client_id
                 WHERE s.id = ?
                """,
                (store_id,),
            )
        if not row:
            _finish_store_runs([run_id], status="failed", message="Loja nao encontrada.")
            return

        cnpj = _display_cnpj(str(row["cnpj"] or ""))
        if not cnpj:
            _finish_store_runs(
                [run_id],
                status="failed",
                message="Loja sem CNPJ configurado. Nao foi possivel rodar de forma isolada.",
            )
            return

        try:
            client_name = str(row["client_name"] or "")
            client_root = Path(str(row["root_dir"]))
            input_dir, output_dir, editor_path = ensure_client_layout(client_root)
            shared_editor_path = get_shared_editor_path(settings)
            bridge = LegacyNotebookBridge(settings.notebook_path)

            counts = global_state_counts(settings.db_path)
            if (
                counts["review_total"] == 0
                and counts["catalog_total"] == 0
                and counts["mappings_total"] == 0
                and shared_editor_path.exists()
            ):
                import_global_state_from_excel(settings.db_path, shared_editor_path)
            export_global_state_to_excel(settings.db_path, shared_editor_path)

            result = bridge.run_for_client(
                xml_dir=input_dir,
                out_dir=output_dir,
                editor_path=editor_path,
                shared_editor_path=shared_editor_path,
                cnpj_to_loja={cnpj: str(row["store_name"] or "LOJA")},
                brand=client_name,
            )
            import_global_state_from_excel(settings.db_path, shared_editor_path)
            _finish_store_runs(
                [run_id],
                status="success",
                message="Processamento concluido.",
                pending_skus=int(result.pending_review_count),
                alerts_count=_period_alert_count(result, periodo),
            )
        except Exception as exc:
            _finish_store_runs(
                [run_id],
                status="failed",
                message=f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=5)}",
            )

    @router.get("/grupos")
    def api_list_groups():
        return _list_groups()

    @router.post("/grupos")
    def api_create_group(payload: GroupPayload, tipo: str = "grupo"):
        tipo_norm = str(tipo or "grupo").strip().lower()
        if tipo_norm not in {"grupo", "independente"}:
            raise HTTPException(status_code=422, detail="tipo deve ser grupo ou independente")
        nome = str(payload.nome or "").strip()
        if not nome:
            raise HTTPException(status_code=422, detail="nome obrigatorio")

        now = utc_now_iso()
        with db_session(settings.db_path) as conn:
            slug = _new_unique_slug(conn, nome)
            root_dir = settings.clients_dir / slug
            root_dir.mkdir(parents=True, exist_ok=True)
            (root_dir / "input").mkdir(parents=True, exist_ok=True)
            (root_dir / "outputs").mkdir(parents=True, exist_ok=True)

            cur = conn.execute(
                """
                INSERT INTO clients (
                  slug, name, economic_group, root_dir, active, created_at, updated_at,
                  contact_name, whatsapp, client_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    slug,
                    nome,
                    slug,
                    str(root_dir),
                    int(payload.ativo),
                    now,
                    now,
                    str(payload.contato or "").strip(),
                    _only_digits(payload.whatsapp),
                    tipo_norm,
                ),
            )
            group_id = int(cur.lastrowid)
            if tipo_norm == "independente":
                conn.execute(
                    """
                    INSERT INTO client_store_maps (
                      client_id, cnpj, store_name, whatsapp, nfstock_token_encrypted, active
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        group_id,
                        _storage_cnpj(""),
                        nome,
                        _only_digits(payload.whatsapp),
                        "",
                        int(payload.ativo),
                    ),
                )
        return _get_group_or_404(group_id)

    @router.get("/grupos/{group_id}")
    def api_get_group(group_id: int):
        return _get_group_or_404(group_id)

    @router.put("/grupos/{group_id}")
    def api_update_group(group_id: int, payload: GroupPayload, tipo: str = "grupo"):
        tipo_norm = str(tipo or "grupo").strip().lower()
        if tipo_norm not in {"grupo", "independente"}:
            raise HTTPException(status_code=422, detail="tipo deve ser grupo ou independente")
        nome = str(payload.nome or "").strip()
        if not nome:
            raise HTTPException(status_code=422, detail="nome obrigatorio")

        with db_session(settings.db_path) as conn:
            exists = fetch_one(conn, "SELECT id FROM clients WHERE id = ?", (group_id,))
            if not exists:
                raise HTTPException(status_code=404, detail="Grupo nao encontrado")
            conn.execute(
                """
                UPDATE clients
                   SET name = ?, contact_name = ?, whatsapp = ?, client_type = ?, active = ?, updated_at = ?
                 WHERE id = ?
                """,
                (
                    nome,
                    str(payload.contato or "").strip(),
                    _only_digits(payload.whatsapp),
                    tipo_norm,
                    int(payload.ativo),
                    utc_now_iso(),
                    group_id,
                ),
            )
        return _get_group_or_404(group_id)

    @router.get("/lojas")
    def api_list_stores(grupo_id: int | None = None):
        return _list_stores(group_id=grupo_id)

    @router.post("/lojas")
    def api_create_store(payload: StorePayload):
        nome = str(payload.nome or "").strip()
        if not nome:
            raise HTTPException(status_code=422, detail="nome da loja obrigatorio")

        with db_session(settings.db_path) as conn:
            group_exists = fetch_one(conn, "SELECT id FROM clients WHERE id = ?", (payload.grupo_id,))
            if not group_exists:
                raise HTTPException(status_code=404, detail="Grupo nao encontrado")
            cur = conn.execute(
                """
                INSERT INTO client_store_maps (
                  client_id, cnpj, store_name, whatsapp, nfstock_token_encrypted, active
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    int(payload.grupo_id),
                    _storage_cnpj(payload.cnpj),
                    nome,
                    _only_digits(payload.whatsapp),
                    _encrypt_token(payload.nfstock_token),
                    int(payload.ativo),
                ),
            )
            store_id = int(cur.lastrowid)
        rows = [row for row in _list_stores(group_id=payload.grupo_id) if int(row["id"]) == store_id]
        return rows[0] if rows else {"id": store_id}

    def _update_store(loja_id: int, payload: StorePayload):
        nome = str(payload.nome or "").strip()
        if not nome:
            raise HTTPException(status_code=422, detail="nome da loja obrigatorio")

        with db_session(settings.db_path) as conn:
            row = fetch_one(
                conn,
                "SELECT id, nfstock_token_encrypted FROM client_store_maps WHERE id = ?",
                (loja_id,),
            )
            if not row:
                raise HTTPException(status_code=404, detail="Loja nao encontrada")
            group_exists = fetch_one(conn, "SELECT id FROM clients WHERE id = ?", (payload.grupo_id,))
            if not group_exists:
                raise HTTPException(status_code=404, detail="Grupo nao encontrado")

            token_encrypted = str(row["nfstock_token_encrypted"] or "")
            token_new = str(payload.nfstock_token or "").strip()
            if token_new:
                token_encrypted = _encrypt_token(token_new)

            conn.execute(
                """
                UPDATE client_store_maps
                   SET client_id = ?,
                       cnpj = ?,
                       store_name = ?,
                       whatsapp = ?,
                       nfstock_token_encrypted = ?,
                       active = ?
                 WHERE id = ?
                """,
                (
                    int(payload.grupo_id),
                    _storage_cnpj(payload.cnpj),
                    nome,
                    _only_digits(payload.whatsapp),
                    token_encrypted,
                    int(payload.ativo),
                    loja_id,
                ),
            )
        rows = [row for row in _list_stores(group_id=payload.grupo_id) if int(row["id"]) == loja_id]
        return rows[0] if rows else {"id": loja_id}

    @router.put("/lojas/{loja_id}")
    def api_update_store(loja_id: int, payload: StorePayload):
        return _update_store(loja_id, payload)

    @router.put("/lojas")
    def api_update_store_compat(payload: StoreUpdatePayload):
        body = StorePayload(
            grupo_id=payload.grupo_id,
            nome=payload.nome,
            cnpj=payload.cnpj,
            whatsapp=payload.whatsapp,
            nfstock_token=payload.nfstock_token,
            ativo=payload.ativo,
        )
        return _update_store(int(payload.id), body)

    @router.get("/alertas/texto")
    def api_alert_text(grupo_id: int, periodo: str = "DIARIO"):
        period_key = _normalize_period(periodo)
        grupo = _get_group_or_404(grupo_id)
        latest_txt = _latest_whatsapp_file(grupo_id, period_key)
        if latest_txt and latest_txt.exists():
            return {
                "grupo_id": grupo_id,
                "periodo": period_key,
                "texto": _read_text_file(latest_txt),
                "source": "pipeline_txt",
                "file_name": latest_txt.name,
            }

        if _group_period_alert_count(grupo, period_key) <= 0:
            return {
                "grupo_id": grupo_id,
                "periodo": period_key,
                "texto": "",
                "source": "no_data",
            }

        stores = [store for store in _list_stores(group_id=grupo_id) if store["ativo"]]
        now_label = datetime.now().strftime("%d/%m/%Y")
        total_pending = int(grupo.get("skus_pendentes") or 0)

        lines = [
            f"*{grupo['nome']}* - {now_label}",
            f"{len(stores)} lojas - {PERIOD_LABELS[period_key]}",
            "",
            f"SKUs pendentes: {total_pending}",
            "",
        ]
        for store in stores:
            lines.append(f"--- {str(store['nome']).upper()} ---")
            status = store["status_run"]
            if status == "running":
                lines.append("Status: PROCESSANDO")
            elif status == "erro":
                lines.append("Status: ERRO")
            else:
                lines.append("Status: OK")
            lines.append(f"Pendentes: {int(store['skus_pendentes'] or 0)}")
            lines.append(f"Ultimo run: {store['ultimo_run'] or '-'}")
            lines.append("")
        if not stores:
            lines.append("Sem lojas ativas nesse grupo.")
        return {
            "grupo_id": grupo_id,
            "periodo": period_key,
            "texto": "\n".join(lines).strip(),
            "source": "fallback_summary",
        }

    @router.get("/alertas/excel")
    def api_alert_excel(grupo_id: int, periodo: str = "DIARIO"):
        _normalize_period(periodo)
        grupo = _get_group_or_404(grupo_id)
        editor_path = _resolve_group_editor_path(grupo_id)
        if not editor_path.exists():
            raise HTTPException(
                status_code=404,
                detail="Arquivo pipeline_editor.xlsx nao encontrado. Rode o processamento primeiro.",
            )

        try:
            compras_df = pd.read_excel(editor_path, sheet_name="Compras")
        except Exception as exc:
            raise HTTPException(
                status_code=404,
                detail=f"Aba 'Compras' nao encontrada para exportacao: {exc}",
            )

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            compras_df.to_excel(writer, index=False, sheet_name="Compras")
        buffer.seek(0)

        file_name = f"compras_{slugify(grupo['nome'])}.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    @router.post("/pipeline/run")
    def api_pipeline_run(payload: PipelineRunPayload, background_tasks: BackgroundTasks):
        period_key = _normalize_period(payload.periodo)
        has_group = payload.grupo_id is not None
        has_store = payload.loja_id is not None
        if has_group == has_store:
            raise HTTPException(
                status_code=422,
                detail="Informe exatamente um alvo: grupo_id ou loja_id.",
            )

        if payload.grupo_id is not None:
            group_id = int(payload.grupo_id)
            _get_group_or_404(group_id)
            with db_session(settings.db_path) as conn:
                store_rows = fetch_all(
                    conn,
                    """
                    SELECT id
                      FROM client_store_maps
                     WHERE client_id = ? AND active = 1
                     ORDER BY store_name
                    """,
                    (group_id,),
                )
                if not store_rows:
                    raise HTTPException(status_code=404, detail="Grupo sem lojas ativas")
                run_ids: list[int] = []
                store_ids: list[int] = []
                for row in store_rows:
                    store_id = int(row["id"])
                    cur = conn.execute(
                        """
                        INSERT INTO store_runs (store_map_id, period, started_at, status, message)
                        VALUES (?, ?, ?, 'running', 'Processamento em andamento')
                        """,
                        (store_id, period_key, utc_now_iso()),
                    )
                    run_ids.append(int(cur.lastrowid))
                    store_ids.append(store_id)
            background_tasks.add_task(_run_group_task, group_id, period_key, run_ids)
            return {
                "ok": True,
                "scope": "grupo",
                "grupo_id": group_id,
                "store_ids": store_ids,
                "periodo": period_key,
            }

        store_id = int(payload.loja_id or 0)
        with db_session(settings.db_path) as conn:
            store_row = fetch_one(
                conn,
                "SELECT id FROM client_store_maps WHERE id = ?",
                (store_id,),
            )
            if not store_row:
                raise HTTPException(status_code=404, detail="Loja nao encontrada")
            cur = conn.execute(
                """
                INSERT INTO store_runs (store_map_id, period, started_at, status, message)
                VALUES (?, ?, ?, 'running', 'Processamento em andamento')
                """,
                (store_id, period_key, utc_now_iso()),
            )
            run_id = int(cur.lastrowid)
        background_tasks.add_task(_run_store_task, store_id, period_key, run_id)
        return {
            "ok": True,
            "scope": "loja",
            "loja_id": store_id,
            "store_ids": [store_id],
            "periodo": period_key,
        }

    return router
