from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlencode

from fastapi import BackgroundTasks, FastAPI, Form, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from .db import db_session, fetch_all, fetch_one, init_db
from .cockpit_api import create_cockpit_router
from .upload_api import create_upload_router
from .global_state_store import (
    apply_global_review_decision,
    export_global_state_to_excel,
    global_review_status_counts,
    global_state_counts,
    import_global_state_from_excel,
    list_global_catalog,
    list_global_mappings,
    list_global_review_rows,
    update_global_review_suggestion,
    upsert_global_catalog,
    upsert_global_mapping,
)
from .repository import (
    create_or_update_client,
    get_client_by_id,
    get_client_stores,
    get_last_run_for_client,
    list_clients,
    set_client_active,
    set_client_stores,
)
from .runner import get_shared_editor_path, run_client_pipeline, slugify
from .settings import Settings, ensure_runtime_dirs, load_settings
from .notebook_bridge import LegacyNotebookBridge


settings: Settings = load_settings()
SKU_ID_PATTERN = re.compile(r"^SKU_(\d+)$", re.IGNORECASE)

app = FastAPI(title="Price Extractor Client Panel", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
app.mount(
    "/static",
    StaticFiles(directory=str(Path(__file__).parent / "static")),
    name="static",
)
app.include_router(create_cockpit_router(settings))
app.include_router(create_upload_router(settings))


@app.on_event("startup")
def on_startup() -> None:
    ensure_runtime_dirs(settings)
    init_db(settings.db_path)


def _client_editor_path(client_root: Path) -> Path:
    return client_root / "outputs" / "pipeline_editor.xlsx"


def _parse_stores_text(raw: str) -> dict[str, str]:
    stores: dict[str, str] = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        if "=" in line:
            left, right = line.split("=", 1)
        elif ";" in line:
            left, right = line.split(";", 1)
        else:
            continue
        digits = "".join(ch for ch in left if ch.isdigit())
        if digits and right.strip():
            stores[digits] = right.strip()
    return stores


def _run_client_task(client_id: int) -> None:
    client = get_client_by_id(settings.db_path, client_id)
    if not client:
        return
    bridge = LegacyNotebookBridge(settings.notebook_path)
    run_client_pipeline(settings, client, bridge)


def _review_redirect_url(status: str, q: str, limit: int) -> str:
    query = urlencode(
        {
            "status": status or "pending",
            "q": q or "",
            "limit": int(limit or 300),
        }
    )
    return f"/global/review?{query}"


def _review_status_to_internal(status: str) -> str:
    raw = status.strip().upper()
    if raw in {"", "ALL"}:
        return "all"
    if raw == "IGNORED":
        return "ignore"
    return raw.lower()


def _normalize_api_status(status: str, *, default: str) -> str:
    value = status.strip().upper()
    if not value:
        return default
    if value in {"IGNORE", "IGNORED"}:
        return "IGNORED"
    return value


def _normalize_mapping_status(status: str) -> str:
    value = status.strip().upper()
    if value in {"", "APPROVED", "ACTIVE"}:
        return "ACTIVE"
    if value in {"IGNORE", "IGNORED"}:
        return "IGNORE"
    return value


def _parse_fp_parts(fingerprint: str) -> dict[str, str]:
    parts = str(fingerprint or "").split("|")
    return {
        "cnpj": parts[0] if len(parts) > 0 else "",
        "ean": parts[1] if len(parts) > 1 else "",
        "desc": parts[2] if len(parts) > 2 else "",
        "ucom_fp": parts[3] if len(parts) > 3 else "",
    }


def _to_store_name(row: dict) -> str:
    return str(
        row.get("loja_name")
        or row.get("dest_xNome")
        or row.get("dest_cnpj")
        or row.get("emit_xNome")
        or ""
    )


def _canonical_name_from_review_row(row: dict) -> str:
    for key in ("suggested_sku_name_canonical", "suggested_sku_name"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _catalog_has_sku_id(sku_id: str) -> bool:
    sku = str(sku_id or "").strip()
    if not sku:
        return False
    with db_session(settings.db_path) as conn:
        row = fetch_one(conn, "SELECT sku_id FROM global_catalog WHERE sku_id = ?", (sku,))
    return row is not None


def _next_catalog_sku_id() -> str:
    with db_session(settings.db_path) as conn:
        rows = fetch_all(conn, "SELECT sku_id FROM global_catalog")
    max_number = 0
    existing_ids = {
        str(row["sku_id"] or "").strip().upper()
        for row in rows
        if str(row["sku_id"] or "").strip()
    }
    for sku in existing_ids:
        match = SKU_ID_PATTERN.match(sku)
        if not match:
            continue
        max_number = max(max_number, int(match.group(1)))

    candidate = max_number + 1
    while True:
        sku_id = f"SKU_{candidate:06d}"
        if sku_id.upper() not in existing_ids:
            return sku_id
        candidate += 1


def _to_review_api_row(row: dict) -> dict:
    fp = str(row.get("fingerprint") or "")
    fp_parts = _parse_fp_parts(fp)
    return {
        "id": fp,
        "fingerprint": fp,
        "xProd_norm": str(row.get("xProd_norm") or row.get("xProd") or fp_parts["desc"] or ""),
        "uCom": str(row.get("uCom") or fp_parts["ucom_fp"] or ""),
        "qCom": row.get("qCom"),
        "vUnCom": row.get("vUnCom"),
        "suggested_sku_id": str(row.get("suggested_sku_id") or ""),
        "suggested_sku_name": str(row.get("suggested_sku_name") or ""),
        "suggested_sku_name_canonical": str(row.get("suggested_sku_name_canonical") or ""),
        "suggested_base_measure": str(row.get("suggested_base_measure") or ""),
        "suggested_base_qty_per_purchase_unit": str(
            row.get("suggested_base_qty_per_purchase_unit") or ""
        ),
        "base_measure_override": str(row.get("base_measure_override") or ""),
        "base_qty_per_purchase_unit_override": str(
            row.get("base_qty_per_purchase_unit_override") or ""
        ),
        "conversion_issue": str(row.get("conversion_issue") or ""),
        "status": _normalize_api_status(str(row.get("status") or ""), default="PENDING"),
        "loja_name": _to_store_name(row),
        "dhEmi_date": str(row.get("dhEmi_date") or row.get("created_at") or ""),
        "created_at": str(row.get("created_at") or ""),
        "updated_at": str(row.get("updated_at") or ""),
    }


def _to_mapping_api_row(row: dict) -> dict:
    fp = str(row.get("fingerprint") or "")
    fp_parts = _parse_fp_parts(fp)
    return {
        "id": fp,
        "fingerprint": fp,
        "sku_id": str(row.get("sku_id") or ""),
        "base_measure_override": str(row.get("base_measure_override") or ""),
        "base_qty_per_purchase_unit_override": str(
            row.get("base_qty_per_purchase_unit_override") or ""
        ),
        "status": _normalize_mapping_status(str(row.get("status") or "")),
        "updated_at": str(row.get("updated_at") or ""),
        "desc": fp_parts["desc"],
        "cnpj": fp_parts["cnpj"],
        "ean": fp_parts["ean"],
        "uCom_fp": fp_parts["ucom_fp"],
    }


class ReviewApprovePayload(BaseModel):
    sku_id: str = ""
    canonical_name_override: str = ""
    base_measure_override: str = ""
    base_qty_per_purchase_unit_override: str = ""


class ReviewResuggestPayload(BaseModel):
    fingerprints: list[str] = []
    status: str = "PENDING"
    search: str = ""
    limit: int = 1000
    only_without_sku: bool = False


class MappingUpdatePayload(BaseModel):
    sku_id: str = ""
    base_measure: str = ""
    qty: str = ""
    status: str = "ACTIVE"


def _find_review_by_fingerprint(fingerprint: str) -> dict | None:
    fp = str(fingerprint or "").strip()
    if not fp:
        return None
    rows = list_global_review_rows(settings.db_path, q=fp, limit=100, status_filter="all")
    for row in rows:
        if str(row.get("fingerprint") or "").strip() == fp:
            return row
    return None


def _find_mapping_by_fingerprint(fingerprint: str) -> dict | None:
    fp = str(fingerprint or "").strip()
    if not fp:
        return None
    rows = list_global_mappings(settings.db_path, q=fp, limit=200)
    for row in rows:
        if str(row.get("fingerprint") or "").strip() == fp:
            return row
    return None


@app.get("/")
def dashboard(request: Request):
    shared_editor_path = get_shared_editor_path(settings)
    counts = global_state_counts(settings.db_path)
    clients_data = []
    for client in list_clients(settings.db_path, only_active=False):
        last_run = get_last_run_for_client(settings.db_path, client.id)
        stores = get_client_stores(settings.db_path, client.id)
        clients_data.append(
            {
                "client": client,
                "stores": stores,
                "last_run": last_run,
                "editor_path": _client_editor_path(client.root_dir),
                "shared_editor_path": get_shared_editor_path(settings),
            }
        )
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "title": "Painel Price Extractor Client",
            "clients_data": clients_data,
            "global_counts": counts,
            "shared_editor_path": str(shared_editor_path),
            "default_clients_root": str(settings.clients_dir),
            "notebook_path": str(settings.notebook_path),
        },
    )


@app.get("/api/review")
def api_review(
    status: str = "ALL",
    loja: str = "",
    search: str = "",
    limit: int = 500,
):
    rows = list_global_review_rows(
        settings.db_path,
        q=search,
        limit=max(1, min(limit, 2000)),
        status_filter=_review_status_to_internal(status),
    )
    if loja.strip():
        loja_norm = loja.strip().lower()
        rows = [row for row in rows if loja_norm in _to_store_name(row).lower()]
    return [_to_review_api_row(row) for row in rows]


@app.get("/api/mappings")
def api_mappings(
    status: str = "ALL",
    search: str = "",
    limit: int = 2000,
):
    rows = list_global_mappings(settings.db_path, q=search, limit=max(1, min(limit, 5000)))
    status_norm = status.strip().upper()
    if status_norm not in {"", "ALL"}:
        rows = [
            row
            for row in rows
            if _normalize_mapping_status(str(row.get("status") or "")) == status_norm
        ]
    return [_to_mapping_api_row(row) for row in rows]


@app.get("/api/catalog")
def api_catalog(
    search: str = "",
    category: str = "",
    limit: int = 2000,
):
    rows = list_global_catalog(settings.db_path, q=search, limit=max(1, min(limit, 5000)))
    if category.strip() and category.strip().lower() != "todas":
        cat_norm = category.strip().lower()
        rows = [row for row in rows if str(row.get("category") or "").lower() == cat_norm]

    with db_session(settings.db_path) as conn:
        counts_rows = fetch_all(
            conn,
            """
            SELECT sku_id, COUNT(*) AS n
              FROM global_mappings
             WHERE TRIM(COALESCE(sku_id, '')) <> ''
             GROUP BY sku_id
            """,
        )
    fp_count_by_sku = {str(row["sku_id"]): int(row["n"] or 0) for row in counts_rows}

    payload = []
    for row in rows:
        sku_id = str(row.get("sku_id") or "")
        payload.append(
            {
                "sku_id": sku_id,
                "sku_name_canonical": str(row.get("sku_name_canonical") or ""),
                "brand": str(row.get("brand") or ""),
                "category": str(row.get("category") or ""),
                "base_measure": str(row.get("base_measure") or ""),
                "fingerprints_count": fp_count_by_sku.get(sku_id, 0),
            }
        )
    return payload


@app.get("/api/catalog/{sku_id}/fingerprints")
def api_catalog_fingerprints(sku_id: str, limit: int = 500):
    rows = list_global_mappings(settings.db_path, q="", limit=max(1, min(limit, 5000)))
    selected = [row for row in rows if str(row.get("sku_id") or "") == sku_id]
    return [_to_mapping_api_row(row) for row in selected]


@app.post("/api/review/{review_id}/approve")
def api_review_approve(review_id: str, payload: ReviewApprovePayload):
    fp = str(review_id or "").strip()
    if not fp:
        raise HTTPException(status_code=400, detail="review_id invalido")

    current = _find_review_by_fingerprint(fp)
    if not current:
        raise HTTPException(status_code=404, detail="Item de review nao encontrado")

    base_measure = (payload.base_measure_override or "").strip() or str(
        current.get("suggested_base_measure") or ""
    ).strip()
    if not base_measure:
        base_measure = str(current.get("uCom") or "").strip()
    qty_override = (payload.base_qty_per_purchase_unit_override or "").strip() or str(
        current.get("suggested_base_qty_per_purchase_unit") or ""
    ).strip()
    sku_id = (payload.sku_id or "").strip() or str(current.get("suggested_sku_id") or "").strip()
    canonical_name = (payload.canonical_name_override or "").strip() or _canonical_name_from_review_row(
        current
    )
    catalog_created = False
    auto_created_sku = False

    if not sku_id:
        if not canonical_name:
            raise HTTPException(
                status_code=422,
                detail="Informe um SKU existente ou um nome canonico para criar novo SKU.",
            )
        sku_id = _next_catalog_sku_id()
        upsert_global_catalog(
            settings.db_path,
            sku_id=sku_id,
            sku_name_canonical=canonical_name,
            base_measure=base_measure,
        )
        catalog_created = True
        auto_created_sku = True
    elif not _catalog_has_sku_id(sku_id):
        if canonical_name:
            upsert_global_catalog(
                settings.db_path,
                sku_id=sku_id,
                sku_name_canonical=canonical_name,
                base_measure=base_measure,
            )
            catalog_created = True
        else:
            raise HTTPException(
                status_code=422,
                detail="sku_id nao existe no catalogo; informe nome canonico para cadastrar.",
            )

    ok = apply_global_review_decision(
        settings.db_path,
        fingerprint=fp,
        action="approve",
        chosen_sku_id=sku_id,
        base_measure_override=base_measure,
        base_qty_per_purchase_unit_override=qty_override,
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Falha ao aprovar item de review")

    upsert_global_mapping(
        settings.db_path,
        fingerprint=fp,
        sku_id=sku_id,
        status="ACTIVE",
        base_measure_override=base_measure,
        base_qty_per_purchase_unit_override=qty_override,
    )
    export_global_state_to_excel(settings.db_path, get_shared_editor_path(settings))

    updated_review = _find_review_by_fingerprint(fp)
    updated_mapping = _find_mapping_by_fingerprint(fp)
    return {
        "ok": True,
        "resolved_sku_id": sku_id,
        "catalog_created": catalog_created,
        "catalog_auto_created": auto_created_sku,
        "review": _to_review_api_row(updated_review) if updated_review else {"id": fp},
        "mapping": _to_mapping_api_row(updated_mapping) if updated_mapping else None,
    }


@app.post("/api/review/{review_id}/ignore")
def api_review_ignore(review_id: str):
    fp = str(review_id or "").strip()
    if not fp:
        raise HTTPException(status_code=400, detail="review_id invalido")

    ok = apply_global_review_decision(
        settings.db_path,
        fingerprint=fp,
        action="ignore",
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Item de review nao encontrado")

    current_mapping = _find_mapping_by_fingerprint(fp)
    if current_mapping:
        upsert_global_mapping(
            settings.db_path,
            fingerprint=fp,
            sku_id=str(current_mapping.get("sku_id") or ""),
            status="IGNORE",
            base_measure_override=str(current_mapping.get("base_measure_override") or ""),
            base_qty_per_purchase_unit_override=str(
                current_mapping.get("base_qty_per_purchase_unit_override") or ""
            ),
        )

    export_global_state_to_excel(settings.db_path, get_shared_editor_path(settings))
    updated_review = _find_review_by_fingerprint(fp)
    return {
        "ok": True,
        "review": _to_review_api_row(updated_review) if updated_review else {"id": fp},
    }


@app.post("/api/review/resuggest")
def api_review_resuggest(payload: ReviewResuggestPayload):
    requested_fps = [
        str(fp or "").strip()
        for fp in (payload.fingerprints or [])
        if str(fp or "").strip()
    ]

    if requested_fps:
        targets: list[dict] = []
        seen: set[str] = set()
        for fp in requested_fps:
            if fp in seen:
                continue
            seen.add(fp)
            row = _find_review_by_fingerprint(fp)
            if row:
                targets.append(row)
    else:
        targets = list_global_review_rows(
            settings.db_path,
            q=str(payload.search or "").strip(),
            limit=max(1, min(int(payload.limit or 1000), 5000)),
            status_filter=_review_status_to_internal(str(payload.status or "PENDING")),
        )

    if payload.only_without_sku:
        targets = [
            row
            for row in targets
            if not str(row.get("suggested_sku_id") or "").strip()
        ]

    if not targets:
        return {"ok": True, "processed": 0, "updated": 0, "skipped": 0}

    catalog_rows = list_global_catalog(settings.db_path, q="", limit=50000)
    bridge = LegacyNotebookBridge(settings.notebook_path)
    runtime_dir = settings.data_dir / "runtime" / "resuggest"
    suggestions = bridge.resuggest_review_rows(
        review_rows=targets,
        catalog_rows=catalog_rows,
        runtime_dir=runtime_dir,
        editor_path=get_shared_editor_path(settings),
        brand="GLOBAL",
    )

    updated = 0
    for fp, sug in suggestions.items():
        ok = update_global_review_suggestion(
            settings.db_path,
            fingerprint=fp,
            suggested_sku_id=sug.get("suggested_sku_id"),
            suggested_sku_name=sug.get("suggested_sku_name"),
            suggested_sku_name_canonical=sug.get("suggested_sku_name_canonical"),
            suggested_base_measure=sug.get("suggested_base_measure"),
            suggested_base_qty_per_purchase_unit=sug.get("suggested_base_qty_per_purchase_unit"),
            confidence=sug.get("confidence"),
            rationale=sug.get("rationale"),
        )
        if ok:
            updated += 1

    export_global_state_to_excel(settings.db_path, get_shared_editor_path(settings))
    return {
        "ok": True,
        "processed": len(targets),
        "updated": int(updated),
        "skipped": int(max(0, len(targets) - updated)),
    }


@app.put("/api/mappings/{mapping_id}")
def api_update_mapping(mapping_id: str, payload: MappingUpdatePayload):
    fp = str(mapping_id or "").strip()
    if not fp:
        raise HTTPException(status_code=400, detail="mapping_id invalido")

    current = _find_mapping_by_fingerprint(fp)
    if not current:
        raise HTTPException(status_code=404, detail="Mapping nao encontrado")

    sku_id = str(payload.sku_id or "").strip()
    if not sku_id and _normalize_mapping_status(payload.status) != "IGNORE":
        raise HTTPException(status_code=422, detail="sku_id obrigatorio para mapping ativo")

    ok = upsert_global_mapping(
        settings.db_path,
        fingerprint=fp,
        sku_id=sku_id,
        status=_normalize_mapping_status(payload.status),
        base_measure_override=str(payload.base_measure or "").strip(),
        base_qty_per_purchase_unit_override=str(payload.qty or "").strip(),
    )
    if not ok:
        raise HTTPException(status_code=400, detail="Falha ao salvar mapping")

    export_global_state_to_excel(settings.db_path, get_shared_editor_path(settings))
    updated = _find_mapping_by_fingerprint(fp)
    if not updated:
        raise HTTPException(status_code=500, detail="Mapping salvo mas nao encontrado para retorno")
    return _to_mapping_api_row(updated)


@app.delete("/api/mappings/{mapping_id}")
def api_delete_mapping(mapping_id: str):
    fp = str(mapping_id or "").strip()
    if not fp:
        raise HTTPException(status_code=400, detail="mapping_id invalido")

    with db_session(settings.db_path) as conn:
        existing = fetch_one(conn, "SELECT fingerprint FROM global_mappings WHERE fingerprint = ?", (fp,))
        if not existing:
            raise HTTPException(status_code=404, detail="Mapping nao encontrado")
        conn.execute("DELETE FROM global_mappings WHERE fingerprint = ?", (fp,))

    export_global_state_to_excel(settings.db_path, get_shared_editor_path(settings))
    return {"ok": True, "id": fp}


@app.post("/clients")
def create_client(
    name: str = Form(...),
    slug: str = Form(""),
    economic_group: str = Form("default"),
    root_dir: str = Form(""),
    stores_text: str = Form(""),
):
    final_slug = slugify(slug or name)
    final_root = Path(root_dir).expanduser() if root_dir.strip() else settings.clients_dir / final_slug
    final_root.mkdir(parents=True, exist_ok=True)
    (final_root / "input").mkdir(parents=True, exist_ok=True)
    (final_root / "outputs").mkdir(parents=True, exist_ok=True)

    client = create_or_update_client(
        settings.db_path,
        slug=final_slug,
        name=name.strip(),
        economic_group=economic_group.strip() or "default",
        root_dir=final_root,
        active=True,
    )
    stores = _parse_stores_text(stores_text)
    set_client_stores(settings.db_path, client.id, stores)
    return RedirectResponse(url="/", status_code=303)


@app.post("/clients/{client_id}/toggle")
def toggle_client(client_id: int):
    client = get_client_by_id(settings.db_path, client_id)
    if client:
        set_client_active(settings.db_path, client_id, not client.active)
    return RedirectResponse(url="/", status_code=303)


@app.post("/clients/{client_id}/run")
def run_client(client_id: int, background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_client_task, client_id)
    return RedirectResponse(url="/", status_code=303)


@app.get("/clients/{client_id}/review")
def review_queue(request: Request, client_id: int):
    return RedirectResponse(url="/global/review", status_code=303)


@app.post("/clients/{client_id}/review")
def apply_review(
    client_id: int,
    fingerprint: str = Form(...),
    action: str = Form(...),
    chosen_sku_id: str = Form(""),
    base_measure_override: str = Form(""),
    base_qty_per_purchase_unit_override: str = Form(""),
    notes: str = Form(""),
):
    apply_global_review_decision(
        settings.db_path,
        fingerprint=fingerprint,
        action=action,
        chosen_sku_id=chosen_sku_id,
        base_measure_override=base_measure_override,
        base_qty_per_purchase_unit_override=base_qty_per_purchase_unit_override,
        notes=notes,
    )
    export_global_state_to_excel(settings.db_path, get_shared_editor_path(settings))
    return RedirectResponse(
        url=_review_redirect_url("pending", "", 300),
        status_code=303,
    )


@app.get("/global/review")
def global_review(request: Request, q: str = "", limit: int = 300, status: str = "pending"):
    shared_editor_path = get_shared_editor_path(settings)
    counts = global_review_status_counts(settings.db_path)
    if counts["all"] == 0:
        bootstrap_sources = [
            shared_editor_path,
            settings.workspace_dir / "outputs" / "pipeline_editor.xlsx",
        ]
        for source in bootstrap_sources:
            if not source.exists():
                continue
            inserted = import_global_state_from_excel(settings.db_path, source)
            if any(int(inserted.get(key, 0)) > 0 for key in ("review", "catalog", "mappings")):
                break
        export_global_state_to_excel(settings.db_path, shared_editor_path)
        counts = global_review_status_counts(settings.db_path)

    review_rows = list_global_review_rows(
        settings.db_path,
        q=q,
        limit=limit,
        status_filter=status,
    )
    return templates.TemplateResponse(
        request=request,
        name="global_review.html",
        context={
            "title": "Review Global",
            "q": q,
            "limit": limit,
            "status_filter": status,
            "rows": review_rows,
            "counts": counts,
            "shared_editor_path": str(shared_editor_path),
        },
    )


@app.post("/global/review/decision")
def global_review_decision(
    fingerprint: str = Form(...),
    action: str = Form(...),
    chosen_sku_id: str = Form(""),
    base_measure_override: str = Form(""),
    base_qty_per_purchase_unit_override: str = Form(""),
    notes: str = Form(""),
    q: str = Form(""),
    limit: int = Form(300),
    status: str = Form("pending"),
):
    apply_global_review_decision(
        settings.db_path,
        fingerprint=fingerprint,
        action=action,
        chosen_sku_id=chosen_sku_id,
        base_measure_override=base_measure_override,
        base_qty_per_purchase_unit_override=base_qty_per_purchase_unit_override,
        notes=notes,
    )
    export_global_state_to_excel(settings.db_path, get_shared_editor_path(settings))
    return RedirectResponse(url=_review_redirect_url(status, q, limit), status_code=303)


@app.get("/global/mappings")
def global_mappings(request: Request, q: str = "", limit: int = 400):
    rows = list_global_mappings(settings.db_path, q=q, limit=limit)
    return templates.TemplateResponse(
        request=request,
        name="global_mappings.html",
        context={
            "title": "Mappings Globais",
            "q": q,
            "limit": limit,
            "rows": rows,
        },
    )


@app.post("/global/mappings")
def save_global_mapping(
    fingerprint: str = Form(...),
    sku_id: str = Form(""),
    status: str = Form(""),
    base_measure_override: str = Form(""),
    base_qty_per_purchase_unit_override: str = Form(""),
):
    upsert_global_mapping(
        settings.db_path,
        fingerprint=fingerprint,
        sku_id=sku_id,
        status=status,
        base_measure_override=base_measure_override,
        base_qty_per_purchase_unit_override=base_qty_per_purchase_unit_override,
    )
    export_global_state_to_excel(settings.db_path, get_shared_editor_path(settings))
    return RedirectResponse(url="/global/mappings", status_code=303)


@app.get("/global/catalog")
def global_catalog(request: Request, q: str = "", limit: int = 400):
    rows = list_global_catalog(settings.db_path, q=q, limit=limit)
    return templates.TemplateResponse(
        request=request,
        name="global_catalog.html",
        context={
            "title": "Catalogo Global",
            "q": q,
            "limit": limit,
            "rows": rows,
        },
    )


@app.post("/global/catalog")
def save_global_catalog(
    sku_id: str = Form(...),
    sku_name_canonical: str = Form(""),
    brand: str = Form(""),
    category: str = Form(""),
    base_measure: str = Form(""),
):
    upsert_global_catalog(
        settings.db_path,
        sku_id=sku_id,
        sku_name_canonical=sku_name_canonical,
        brand=brand,
        category=category,
        base_measure=base_measure,
    )
    export_global_state_to_excel(settings.db_path, get_shared_editor_path(settings))
    return RedirectResponse(url="/global/catalog", status_code=303)


@app.post("/global/sync/import")
def global_sync_import():
    import_global_state_from_excel(settings.db_path, get_shared_editor_path(settings))
    return RedirectResponse(url="/", status_code=303)


@app.post("/global/sync/export")
def global_sync_export():
    export_global_state_to_excel(settings.db_path, get_shared_editor_path(settings))
    return RedirectResponse(url="/", status_code=303)
