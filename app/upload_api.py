from __future__ import annotations

import re
import shutil
import uuid
import zipfile
from pathlib import Path
from threading import Lock
from typing import Any
import xml.etree.ElementTree as ET

from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, UploadFile

from .db import db_session, fetch_all, fetch_one, utc_now_iso
from .global_state_store import (
    export_global_state_to_excel,
    global_state_counts,
    import_global_state_from_excel,
)
from .notebook_bridge import LegacyNotebookBridge
from .runner import ensure_client_layout, get_shared_editor_path
from .settings import Settings

try:
    import rarfile  # type: ignore
except Exception:  # pragma: no cover - opcional em ambiente sem unrar
    rarfile = None


def create_upload_router(settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/api/upload", tags=["upload"])

    jobs: dict[str, dict[str, Any]] = {}
    jobs_lock = Lock()

    def _safe_name(name: str) -> str:
        raw = str(name or "").strip()
        base = Path(raw).name
        clean = re.sub(r"[^A-Za-z0-9._-]+", "_", base).strip("._")
        return clean or "arquivo"

    def _file_ext(name: str) -> str:
        return Path(str(name or "")).suffix.lower().lstrip(".")

    def _is_supported_upload(name: str) -> bool:
        return _file_ext(name) in {"xml", "zip", "rar"}

    def _local_name(tag: str) -> str:
        if "}" in str(tag):
            return str(tag).split("}", 1)[1]
        return str(tag)

    def _child_text(parent: ET.Element, local: str) -> str:
        for child in list(parent):
            if _local_name(child.tag) != local:
                continue
            value = str(child.text or "").strip()
            if value:
                return value
        return ""

    def _first_text(root: ET.Element, locals_names: list[str]) -> str:
        wanted = set(locals_names)
        for elem in root.iter():
            if _local_name(elem.tag) not in wanted:
                continue
            value = str(elem.text or "").strip()
            if value:
                return value
        return ""

    def _to_brl_float(raw: str) -> float:
        value = str(raw or "").strip()
        if not value:
            return 0.0
        value = value.replace(" ", "")
        if "," in value and "." in value:
            value = value.replace(".", "").replace(",", ".")
        else:
            value = value.replace(",", ".")
        try:
            return float(value)
        except Exception:
            return 0.0

    def _normalize_date(raw: str) -> str:
        value = str(raw or "").strip()
        if not value:
            return ""
        if len(value) >= 10 and value[4] == "-" and value[7] == "-":
            return value[:10]
        digits = "".join(ch for ch in value if ch.isdigit())
        if len(digits) >= 8:
            return f"{digits[0:4]}-{digits[4:6]}-{digits[6:8]}"
        return ""

    def _digits(raw: str) -> str:
        return "".join(ch for ch in str(raw or "") if ch.isdigit())

    def _parse_xml_summary(path: Path) -> dict[str, Any]:
        base = {
            "arquivo": path.name,
            "fornecedor": "",
            "cnpj_emit": "",
            "cnpj_dest": "",
            "data": "",
            "valor": 0.0,
            "novos_skus": 0,
        }
        try:
            root = ET.parse(path).getroot()

            fornecedor = ""
            cnpj_emit = ""
            cnpj_dest = ""
            for elem in root.iter():
                if _local_name(elem.tag) != "emit":
                    continue
                fornecedor = _child_text(elem, "xNome")
                cnpj_emit = _child_text(elem, "CNPJ") or _child_text(elem, "CPF")
                break
            for elem in root.iter():
                if _local_name(elem.tag) != "dest":
                    continue
                cnpj_dest = _child_text(elem, "CNPJ") or _child_text(elem, "CPF")
                break

            if not fornecedor:
                fornecedor = _first_text(root, ["xNome"])
            if not cnpj_emit:
                cnpj_emit = _first_text(root, ["CNPJ", "CPF"])

            data = _normalize_date(
                _first_text(root, ["dhEmi", "dEmi", "dhSaiEnt"])
            )

            valor_raw = ""
            for elem in root.iter():
                if _local_name(elem.tag) != "ICMSTot":
                    continue
                valor_raw = _child_text(elem, "vNF")
                if valor_raw:
                    break
            if not valor_raw:
                valor_raw = _first_text(root, ["vNF"])

            base.update(
                {
                    "fornecedor": fornecedor,
                    "cnpj_emit": cnpj_emit,
                    "cnpj_dest": cnpj_dest,
                    "data": data,
                    "valor": _to_brl_float(valor_raw),
                }
            )
            return base
        except Exception as exc:
            base["erro"] = f"{type(exc).__name__}: {exc}"
            return base

    def _unique_path(folder: Path, file_name: str) -> Path:
        folder.mkdir(parents=True, exist_ok=True)
        stem = Path(file_name).stem
        suffix = Path(file_name).suffix
        candidate = folder / f"{stem}{suffix}"
        idx = 2
        while candidate.exists():
            candidate = folder / f"{stem}_{idx}{suffix}"
            idx += 1
        return candidate

    def _extract_zip_xmls(zip_path: Path, out_dir: Path) -> list[Path]:
        extracted: list[Path] = []
        with zipfile.ZipFile(zip_path, "r") as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                member_name = Path(info.filename).name
                if _file_ext(member_name) != "xml":
                    continue
                raw = zf.read(info)
                target = _unique_path(out_dir, _safe_name(member_name))
                target.write_bytes(raw)
                extracted.append(target)
        return extracted

    def _extract_rar_xmls(rar_path: Path, out_dir: Path) -> list[Path]:
        if rarfile is None:
            raise RuntimeError(
                "Suporte a RAR indisponivel: pacote 'rarfile' nao instalado."
            )
        extracted: list[Path] = []
        try:
            with rarfile.RarFile(rar_path) as rf:  # type: ignore[attr-defined]
                for info in rf.infolist():
                    is_dir = False
                    if hasattr(info, "isdir"):
                        try:
                            is_dir = bool(info.isdir())
                        except Exception:
                            is_dir = False
                    if is_dir:
                        continue
                    member_name = Path(str(getattr(info, "filename", ""))).name
                    if _file_ext(member_name) != "xml":
                        continue
                    raw = rf.read(info)
                    target = _unique_path(out_dir, _safe_name(member_name))
                    target.write_bytes(raw)
                    extracted.append(target)
        except Exception as exc:
            raise RuntimeError(
                "Falha ao extrair RAR. Verifique se o 'unrar' esta instalado no sistema."
            ) from exc
        return extracted

    def _get_store_row(loja_id: int):
        with db_session(settings.db_path) as conn:
            row = fetch_one(
                conn,
                """
                SELECT s.id,
                       s.client_id,
                       s.store_name,
                       s.cnpj,
                       s.active,
                       c.name AS client_name,
                       c.root_dir
                  FROM client_store_maps s
                  JOIN clients c ON c.id = s.client_id
                 WHERE s.id = ?
                """,
                (int(loja_id),),
            )
        if not row:
            raise HTTPException(status_code=404, detail="Loja nao encontrada.")
        return row

    def _list_active_store_rows() -> list[Any]:
        with db_session(settings.db_path) as conn:
            rows = fetch_all(
                conn,
                """
                SELECT s.id,
                       s.client_id,
                       s.store_name,
                       s.cnpj,
                       s.active,
                       c.name AS client_name,
                       c.root_dir
                  FROM client_store_maps s
                  JOIN clients c ON c.id = s.client_id
                 WHERE s.active = 1
                """,
            )
        return rows

    def _start_store_run(store_id: int) -> int:
        with db_session(settings.db_path) as conn:
            cur = conn.execute(
                """
                INSERT INTO store_runs (store_map_id, period, started_at, status, message)
                VALUES (?, 'UPLOAD', ?, 'running', 'Upload em processamento')
                """,
                (int(store_id), utc_now_iso()),
            )
            return int(cur.lastrowid)

    def _finish_store_run(
        run_id: int,
        *,
        status: str,
        message: str,
        pending_skus: int = 0,
        alerts_count: int = 0,
    ) -> None:
        with db_session(settings.db_path) as conn:
            conn.execute(
                """
                UPDATE store_runs
                   SET finished_at = ?,
                       status = ?,
                       message = ?,
                       pending_skus = ?,
                       alerts_count = ?
                 WHERE id = ?
                """,
                (
                    utc_now_iso(),
                    status,
                    message,
                    int(pending_skus),
                    int(alerts_count),
                    int(run_id),
                ),
            )

    def _set_job(job_id: str, **fields: Any) -> None:
        with jobs_lock:
            current = jobs.get(job_id)
            if not current:
                return
            current.update(fields)
            if "progresso" in fields and "progress" not in fields:
                current["progress"] = fields["progresso"]
            if "progress" in fields and "progresso" not in fields:
                current["progresso"] = fields["progress"]
            current["updated_at"] = utc_now_iso()

    def _get_job(job_id: str) -> dict[str, Any] | None:
        with jobs_lock:
            current = jobs.get(job_id)
            if not current:
                return None
            return dict(current)

    def _process_upload_job(
        job_id: str,
        loja_id: int | None,
        staged_files: list[dict[str, Any]],
    ) -> None:
        job_root = settings.data_dir / "upload_jobs" / job_id
        extracted_dir = job_root / "extracted"
        extracted_dir.mkdir(parents=True, exist_ok=True)

        nfes: list[dict[str, Any]] = []
        extracted_xmls: list[Path] = []
        xml_meta: list[dict[str, Any]] = []

        try:
            _set_job(job_id, status="extraindo", progresso=5)

            total_files = max(1, len(staged_files))
            for idx, file_info in enumerate(staged_files):
                src = Path(str(file_info["path"]))
                src_name = str(file_info.get("name") or src.name)
                ext = _file_ext(src_name)
                try:
                    if ext == "xml":
                        target = _unique_path(extracted_dir, _safe_name(src_name))
                        shutil.copy2(src, target)
                        extracted_xmls.append(target)
                    elif ext == "zip":
                        extracted_xmls.extend(_extract_zip_xmls(src, extracted_dir))
                    elif ext == "rar":
                        extracted_xmls.extend(_extract_rar_xmls(src, extracted_dir))
                    else:
                        nfes.append(
                            {
                                "arquivo": src_name,
                                "fornecedor": "",
                                "cnpj_emit": "",
                                "data": "",
                                "valor": 0.0,
                                "novos_skus": 0,
                                "erro": "Formato nao suportado. Use XML, ZIP ou RAR.",
                            }
                        )
                except Exception as exc:
                    nfes.append(
                        {
                            "arquivo": src_name,
                            "fornecedor": "",
                            "cnpj_emit": "",
                            "data": "",
                            "valor": 0.0,
                            "novos_skus": 0,
                            "erro": f"{type(exc).__name__}: {exc}",
                        }
                    )

                progress = 5 + int(((idx + 1) / total_files) * 40)
                _set_job(job_id, progresso=progress)

            for xml_path in extracted_xmls:
                summary = _parse_xml_summary(xml_path)
                nfes.append(summary)
                xml_meta.append({"path": xml_path, "summary": summary})

            if not extracted_xmls:
                result = {
                    "total": len(nfes),
                    "ok": sum(1 for item in nfes if not item.get("erro")),
                    "erros": sum(1 for item in nfes if item.get("erro")),
                    "novos_skus": 0,
                    "nfes": nfes,
                }
                _set_job(job_id, status="concluido", progresso=100, result=result)
                _finish_store_run(
                    run_id,
                    status="failed",
                    message="Upload sem XML valido para processamento.",
                )
                return

            _set_job(job_id, status="processando", progresso=55)
            manual_store = _get_store_row(int(loja_id)) if loja_id is not None else None
            active_stores = _list_active_store_rows()
            store_by_id = {int(row["id"]): row for row in active_stores}
            store_by_cnpj = {}
            for row in active_stores:
                cnpj_digits = _digits(str(row["cnpj"] or ""))
                if cnpj_digits and cnpj_digits not in store_by_cnpj:
                    store_by_cnpj[cnpj_digits] = row

            xmls_by_store: dict[int, list[Path]] = {}
            for item in xml_meta:
                xml_path = item["path"]
                summary = item["summary"]
                cnpj_dest_digits = _digits(str(summary.get("cnpj_dest") or ""))
                target_store = store_by_cnpj.get(cnpj_dest_digits)
                if not target_store and manual_store is not None:
                    target_store = manual_store

                if not target_store:
                    if not summary.get("erro"):
                        summary["erro"] = "CNPJ destinatario nao vinculado a nenhuma loja ativa."
                    continue

                store_id = int(target_store["id"])
                summary["loja_id"] = store_id
                summary["loja_name"] = str(target_store["store_name"] or "")
                xmls_by_store.setdefault(store_id, []).append(xml_path)

            if not xmls_by_store:
                payload = {
                    "total": len(nfes),
                    "ok": sum(1 for item in nfes if not item.get("erro")),
                    "erros": sum(1 for item in nfes if item.get("erro")),
                    "novos_skus": 0,
                    "nfes": nfes,
                }
                _set_job(
                    job_id,
                    status="erro",
                    progresso=100,
                    error="Nenhum XML foi vinculado a loja ativa pelo CNPJ destinatario.",
                    result=payload,
                )
                return

            shared_editor_path = get_shared_editor_path(settings)
            counts_before = global_state_counts(settings.db_path)
            if (
                counts_before["review_total"] == 0
                and counts_before["catalog_total"] == 0
                and counts_before["mappings_total"] == 0
                and shared_editor_path.exists()
            ):
                import_global_state_from_excel(settings.db_path, shared_editor_path)

            export_global_state_to_excel(settings.db_path, shared_editor_path)
            bridge = LegacyNotebookBridge(settings.notebook_path)

            total_groups = max(1, len(xmls_by_store))
            successful_groups = 0
            processed_xml_count = 0

            for group_idx, (store_id, store_xmls) in enumerate(xmls_by_store.items(), start=1):
                run_id = _start_store_run(store_id)
                store = store_by_id.get(int(store_id)) or _get_store_row(int(store_id))

                client_root = Path(str(store["root_dir"]))
                _, output_dir, editor_path = ensure_client_layout(client_root)

                store_xml_dir = extracted_dir / f"store_{store_id}"
                if store_xml_dir.exists():
                    shutil.rmtree(store_xml_dir, ignore_errors=True)
                store_xml_dir.mkdir(parents=True, exist_ok=True)
                for source_xml in store_xmls:
                    shutil.copy2(source_xml, _unique_path(store_xml_dir, source_xml.name))

                cnpj_digits = _digits(str(store["cnpj"] or ""))
                store_name = str(store["store_name"] or "LOJA")
                cnpj_map = {cnpj_digits: store_name} if cnpj_digits else None

                try:
                    result = bridge.run_for_client(
                        xml_dir=store_xml_dir,
                        out_dir=output_dir,
                        editor_path=editor_path,
                        shared_editor_path=shared_editor_path,
                        cnpj_to_loja=cnpj_map,
                        brand=str(store["client_name"] or ""),
                    )
                    successful_groups += 1
                    processed_xml_count += len(store_xmls)
                    _finish_store_run(
                        run_id,
                        status="success",
                        message=f"Upload processado com {len(store_xmls)} XML(s).",
                        pending_skus=int(result.pending_review_count),
                        alerts_count=int(result.alerts_diario_count),
                    )
                except Exception as exc:
                    message = f"{type(exc).__name__}: {exc}"
                    _finish_store_run(
                        run_id,
                        status="failed",
                        message=message,
                    )
                    for summary in nfes:
                        if int(summary.get("loja_id") or 0) == int(store_id) and not summary.get("erro"):
                            summary["erro"] = f"Falha no processamento da loja {store_name}: {message}"

                progress = 55 + int((group_idx / total_groups) * 37)
                _set_job(job_id, progresso=progress)

            _set_job(job_id, progresso=92)
            import_global_state_from_excel(settings.db_path, shared_editor_path)
            counts_after = global_state_counts(settings.db_path)
            novos_skus = max(
                0,
                int(counts_after["review_pending"]) - int(counts_before["review_pending"]),
            )

            payload = {
                "total": len(nfes),
                "ok": sum(1 for item in nfes if not item.get("erro")),
                "erros": sum(1 for item in nfes if item.get("erro")),
                "novos_skus": novos_skus,
                "nfes": nfes,
                "processed_xml_count": int(processed_xml_count),
                "processed_stores": int(successful_groups),
            }
            if successful_groups > 0:
                _set_job(job_id, status="concluido", progresso=100, result=payload)
            else:
                _set_job(
                    job_id,
                    status="erro",
                    progresso=100,
                    result=payload,
                    error="Nenhuma loja foi processada com sucesso.",
                )
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            _set_job(job_id, status="erro", progresso=100, error=message)

    @router.post("")
    async def api_upload(
        background_tasks: BackgroundTasks,
        loja_id: int | None = Form(None),
        files: list[UploadFile] = File(...),
    ):
        store = None
        if loja_id is not None:
            store = _get_store_row(int(loja_id))
            if not bool(store["active"]):
                raise HTTPException(status_code=422, detail="Loja inativa.")
        if not files:
            raise HTTPException(status_code=422, detail="Envie ao menos um arquivo.")

        accepted = [file for file in files if _is_supported_upload(str(file.filename or ""))]
        if not accepted:
            raise HTTPException(status_code=422, detail="Nenhum arquivo valido. Use XML, ZIP ou RAR.")

        job_id = uuid.uuid4().hex
        now = utc_now_iso()
        job_root = settings.data_dir / "upload_jobs" / job_id
        raw_dir = job_root / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        staged_files: list[dict[str, Any]] = []
        for idx, upload in enumerate(accepted, start=1):
            safe_name = _safe_name(str(upload.filename or f"arquivo_{idx}"))
            target = raw_dir / f"{idx:03d}_{safe_name}"
            content = await upload.read()
            target.write_bytes(content)
            staged_files.append(
                {
                    "name": str(upload.filename or safe_name),
                    "path": str(target),
                    "size": int(len(content)),
                }
            )
            await upload.close()

        with jobs_lock:
            jobs[job_id] = {
                "job_id": job_id,
                "loja_id": int(loja_id) if loja_id is not None else None,
                "loja_nome": str(store["store_name"] or "") if store else "Auto por CNPJ dest",
                "status": "queued",
                "progresso": 0,
                "progress": 0,
                "created_at": now,
                "updated_at": now,
                "result": {
                    "total": 0,
                    "ok": 0,
                    "erros": 0,
                    "novos_skus": 0,
                    "nfes": [],
                },
                "error": "",
                "files": [
                    {
                        "name": str(item["name"]),
                        "size": int(item["size"]),
                        "ext": _file_ext(str(item["name"])),
                    }
                    for item in staged_files
                ],
            }

        background_tasks.add_task(_process_upload_job, job_id, int(loja_id) if loja_id is not None else None, staged_files)
        return {
            "job_id": job_id,
            "status": "queued",
            "progresso": 0,
            "total_arquivos": len(staged_files),
        }

    @router.get("/status/{job_id}")
    def api_upload_status(job_id: str):
        job = _get_job(str(job_id).strip())
        if not job:
            raise HTTPException(status_code=404, detail="Job nao encontrado.")
        return job

    return router
