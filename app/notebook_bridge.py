from __future__ import annotations

import json
import os
import re
import io
from contextlib import redirect_stdout, redirect_stderr
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class PipelineRunResult:
    xml_count: int
    pending_review_count: int
    alerts_diario_count: int
    alerts_semanal_count: int
    alerts_semanal_atual_count: int
    alerts_mensal_count: int
    editor_path: Path
    out_dir: Path


class LegacyNotebookBridge:
    """
    Carrega e executa o notebook legado como modulo Python por cliente.
    Mantem a logica de negocio sem reescrever as regras de alerta/normalizacao.
    """

    HARDCODED_KEY_PATTERN = (
        r'OPENAI_API_KEY\s*=\s*os\.getenv\("OPENAI_API_KEY",\s*"[^"]*"\)\.strip\(\)'
    )

    def __init__(self, notebook_path: Path):
        self.notebook_path = notebook_path
        self._cells = self._read_cells(notebook_path)

    @staticmethod
    def _read_cells(notebook_path: Path) -> list[tuple[int, str]]:
        if not notebook_path.exists():
            raise FileNotFoundError(
                f"Notebook legado nao encontrado em: {notebook_path}"
            )
        nb = json.loads(notebook_path.read_text(encoding="utf-8"))
        cells: list[tuple[int, str]] = []
        for idx, cell in enumerate(nb.get("cells", [])):
            if cell.get("cell_type") != "code":
                continue
            src = cell.get("source", "")
            if isinstance(src, list):
                src = "".join(src)
            if not str(src).strip():
                continue
            cells.append((idx, str(src)))
        return cells

    def _sanitize_source(self, idx: int, src: str) -> str:
        if idx == 15:
            # Celula de execucao interativa do notebook.
            return ""

        if idx == 1:
            src = re.sub(
                self.HARDCODED_KEY_PATTERN,
                'OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()',
                src,
            )

        if idx == 16:
            # Corrige string quebrada no notebook exportado.
            src = src.replace('return "\n".join(lines)', 'return "\\n".join(lines)')
            src = re.sub(
                r"^\s*arquivos_wpp\s*=\s*generate_all_whatsapp_files\(EDITOR_PATH,\s*OUTPUT_DIR\)\s*$",
                "",
                src,
                flags=re.M,
            )

        if idx == 17:
            src = re.sub(
                r"^\s*HTML_DIR\s*=\s*OUTPUT_DIR\s*/\s*\"html_reports\"\s*$",
                "",
                src,
                flags=re.M,
            )
            src = re.sub(
                r"^\s*arquivos_html\s*=\s*generate_all_html_reports\(EDITOR_PATH,\s*HTML_DIR\)\s*$",
                "",
                src,
                flags=re.M,
            )

        return src

    @contextmanager
    def _temp_env(self, values: dict[str, str]):
        previous: dict[str, str | None] = {}
        for key, value in values.items():
            previous[key] = os.environ.get(key)
            os.environ[key] = value
        try:
            yield
        finally:
            for key, old_value in previous.items():
                if old_value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = old_value

    def _load_namespace(
        self,
        *,
        xml_dir: Path,
        out_dir: Path,
        editor_path: Path,
        cnpj_to_loja: dict[str, str] | None,
        brand: str,
    ) -> dict[str, Any]:
        ns: dict[str, Any] = {}
        env_values = {
            "XML_DIR": str(xml_dir),
            "OUT_DIR": str(out_dir),
            "EDITOR_PATH": str(editor_path),
            "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY", ""),
            "ALERTS_TOP_N": os.getenv("ALERTS_TOP_N", "5"),
            "MAX_ITEMS_PER_SECTION": os.getenv("MAX_ITEMS_PER_SECTION", "3"),
            "MIN_IMPACT_RS": os.getenv("MIN_IMPACT_RS", "20"),
        }

        with self._temp_env(env_values):
            for idx, raw_src in self._cells:
                src = self._sanitize_source(idx, raw_src)
                if not src.strip():
                    continue
                compiled = compile(src, f"legacy_notebook_cell_{idx}", "exec")
                exec(compiled, ns)

        # Garante parametros por cliente.
        ns["XML_DIR"] = xml_dir
        ns["OUT_DIR"] = out_dir
        ns["EDITOR_PATH"] = editor_path
        ns["BRAND"] = brand
        if cnpj_to_loja:
            ns["CNPJ_TO_LOJA"] = cnpj_to_loja
        self._install_runtime_patches(ns)
        return ns

    @staticmethod
    def _install_runtime_patches(ns: dict[str, Any]) -> None:
        """
        Corrige edge-cases conhecidos do notebook legado sem alterar a logica principal.
        """
        original_build_alerts = ns.get("build_price_alerts")
        if callable(original_build_alerts):
            def _safe_build_price_alerts(compras, as_of_date=None):
                if compras is None:
                    return pd.DataFrame()
                if getattr(compras, "empty", False):
                    return pd.DataFrame()
                if "dhEmi_date" not in getattr(compras, "columns", []):
                    return pd.DataFrame()
                return original_build_alerts(compras, as_of_date=as_of_date)

            ns["build_price_alerts"] = _safe_build_price_alerts

    @staticmethod
    def _count_alert_rows(editor_path: Path, sheet_name: str) -> int:
        try:
            df = pd.read_excel(editor_path, sheet_name=sheet_name)
        except Exception:
            return 0
        return int(len(df.index))

    @staticmethod
    def _count_pending_review(review_df: pd.DataFrame) -> int:
        if review_df.empty:
            return 0
        if "status" not in review_df.columns:
            return int(len(review_df.index))

        status = (
            review_df["status"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.upper()
        )
        pending_mask = status.isin({"", "PENDING", "REVIEW"})
        return int(pending_mask.sum())

    def run_for_client(
        self,
        *,
        xml_dir: Path,
        out_dir: Path,
        editor_path: Path,
        shared_editor_path: Path | None,
        cnpj_to_loja: dict[str, str] | None,
        brand: str,
    ) -> PipelineRunResult:
        out_dir.mkdir(parents=True, exist_ok=True)
        editor_path.parent.mkdir(parents=True, exist_ok=True)
        active_editor_path = shared_editor_path or editor_path
        active_editor_path.parent.mkdir(parents=True, exist_ok=True)

        ns = self._load_namespace(
            xml_dir=xml_dir,
            out_dir=out_dir,
            editor_path=active_editor_path,
            cnpj_to_loja=cnpj_to_loja,
            brand=brand,
        )

        run_pipeline = ns["run_pipeline"]
        write_editor_excel = ns["write_editor_excel"]
        generate_wpp = ns["generate_all_whatsapp_files"]
        generate_html = ns["generate_all_html_reports"]

        review_df, catalog_df, mappings_df, compras_df = run_pipeline()

        if active_editor_path.resolve() != editor_path.resolve():
            write_editor_excel(editor_path, review_df, catalog_df, mappings_df, compras_df)

        whatsapp_dir = out_dir / "whatsapp_alertas"
        html_dir = whatsapp_dir / "html_reports"
        whatsapp_dir.mkdir(parents=True, exist_ok=True)
        html_dir.mkdir(parents=True, exist_ok=True)

        alert_counts = {
            "diario": self._count_alert_rows(editor_path, "Alertas_Diario"),
            "semanal": self._count_alert_rows(editor_path, "Alertas_Semanal"),
            "semanal_atual": self._count_alert_rows(editor_path, "Alertas_Semanal_Atual"),
            "mensal": self._count_alert_rows(editor_path, "Alertas_Mensal"),
        }
        has_alert_data = any(v > 0 for v in alert_counts.values())

        # Alguns cenarios (ex.: cliente novo sem compras) nao possuem alertas para renderizacao.
        if has_alert_data:
            try:
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    generate_wpp(editor_path, whatsapp_dir)
            except Exception as exc:
                print(f"[WARN] Falha ao gerar arquivos WhatsApp: {exc}")
            try:
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    generate_html(editor_path, html_dir, brand=brand)
            except Exception as exc:
                print(f"[WARN] Falha ao gerar HTML: {exc}")

        xml_count = len(list(xml_dir.glob("**/*.xml")))
        pending_review_count = self._count_pending_review(review_df)

        return PipelineRunResult(
            xml_count=xml_count,
            pending_review_count=pending_review_count,
            alerts_diario_count=alert_counts["diario"],
            alerts_semanal_count=alert_counts["semanal"],
            alerts_semanal_atual_count=alert_counts["semanal_atual"],
            alerts_mensal_count=alert_counts["mensal"],
            editor_path=editor_path,
            out_dir=out_dir,
        )
