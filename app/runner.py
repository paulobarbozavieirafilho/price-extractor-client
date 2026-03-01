from __future__ import annotations

import re
import traceback
from dataclasses import dataclass
from pathlib import Path

from .notebook_bridge import LegacyNotebookBridge, PipelineRunResult
from .global_state_store import (
    export_global_state_to_excel,
    global_state_counts,
    import_global_state_from_excel,
)
from .repository import (
    Client,
    create_run,
    finish_run,
    get_client_stores,
    list_clients,
)
from .settings import Settings


@dataclass
class ClientRunOutcome:
    client: Client
    ok: bool
    message: str
    run_id: int
    result: PipelineRunResult | None = None


def slugify(name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", name.strip().lower()).strip("-")
    return slug or "cliente"


def ensure_client_layout(client_root: Path) -> tuple[Path, Path, Path]:
    input_dir = client_root / "input"
    output_dir = client_root / "outputs"
    editor_path = output_dir / "pipeline_editor.xlsx"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return input_dir, output_dir, editor_path


def get_shared_editor_path(settings: Settings) -> Path:
    shared_dir = settings.data_dir / "shared_state" / "global"
    shared_dir.mkdir(parents=True, exist_ok=True)
    return shared_dir / "pipeline_editor_shared.xlsx"


def run_client_pipeline(
    settings: Settings,
    client: Client,
    bridge: LegacyNotebookBridge,
) -> ClientRunOutcome:
    run_id = create_run(settings.db_path, client.id)
    input_dir, output_dir, editor_path = ensure_client_layout(client.root_dir)
    shared_editor_path = get_shared_editor_path(settings)
    stores = get_client_stores(settings.db_path, client.id)

    try:
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
            cnpj_to_loja=stores,
            brand=client.name,
        )
        import_global_state_from_excel(settings.db_path, shared_editor_path)
        finish_run(
            settings.db_path,
            run_id,
            status="success",
            message="Processamento concluido com sucesso.",
            xml_count=result.xml_count,
            pending_review_count=result.pending_review_count,
            alerts_diario_count=result.alerts_diario_count,
            alerts_semanal_count=result.alerts_semanal_count,
            alerts_semanal_atual_count=result.alerts_semanal_atual_count,
            alerts_mensal_count=result.alerts_mensal_count,
            output_dir=str(result.out_dir),
        )
        return ClientRunOutcome(
            client=client,
            ok=True,
            message="ok",
            run_id=run_id,
            result=result,
        )
    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}"
        finish_run(
            settings.db_path,
            run_id,
            status="failed",
            message=f"{error_message}\n{traceback.format_exc(limit=5)}",
            output_dir=str(output_dir),
        )
        return ClientRunOutcome(
            client=client,
            ok=False,
            message=error_message,
            run_id=run_id,
            result=None,
        )


def run_all_clients(settings: Settings, *, only_active: bool = True) -> list[ClientRunOutcome]:
    clients = list_clients(settings.db_path, only_active=only_active)
    if not clients:
        return []
    bridge = LegacyNotebookBridge(settings.notebook_path)
    outcomes = [run_client_pipeline(settings, client, bridge) for client in clients]
    return outcomes
