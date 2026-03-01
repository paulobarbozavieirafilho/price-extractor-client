from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    project_dir: Path
    workspace_dir: Path
    notebook_path: Path
    clients_dir: Path
    data_dir: Path
    logs_dir: Path
    db_path: Path
    timezone: str
    schedule_hour: int
    schedule_minute: int


def load_settings() -> Settings:
    project_dir = Path(__file__).resolve().parents[1]
    workspace_dir = project_dir.parent

    notebook_default = workspace_dir / "nfe_sku_pipeline_v10.ipynb"
    notebook_path = Path(os.getenv("PEC_NOTEBOOK_PATH", str(notebook_default))).resolve()

    clients_dir = Path(os.getenv("PEC_CLIENTS_DIR", str(project_dir / "clientes"))).resolve()
    data_dir = Path(os.getenv("PEC_DATA_DIR", str(project_dir / "data"))).resolve()
    logs_dir = Path(os.getenv("PEC_LOGS_DIR", str(project_dir / "logs"))).resolve()

    db_path = Path(os.getenv("PEC_DB_PATH", str(data_dir / "app.db"))).resolve()
    timezone = os.getenv("PEC_TIMEZONE", "America/Sao_Paulo")
    schedule_hour = int(os.getenv("PEC_SCHEDULE_HOUR", "7"))
    schedule_minute = int(os.getenv("PEC_SCHEDULE_MINUTE", "0"))

    return Settings(
        project_dir=project_dir,
        workspace_dir=workspace_dir,
        notebook_path=notebook_path,
        clients_dir=clients_dir,
        data_dir=data_dir,
        logs_dir=logs_dir,
        db_path=db_path,
        timezone=timezone,
        schedule_hour=schedule_hour,
        schedule_minute=schedule_minute,
    )


def ensure_runtime_dirs(settings: Settings) -> None:
    settings.clients_dir.mkdir(parents=True, exist_ok=True)
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.logs_dir.mkdir(parents=True, exist_ok=True)

