from __future__ import annotations

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from .runner import run_all_clients
from .settings import ensure_runtime_dirs, load_settings
from .db import init_db


def run_once() -> None:
    settings = load_settings()
    ensure_runtime_dirs(settings)
    init_db(settings.db_path)
    outcomes = run_all_clients(settings, only_active=True)
    if not outcomes:
        print("Nenhum cliente ativo cadastrado.")
        return
    ok = sum(1 for x in outcomes if x.ok)
    fail = len(outcomes) - ok
    print(f"Execucao finalizada. Sucesso: {ok} | Falhas: {fail}")


def start_blocking_scheduler() -> None:
    settings = load_settings()
    ensure_runtime_dirs(settings)
    init_db(settings.db_path)

    scheduler = BlockingScheduler(timezone=settings.timezone)
    trigger = CronTrigger(hour=settings.schedule_hour, minute=settings.schedule_minute)
    scheduler.add_job(run_once, trigger=trigger, id="daily_clients_run", replace_existing=True)

    print(
        "Scheduler ativo. "
        f"Rodando todo dia as {settings.schedule_hour:02d}:{settings.schedule_minute:02d} ({settings.timezone})."
    )
    scheduler.start()

