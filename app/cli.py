from __future__ import annotations

import argparse
from pathlib import Path

from .db import init_db
from .repository import (
    create_or_update_client,
    get_client_by_slug,
    set_client_stores,
)
from .runner import run_all_clients, run_client_pipeline, slugify
from .settings import ensure_runtime_dirs, load_settings
from .notebook_bridge import LegacyNotebookBridge


def _parse_stores(values: list[str]) -> dict[str, str]:
    stores: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            continue
        left, right = item.split("=", 1)
        digits = "".join(ch for ch in left if ch.isdigit())
        if digits and right.strip():
            stores[digits] = right.strip()
    return stores


def cmd_init_db(_: argparse.Namespace) -> int:
    settings = load_settings()
    ensure_runtime_dirs(settings)
    init_db(settings.db_path)
    print(f"Banco inicializado em: {settings.db_path}")
    return 0


def cmd_add_client(args: argparse.Namespace) -> int:
    settings = load_settings()
    ensure_runtime_dirs(settings)
    init_db(settings.db_path)

    slug = slugify(args.slug or args.name)
    root_dir = Path(args.root_dir).expanduser() if args.root_dir else settings.clients_dir / slug
    root_dir.mkdir(parents=True, exist_ok=True)
    (root_dir / "input").mkdir(parents=True, exist_ok=True)
    (root_dir / "outputs").mkdir(parents=True, exist_ok=True)

    client = create_or_update_client(
        settings.db_path,
        slug=slug,
        name=args.name.strip(),
        economic_group=(args.economic_group.strip() or "default"),
        root_dir=root_dir,
        active=not args.inactive,
    )

    if args.store:
        stores = _parse_stores(args.store)
        set_client_stores(settings.db_path, client.id, stores)
    print(f"Cliente salvo: {client.name} ({client.slug}) em {client.root_dir}")
    return 0


def cmd_run_all(_: argparse.Namespace) -> int:
    settings = load_settings()
    ensure_runtime_dirs(settings)
    init_db(settings.db_path)
    outcomes = run_all_clients(settings, only_active=True)
    if not outcomes:
        print("Nenhum cliente ativo encontrado.")
        return 0
    for out in outcomes:
        print(f"[{out.client.slug}] {'OK' if out.ok else 'FALHOU'} - {out.message}")
    return 0


def cmd_run_client(args: argparse.Namespace) -> int:
    settings = load_settings()
    ensure_runtime_dirs(settings)
    init_db(settings.db_path)

    client = get_client_by_slug(settings.db_path, args.slug)
    if not client:
        print(f"Cliente nao encontrado: {args.slug}")
        return 1

    bridge = LegacyNotebookBridge(settings.notebook_path)
    outcome = run_client_pipeline(settings, client, bridge)
    print(f"[{client.slug}] {'OK' if outcome.ok else 'FALHOU'} - {outcome.message}")
    return 0 if outcome.ok else 1


def cmd_serve(args: argparse.Namespace) -> int:
    try:
        import fastapi  # noqa: F401
        import uvicorn
    except ModuleNotFoundError:
        if not args.allow_fallback:
            print(
                "FastAPI/Uvicorn nao encontrados no ambiente atual. "
                "Instale as dependencias e rode novamente."
            )
            print("Sugestao: .\\.venv\\Scripts\\python.exe -m pip install -e .")
            return 1
        from .fallback_server import start_fallback_server
        from .settings import load_settings

        print("FastAPI/Uvicorn indisponivel; iniciando servidor fallback (stdlib).")
        start_fallback_server(load_settings(), args.host, args.port)
        return 0

    uvicorn.run("app.web:app", host=args.host, port=args.port, reload=args.reload)
    return 0


def cmd_schedule(args: argparse.Namespace) -> int:
    from .scheduler import run_once, start_blocking_scheduler

    if args.run_now:
        run_once()
    start_blocking_scheduler()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Price Extractor Client CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init-db", help="Inicializa o banco SQLite")
    p_init.set_defaults(func=cmd_init_db)

    p_add = sub.add_parser("add-client", help="Cadastra/atualiza cliente")
    p_add.add_argument("--name", required=True, help="Nome do cliente")
    p_add.add_argument("--slug", default="", help="Slug do cliente")
    p_add.add_argument(
        "--economic-group",
        default="default",
        help="Grupo economico (metadado para agrupamento de lojas/cliente)",
    )
    p_add.add_argument("--root-dir", default="", help="Diretorio raiz do cliente")
    p_add.add_argument(
        "--store",
        action="append",
        help="Mapeamento CNPJ=LOJA (pode repetir)",
    )
    p_add.add_argument("--inactive", action="store_true", help="Cadastra como inativo")
    p_add.set_defaults(func=cmd_add_client)

    p_run_all = sub.add_parser("run-all", help="Roda pipeline em todos os clientes ativos")
    p_run_all.set_defaults(func=cmd_run_all)

    p_run_client = sub.add_parser("run-client", help="Roda pipeline para um cliente")
    p_run_client.add_argument("--slug", required=True, help="Slug do cliente")
    p_run_client.set_defaults(func=cmd_run_client)

    p_serve = sub.add_parser("serve", help="Sobe painel web local")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8000)
    p_serve.add_argument("--reload", action="store_true")
    p_serve.add_argument(
        "--allow-fallback",
        action="store_true",
        help="Permite iniciar o servidor fallback (stdlib) se FastAPI/Uvicorn nao estiverem instalados.",
    )
    p_serve.set_defaults(func=cmd_serve)

    p_schedule = sub.add_parser("schedule", help="Inicia scheduler diario")
    p_schedule.add_argument("--run-now", action="store_true", help="Executa uma vez antes de agendar")
    p_schedule.set_defaults(func=cmd_schedule)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
