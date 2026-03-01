from __future__ import annotations

import html
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .db import init_db
from .global_state_store import (
    apply_global_review_decision,
    export_global_state_to_excel,
    global_state_counts,
    import_global_state_from_excel,
    list_global_catalog,
    list_global_mappings,
    list_global_pending_review,
    upsert_global_catalog,
    upsert_global_mapping,
)
from .notebook_bridge import LegacyNotebookBridge
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
from .settings import Settings, ensure_runtime_dirs


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


def _render_page(title: str, body: str) -> str:
    return (
        "<!doctype html>"
        "<html lang='pt-BR'>"
        "<head>"
        "<meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        f"<title>{html.escape(title)}</title>"
        "<link rel='stylesheet' href='/static/styles.css'>"
        "</head><body>"
        "<header class='topbar'><div class='container'>"
        "<a class='brand' href='/'>Price Extractor Client</a>"
        "</div></header>"
        f"<main class='container'>{body}</main>"
        "</body></html>"
    )


def _render_dashboard(settings: Settings) -> str:
    clients = list_clients(settings.db_path, only_active=False)
    counts = global_state_counts(settings.db_path)
    shared_editor = get_shared_editor_path(settings)
    rows = []
    for client in clients:
        run = get_last_run_for_client(settings.db_path, client.id)
        stores = get_client_stores(settings.db_path, client.id)
        badge = "neutral"
        status = "nunca rodou"
        if run:
            status = run.status
            badge = run.status
        stores_html = (
            "".join(
                f"<div class='muted'>{html.escape(cnpj)} - {html.escape(name)}</div>"
                for cnpj, name in stores.items()
            )
            if stores
            else "<span class='muted'>Sem mapeamento</span>"
        )
        rows.append(
            "<tr>"
            f"<td><strong>{html.escape(client.name)}</strong><div class='muted'>{html.escape(client.slug)}</div>"
            f"<div class='muted'>Grupo: {html.escape(client.economic_group)}</div>"
            f"<div class='muted'><code>{html.escape(str(client.root_dir))}</code></div></td>"
            f"<td><span class='badge {badge}'>{html.escape(status)}</span>"
            f"<div class='muted'>{'ativo' if client.active else 'inativo'}</div></td>"
            f"<td>{html.escape(run.started_at) if run else '-'}</td>"
            f"<td>{run.pending_review_count if run else 0}</td>"
            f"<td>{run.xml_count if run else 0}</td>"
            f"<td>{stores_html}</td>"
            "<td class='actions'>"
            f"<form method='post' action='/clients/{client.id}/run'><button type='submit'>Reprocessar</button></form>"
            f"<form method='post' action='/clients/{client.id}/toggle'><button type='submit'>{'Desativar' if client.active else 'Ativar'}</button></form>"
            f"<a class='button-link' href='/clients/{client.id}/review'>Review</a>"
            "</td>"
            "</tr>"
        )

    body = (
        "<section class='card'>"
        "<h1>Painel de clientes</h1>"
        f"<p class='muted'>Notebook legado: <code>{html.escape(str(settings.notebook_path))}</code></p>"
        f"<p class='muted'>Cadastro global: <code>{html.escape(str(shared_editor))}</code></p>"
        f"<p class='muted'>Review pendente: {counts['review_pending']} | Catalogo: {counts['catalog_total']} | Mappings: {counts['mappings_total']}</p>"
        "<p>"
        "<a class='button-link' href='/global/review'>Review Global</a> "
        "<a class='button-link' href='/global/mappings'>Mappings</a> "
        "<a class='button-link' href='/global/catalog'>Catalogo</a>"
        "</p>"
        "<form method='post' action='/global/sync/import' style='display:inline-block; margin-right:8px;'><button type='submit'>Importar do Excel</button></form>"
        "<form method='post' action='/global/sync/export' style='display:inline-block;'><button type='submit'>Exportar para Excel</button></form>"
        "</section>"
        "<section class='card'>"
        "<h2>Novo cliente</h2>"
        "<form method='post' action='/clients' class='form-grid'>"
        "<label>Nome<input name='name' required></label>"
        "<label>Slug<input name='slug'></label>"
        "<label>Grupo economico<input name='economic_group' value='default'></label>"
        "<label>Root dir<input name='root_dir'></label>"
        "<label class='full'>Lojas (CNPJ=Nome, 1 por linha)<textarea name='stores_text' rows='4'></textarea></label>"
        "<button type='submit'>Salvar cliente</button>"
        "</form>"
        "</section>"
        "<section class='card'>"
        "<h2>Clientes cadastrados</h2>"
        "<div class='table-wrap'><table><thead><tr>"
        "<th>Cliente</th><th>Status</th><th>Ultimo run</th><th>Pendentes</th><th>XMLs</th><th>Lojas</th><th>Acoes</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table></div></section>"
    )
    return _render_page("Painel Price Extractor Client", body)


def _render_global_review(settings: Settings, q: str = "") -> str:
    pending = list_global_pending_review(settings.db_path, q=q, limit=400)
    items = []
    for row in pending:
        items.append(
            "<article class='review-item'>"
            f"<h3>{html.escape(str(row.get('xProd') or row.get('xProd_norm') or row.get('fingerprint') or 'Item'))}</h3>"
            f"<div class='muted'>FP: <code>{html.escape(str(row.get('fingerprint','')))}</code></div>"
            f"<div class='muted'>Sugestao: {html.escape(str(row.get('suggested_sku_id','')))} - {html.escape(str(row.get('suggested_sku_name_canonical','')))}</div>"
            "<form method='post' action='/global/review/decision' class='review-form'>"
            f"<input type='hidden' name='fingerprint' value='{html.escape(str(row.get('fingerprint','')))}'>"
            f"<label>SKU escolhido<input name='chosen_sku_id' value='{html.escape(str(row.get('chosen_sku_id','')))}'></label>"
            f"<label>Notas<input name='notes' value='{html.escape(str(row.get('notes','')))}'></label>"
            "<div class='actions'>"
            "<button type='submit' name='action' value='approve'>Aprovar</button>"
            "<button type='submit' name='action' value='ignore'>Ignorar</button>"
            "</div></form></article>"
        )
    body = (
        "<section class='card'>"
        "<h1>Review Global</h1>"
        "<p><a href='/'>Voltar</a></p>"
        f"<p class='muted'>Cadastro global: <code>{html.escape(str(get_shared_editor_path(settings)))}</code></p>"
        "<form method='get' action='/global/review' class='form-grid'>"
        f"<label>Busca<input name='q' value='{html.escape(q)}'></label>"
        "<button type='submit'>Filtrar</button>"
        "</form>"
        "</section>"
        "<section class='card'>"
        + ("".join(items) if items else "<p class='muted'>Sem itens pendentes.</p>")
        + "</section>"
    )
    return _render_page("Review Global", body)


def _render_global_mappings(settings: Settings, q: str = "") -> str:
    rows = list_global_mappings(settings.db_path, q=q, limit=600)
    cards = []
    for row in rows:
        cards.append(
            "<article class='review-item'>"
            f"<h3>{html.escape(str(row.get('fingerprint','')))}</h3>"
            "<form method='post' action='/global/mappings' class='review-form'>"
            f"<input type='hidden' name='fingerprint' value='{html.escape(str(row.get('fingerprint','')))}'>"
            f"<label>SKU ID<input name='sku_id' value='{html.escape(str(row.get('sku_id','')))}'></label>"
            f"<label>Status<input name='status' value='{html.escape(str(row.get('status','')))}'></label>"
            f"<label>Base measure override<input name='base_measure_override' value='{html.escape(str(row.get('base_measure_override','')))}'></label>"
            f"<label>Base qty override<input name='base_qty_per_purchase_unit_override' value='{html.escape(str(row.get('base_qty_per_purchase_unit_override','')))}'></label>"
            "<button type='submit'>Salvar mapping</button>"
            "</form></article>"
        )

    body = (
        "<section class='card'>"
        "<h1>Mappings Globais</h1>"
        "<p><a href='/'>Voltar</a></p>"
        "<form method='get' action='/global/mappings' class='form-grid'>"
        f"<label>Busca<input name='q' value='{html.escape(q)}'></label>"
        "<button type='submit'>Filtrar</button>"
        "</form>"
        "</section>"
        "<section class='card'>"
        "<h2>Novo mapping</h2>"
        "<form method='post' action='/global/mappings' class='form-grid'>"
        "<label>Fingerprint<input name='fingerprint' required></label>"
        "<label>SKU ID<input name='sku_id'></label>"
        "<label>Status<input name='status'></label>"
        "<label>Base measure override<input name='base_measure_override'></label>"
        "<label>Base qty override<input name='base_qty_per_purchase_unit_override'></label>"
        "<button type='submit'>Salvar mapping</button>"
        "</form>"
        "</section>"
        "<section class='card'>"
        + ("".join(cards) if cards else "<p class='muted'>Nenhum mapping encontrado.</p>")
        + "</section>"
    )
    return _render_page("Mappings Globais", body)


def _render_global_catalog(settings: Settings, q: str = "") -> str:
    rows = list_global_catalog(settings.db_path, q=q, limit=600)
    cards = []
    for row in rows:
        cards.append(
            "<article class='review-item'>"
            f"<h3>{html.escape(str(row.get('sku_id','')))} - {html.escape(str(row.get('sku_name_canonical','')))}</h3>"
            "<form method='post' action='/global/catalog' class='review-form'>"
            f"<input type='hidden' name='sku_id' value='{html.escape(str(row.get('sku_id','')))}'>"
            f"<label>Nome canonico<input name='sku_name_canonical' value='{html.escape(str(row.get('sku_name_canonical','')))}'></label>"
            f"<label>Brand<input name='brand' value='{html.escape(str(row.get('brand','')))}'></label>"
            f"<label>Category<input name='category' value='{html.escape(str(row.get('category','')))}'></label>"
            f"<label>Base measure<input name='base_measure' value='{html.escape(str(row.get('base_measure','')))}'></label>"
            "<button type='submit'>Salvar catalogo</button>"
            "</form></article>"
        )

    body = (
        "<section class='card'>"
        "<h1>Catalogo Global</h1>"
        "<p><a href='/'>Voltar</a></p>"
        "<form method='get' action='/global/catalog' class='form-grid'>"
        f"<label>Busca<input name='q' value='{html.escape(q)}'></label>"
        "<button type='submit'>Filtrar</button>"
        "</form>"
        "</section>"
        "<section class='card'>"
        "<h2>Novo SKU</h2>"
        "<form method='post' action='/global/catalog' class='form-grid'>"
        "<label>SKU ID<input name='sku_id' required></label>"
        "<label>Nome canonico<input name='sku_name_canonical'></label>"
        "<label>Brand<input name='brand'></label>"
        "<label>Category<input name='category'></label>"
        "<label>Base measure<input name='base_measure'></label>"
        "<button type='submit'>Salvar catalogo</button>"
        "</form>"
        "</section>"
        "<section class='card'>"
        + ("".join(cards) if cards else "<p class='muted'>Nenhum SKU encontrado.</p>")
        + "</section>"
    )
    return _render_page("Catalogo Global", body)


def start_fallback_server(settings: Settings, host: str, port: int) -> None:
    ensure_runtime_dirs(settings)
    init_db(settings.db_path)

    static_css = Path(__file__).parent / "static" / "styles.css"

    def run_client_async(client_id: int) -> None:
        client = get_client_by_id(settings.db_path, client_id)
        if not client:
            return
        bridge = LegacyNotebookBridge(settings.notebook_path)
        run_client_pipeline(settings, client, bridge)

    class Handler(BaseHTTPRequestHandler):
        def _send_html(self, html_text: str, status: int = 200) -> None:
            body = html_text.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_css(self) -> None:
            if not static_css.exists():
                self.send_error(404, "Arquivo nao encontrado")
                return
            data = static_css.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/css; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _redirect(self, location: str) -> None:
            self.send_response(303)
            self.send_header("Location", location)
            self.end_headers()

        def _parse_form(self) -> dict[str, str]:
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length).decode("utf-8", errors="ignore")
            parsed = parse_qs(raw, keep_blank_values=True)
            return {k: v[0] if v else "" for k, v in parsed.items()}

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query, keep_blank_values=True)
            if path == "/":
                self._send_html(_render_dashboard(settings))
                return
            if path == "/static/styles.css":
                self._send_css()
                return
            if path == "/global/review":
                self._send_html(_render_global_review(settings, q=(query.get("q", [""])[0])))
                return
            if path == "/global/mappings":
                self._send_html(_render_global_mappings(settings, q=(query.get("q", [""])[0])))
                return
            if path == "/global/catalog":
                self._send_html(_render_global_catalog(settings, q=(query.get("q", [""])[0])))
                return
            if path.startswith("/clients/") and path.endswith("/review"):
                self._redirect("/global/review")
                return
            self.send_error(404, "Nao encontrado")

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            form = self._parse_form()

            if path == "/clients":
                name = form.get("name", "").strip()
                if not name:
                    self._redirect("/")
                    return
                slug = slugify(form.get("slug", "") or name)
                root_dir_raw = form.get("root_dir", "").strip()
                root_dir = Path(root_dir_raw) if root_dir_raw else (settings.clients_dir / slug)
                root_dir.mkdir(parents=True, exist_ok=True)
                (root_dir / "input").mkdir(parents=True, exist_ok=True)
                (root_dir / "outputs").mkdir(parents=True, exist_ok=True)
                client = create_or_update_client(
                    settings.db_path,
                    slug=slug,
                    name=name,
                    economic_group=(form.get("economic_group", "").strip() or "default"),
                    root_dir=root_dir,
                    active=True,
                )
                set_client_stores(settings.db_path, client.id, _parse_stores_text(form.get("stores_text", "")))
                self._redirect("/")
                return

            if path.startswith("/clients/"):
                parts = [p for p in path.split("/") if p]
                if len(parts) >= 3 and parts[0] == "clients":
                    try:
                        client_id = int(parts[1])
                    except ValueError:
                        self.send_error(400, "ID invalido")
                        return
                    action = parts[2]
                    if action == "toggle":
                        client = get_client_by_id(settings.db_path, client_id)
                        if client:
                            set_client_active(settings.db_path, client_id, not client.active)
                        self._redirect("/")
                        return
                    if action == "run":
                        threading.Thread(
                            target=run_client_async,
                            args=(client_id,),
                            daemon=True,
                        ).start()
                        self._redirect("/")
                        return
                    if action == "review":
                        self._redirect("/global/review")
                        return

            if path == "/global/review/decision":
                apply_global_review_decision(
                    settings.db_path,
                    fingerprint=form.get("fingerprint", ""),
                    action=form.get("action", ""),
                    chosen_sku_id=form.get("chosen_sku_id", ""),
                    notes=form.get("notes", ""),
                )
                export_global_state_to_excel(settings.db_path, get_shared_editor_path(settings))
                self._redirect("/global/review")
                return

            if path == "/global/mappings":
                upsert_global_mapping(
                    settings.db_path,
                    fingerprint=form.get("fingerprint", ""),
                    sku_id=form.get("sku_id", ""),
                    status=form.get("status", ""),
                    base_measure_override=form.get("base_measure_override", ""),
                    base_qty_per_purchase_unit_override=form.get(
                        "base_qty_per_purchase_unit_override", ""
                    ),
                )
                export_global_state_to_excel(settings.db_path, get_shared_editor_path(settings))
                self._redirect("/global/mappings")
                return

            if path == "/global/catalog":
                upsert_global_catalog(
                    settings.db_path,
                    sku_id=form.get("sku_id", ""),
                    sku_name_canonical=form.get("sku_name_canonical", ""),
                    brand=form.get("brand", ""),
                    category=form.get("category", ""),
                    base_measure=form.get("base_measure", ""),
                )
                export_global_state_to_excel(settings.db_path, get_shared_editor_path(settings))
                self._redirect("/global/catalog")
                return

            if path == "/global/sync/import":
                import_global_state_from_excel(settings.db_path, get_shared_editor_path(settings))
                self._redirect("/")
                return

            if path == "/global/sync/export":
                export_global_state_to_excel(settings.db_path, get_shared_editor_path(settings))
                self._redirect("/")
                return

            self.send_error(404, "Nao encontrado")

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Servidor fallback ativo em http://{host}:{port}")
    try:
        server.serve_forever()
    finally:
        server.server_close()
