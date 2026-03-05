import { useCallback, useEffect, useMemo, useState } from "react";

const PERIODS = [
  { value: "DIARIO", label: "Diario D-1" },
  { value: "SEMANAL", label: "Semanal fechado" },
  { value: "SEMANAL_ATUAL", label: "Semana atual" },
  { value: "MENSAL", label: "Mensal fechado" },
];

const fmtCnpj = (value) => {
  const v = String(value || "").replace(/\D/g, "");
  if (v.length !== 14) return value || "-";
  return v.replace(/(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, "$1.$2.$3/$4-$5");
};

const fmtPhone = (value) => {
  const v = String(value || "").replace(/\D/g, "");
  if (v.length === 11) return v.replace(/(\d{2})(\d{5})(\d{4})/, "($1) $2-$3");
  if (v.length === 10) return v.replace(/(\d{2})(\d{4})(\d{4})/, "($1) $2-$3");
  return value || "-";
};

const safeJson = async (res) => {
  const raw = await res.text();
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch {
    return { detail: raw };
  }
};

const readError = async (res) => {
  const payload = await safeJson(res);
  if (typeof payload?.detail === "string" && payload.detail) return payload.detail;
  return `HTTP ${res.status}`;
};

const Btn = ({ children, onClick, disabled = false, kind = "default", small = false }) => {
  const map = {
    default: { bg: "#f9fafb", color: "#374151", border: "#e5e7eb" },
    primary: { bg: "#1d4ed8", color: "#fff", border: "#1d4ed8" },
    success: { bg: "#166534", color: "#fff", border: "#166534" },
    whatsapp: { bg: "#25d366", color: "#fff", border: "#1da851" },
    danger: { bg: "#fff1f2", color: "#9f1239", border: "#fecaca" },
  };
  const c = map[kind] || map.default;
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        padding: small ? "5px 10px" : "7px 14px",
        borderRadius: 7,
        fontSize: 12,
        fontWeight: 600,
        border: `1px solid ${disabled ? "#e5e7eb" : c.border}`,
        background: disabled ? "#f3f4f6" : c.bg,
        color: disabled ? "#9ca3af" : c.color,
        cursor: disabled ? "not-allowed" : "pointer",
      }}
    >
      {children}
    </button>
  );
};

const Input = ({ ...props }) => (
  <input
    {...props}
    style={{
      width: "100%",
      border: "1px solid #e5e7eb",
      borderRadius: 8,
      padding: "8px 10px",
      fontSize: 13,
      color: "#111827",
      outline: "none",
      boxSizing: "border-box",
      ...(props.style || {}),
    }}
  />
);

const SELECT_STYLE = {
  width: "100%",
  border: "1px solid #e5e7eb",
  borderRadius: 8,
  padding: "8px 10px",
  fontSize: 13,
  color: "#111827",
  outline: "none",
  boxSizing: "border-box",
  background: "#fff",
};

const TH_S = {
  textAlign: "left",
  fontSize: 10,
  fontWeight: 600,
  color: "#9ca3af",
  padding: "8px 12px",
  textTransform: "uppercase",
  letterSpacing: "0.08em",
};

const TD_S = {
  padding: "10px 12px",
  fontSize: 13,
  color: "#111827",
  verticalAlign: "top",
};

const Badge = ({ children, bg = "#f3f4f6", color = "#6b7280", border = "#e5e7eb" }) => (
  <span
    style={{
      display: "inline-flex",
      alignItems: "center",
      padding: "2px 8px",
      borderRadius: 99,
      fontSize: 11,
      fontWeight: 600,
      background: bg,
      color,
      border: `1px solid ${border}`,
    }}
  >
    {children}
  </span>
);

const StatusBadge = ({ status }) => {
  if (status === "running") {
    return <Badge bg="#eff6ff" color="#1d4ed8" border="#dbeafe">Processando</Badge>;
  }
  if (status === "erro") {
    return <Badge bg="#fff1f2" color="#9f1239" border="#fecaca">Erro</Badge>;
  }
  return <Badge bg="#f0fdf4" color="#166534" border="#bbf7d0">OK</Badge>;
};

const Modal = ({ title, children, onClose }) => (
  <div
    onClick={onClose}
    style={{
      position: "fixed",
      inset: 0,
      background: "rgba(0,0,0,0.4)",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      zIndex: 1000,
    }}
  >
    <div
      onClick={(e) => e.stopPropagation()}
      style={{
        width: 620,
        maxWidth: "calc(100vw - 24px)",
        maxHeight: "90vh",
        overflow: "auto",
        background: "#fff",
        borderRadius: 14,
        boxShadow: "0 20px 60px rgba(0,0,0,0.2)",
        padding: 22,
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
        <div style={{ fontSize: 16, fontWeight: 700, color: "#111827" }}>{title}</div>
        <button onClick={onClose} style={{ border: "none", background: "none", cursor: "pointer", color: "#9ca3af" }}>x</button>
      </div>
      {children}
    </div>
  </div>
);

export default function CockpitTab() {
  const [groups, setGroups] = useState([]);
  const [stores, setStores] = useState([]);
  const [view, setView] = useState("operacoes");
  const [period, setPeriod] = useState("DIARIO");
  const [expanded, setExpanded] = useState(new Set());
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [busyKey, setBusyKey] = useState("");
  const [alertModal, setAlertModal] = useState(null);
  const [groupModal, setGroupModal] = useState(null);
  const [storeModal, setStoreModal] = useState(null);

  const storesByGroup = useMemo(() => {
    const m = new Map();
    for (const s of stores) {
      if (!m.has(s.grupo_id)) m.set(s.grupo_id, []);
      m.get(s.grupo_id).push(s);
    }
    return m;
  }, [stores]);

  const loadData = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const [groupsRes, storesRes] = await Promise.all([fetch("/api/grupos"), fetch("/api/lojas")]);
      if (!groupsRes.ok) throw new Error(await readError(groupsRes));
      if (!storesRes.ok) throw new Error(await readError(storesRes));
      const [groupsRows, storesRows] = await Promise.all([groupsRes.json(), storesRes.json()]);
      setGroups(groupsRows || []);
      setStores(storesRows || []);
      setExpanded((prev) => (prev.size ? prev : new Set((groupsRows || []).map((g) => g.id))));
    } catch (err) {
      setError(String(err?.message || err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);

  useEffect(() => {
    const timer = setInterval(() => loadData(), 9000);
    return () => clearInterval(timer);
  }, [loadData]);

  const runBusy = async (key, fn) => {
    setBusyKey(key);
    setError("");
    try {
      await fn();
      await loadData();
    } catch (err) {
      setError(String(err?.message || err));
    } finally {
      setBusyKey("");
    }
  };

  const runGroup = async (groupId) => {
    await runBusy(`run-group-${groupId}`, async () => {
      const res = await fetch("/api/pipeline/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ grupo_id: groupId, periodo: period }),
      });
      if (!res.ok) throw new Error(await readError(res));
    });
  };

  const runStore = async (storeId) => {
    await runBusy(`run-store-${storeId}`, async () => {
      const res = await fetch("/api/pipeline/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ loja_id: storeId, periodo: period }),
      });
      if (!res.ok) throw new Error(await readError(res));
    });
  };

  const openAlert = async (group) => {
    await runBusy(`alert-${group.id}`, async () => {
      const res = await fetch(`/api/alertas/texto?grupo_id=${group.id}&periodo=${period}`);
      if (!res.ok) throw new Error(await readError(res));
      const payload = await res.json();
      setAlertModal({ group, text: payload?.texto || "" });
    });
  };

  const saveGroup = async (payload, mode, id) => {
    await runBusy("save-group", async () => {
      const type = mode === "independente" ? "independente" : "grupo";
      const url = id ? `/api/grupos/${id}?tipo=${type}` : `/api/grupos?tipo=${type}`;
      const method = id ? "PUT" : "POST";
      const res = await fetch(url, {
        method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) throw new Error(await readError(res));
      setGroupModal(null);
    });
  };

  const saveStore = async (payload, id) => {
    await runBusy("save-store", async () => {
      const url = id ? `/api/lojas/${id}` : "/api/lojas";
      const method = id ? "PUT" : "POST";
      const res = await fetch(url, {
        method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) throw new Error(await readError(res));
      setStoreModal(null);
    });
  };

  return (
    <div>
      <h1 style={{ fontSize: 20, fontWeight: 700, color: "#111827", marginBottom: 4, letterSpacing: "-0.4px" }}>Cockpit</h1>
      <p style={{ fontSize: 13, color: "#6b7280", marginBottom: 16 }}>
        {stores.filter((s) => s.ativo).length} lojas ativas | {groups.length} clientes
      </p>

      <div style={{ display: "flex", gap: 3, background: "#f3f4f6", borderRadius: 9, padding: 3, width: "fit-content", marginBottom: 16 }}>
        {[
          { key: "operacoes", label: "Operacoes" },
          { key: "clientes", label: "Clientes e Lojas" },
        ].map((item) => (
          <button
            key={item.key}
            onClick={() => setView(item.key)}
            style={{
              padding: "6px 14px",
              borderRadius: 7,
              border: "none",
              cursor: "pointer",
              fontSize: 13,
              fontWeight: 600,
              background: view === item.key ? "#fff" : "transparent",
              color: view === item.key ? "#111827" : "#6b7280",
            }}
          >
            {item.label}
          </button>
        ))}
      </div>

      {error && <div style={{ marginBottom: 10, color: "#991b1b", background: "#fff1f2", border: "1px solid #fecaca", borderRadius: 8, padding: "8px 10px", fontSize: 13 }}>{error}</div>}
      {loading && <div style={{ marginBottom: 10, fontSize: 13, color: "#6b7280" }}>Carregando...</div>}

      {view === "operacoes" && (
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14, flexWrap: "wrap" }}>
            <span style={{ fontSize: 12, fontWeight: 600, color: "#6b7280" }}>Periodo:</span>
            {PERIODS.map((p) => (
              <Btn key={p.value} small kind={period === p.value ? "primary" : "default"} onClick={() => setPeriod(p.value)}>
                {p.label}
              </Btn>
            ))}
          </div>
          {groups.map((g) => {
            const list = storesByGroup.get(g.id) || [];
            if (!list.length) return null;
            const isOpen = expanded.has(g.id);
            const pending = Number(g.skus_pendentes ?? 0);
            return (
              <div key={g.id} style={{ background: "#fff", border: "1px solid #e5e7eb", borderRadius: 12, marginBottom: 12, overflow: "hidden" }}>
                <div style={{ display: "flex", gap: 8, alignItems: "center", padding: "12px 14px", borderBottom: isOpen ? "1px solid #f3f4f6" : "none" }}>
                  <button onClick={() => setExpanded((prev) => { const n = new Set(prev); n.has(g.id) ? n.delete(g.id) : n.add(g.id); return n; })} style={{ border: "none", background: "none", cursor: "pointer", color: "#9ca3af" }}>{isOpen ? "▾" : "▸"}</button>
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: 14, fontWeight: 700, color: "#111827" }}>{g.nome}</div>
                    <div style={{ fontSize: 11, color: "#9ca3af" }}>{list.filter((s) => s.ativo).length} lojas ativas</div>
                  </div>
                  {pending > 0 && <Badge bg="#fffbeb" color="#d97706" border="#fde68a">{pending} SKUs pendentes</Badge>}
                  <Btn small onClick={() => runGroup(g.id)} disabled={!g.ativo || busyKey === `run-group-${g.id}`}>Rodar grupo</Btn>
                  <Btn small kind="whatsapp" onClick={() => openAlert(g)} disabled={!g.ativo || busyKey === `alert-${g.id}`}>Alerta</Btn>
                  <Btn small onClick={() => window.open(`/api/alertas/excel?grupo_id=${g.id}&periodo=${period}`, "_blank")} disabled={!g.ativo}>Excel</Btn>
                </div>
                {isOpen && (
                  <table style={{ width: "100%", borderCollapse: "collapse" }}>
                    <thead><tr style={{ background: "#f9fafb" }}>{["Loja", "CNPJ", "Ultimo run", "Status", "Pendentes", ""].map((h) => <th key={h} style={{ textAlign: "left", fontSize: 10, fontWeight: 600, color: "#9ca3af", padding: "8px 12px", textTransform: "uppercase" }}>{h}</th>)}</tr></thead>
                    <tbody>
                      {list.map((s) => (
                        <tr key={s.id} style={{ borderTop: "1px solid #f3f4f6", opacity: s.ativo ? 1 : 0.6 }}>
                          <td style={TD_S}>{s.nome}</td>
                          <td style={{ ...TD_S, fontFamily: "'DM Mono',monospace", fontSize: 11 }}>{fmtCnpj(s.cnpj)}</td>
                          <td style={{ ...TD_S, fontFamily: "'DM Mono',monospace", fontSize: 11 }}>{s.ultimo_run || "-"}</td>
                          <td style={TD_S}><StatusBadge status={s.status_run} /></td>
                          <td style={TD_S}>{Number(s.skus_pendentes || 0)}</td>
                          <td style={TD_S}><Btn small onClick={() => runStore(s.id)} disabled={!s.ativo || busyKey === `run-store-${s.id}`}>Rodar</Btn></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            );
          })}
        </div>
      )}

      {view === "clientes" && (
        <div>
          <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
            <Btn kind="default" onClick={() => setGroupModal({ mode: "independente", data: null })}>+ Cliente independente</Btn>
            <Btn kind="primary" onClick={() => setGroupModal({ mode: "grupo", data: null })}>+ Novo grupo</Btn>
            <Btn kind="primary" onClick={() => setStoreModal({ data: null })}>+ Nova loja</Btn>
          </div>
          <div style={{ background: "#fff", border: "1px solid #e5e7eb", borderRadius: 12, overflow: "hidden", marginBottom: 14 }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead><tr style={{ background: "#f9fafb" }}>{["Nome", "Tipo", "Contato", "WhatsApp", "Lojas", "Status", ""].map((h) => <th key={h} style={TH_S}>{h}</th>)}</tr></thead>
              <tbody>
                {groups.map((g, idx) => (
                  <tr key={g.id} style={{ borderTop: idx ? "1px solid #f3f4f6" : "none" }}>
                    <td style={TD_S}>{g.nome}</td><td style={TD_S}>{g.tipo}</td><td style={TD_S}>{g.contato || "-"}</td><td style={TD_S}>{fmtPhone(g.whatsapp)}</td><td style={TD_S}>{(storesByGroup.get(g.id) || []).length}</td><td style={TD_S}>{g.ativo ? "Ativo" : "Inativo"}</td>
                    <td style={TD_S}><Btn small onClick={() => setGroupModal({ mode: g.tipo === "independente" ? "independente" : "grupo", data: g })}>Editar</Btn></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div style={{ background: "#fff", border: "1px solid #e5e7eb", borderRadius: 12, overflow: "hidden" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead><tr style={{ background: "#f9fafb" }}>{["Loja", "Grupo", "CNPJ", "WhatsApp", "NFStock", "Status", ""].map((h) => <th key={h} style={TH_S}>{h}</th>)}</tr></thead>
              <tbody>
                {stores.map((s, idx) => (
                  <tr key={s.id} style={{ borderTop: idx ? "1px solid #f3f4f6" : "none", opacity: s.ativo ? 1 : 0.6 }}>
                    <td style={TD_S}>{s.nome}</td><td style={TD_S}>{groups.find((g) => g.id === s.grupo_id)?.nome || "-"}</td><td style={{ ...TD_S, fontFamily: "'DM Mono',monospace", fontSize: 11 }}>{fmtCnpj(s.cnpj)}</td><td style={{ ...TD_S, fontFamily: "'DM Mono',monospace", fontSize: 11 }}>{fmtPhone(s.whatsapp)}</td><td style={TD_S}>{s.nfstock_token_configurado ? "********" : "Nao configurado"}</td><td style={TD_S}>{s.ativo ? "Ativa" : "Inativa"}</td>
                    <td style={TD_S}><Btn small onClick={() => setStoreModal({ data: s })}>Editar</Btn></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {alertModal && (
        <Modal title={`Alerta - ${alertModal.group.nome}`} onClose={() => setAlertModal(null)}>
          <div style={{ background: "#dcf8c6", borderRadius: 10, padding: "12px 14px", whiteSpace: "pre-wrap", fontSize: 13, lineHeight: 1.5, marginBottom: 12 }}>
            {alertModal.text}
          </div>
          <div style={{ display: "flex", justifyContent: "flex-end", gap: 8 }}>
            <Btn onClick={() => setAlertModal(null)}>Fechar</Btn>
            <Btn kind="whatsapp" onClick={() => navigator.clipboard.writeText(alertModal.text || "")}>Copiar texto</Btn>
          </div>
        </Modal>
      )}

      {groupModal && (
        <Modal title={groupModal.data?.id ? "Editar grupo" : (groupModal.mode === "independente" ? "Novo independente" : "Novo grupo")} onClose={() => setGroupModal(null)}>
          <GroupForm
            mode={groupModal.mode}
            initial={groupModal.data}
            loading={busyKey === "save-group"}
            onSave={(payload) => saveGroup(payload, groupModal.mode, groupModal.data?.id)}
          />
        </Modal>
      )}

      {storeModal && (
        <Modal title={storeModal.data?.id ? "Editar loja" : "Nova loja"} onClose={() => setStoreModal(null)}>
          <StoreForm
            groups={groups}
            initial={storeModal.data}
            loading={busyKey === "save-store"}
            onSave={(payload) => saveStore(payload, storeModal.data?.id)}
          />
        </Modal>
      )}
    </div>
  );
}

function GroupForm({ mode, initial, loading, onSave }) {
  const [form, setForm] = useState(initial || { nome: "", contato: "", whatsapp: "", ativo: true });
  return (
    <div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        <div style={{ gridColumn: "1 / -1" }}><Input placeholder="Nome" value={form.nome} onChange={(e) => setForm((p) => ({ ...p, nome: e.target.value }))} /></div>
        <div><Input placeholder="Contato" value={form.contato || ""} onChange={(e) => setForm((p) => ({ ...p, contato: e.target.value }))} /></div>
        <div><Input placeholder="WhatsApp" value={form.whatsapp || ""} onChange={(e) => setForm((p) => ({ ...p, whatsapp: e.target.value.replace(/\D/g, "") }))} /></div>
      </div>
      <label style={{ display: "inline-flex", gap: 8, alignItems: "center", marginTop: 10 }}>
        <input type="checkbox" checked={!!form.ativo} onChange={(e) => setForm((p) => ({ ...p, ativo: e.target.checked }))} />
        <span style={{ fontSize: 13, color: "#374151" }}>{mode === "independente" ? "Cliente independente ativo" : "Grupo ativo"}</span>
      </label>
      <div style={{ display: "flex", justifyContent: "flex-end", marginTop: 14 }}>
        <Btn kind="primary" onClick={() => onSave(form)} disabled={loading}>{loading ? "Salvando..." : "Salvar"}</Btn>
      </div>
    </div>
  );
}

function StoreForm({ groups, initial, loading, onSave }) {
  const [form, setForm] = useState(initial || { grupo_id: "", nome: "", cnpj: "", whatsapp: "", nfstock_token: "", ativo: true });
  return (
    <div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        <div style={{ gridColumn: "1 / -1" }}>
          <select value={form.grupo_id} onChange={(e) => setForm((p) => ({ ...p, grupo_id: Number(e.target.value) }))} style={SELECT_STYLE}>
            <option value="">- Grupo -</option>
            {groups.map((g) => <option key={g.id} value={g.id}>{g.nome}</option>)}
          </select>
        </div>
        <div><Input placeholder="Nome da loja" value={form.nome} onChange={(e) => setForm((p) => ({ ...p, nome: e.target.value }))} /></div>
        <div><Input placeholder="CNPJ" value={form.cnpj || ""} onChange={(e) => setForm((p) => ({ ...p, cnpj: e.target.value.replace(/\D/g, "") }))} /></div>
        <div><Input placeholder="WhatsApp" value={form.whatsapp || ""} onChange={(e) => setForm((p) => ({ ...p, whatsapp: e.target.value.replace(/\D/g, "") }))} /></div>
        <div><Input type="password" placeholder={initial?.nfstock_token_configurado ? "********" : "Token NFStock"} value={form.nfstock_token || ""} onChange={(e) => setForm((p) => ({ ...p, nfstock_token: e.target.value }))} /></div>
      </div>
      <label style={{ display: "inline-flex", gap: 8, alignItems: "center", marginTop: 10 }}>
        <input type="checkbox" checked={!!form.ativo} onChange={(e) => setForm((p) => ({ ...p, ativo: e.target.checked }))} />
        <span style={{ fontSize: 13, color: "#374151" }}>Loja ativa</span>
      </label>
      <div style={{ display: "flex", justifyContent: "flex-end", marginTop: 14 }}>
        <Btn kind="primary" onClick={() => onSave(form)} disabled={loading}>{loading ? "Salvando..." : "Salvar"}</Btn>
      </div>
    </div>
  );
}
