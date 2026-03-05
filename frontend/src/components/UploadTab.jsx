import { useEffect, useMemo, useRef, useState } from "react";

const ACCEPTED_EXTENSIONS = new Set(["xml", "zip", "rar"]);

const CARD_STYLE = {
  background: "#fff",
  border: "1px solid #e5e7eb",
  borderRadius: 12,
};

const TH_STYLE = {
  textAlign: "left",
  fontSize: 10,
  fontWeight: 600,
  color: "#9ca3af",
  padding: "8px 12px",
  textTransform: "uppercase",
  letterSpacing: "0.08em",
};

const TD_STYLE = {
  padding: "10px 12px",
  fontSize: 13,
  color: "#111827",
  verticalAlign: "top",
};

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

const Btn = ({ children, onClick, disabled = false, kind = "default", small = false }) => {
  const map = {
    default: { bg: "#f9fafb", color: "#374151", border: "#e5e7eb" },
    primary: { bg: "#1d4ed8", color: "#fff", border: "#1d4ed8" },
    success: { bg: "#166534", color: "#fff", border: "#166534" },
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

const StatusPill = ({ status }) => {
  const map = {
    aguardando: { bg: "#f9fafb", color: "#6b7280", border: "#e5e7eb", label: "Aguardando" },
    sem_loja: { bg: "#fffbeb", color: "#d97706", border: "#fde68a", label: "Definir loja" },
    enviando: { bg: "#eff6ff", color: "#1d4ed8", border: "#dbeafe", label: "Enviando" },
    queued: { bg: "#eff6ff", color: "#1d4ed8", border: "#dbeafe", label: "Na fila" },
    extraindo: { bg: "#eff6ff", color: "#1d4ed8", border: "#dbeafe", label: "Extraindo" },
    processando: { bg: "#eff6ff", color: "#1d4ed8", border: "#dbeafe", label: "Processando" },
    concluido: { bg: "#f0fdf4", color: "#166534", border: "#bbf7d0", label: "Concluido" },
    erro: { bg: "#fff1f2", color: "#9f1239", border: "#fecaca", label: "Erro" },
  };
  const c = map[status] || map.aguardando;
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        padding: "2px 8px",
        borderRadius: 99,
        fontSize: 11,
        fontWeight: 600,
        background: c.bg,
        color: c.color,
        border: `1px solid ${c.border}`,
      }}
    >
      {c.label}
    </span>
  );
};

const TypePill = ({ ext }) => {
  const key = String(ext || "").toLowerCase();
  const map = {
    xml: { bg: "#eff6ff", color: "#1d4ed8", border: "#bfdbfe", label: "XML" },
    zip: { bg: "#fff7ed", color: "#9a3412", border: "#fed7aa", label: "ZIP" },
    rar: { bg: "#fdf4ff", color: "#7e22ce", border: "#e9d5ff", label: "RAR" },
  };
  const c = map[key] || { bg: "#f3f4f6", color: "#374151", border: "#e5e7eb", label: "?" };
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        padding: "2px 7px",
        borderRadius: 6,
        fontSize: 10,
        fontWeight: 700,
        background: c.bg,
        color: c.color,
        border: `1px solid ${c.border}`,
        fontFamily: "'DM Mono',monospace",
      }}
    >
      {c.label}
    </span>
  );
};

const fmtSize = (bytes) => {
  const n = Number(bytes || 0);
  if (!n) return "0 B";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
};

const fmtCurrency = (value) => {
  const num = Number(value || 0);
  return num.toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
};

const extOf = (name) => {
  const parts = String(name || "").split(".");
  return parts.length > 1 ? parts.pop().toLowerCase() : "";
};

const isProcessingStatus = (status) =>
  ["enviando", "queued", "extraindo", "processando"].includes(status);

const isPendingStatus = (status) => ["aguardando"].includes(status);

const ResultModal = ({ row, onClose }) => {
  const result = row?.result || { total: 0, ok: 0, erros: 0, novos_skus: 0, nfes: [] };
  const nfes = Array.isArray(result.nfes) ? result.nfes : [];
  return (
    <div
      onClick={onClose}
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(0,0,0,0.4)",
        zIndex: 1000,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          width: 920,
          maxWidth: "calc(100vw - 24px)",
          maxHeight: "88vh",
          overflow: "auto",
          background: "#fff",
          borderRadius: 14,
          boxShadow: "0 20px 60px rgba(0,0,0,0.2)",
          padding: 18,
        }}
      >
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
          <div style={{ fontSize: 16, fontWeight: 700, color: "#111827" }}>
            Resultado do upload
          </div>
          <button onClick={onClose} style={{ border: "none", background: "none", cursor: "pointer", color: "#9ca3af" }}>
            x
          </button>
        </div>
        <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 12 }}>
          {row.name} | {row.storeName || "Sem loja"}
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(4,minmax(0,1fr))", gap: 8, marginBottom: 12 }}>
          <div style={{ border: "1px solid #e5e7eb", borderRadius: 8, padding: "8px 10px" }}>
            <div style={{ fontSize: 11, color: "#6b7280" }}>Total</div>
            <div style={{ fontSize: 20, fontWeight: 700, color: "#111827", lineHeight: 1.1 }}>{Number(result.total || 0)}</div>
          </div>
          <div style={{ border: "1px solid #bbf7d0", background: "#f0fdf4", borderRadius: 8, padding: "8px 10px" }}>
            <div style={{ fontSize: 11, color: "#166534" }}>OK</div>
            <div style={{ fontSize: 20, fontWeight: 700, color: "#166534", lineHeight: 1.1 }}>{Number(result.ok || 0)}</div>
          </div>
          <div style={{ border: "1px solid #fecaca", background: "#fff1f2", borderRadius: 8, padding: "8px 10px" }}>
            <div style={{ fontSize: 11, color: "#9f1239" }}>Erros</div>
            <div style={{ fontSize: 20, fontWeight: 700, color: "#9f1239", lineHeight: 1.1 }}>{Number(result.erros || 0)}</div>
          </div>
          <div style={{ border: "1px solid #fde68a", background: "#fffbeb", borderRadius: 8, padding: "8px 10px" }}>
            <div style={{ fontSize: 11, color: "#d97706" }}>Novos SKUs</div>
            <div style={{ fontSize: 20, fontWeight: 700, color: "#d97706", lineHeight: 1.1 }}>{Number(result.novos_skus || 0)}</div>
          </div>
        </div>
        <div style={{ ...CARD_STYLE, overflow: "hidden" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ background: "#f9fafb" }}>
                {["Arquivo", "Fornecedor", "CNPJ Emit", "CNPJ Dest", "Loja", "Data", "Valor", "Erro"].map((h) => (
                  <th key={h} style={TH_STYLE}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {nfes.map((item, idx) => (
                <tr key={`${row.id}-${idx}`} style={{ borderTop: "1px solid #f3f4f6", background: item.erro ? "#fff9f9" : "#fff" }}>
                  <td style={{ ...TD_STYLE, fontFamily: "'DM Mono',monospace", fontSize: 11 }}>{item.arquivo || "-"}</td>
                  <td style={TD_STYLE}>{item.fornecedor || "-"}</td>
                  <td style={{ ...TD_STYLE, fontFamily: "'DM Mono',monospace", fontSize: 11 }}>{item.cnpj_emit || "-"}</td>
                  <td style={{ ...TD_STYLE, fontFamily: "'DM Mono',monospace", fontSize: 11 }}>{item.cnpj_dest || "-"}</td>
                  <td style={TD_STYLE}>{item.loja_name || "-"}</td>
                  <td style={{ ...TD_STYLE, fontFamily: "'DM Mono',monospace", fontSize: 11 }}>{item.data || "-"}</td>
                  <td style={{ ...TD_STYLE, fontFamily: "'DM Mono',monospace", fontSize: 11 }}>{fmtCurrency(item.valor || 0)}</td>
                  <td style={{ ...TD_STYLE, color: item.erro ? "#991b1b" : "#9ca3af" }}>{item.erro || "-"}</td>
                </tr>
              ))}
              {nfes.length === 0 && (
                <tr>
                  <td colSpan={8} style={{ ...TD_STYLE, color: "#9ca3af" }}>Sem itens no resultado.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default function UploadTab() {
  const [groups, setGroups] = useState([]);
  const [stores, setStores] = useState([]);
  const [rows, setRows] = useState([]);
  const [dragging, setDragging] = useState(false);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [runningAll, setRunningAll] = useState(false);
  const [resultRow, setResultRow] = useState(null);
  const inputRef = useRef(null);
  const pollsRef = useRef(new Map());

  useEffect(() => {
    return () => {
      for (const timer of pollsRef.current.values()) {
        clearInterval(timer);
      }
      pollsRef.current.clear();
    };
  }, []);

  const loadStores = async () => {
    setLoading(true);
    setError("");
    try {
      const [groupsRes, storesRes] = await Promise.all([fetch("/api/grupos"), fetch("/api/lojas")]);
      if (!groupsRes.ok || !storesRes.ok) {
        throw new Error("Falha ao carregar grupos/lojas.");
      }
      const [groupsRows, storesRows] = await Promise.all([groupsRes.json(), storesRes.json()]);
      setGroups(Array.isArray(groupsRows) ? groupsRows : []);
      setStores(Array.isArray(storesRows) ? storesRows : []);
    } catch (err) {
      setError(String(err?.message || err));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadStores();
  }, []);

  const groupedStores = useMemo(() => {
    const groupsById = new Map(groups.map((g) => [Number(g.id), g]));
    const activeStores = stores.filter((s) => s.ativo);
    const map = new Map();
    for (const store of activeStores) {
      const gid = Number(store.grupo_id);
      if (!map.has(gid)) {
        map.set(gid, {
          group: groupsById.get(gid) || { id: gid, nome: `Grupo ${gid}` },
          stores: [],
        });
      }
      map.get(gid).stores.push(store);
    }
    return Array.from(map.values());
  }, [groups, stores]);

  const updateRow = (rowId, patch) => {
    setRows((prev) => prev.map((row) => (row.id === rowId ? { ...row, ...patch } : row)));
  };

  const addFiles = (fileList) => {
    const accepted = Array.from(fileList || []).filter((f) => ACCEPTED_EXTENSIONS.has(extOf(f.name)));
    const rejected = Array.from(fileList || []).length - accepted.length;

    if (rejected > 0) {
      setError(`Alguns arquivos foram ignorados. Permitido: .xml, .zip, .rar`);
    } else {
      setError("");
    }
    if (!accepted.length) {
      return;
    }

    const now = Date.now();
    const mapped = accepted.map((file, idx) => ({
      id: `${now}-${idx}-${Math.random().toString(36).slice(2, 9)}`,
      file,
      name: file.name,
      ext: extOf(file.name),
      size: file.size,
      lojaId: "",
      storeName: "",
      status: "aguardando",
      progress: 0,
      jobId: "",
      result: null,
      error: "",
    }));
    setRows((prev) => [...prev, ...mapped]);
  };

  const onDrop = (event) => {
    event.preventDefault();
    setDragging(false);
    addFiles(event.dataTransfer.files);
  };

  const onPick = (event) => {
    addFiles(event.target.files);
    if (event.target) {
      event.target.value = "";
    }
  };

  const setStore = (rowId, lojaId) => {
    const selected = stores.find((store) => Number(store.id) === Number(lojaId));
    updateRow(rowId, {
      lojaId: lojaId ? String(lojaId) : "",
      storeName: selected?.nome || "",
      status: "aguardando",
      error: "",
    });
  };

  const clearPoll = (rowId) => {
    const timer = pollsRef.current.get(rowId);
    if (timer) {
      clearInterval(timer);
      pollsRef.current.delete(rowId);
    }
  };

  const pollJob = (rowId, jobId) => {
    clearPoll(rowId);
    const timer = setInterval(async () => {
      try {
        const res = await fetch(`/api/upload/status/${jobId}`);
        if (!res.ok) {
          throw new Error(`Falha ao consultar status (${res.status})`);
        }
        const payload = await res.json();
        const status = String(payload.status || "queued");
        const progress = Number(payload.progresso ?? payload.progress ?? 0);
        updateRow(rowId, {
          status,
          progress,
          result: payload.result || null,
          error: payload.error || "",
          storeName: String(payload.loja_nome || ""),
        });
        if (status === "concluido" || status === "erro") {
          clearPoll(rowId);
        }
      } catch (err) {
        clearPoll(rowId);
        updateRow(rowId, {
          status: "erro",
          progress: 100,
          error: String(err?.message || err),
        });
      }
    }, 1200);
    pollsRef.current.set(rowId, timer);
  };

  const processOne = async (rowId) => {
    const row = rows.find((item) => item.id === rowId);
    if (!row) {
      return;
    }
    if (isProcessingStatus(row.status)) {
      return;
    }
    updateRow(rowId, { status: "enviando", progress: 6, error: "" });

    try {
      const form = new FormData();
      if (row.lojaId) {
        form.append("loja_id", String(row.lojaId));
      }
      form.append("files", row.file, row.file.name);

      const res = await fetch("/api/upload", { method: "POST", body: form });
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(String(payload?.detail || `Falha no upload (${res.status})`));
      }

      const jobId = String(payload.job_id || "").trim();
      if (!jobId) {
        throw new Error("Backend nao retornou job_id.");
      }

      updateRow(rowId, {
        status: String(payload.status || "queued"),
        progress: Number(payload.progresso ?? 8),
        jobId,
      });
      pollJob(rowId, jobId);
    } catch (err) {
      updateRow(rowId, {
        status: "erro",
        progress: 100,
        error: String(err?.message || err),
      });
    }
  };

  const processAll = async () => {
    const pendingRows = rows.filter((row) => isPendingStatus(row.status) || row.status === "erro");
    if (!pendingRows.length) {
      return;
    }
    setRunningAll(true);
    try {
      for (const row of pendingRows) {
        await processOne(row.id);
      }
    } finally {
      setRunningAll(false);
    }
  };

  const removeRow = (rowId) => {
    clearPoll(rowId);
    setRows((prev) => prev.filter((row) => row.id !== rowId));
  };

  const pendingCount = rows.filter((row) => isPendingStatus(row.status)).length;
  const processingCount = rows.filter((row) => isProcessingStatus(row.status)).length;
  const doneCount = rows.filter((row) => row.status === "concluido").length;
  const errorCount = rows.filter((row) => row.status === "erro").length;

  return (
    <div>
      <h1 style={{ fontSize: 20, fontWeight: 700, color: "#111827", marginBottom: 4, letterSpacing: "-0.4px" }}>
        Upload de XMLs
      </h1>
      <p style={{ fontSize: 13, color: "#6b7280", marginBottom: 16 }}>
        Envie XML, ZIP ou RAR. O sistema vincula automaticamente pela tag CNPJ destinatario do XML.
      </p>

      <div
        onDragOver={(e) => {
          e.preventDefault();
          setDragging(true);
        }}
        onDragLeave={() => setDragging(false)}
        onDrop={onDrop}
        onClick={() => inputRef.current?.click()}
        style={{
          ...CARD_STYLE,
          marginBottom: 14,
          border: dragging ? "1px solid #60a5fa" : "1px dashed #cbd5e1",
          background: dragging ? "#eff6ff" : "#fff",
          padding: "20px 18px",
          cursor: "pointer",
        }}
      >
        <input
          ref={inputRef}
          type="file"
          multiple
          accept=".xml,.zip,.rar"
          style={{ display: "none" }}
          onChange={onPick}
        />
        <div style={{ fontSize: 14, fontWeight: 600, color: "#111827", marginBottom: 4 }}>
          Arraste arquivos aqui ou clique para selecionar
        </div>
        <div style={{ fontSize: 12, color: "#6b7280" }}>
          Formatos aceitos: .xml, .zip, .rar
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(4,minmax(0,1fr))", gap: 8, marginBottom: 12 }}>
        <div style={{ ...CARD_STYLE, padding: "9px 12px" }}>
          <div style={{ fontSize: 11, color: "#6b7280" }}>Pendentes</div>
          <div style={{ fontSize: 20, fontWeight: 700, color: "#d97706", lineHeight: 1.1 }}>{pendingCount}</div>
        </div>
        <div style={{ ...CARD_STYLE, padding: "9px 12px" }}>
          <div style={{ fontSize: 11, color: "#6b7280" }}>Processando</div>
          <div style={{ fontSize: 20, fontWeight: 700, color: "#1d4ed8", lineHeight: 1.1 }}>{processingCount}</div>
        </div>
        <div style={{ ...CARD_STYLE, padding: "9px 12px" }}>
          <div style={{ fontSize: 11, color: "#6b7280" }}>Concluidos</div>
          <div style={{ fontSize: 20, fontWeight: 700, color: "#166534", lineHeight: 1.1 }}>{doneCount}</div>
        </div>
        <div style={{ ...CARD_STYLE, padding: "9px 12px" }}>
          <div style={{ fontSize: 11, color: "#6b7280" }}>Erros</div>
          <div style={{ fontSize: 20, fontWeight: 700, color: "#9f1239", lineHeight: 1.1 }}>{errorCount}</div>
        </div>
      </div>

      <div style={{ display: "flex", gap: 8, marginBottom: 12, flexWrap: "wrap" }}>
        <Btn kind="primary" onClick={processAll} disabled={runningAll || !rows.length}>
          {runningAll ? "Enviando..." : "Processar todos"}
        </Btn>
        <Btn
          onClick={() => {
            for (const key of pollsRef.current.keys()) clearPoll(key);
            setRows([]);
          }}
          disabled={!rows.length}
        >
          Limpar fila
        </Btn>
        <Btn onClick={loadStores} disabled={loading}>
          {loading ? "Atualizando..." : "Atualizar lojas"}
        </Btn>
      </div>

      {error && (
        <div style={{ marginBottom: 10, color: "#991b1b", background: "#fff1f2", border: "1px solid #fecaca", borderRadius: 8, padding: "8px 10px", fontSize: 13 }}>
          {error}
        </div>
      )}

      <div style={{ ...CARD_STYLE, overflow: "hidden" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ background: "#f9fafb" }}>
              {["Arquivo", "Tipo", "Tamanho", "Loja (opcional)", "Status", "Progresso", "Acoes"].map((h) => (
                <th key={h} style={TH_STYLE}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, idx) => (
              <tr key={row.id} style={{ borderTop: idx ? "1px solid #f3f4f6" : "none" }}>
                <td style={TD_STYLE}>
                  <div style={{ fontWeight: 600, color: "#111827" }}>{row.name}</div>
                  {row.error && <div style={{ fontSize: 11, color: "#9f1239", marginTop: 2 }}>{row.error}</div>}
                </td>
                <td style={TD_STYLE}><TypePill ext={row.ext} /></td>
                <td style={{ ...TD_STYLE, fontFamily: "'DM Mono',monospace", fontSize: 11, color: "#6b7280" }}>{fmtSize(row.size)}</td>
                <td style={{ ...TD_STYLE, minWidth: 260 }}>
                  <select
                    value={row.lojaId}
                    onChange={(e) => setStore(row.id, e.target.value)}
                    style={SELECT_STYLE}
                    disabled={isProcessingStatus(row.status)}
                  >
                    <option value="">Auto por CNPJ dest</option>
                    {groupedStores.map((entry) => (
                      <optgroup key={entry.group.id} label={entry.group.nome}>
                        {entry.stores.map((store) => (
                          <option key={store.id} value={store.id}>
                            {store.nome}
                          </option>
                        ))}
                      </optgroup>
                    ))}
                  </select>
                </td>
                <td style={TD_STYLE}><StatusPill status={row.status} /></td>
                <td style={{ ...TD_STYLE, minWidth: 180 }}>
                  <div style={{ width: "100%", height: 8, borderRadius: 999, background: "#f1f5f9", overflow: "hidden", border: "1px solid #e2e8f0" }}>
                    <div style={{ width: `${Math.max(0, Math.min(100, Number(row.progress || 0)))}%`, height: "100%", background: row.status === "erro" ? "#f43f5e" : "#3b82f6", transition: "width 0.25s ease" }} />
                  </div>
                  <div style={{ marginTop: 4, fontSize: 11, color: "#6b7280" }}>{Number(row.progress || 0)}%</div>
                </td>
                <td style={TD_STYLE}>
                  <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                    <Btn small kind="success" onClick={() => processOne(row.id)} disabled={isProcessingStatus(row.status)}>
                      Processar
                    </Btn>
                    <Btn small onClick={() => setResultRow(row)} disabled={!row.result}>Resultado</Btn>
                    <Btn small kind="danger" onClick={() => removeRow(row.id)} disabled={isProcessingStatus(row.status)}>
                      Remover
                    </Btn>
                  </div>
                </td>
              </tr>
            ))}
            {rows.length === 0 && (
              <tr>
                <td colSpan={7} style={{ ...TD_STYLE, color: "#9ca3af" }}>
                  Nenhum arquivo na fila.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {resultRow && <ResultModal row={resultRow} onClose={() => setResultRow(null)} />}
    </div>
  );
}
