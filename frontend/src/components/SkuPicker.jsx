import { useEffect, useRef, useState } from "react";
import BMPill from "./BMPill";

const Icon = ({ d, size = 16 }) => (
  <svg
    width={size}
    height={size}
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <path d={d} />
  </svg>
);

const SearchIcon = (p) => <Icon {...p} d="M21 21l-4.35-4.35M17 11A6 6 0 1 1 5 11a6 6 0 0 1 12 0z" />;
const CheckIcon = (p) => <Icon {...p} d="M20 6L9 17l-5-5" />;
const ClearIcon = (p) => <Icon {...p} d="M18 6L6 18M6 6l12 12" />;

const CAT_COLORS = {
  "Proteina Bovina": { bg: "#fff7ed", color: "#9a3412" },
  "Proteina Suina": { bg: "#fdf4ff", color: "#7e22ce" },
  Hortifruti: { bg: "#f0fdf4", color: "#166534" },
  Congelados: { bg: "#eff6ff", color: "#1e40af" },
  Bebidas: { bg: "#ecfeff", color: "#155e75" },
  Molhos: { bg: "#fff1f2", color: "#9f1239" },
  "Frutos do Mar": { bg: "#f0f9ff", color: "#0369a1" },
};

export default function SkuPicker({
  value,
  onChange,
  options,
  placeholder = "Buscar SKU...",
}) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const wrapRef = useRef(null);

  const selected = options.find((s) => s.sku_id === value);

  useEffect(() => {
    const handler = (e) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const filtered = options
    .filter((s) => {
      if (!query) {
        return true;
      }
      const q = query.toLowerCase();
      return (
        String(s.sku_name_canonical || "").toLowerCase().includes(q) ||
        String(s.sku_id || "").toLowerCase().includes(q) ||
        String(s.category || "").toLowerCase().includes(q)
      );
    })
    .slice(0, 12);

  const handleSelect = (sku) => {
    onChange(sku.sku_id);
    setQuery("");
    setOpen(false);
  };

  const handleClear = (e) => {
    e.stopPropagation();
    onChange("");
    setQuery("");
  };

  return (
    <div ref={wrapRef} style={{ position: "relative", width: "100%" }}>
      <div
        onClick={() => setOpen(true)}
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          border: "1px solid " + (open ? "#2563eb" : "#e5e7eb"),
          borderRadius: 8,
          padding: "7px 10px",
          background: "#fff",
          cursor: "text",
          boxShadow: open ? "0 0 0 3px rgba(37,99,235,0.1)" : "none",
          transition: "all 0.15s",
        }}
      >
        <span style={{ color: "#9ca3af", flexShrink: 0 }}>
          <SearchIcon size={14} />
        </span>
        {selected && !open ? (
          <div style={{ flex: 1, display: "flex", alignItems: "center", gap: 8, minWidth: 0 }}>
            <span
              style={{
                fontFamily: "'DM Mono', monospace",
                fontSize: 11,
                color: "#2563eb",
                fontWeight: 700,
                flexShrink: 0,
              }}
            >
              {selected.sku_id}
            </span>
            <span
              style={{
                fontSize: 13,
                color: "#111827",
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
              }}
            >
              {selected.sku_name_canonical}
            </span>
            <BMPill bm={selected.base_measure} />
          </div>
        ) : (
          <input
            autoFocus={open}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onFocus={() => setOpen(true)}
            placeholder={selected ? selected.sku_name_canonical : placeholder}
            style={{
              flex: 1,
              border: "none",
              outline: "none",
              fontSize: 13,
              color: "#111827",
              background: "transparent",
              minWidth: 0,
            }}
          />
        )}
        {value ? (
          <button
            onClick={handleClear}
            style={{
              background: "none",
              border: "none",
              cursor: "pointer",
              color: "#9ca3af",
              padding: 0,
              display: "flex",
              flexShrink: 0,
            }}
          >
            <ClearIcon size={13} />
          </button>
        ) : null}
      </div>

      {open ? (
        <div
          style={{
            position: "absolute",
            top: "calc(100% + 4px)",
            left: 0,
            right: 0,
            zIndex: 999,
            background: "#fff",
            border: "1px solid #e5e7eb",
            borderRadius: 10,
            boxShadow: "0 8px 30px rgba(0,0,0,0.12)",
            overflow: "hidden",
            maxHeight: 320,
            overflowY: "auto",
          }}
        >
          {filtered.length === 0 ? (
            <div style={{ padding: "16px 14px", fontSize: 13, color: "#9ca3af", textAlign: "center" }}>
              Nenhum SKU encontrado
            </div>
          ) : (
            filtered.map((sku) => {
              const isSelected = sku.sku_id === value;
              const catStyle = CAT_COLORS[sku.category] || { bg: "#f9fafb", color: "#374151" };
              return (
                <div
                  key={sku.sku_id}
                  onClick={() => handleSelect(sku)}
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: 10,
                    padding: "10px 14px",
                    cursor: "pointer",
                    borderBottom: "1px solid #f9fafb",
                    background: isSelected ? "#eff6ff" : "#fff",
                  }}
                >
                  <span
                    style={{
                      fontFamily: "'DM Mono', monospace",
                      fontSize: 11,
                      color: "#2563eb",
                      fontWeight: 700,
                      flexShrink: 0,
                      minWidth: 88,
                    }}
                  >
                    {sku.sku_id}
                  </span>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div
                      style={{
                        fontSize: 13,
                        fontWeight: 600,
                        color: "#111827",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {sku.sku_name_canonical}
                    </div>
                    {sku.brand ? <div style={{ fontSize: 11, color: "#9ca3af" }}>{sku.brand}</div> : null}
                  </div>
                  <span
                    style={{
                      display: "inline-flex",
                      padding: "2px 7px",
                      borderRadius: 99,
                      fontSize: 10,
                      fontWeight: 600,
                      background: catStyle.bg,
                      color: catStyle.color,
                      flexShrink: 0,
                    }}
                  >
                    {sku.category || "-"}
                  </span>
                  <BMPill bm={sku.base_measure} />
                  {isSelected ? (
                    <span style={{ color: "#2563eb", flexShrink: 0 }}>
                      <CheckIcon size={13} />
                    </span>
                  ) : null}
                </div>
              );
            })
          )}
        </div>
      ) : null}
    </div>
  );
}
