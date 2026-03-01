const BM_COLORS = {
  KG: { bg: "#eff6ff", color: "#1d4ed8", border: "#bfdbfe" },
  UN: { bg: "#f5f3ff", color: "#6d28d9", border: "#ddd6fe" },
  L: { bg: "#ecfdf5", color: "#065f46", border: "#a7f3d0" },
  CX: { bg: "#fff7ed", color: "#9a3412", border: "#fed7aa" },
  SACO: { bg: "#fdf4ff", color: "#7e22ce", border: "#e9d5ff" },
};

export default function BMPill({ bm }) {
  const key = String(bm || "").toUpperCase();
  const c = BM_COLORS[key] || {
    bg: "#f3f4f6",
    color: "#374151",
    border: "#e5e7eb",
  };
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        padding: "2px 8px",
        borderRadius: 99,
        fontSize: 11,
        fontWeight: 700,
        background: c.bg,
        color: c.color,
        border: "1px solid " + c.border,
        fontFamily: "'DM Mono', monospace",
      }}
    >
      {key || "-"}
    </span>
  );
}
