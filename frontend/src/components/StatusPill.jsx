const STATUS_MAP = {
  PENDING: { bg: "#fffbeb", color: "#92400e", border: "#fde68a", label: "Pendente" },
  APPROVED: { bg: "#f0fdf4", color: "#166534", border: "#bbf7d0", label: "Aprovado" },
  IGNORED: { bg: "#f9fafb", color: "#6b7280", border: "#e5e7eb", label: "Ignorado" },
  ACTIVE: { bg: "#f0fdf4", color: "#166534", border: "#bbf7d0", label: "Ativo" },
  IGNORE: { bg: "#f9fafb", color: "#6b7280", border: "#e5e7eb", label: "Ignorado" },
};

export default function StatusPill({ status }) {
  const key = String(status || "").toUpperCase();
  const c = STATUS_MAP[key] || STATUS_MAP.PENDING;
  return (
    <span
      style={{
        display: "inline-flex",
        padding: "3px 9px",
        borderRadius: 99,
        fontSize: 11,
        fontWeight: 600,
        background: c.bg,
        color: c.color,
        border: "1px solid " + c.border,
      }}
    >
      {c.label}
    </span>
  );
}
