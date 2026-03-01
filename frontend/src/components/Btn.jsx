export default function Btn({ children, onClick, variant = "default", small = false, disabled = false }) {
  const styles = {
    approve: { bg: "#f0fdf4", color: "#166534", border: "#bbf7d0" },
    ignore: { bg: "#f9fafb", color: "#6b7280", border: "#e5e7eb" },
    edit: { bg: "#f8faff", color: "#1d4ed8", border: "#dbeafe" },
    delete: { bg: "#fff1f2", color: "#9f1239", border: "#fecaca" },
    save: { bg: "#1d4ed8", color: "#fff", border: "#1d4ed8" },
    default: { bg: "#f9fafb", color: "#374151", border: "#e5e7eb" },
  };
  const c = styles[variant] || styles.default;
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 4,
        padding: small ? "4px 9px" : "6px 14px",
        borderRadius: 6,
        fontSize: 12,
        fontWeight: 600,
        cursor: disabled ? "not-allowed" : "pointer",
        opacity: disabled ? 0.55 : 1,
        background: c.bg,
        color: c.color,
        border: "1px solid " + c.border,
      }}
    >
      {children}
    </button>
  );
}
