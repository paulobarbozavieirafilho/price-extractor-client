import { Fragment, useCallback, useState, useRef, useEffect, useMemo } from "react";

const REVIEW_DATA = [
  { id: 1, fingerprint: "10915021000190|7894904006458|BACON DEFUMADO SEARA|KG",
    xProd_norm: "BACON DEFUMADO SEARA", uCom: "KG", qCom: 13.07, vUnCom: 26.50,
    suggested_sku_id: "SKU_000039", suggested_sku_name: "BACON MANTA",
    suggested_sku_name_canonical: "BACON MANTA", suggested_base_measure: "KG",
    suggested_base_qty_per_purchase_unit: "1",
    status: "PENDING", conversion_issue: "", loja: "LEBLON", date: "2026-02-24" },
  { id: 2, fingerprint: "38345921000169||COXAO MOLE RESF FRIGOMARCA CX 26KG|CX",
    xProd_norm: "COXAO MOLE RESF FRIGOMARCA CX 26KG", uCom: "CX", qCom: 1, vUnCom: 462.00,
    suggested_sku_id: "SKU_000072", suggested_sku_name: "COXAO MOLE",
    suggested_sku_name_canonical: "COXAO MOLE", suggested_base_measure: "KG",
    suggested_base_qty_per_purchase_unit: "26",
    status: "PENDING", conversion_issue: "", loja: "MEIER", date: "2026-02-23" },
  { id: 3, fingerprint: "57531348000122||BATATA ASTERIX SC 25K|SACO",
    xProd_norm: "BATATA ASTERIX SC 25K", uCom: "SACO", qCom: 2, vUnCom: 87.50,
    suggested_sku_id: "SKU_000040", suggested_sku_name: "BATATA ASTERIX",
    suggested_sku_name_canonical: "BATATA ASTERIX", suggested_base_measure: "KG",
    suggested_base_qty_per_purchase_unit: "25",
    status: "PENDING", conversion_issue: "un_pack_count_unknown", loja: "JB", date: "2026-02-23" },
  { id: 4, fingerprint: "33118584000153||FILE MIGNON S C 4 5 GRILL|KG",
    xProd_norm: "FILE MIGNON S C 4 5 GRILL", uCom: "KG", qCom: 9.5, vUnCom: 89.00,
    suggested_sku_id: "SKU_000056", suggested_sku_name: "FILE MIGNON",
    suggested_sku_name_canonical: "FILE MIGNON", suggested_base_measure: "KG",
    suggested_base_qty_per_purchase_unit: "1",
    status: "PENDING", conversion_issue: "", loja: "BURGER", date: "2026-02-22" },
  { id: 5, fingerprint: "21752699000116|7898994771012|LICOR DON LUIZ LECHE CREAM 750ML|UN",
    xProd_norm: "LICOR DON LUIZ LECHE CREAM 750ML", uCom: "UN", qCom: 6, vUnCom: 38.00,
    suggested_sku_id: "", suggested_sku_name: "", suggested_sku_name_canonical: "",
    suggested_base_measure: "UN", suggested_base_qty_per_purchase_unit: "1",
    status: "PENDING", conversion_issue: "", loja: "LEBLON", date: "2026-02-22" },
  { id: 6, fingerprint: "02916265009973|7898302290310|CORACAO DA ALCATRA BOVINO|KG",
    xProd_norm: "CORACAO DA ALCATRA BOVINO", uCom: "KG", qCom: 22.4, vUnCom: 55.00,
    suggested_sku_id: "SKU_000038", suggested_sku_name: "CORACAO DA ALCATRA",
    suggested_sku_name_canonical: "CORACAO DA ALCATRA", suggested_base_measure: "KG",
    suggested_base_qty_per_purchase_unit: "1",
    status: "APPROVED", conversion_issue: "", loja: "LEBLON", date: "2026-02-21" },
  { id: 7, fingerprint: "49151483003725||BALDE GELO 900ML INOX|UN",
    xProd_norm: "BALDE GELO 900ML INOX", uCom: "UN", qCom: 2, vUnCom: 45.00,
    suggested_sku_id: "", suggested_sku_name: "", suggested_sku_name_canonical: "",
    suggested_base_measure: "UN", suggested_base_qty_per_purchase_unit: "1",
    status: "IGNORED", conversion_issue: "", loja: "JB", date: "2026-02-20" },
];

const MAPPINGS_INITIAL = [
  { id: 1, fingerprint: "02916265009973|7898302290310|CORACAO DA ALCATRA BOVINO RESFRIADO|KG", sku_id: "SKU_000038", base_measure: "KG", qty: "1", status: "ACTIVE", updated: "2026-02-21" },
  { id: 2, fingerprint: "10915021000190|7894904006458|BACON DEFUMADO SEARA|KG", sku_id: "SKU_000039", base_measure: "KG", qty: "", status: "ACTIVE", updated: "2026-02-21" },
  { id: 3, fingerprint: "08969770000159|7894904006458|BACON MANTA DEF SEARA PECA|KG1", sku_id: "SKU_000039", base_measure: "KG", qty: "1", status: "ACTIVE", updated: "2026-02-24" },
  { id: 4, fingerprint: "33381286001980||BATATA ASTERIX KG|KG", sku_id: "SKU_000040", base_measure: "KG", qty: "", status: "ACTIVE", updated: "2026-02-23" },
  { id: 5, fingerprint: "57531348000122||BATATA ASTERIX SC 25K|SACO", sku_id: "SKU_000040", base_measure: "KG", qty: "25", status: "ACTIVE", updated: "2026-02-23" },
  { id: 6, fingerprint: "37644049000197|7891149101603|CHOPP BRAHMA CLARO 50L BARRIL|UN", sku_id: "SKU_000048", base_measure: "L", qty: "50", status: "ACTIVE", updated: "2026-02-21" },
  { id: 7, fingerprint: "49151483003725||BALDE GELO 900ML INOX CK4089|UN", sku_id: "", base_measure: "", qty: "", status: "IGNORE", updated: "2026-02-19" },
  { id: 8, fingerprint: "33377249000170|7898627980019|BOBINA PICOTADA 20X30 PICOFLEX|UN", sku_id: "", base_measure: "", qty: "", status: "IGNORE", updated: "2026-02-19" },
];

const CATALOG_DATA = [
  { sku_id: "SKU_000038", name: "CORACAO DA ALCATRA", brand: "", category: "Proteina Bovina", base_measure: "KG", mappings: 7 },
  { sku_id: "SKU_000039", name: "BACON MANTA", brand: "", category: "Proteina Suina", base_measure: "KG", mappings: 5 },
  { sku_id: "SKU_000040", name: "BATATA ASTERIX", brand: "", category: "Hortifruti", base_measure: "KG", mappings: 4 },
  { sku_id: "SKU_000041", name: "BATATA SURECRISP CONG 7MM", brand: "McCain", category: "Congelados", base_measure: "KG", mappings: 2 },
  { sku_id: "SKU_000048", name: "CHOPP BRAHMA CLARO", brand: "Brahma", category: "Bebidas", base_measure: "L", mappings: 18 },
  { sku_id: "SKU_000050", name: "KETCHUP", brand: "", category: "Molhos", base_measure: "KG", mappings: 7 },
  { sku_id: "SKU_000056", name: "FILE MIGNON", brand: "", category: "Proteina Bovina", base_measure: "KG", mappings: 9 },
  { sku_id: "SKU_000072", name: "COXAO MOLE", brand: "", category: "Proteina Bovina", base_measure: "KG", mappings: 6 },
  { sku_id: "SKU_000084", name: "TEQUILA CUERVO SILVER", brand: "Jose Cuervo", category: "Bebidas", base_measure: "UN", mappings: 3 },
  { sku_id: "SKU_000085", name: "TEQUILA CUERVO GOLD", brand: "Jose Cuervo", category: "Bebidas", base_measure: "UN", mappings: 2 },
  { sku_id: "SKU_000086", name: "WHISKY BLACK LABEL", brand: "Johnnie Walker", category: "Bebidas", base_measure: "UN", mappings: 4 },
  { sku_id: "SKU_000103", name: "ALCATRA BOVINA", brand: "", category: "Proteina Bovina", base_measure: "KG", mappings: 11 },
  { sku_id: "SKU_000108", name: "TILAPIA FILÉ", brand: "", category: "Frutos do Mar", base_measure: "KG", mappings: 2 },
];

const LOJAS = ["Todas", "LEBLON", "MEIER", "JB", "BURGER"];
const CATEGORIES = ["Todas", "Proteina Bovina", "Proteina Suina", "Hortifruti", "Congelados", "Bebidas", "Molhos", "Frutos do Mar"];

// ── Icons ─────────────────────────────────────────────────────────
const Icon = ({ d, size = 16 }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d={d}/></svg>
);
const CheckIcon  = p => <Icon {...p} d="M20 6L9 17l-5-5"/>;
const XIcon      = p => <Icon {...p} d="M18 6L6 18M6 6l12 12"/>;
const SearchIcon = p => <Icon {...p} d="M21 21l-4.35-4.35M17 11A6 6 0 1 1 5 11a6 6 0 0 1 12 0z"/>;
const AlertIcon  = p => <Icon {...p} d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0-3.42 0zM12 9v4M12 17h.01"/>;
const MapIcon    = p => <Icon {...p} d="M1 6v16l7-4 8 4 7-4V2l-7 4-8-4-7 4zM8 2v16M16 6v16"/>;
const BookIcon   = p => <Icon {...p} d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20M4 19.5A2.5 2.5 0 0 0 6.5 22H20V2H6.5A2.5 2.5 0 0 0 4 4.5v15z"/>;
const EditIcon   = p => <Icon {...p} d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>;
const TrashIcon  = p => <Icon {...p} d="M3 6h18M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2"/>;
const SaveIcon   = p => <Icon {...p} d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2zM17 21v-8H7v8M7 3v5h8"/>;
const ClearIcon  = p => <Icon {...p} d="M18 6L6 18M6 6l12 12"/>;

const fpParts = fp => { const p = fp.split("|"); return { cnpj:p[0]||"", ean:p[1]||"", desc:p[2]||fp, ucom:p[3]||"" }; };
const fmtBR = (n,d=2) => n==null||isNaN(n) ? "-" : Number(n).toLocaleString("pt-BR",{minimumFractionDigits:d,maximumFractionDigits:d});

const BM_COLORS = {
  KG:{bg:"#eff6ff",color:"#1d4ed8",border:"#bfdbfe"},
  UN:{bg:"#f5f3ff",color:"#6d28d9",border:"#ddd6fe"},
  L:{bg:"#ecfdf5",color:"#065f46",border:"#a7f3d0"},
  CX:{bg:"#fff7ed",color:"#9a3412",border:"#fed7aa"},
  SACO:{bg:"#fdf4ff",color:"#7e22ce",border:"#e9d5ff"},
};
const BMPill = ({bm}) => {
  const c = BM_COLORS[bm]||{bg:"#f3f4f6",color:"#374151",border:"#e5e7eb"};
  return <span style={{display:"inline-flex",alignItems:"center",padding:"2px 8px",borderRadius:99,fontSize:11,fontWeight:700,background:c.bg,color:c.color,border:"1px solid "+c.border,fontFamily:"'DM Mono',monospace"}}>{bm||"-"}</span>;
};
const STATUS_MAP = {
  PENDING:{bg:"#fffbeb",color:"#92400e",border:"#fde68a",label:"Pendente"},
  APPROVED:{bg:"#f0fdf4",color:"#166534",border:"#bbf7d0",label:"Aprovado"},
  IGNORED:{bg:"#f9fafb",color:"#6b7280",border:"#e5e7eb",label:"Ignorado"},
  ACTIVE:{bg:"#f0fdf4",color:"#166534",border:"#bbf7d0",label:"Ativo"},
  IGNORE:{bg:"#f9fafb",color:"#6b7280",border:"#e5e7eb",label:"Ignorado"},
};
const StatusPill = ({status}) => {
  const c = STATUS_MAP[status]||STATUS_MAP.PENDING;
  return <span style={{display:"inline-flex",padding:"3px 9px",borderRadius:99,fontSize:11,fontWeight:600,background:c.bg,color:c.color,border:"1px solid "+c.border}}>{c.label}</span>;
};
const CAT_COLORS = {
  "Proteina Bovina":{bg:"#fff7ed",color:"#9a3412"},
  "Proteina Suina":{bg:"#fdf4ff",color:"#7e22ce"},
  "Hortifruti":{bg:"#f0fdf4",color:"#166534"},
  "Congelados":{bg:"#eff6ff",color:"#1e40af"},
  "Bebidas":{bg:"#ecfeff",color:"#155e75"},
  "Molhos":{bg:"#fff1f2",color:"#9f1239"},
  "Frutos do Mar":{bg:"#f0f9ff",color:"#0369a1"},
};
const Btn = ({children,onClick,variant="default",small=false,disabled=false}) => {
  const styles = {
    approve:{bg:"#f0fdf4",color:"#166534",border:"#bbf7d0"},
    ignore:{bg:"#f9fafb",color:"#6b7280",border:"#e5e7eb"},
    edit:{bg:"#f8faff",color:"#1d4ed8",border:"#dbeafe"},
    delete:{bg:"#fff1f2",color:"#9f1239",border:"#fecaca"},
    save:{bg:"#1d4ed8",color:"#fff",border:"#1d4ed8"},
    default:{bg:"#f9fafb",color:"#374151",border:"#e5e7eb"},
  };
  const c = styles[variant]||styles.default;
  return <button disabled={disabled} onClick={onClick} style={{display:"inline-flex",alignItems:"center",gap:4,padding:small?"4px 9px":"6px 14px",borderRadius:6,fontSize:12,fontWeight:600,cursor:disabled?"not-allowed":"pointer",background:c.bg,color:c.color,border:"1px solid "+c.border,opacity:disabled?0.6:1}}>{children}</button>;
};

const SELECT_STYLE = {border:"1px solid #e5e7eb",borderRadius:8,padding:"8px 12px",fontSize:13,color:"#374151",background:"#fff",outline:"none",cursor:"pointer"};
const TH_STYLE = {textAlign:"left",fontSize:11,fontWeight:600,color:"#6b7280",padding:"10px 14px",letterSpacing:"0.04em",textTransform:"uppercase",whiteSpace:"nowrap"};
const TD_STYLE = {padding:"13px 14px",verticalAlign:"top"};

// ══════════════════════════════════════════════════════════════════
// SKU PICKER — componente de busca de SKU com dropdown
// ══════════════════════════════════════════════════════════════════
const SkuPicker = ({ value, onChange, placeholder = "Buscar SKU...", options = [] }) => {
  const [query, setQuery]       = useState("");
  const [open, setOpen]         = useState(false);
  const [focused, setFocused]   = useState(false);
  const wrapRef = useRef(null);

  const selected = options.find(s => s.sku_id === value);

  // fecha ao clicar fora
  useEffect(() => {
    const handler = e => { if (wrapRef.current && !wrapRef.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const filtered = options.filter(s => {
    if (!query) return true;
    const q = query.toLowerCase();
    return s.name.toLowerCase().includes(q) || s.sku_id.toLowerCase().includes(q) || s.category.toLowerCase().includes(q);
  }).slice(0, 12);

  const handleSelect = sku => {
    onChange(sku.sku_id);
    setQuery("");
    setOpen(false);
  };

  const handleClear = e => { e.stopPropagation(); onChange(""); setQuery(""); };

  return (
    <div ref={wrapRef} style={{position:"relative",width:"100%"}}>
      {/* Display / search field */}
      <div
        onClick={() => { setOpen(true); setFocused(true); }}
        style={{
          display:"flex", alignItems:"center", gap:8,
          border:"1px solid "+(open?"#2563eb":"#e5e7eb"),
          borderRadius:8, padding:"7px 10px",
          background:"#fff", cursor:"text",
          boxShadow: open?"0 0 0 3px rgba(37,99,235,0.1)":"none",
          transition:"all 0.15s",
        }}
      >
        <span style={{color:"#9ca3af",flexShrink:0}}><SearchIcon size={14}/></span>
        {selected && !open ? (
          <div style={{flex:1,display:"flex",alignItems:"center",gap:8,minWidth:0}}>
            <span style={{fontFamily:"'DM Mono',monospace",fontSize:11,color:"#2563eb",fontWeight:700,flexShrink:0}}>{selected.sku_id}</span>
            <span
              title={selected.name}
              style={{fontSize:13,color:"#111827",overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}
            >
              {selected.name}
            </span>
            <BMPill bm={selected.base_measure}/>
          </div>
        ) : (
          <input
            autoFocus={open}
            value={query}
            onChange={e=>setQuery(e.target.value)}
            onFocus={()=>setOpen(true)}
            placeholder={selected ? selected.name : placeholder}
            style={{flex:1,border:"none",outline:"none",fontSize:13,color:"#111827",background:"transparent",minWidth:0}}
          />
        )}
        {value && (
          <button onClick={handleClear} style={{background:"none",border:"none",cursor:"pointer",color:"#9ca3af",padding:0,display:"flex",flexShrink:0}}>
            <ClearIcon size={13}/>
          </button>
        )}
      </div>

      {/* Dropdown */}
      {open && (
        <div style={{
          position:"absolute", top:"calc(100% + 4px)", left:0, right:0, zIndex:999,
          background:"#fff", border:"1px solid #e5e7eb", borderRadius:10,
          boxShadow:"0 8px 30px rgba(0,0,0,0.12)", overflow:"hidden", maxHeight:320, overflowY:"auto",
          minWidth:"min(560px, calc(100vw - 32px))",
        }}>
          {filtered.length === 0 ? (
            <div style={{padding:"16px 14px",fontSize:13,color:"#9ca3af",textAlign:"center"}}>Nenhum SKU encontrado</div>
          ) : (
            filtered.map(sku => {
              const isSelected = sku.sku_id === value;
              const catStyle = CAT_COLORS[sku.category]||{bg:"#f9fafb",color:"#374151"};
              return (
                <div key={sku.sku_id} onClick={()=>handleSelect(sku)}
                  style={{
                    display:"flex", alignItems:"center", gap:10, padding:"10px 14px",
                    cursor:"pointer", borderBottom:"1px solid #f9fafb",
                    background: isSelected ? "#eff6ff" : "#fff",
                    transition:"background 0.1s",
                  }}
                  onMouseEnter={e=>e.currentTarget.style.background=isSelected?"#eff6ff":"#f9fafb"}
                  onMouseLeave={e=>e.currentTarget.style.background=isSelected?"#eff6ff":"#fff"}
                >
                  <span style={{fontFamily:"'DM Mono',monospace",fontSize:11,color:"#2563eb",fontWeight:700,flexShrink:0,minWidth:88}}>{sku.sku_id}</span>
                  <div style={{flex:1,minWidth:0}}>
                    <div
                      title={sku.name}
                      style={{
                        fontSize:13,
                        fontWeight:600,
                        color:"#111827",
                        whiteSpace:"normal",
                        wordBreak:"break-word",
                        lineHeight:1.2,
                      }}
                    >
                      {sku.name}
                    </div>
                    {sku.brand && <div style={{fontSize:11,color:"#9ca3af"}}>{sku.brand}</div>}
                  </div>
                  <span style={{display:"inline-flex",padding:"2px 7px",borderRadius:99,fontSize:10,fontWeight:600,background:catStyle.bg,color:catStyle.color,flexShrink:0}}>{sku.category}</span>
                  <BMPill bm={sku.base_measure}/>
                  {isSelected && <span style={{color:"#2563eb",flexShrink:0}}><CheckIcon size={13}/></span>}
                </div>
              );
            })
          )}
        </div>
      )}
    </div>
  );
};

// ── Edit Modal (Mappings) ──────────────────────────────────────────
const EditModal = ({mapping, catalogItems, onSave, onClose}) => {
  const [vals, setVals] = useState({sku_id:mapping.sku_id, base_measure:mapping.base_measure, qty:mapping.qty, status:mapping.status});
  const fp = fpParts(mapping.fingerprint);

  const handleSkuSelect = sku_id => {
    const found = catalogItems.find(s=>s.sku_id===sku_id);
    setVals(v=>({...v, sku_id, base_measure: found ? found.base_measure : v.base_measure}));
  };

  return (
    <div style={{position:"fixed",inset:0,background:"rgba(0,0,0,0.35)",zIndex:1000,display:"flex",alignItems:"center",justifyContent:"center"}} onClick={onClose}>
      <div style={{background:"#fff",borderRadius:14,padding:28,width:520,boxShadow:"0 20px 60px rgba(0,0,0,0.15)"}} onClick={e=>e.stopPropagation()}>
        <div style={{fontSize:15,fontWeight:700,color:"#111827",marginBottom:12}}>Editar Mapping</div>
        <div style={{fontSize:12,color:"#6b7280",fontFamily:"'DM Mono',monospace",background:"#f9fafb",padding:"10px 12px",borderRadius:8,border:"1px solid #e5e7eb",marginBottom:20}}>
          <div style={{fontWeight:600,color:"#374151",marginBottom:2}}>{fp.desc}</div>
          <div style={{color:"#9ca3af"}}>{fp.cnpj} &middot; uCom: {fp.ucom}</div>
        </div>

        {/* SKU Picker */}
        <div style={{marginBottom:16}}>
          <label style={{fontSize:11,fontWeight:600,color:"#6b7280",display:"block",marginBottom:6,textTransform:"uppercase",letterSpacing:"0.04em"}}>SKU Canonico</label>
          <SkuPicker value={vals.sku_id} onChange={handleSkuSelect} options={catalogItems} placeholder="Digite o nome ou categoria do produto..."/>
        </div>

        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12,marginBottom:20}}>
          <div>
            <label style={{fontSize:11,fontWeight:600,color:"#6b7280",display:"block",marginBottom:6,textTransform:"uppercase",letterSpacing:"0.04em"}}>Base Measure</label>
            <select value={vals.base_measure} onChange={e=>setVals(v=>({...v,base_measure:e.target.value}))}
              style={{...SELECT_STYLE,width:"100%"}}>
              <option value="">-</option><option>KG</option><option>UN</option><option>L</option>
            </select>
          </div>
          <div>
            <label style={{fontSize:11,fontWeight:600,color:"#6b7280",display:"block",marginBottom:6,textTransform:"uppercase",letterSpacing:"0.04em"}}>Qty por unidade de compra</label>
            <input value={vals.qty} onChange={e=>setVals(v=>({...v,qty:e.target.value}))} placeholder="ex: 25"
              style={{width:"100%",border:"1px solid #e5e7eb",borderRadius:8,padding:"8px 10px",fontSize:13,color:"#111827",outline:"none",boxSizing:"border-box"}}/>
          </div>
          <div>
            <label style={{fontSize:11,fontWeight:600,color:"#6b7280",display:"block",marginBottom:6,textTransform:"uppercase",letterSpacing:"0.04em"}}>Status</label>
            <select value={vals.status} onChange={e=>setVals(v=>({...v,status:e.target.value}))}
              style={{...SELECT_STYLE,width:"100%"}}>
              <option value="ACTIVE">ACTIVE</option><option value="IGNORE">IGNORE</option>
            </select>
          </div>
        </div>

        <div style={{display:"flex",justifyContent:"flex-end",gap:8}}>
          <Btn onClick={onClose} variant="ignore">Cancelar</Btn>
          <Btn onClick={()=>onSave(vals)} variant="save"><SaveIcon size={12}/> Salvar</Btn>
        </div>
      </div>
    </div>
  );
};

const ConfirmModal = ({text, onConfirm, onClose}) => (
  <div style={{position:"fixed",inset:0,background:"rgba(0,0,0,0.35)",zIndex:1000,display:"flex",alignItems:"center",justifyContent:"center"}} onClick={onClose}>
    <div style={{background:"#fff",borderRadius:12,padding:24,width:380,boxShadow:"0 20px 60px rgba(0,0,0,0.15)"}} onClick={e=>e.stopPropagation()}>
      <div style={{fontSize:14,fontWeight:700,color:"#111827",marginBottom:8}}>Confirmar exclusao</div>
      <div style={{fontSize:13,color:"#6b7280",marginBottom:20}}>{text}</div>
      <div style={{display:"flex",justifyContent:"flex-end",gap:8}}>
        <Btn onClick={onClose} variant="ignore">Cancelar</Btn>
        <Btn onClick={onConfirm} variant="delete"><TrashIcon size={12}/> Deletar</Btn>
      </div>
    </div>
  </div>
);

const Toolbar = ({value, onChange, placeholder, extra}) => (
  <div style={{display:"flex",gap:8,marginBottom:16}}>
    <div style={{position:"relative",flex:1}}>
      <span style={{position:"absolute",left:10,top:"50%",transform:"translateY(-50%)",color:"#9ca3af"}}><SearchIcon size={14}/></span>
      <input style={{width:"100%",border:"1px solid #e5e7eb",borderRadius:8,padding:"8px 12px 8px 34px",fontSize:13,color:"#111827",outline:"none",background:"#fff",boxSizing:"border-box"}}
        placeholder={placeholder} value={value} onChange={e=>onChange(e.target.value)}/>
    </div>
    {extra}
  </div>
);

const Table = ({headers, children}) => (
  <div style={{background:"#fff",border:"1px solid #e5e7eb",borderRadius:12,overflow:"hidden"}}>
    <table style={{width:"100%",borderCollapse:"collapse"}}>
      <thead><tr style={{background:"#f9fafb",borderBottom:"1px solid #e5e7eb"}}>
        {headers.map(h=><th key={h} style={TH_STYLE}>{h}</th>)}
      </tr></thead>
      <tbody>{children}</tbody>
    </table>
  </div>
);

// ══════════════════════════════════════════════════════════════════
export default function App() {
  const [tab, setTab]           = useState("review");
  const [loading, setLoading]   = useState(false);
  const [loadError, setLoadError] = useState("");
  const [actionError, setActionError] = useState("");
  const [busyKey, setBusyKey] = useState("");
  const [reviewItems, setReviewItems] = useState(REVIEW_DATA);
  const [search, setSearch]     = useState("");
  const [lojaFilter, setLojaFilter] = useState("Todas");
  const [statusFilter, setStatusFilter] = useState("PENDING");
  const [editingRow, setEditingRow]   = useState(null);
  const [editValues, setEditValues]   = useState({});
  const [mappings, setMappings] = useState(MAPPINGS_INITIAL);
  const [catalogData, setCatalogData] = useState(CATALOG_DATA);
  const [mapSearch, setMapSearch]     = useState("");
  const [mapStatusFilter, setMapStatusFilter] = useState("ALL");
  const [editingMapping, setEditingMapping]   = useState(null);
  const [confirmDelete, setConfirmDelete]     = useState(null);
  const [catSearch, setCatSearch]   = useState("");
  const [catCategory, setCatCategory] = useState("Todas");
  const [expandedSku, setExpandedSku] = useState(null);

  const lojas = useMemo(
    () => ["Todas", ...Array.from(new Set(reviewItems.map(r => r.loja).filter(Boolean)))],
    [reviewItems]
  );
  const categories = useMemo(
    () => ["Todas", ...Array.from(new Set(catalogData.map(c => c.category).filter(Boolean)))],
    [catalogData]
  );

  const pendingCount = reviewItems.filter(r=>r.status==="PENDING").length;

  const loadFromApi = useCallback(async () => {
    setLoading(true);
    setLoadError("");
    try {
      const [reviewRes, mappingsRes, catalogRes] = await Promise.all([
        fetch("/api/review?status=ALL&limit=3000"),
        fetch("/api/mappings?status=ALL&limit=3000"),
        fetch("/api/catalog?limit=3000"),
      ]);

      if (!reviewRes.ok || !mappingsRes.ok || !catalogRes.ok) {
        throw new Error("Falha ao carregar dados da API.");
      }

      const [reviewRows, mappingRows, catalogRows] = await Promise.all([
        reviewRes.json(),
        mappingsRes.json(),
        catalogRes.json(),
      ]);

      setReviewItems(
        (reviewRows || []).map((row, idx) => ({
          id: row.id || row.fingerprint || String(idx),
          fingerprint: row.fingerprint || "",
          xProd_norm: row.xProd_norm || "",
          uCom: row.uCom || "",
          qCom: row.qCom,
          vUnCom: row.vUnCom,
          suggested_sku_id: row.suggested_sku_id || "",
          suggested_sku_name: row.suggested_sku_name || "",
          suggested_sku_name_canonical: row.suggested_sku_name_canonical || "",
          suggested_base_measure: row.suggested_base_measure || "",
          suggested_base_qty_per_purchase_unit: String(
            row.suggested_base_qty_per_purchase_unit || "1"
          ),
          status: String(row.status || "PENDING").toUpperCase(),
          conversion_issue: row.conversion_issue || "",
          loja: row.loja_name || "SEM_LOJA",
          date: String(row.dhEmi_date || "").slice(0, 10),
        }))
      );

      setMappings(
        (mappingRows || []).map((row, idx) => ({
          id: row.id || row.fingerprint || String(idx),
          fingerprint: row.fingerprint || "",
          sku_id: row.sku_id || "",
          base_measure: row.base_measure_override || "",
          qty: String(row.base_qty_per_purchase_unit_override || ""),
          status: String(row.status || "ACTIVE").toUpperCase(),
          updated: String(row.updated_at || "").slice(0, 10),
        }))
      );

      setCatalogData(
        (catalogRows || []).map((row) => ({
          sku_id: row.sku_id || "",
          name: row.sku_name_canonical || "",
          brand: row.brand || "",
          category: row.category || "",
          base_measure: row.base_measure || "",
          mappings: Number(row.fingerprints_count || 0),
        }))
      );
    } catch (err) {
      setLoadError(String(err?.message || err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadFromApi();
  }, [loadFromApi]);

  const readErrorMessage = async (res) => {
    try {
      const payload = await res.json();
      if (typeof payload?.detail === "string" && payload.detail) {
        return payload.detail;
      }
      if (payload?.detail) {
        return JSON.stringify(payload.detail);
      }
    } catch {
      // ignored
    }
    return `HTTP ${res.status}`;
  };

  const runBusyAction = async (key, actionFn) => {
    setBusyKey(key);
    setActionError("");
    try {
      await actionFn();
    } catch (err) {
      setActionError(String(err?.message || err));
    } finally {
      setBusyKey("");
    }
  };

  const handleApprove = async id => {
    const row = reviewItems.find(r => r.id === id);
    if (!row) return;
    const v = editingRow === id ? editValues : {};
    const payload = {
      sku_id: (v.sku_id || row.suggested_sku_id || "").trim(),
      base_measure_override: String(v.bm || row.suggested_base_measure || "").trim(),
      base_qty_per_purchase_unit_override: String(
        v.qty || row.suggested_base_qty_per_purchase_unit || ""
      ).trim(),
    };

    await runBusyAction(`review:${id}`, async () => {
      const res = await fetch(`/api/review/${encodeURIComponent(String(id))}/approve`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        throw new Error(await readErrorMessage(res));
      }
      setEditingRow(null);
      await loadFromApi();
    });
  };

  const handleIgnore = async id => {
    await runBusyAction(`review:${id}`, async () => {
      const res = await fetch(`/api/review/${encodeURIComponent(String(id))}/ignore`, {
        method: "POST",
      });
      if (!res.ok) {
        throw new Error(await readErrorMessage(res));
      }
      setEditingRow(null);
      await loadFromApi();
    });
  };

  const startEdit     = row => {
    setEditingRow(row.id);
    setEditValues({sku_id:row.suggested_sku_id, bm:row.suggested_base_measure, qty:row.suggested_base_qty_per_purchase_unit});
  };
  const saveMapping = async (id, vals) => {
    await runBusyAction(`mapping:${id}`, async () => {
      const res = await fetch(`/api/mappings/${encodeURIComponent(String(id))}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sku_id: vals.sku_id || "",
          base_measure: vals.base_measure || "",
          qty: String(vals.qty || ""),
          status: vals.status || "ACTIVE",
        }),
      });
      if (!res.ok) {
        throw new Error(await readErrorMessage(res));
      }
      setEditingMapping(null);
      await loadFromApi();
    });
  };

  const deleteMapping = async id => {
    await runBusyAction(`mapping:${id}`, async () => {
      const res = await fetch(`/api/mappings/${encodeURIComponent(String(id))}`, {
        method: "DELETE",
      });
      if (!res.ok) {
        throw new Error(await readErrorMessage(res));
      }
      setConfirmDelete(null);
      await loadFromApi();
    });
  };

  const filteredReview = reviewItems.filter(r=>{
    const matchLoja   = lojaFilter==="Todas"||r.loja===lojaFilter;
    const matchStatus = statusFilter==="ALL"||r.status===statusFilter;
    const matchSearch = !search||r.xProd_norm.toLowerCase().includes(search.toLowerCase())||r.fingerprint.toLowerCase().includes(search.toLowerCase());
    return matchLoja&&matchStatus&&matchSearch;
  });
  const filteredMappings = mappings.filter(m=>{
    const matchStatus = mapStatusFilter==="ALL"||m.status===mapStatusFilter;
    const matchSearch = !mapSearch||m.fingerprint.toLowerCase().includes(mapSearch.toLowerCase())||m.sku_id.toLowerCase().includes(mapSearch.toLowerCase());
    return matchStatus&&matchSearch;
  });
  const filteredCatalog = catalogData.filter(c=>{
    const matchCat    = catCategory==="Todas"||c.category===catCategory;
    const matchSearch = !catSearch||c.name.toLowerCase().includes(catSearch.toLowerCase())||c.sku_id.toLowerCase().includes(catSearch.toLowerCase());
    return matchCat&&matchSearch;
  });

  // ── Inline SKU picker for Review editing ─────────────────────────
  const ReviewSkuInline = () => (
    <SkuPicker
      value={editValues.sku_id}
      onChange={sku_id => {
        const found = catalogData.find(s=>s.sku_id===sku_id);
        setEditValues(v=>({...v, sku_id, bm: found ? found.base_measure : v.bm}));
      }}
      options={catalogData}
      placeholder="Buscar SKU pelo nome ou categoria..."
    />
  );

  const ReviewTab = () => (
    <div>
      <div style={{marginBottom:24}}>
        <h1 style={{fontSize:20,fontWeight:700,color:"#111827",marginBottom:4,letterSpacing:"-0.4px"}}>Review de SKUs</h1>
        <p style={{fontSize:13,color:"#6b7280"}}><span style={{color:"#d97706",fontWeight:600}}>{pendingCount} itens</span> aguardando aprovacao</p>
      </div>
      <div style={{display:"flex",gap:12,marginBottom:20}}>
        {[
          {label:"Pendentes",value:reviewItems.filter(r=>r.status==="PENDING").length,color:"#d97706",bg:"#fffbeb",border:"#fde68a"},
          {label:"Aprovados",value:reviewItems.filter(r=>r.status==="APPROVED").length,color:"#166534",bg:"#f0fdf4",border:"#bbf7d0"},
          {label:"Ignorados",value:reviewItems.filter(r=>r.status==="IGNORED").length,color:"#6b7280",bg:"#f9fafb",border:"#e5e7eb"},
          {label:"Com erro", value:reviewItems.filter(r=>r.conversion_issue).length,  color:"#991b1b",bg:"#fff1f2",border:"#fecaca"},
        ].map(s=>(
          <div key={s.label} style={{flex:1,background:s.bg,border:"1px solid "+s.border,borderRadius:10,padding:"12px 16px"}}>
            <div style={{fontSize:22,fontWeight:700,color:s.color,fontFamily:"'DM Mono',monospace",lineHeight:1}}>{s.value}</div>
            <div style={{fontSize:11,color:s.color,opacity:0.8,marginTop:3,fontWeight:500}}>{s.label}</div>
          </div>
        ))}
      </div>
      <Toolbar value={search} onChange={setSearch} placeholder="Buscar por produto ou fingerprint..."
        extra={<>
          <select style={SELECT_STYLE} value={lojaFilter} onChange={e=>setLojaFilter(e.target.value)}>{lojas.map(l=><option key={l}>{l}</option>)}</select>
          <select style={SELECT_STYLE} value={statusFilter} onChange={e=>setStatusFilter(e.target.value)}>
            <option value="ALL">Todos</option><option value="PENDING">Pendentes</option><option value="APPROVED">Aprovados</option><option value="IGNORED">Ignorados</option>
          </select>
        </>}
      />
      <Table headers={["Produto NF-e","Qtd / Preco unit","SKU Canonico Sugerido","Conversao","Status","Acoes"]}>
        {filteredReview.map((row,idx)=>{
          const isEditing = editingRow===row.id;
          const hasIssue  = !!row.conversion_issue;
          const rowBusy = busyKey === `review:${row.id}`;
          return (
            <tr key={row.id} style={{borderBottom:idx<filteredReview.length-1?"1px solid #f3f4f6":"none",background:hasIssue?"#fff9f9":"#fff"}}>

              <td style={{...TD_STYLE,maxWidth:260}}>
                <div style={{display:"flex",gap:7}}>
                  {hasIssue&&<span style={{marginTop:4,display:"inline-block",width:6,height:6,borderRadius:"50%",background:"#ef4444",flexShrink:0}}/>}
                  <div>
                    <div style={{fontWeight:600,fontSize:13,color:"#111827",marginBottom:3,lineHeight:1.3}}>{row.xProd_norm}</div>
                    <div style={{display:"flex",gap:8,alignItems:"center",marginBottom:3}}>
                      <BMPill bm={row.uCom}/>
                      <span style={{fontSize:11,color:"#9ca3af",fontFamily:"'DM Mono',monospace"}}>{row.loja} &middot; {row.date}</span>
                    </div>
                    {hasIssue&&<span style={{display:"inline-flex",padding:"2px 7px",borderRadius:99,fontSize:10,fontWeight:600,background:"#fee2e2",color:"#991b1b",border:"1px solid #fca5a5"}}>! {row.conversion_issue}</span>}
                  </div>
                </div>
              </td>

              <td style={{...TD_STYLE,whiteSpace:"nowrap"}}>
                <div style={{fontFamily:"'DM Mono',monospace",fontSize:13,fontWeight:600,color:"#111827"}}>{fmtBR(row.qCom,row.uCom==="KG"?3:0)} {row.uCom}</div>
                <div style={{fontSize:11,color:"#6b7280",marginTop:2}}>R$ {fmtBR(row.vUnCom)}/{row.uCom}</div>
              </td>

              {/* SKU — picker quando editando, display normal caso contrário */}
              <td style={{...TD_STYLE,minWidth:220}}>
                {isEditing ? (
                  <ReviewSkuInline/>
                ) : row.suggested_sku_id ? (
                  <div>
                    <div style={{fontFamily:"'DM Mono',monospace",fontSize:11,color:"#2563eb",fontWeight:700}}>{row.suggested_sku_id}</div>
                    <div style={{fontSize:12,color:"#374151",marginTop:1}}>{row.suggested_sku_name_canonical}</div>
                    {row.suggested_sku_name!==row.suggested_sku_name_canonical&&<div style={{fontSize:10,color:"#9ca3af",marginTop:1}}>NF: {row.suggested_sku_name}</div>}
                  </div>
                ) : <span style={{fontSize:12,color:"#d1d5db",fontStyle:"italic"}}>sem sugestao</span>}
              </td>

              <td style={{...TD_STYLE,minWidth:150}}>
                {isEditing ? (
                  <div style={{display:"flex",gap:6}}>
                    <select style={{border:"1px solid #d1d5db",borderRadius:6,padding:"5px 8px",fontSize:12,background:"#fff",outline:"none",color:"#374151"}}
                      value={editValues.bm} onChange={e=>setEditValues(v=>({...v,bm:e.target.value}))}>
                      <option>KG</option><option>UN</option><option>L</option>
                    </select>
                    <input style={{border:"1px solid #d1d5db",borderRadius:6,padding:"5px 8px",fontSize:12,outline:"none",width:56,color:"#374151"}}
                      value={editValues.qty} onChange={e=>setEditValues(v=>({...v,qty:e.target.value}))} placeholder="Qty"/>
                  </div>
                ) : (
                  <div style={{display:"flex",gap:6,alignItems:"center"}}>
                    <BMPill bm={row.suggested_base_measure}/>
                    {row.suggested_base_qty_per_purchase_unit&&row.suggested_base_qty_per_purchase_unit!=="1"&&
                      <span style={{fontFamily:"'DM Mono',monospace",fontSize:11,color:"#6b7280"}}>x {row.suggested_base_qty_per_purchase_unit}</span>}
                  </div>
                )}
              </td>

              <td style={TD_STYLE}><StatusPill status={row.status}/></td>

              <td style={TD_STYLE}>
                {row.status==="PENDING"&&(
                  <div style={{display:"flex",gap:5,flexWrap:"wrap"}}>
                    {!isEditing&&<Btn disabled={rowBusy} onClick={()=>startEdit(row)} variant="edit" small><EditIcon size={11}/> Editar</Btn>}
                    <Btn disabled={rowBusy} onClick={()=>handleApprove(row.id)} variant="approve" small><CheckIcon size={11}/> {rowBusy ? "Salvando..." : (isEditing?"Salvar":"Aprovar")}</Btn>
                    <Btn disabled={rowBusy} onClick={()=>handleIgnore(row.id)} variant="ignore" small><XIcon size={11}/> Ignorar</Btn>
                  </div>
                )}
              </td>
            </tr>
          );
        })}
      </Table>
    </div>
  );

  const MappingsTab = () => (
    <div>
      {editingMapping&&<EditModal mapping={editingMapping} catalogItems={catalogData} onSave={vals=>saveMapping(editingMapping.id,vals)} onClose={()=>setEditingMapping(null)}/>}
      {confirmDelete&&<ConfirmModal text={"Deletar o mapping \""+fpParts(confirmDelete.fingerprint).desc+"\"? Esta acao nao pode ser desfeita."} onConfirm={()=>deleteMapping(confirmDelete.id)} onClose={()=>setConfirmDelete(null)}/>}
      <div style={{marginBottom:24}}>
        <h1 style={{fontSize:20,fontWeight:700,color:"#111827",marginBottom:4,letterSpacing:"-0.4px"}}>Mappings</h1>
        <p style={{fontSize:13,color:"#6b7280"}}>
          <span style={{color:"#166534",fontWeight:600}}>{mappings.filter(m=>m.status==="ACTIVE").length} ativos</span>
          {" · "}
          <span style={{fontWeight:600}}>{mappings.filter(m=>m.status==="IGNORE").length} ignorados</span>
        </p>
      </div>
      <Toolbar value={mapSearch} onChange={setMapSearch} placeholder="Cole o fingerprint completo ou busque por descricao / SKU..."
        extra={<select style={SELECT_STYLE} value={mapStatusFilter} onChange={e=>setMapStatusFilter(e.target.value)}>
          <option value="ALL">Todos</option><option value="ACTIVE">Ativos</option><option value="IGNORE">Ignorados</option>
        </select>}
      />
      <div style={{background:"#eff6ff",border:"1px solid #dbeafe",borderRadius:8,padding:"10px 14px",fontSize:12,color:"#1e40af",marginBottom:14}}>
        Dica: cole o fingerprint completo no formato <b>CNPJ|EAN|DESCRICAO|uCom</b> no campo de busca para encontrar um mapping especifico.
      </div>
      <Table headers={["Produto / Fingerprint","SKU","Base","Qty","Status","Atualizado","Acoes"]}>
        {filteredMappings.map((m,idx)=>{
          const fp = fpParts(m.fingerprint);
          const rowBusy = busyKey === `mapping:${m.id}`;
          return (
            <tr key={m.id} style={{borderBottom:idx<filteredMappings.length-1?"1px solid #f3f4f6":"none"}}>
              <td style={{...TD_STYLE,maxWidth:360}}>
                <div style={{fontWeight:600,fontSize:13,color:"#111827",marginBottom:4}}>{fp.desc}</div>
                <div style={{display:"flex",gap:6,flexWrap:"wrap"}}>
                  {fp.ucom&&<span style={{fontFamily:"'DM Mono',monospace",fontSize:10,background:"#f3f4f6",color:"#6b7280",padding:"1px 6px",borderRadius:4,border:"1px solid #e5e7eb"}}>{fp.ucom}</span>}
                  <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:"#d1d5db"}}>{fp.cnpj.slice(0,8)}...</span>
                  {fp.ean&&<span style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:"#d1d5db"}}>EAN: {fp.ean.slice(0,8)}...</span>}
                </div>
              </td>
              <td style={TD_STYLE}>{m.sku_id?<span style={{fontFamily:"'DM Mono',monospace",fontSize:12,color:"#2563eb",fontWeight:600}}>{m.sku_id}</span>:<span style={{color:"#d1d5db"}}>-</span>}</td>
              <td style={TD_STYLE}>{m.base_measure?<BMPill bm={m.base_measure}/>:<span style={{color:"#d1d5db"}}>-</span>}</td>
              <td style={{...TD_STYLE,fontFamily:"'DM Mono',monospace",fontSize:12,color:"#374151"}}>{m.qty||<span style={{color:"#d1d5db"}}>-</span>}</td>
              <td style={TD_STYLE}><StatusPill status={m.status}/></td>
              <td style={{...TD_STYLE,fontFamily:"'DM Mono',monospace",fontSize:11,color:"#9ca3af",whiteSpace:"nowrap"}}>{m.updated}</td>
              <td style={TD_STYLE}>
                <div style={{display:"flex",gap:5}}>
                  <Btn disabled={rowBusy} onClick={()=>setEditingMapping(m)} variant="edit" small><EditIcon size={11}/> Editar</Btn>
                  <Btn disabled={rowBusy} onClick={()=>setConfirmDelete(m)} variant="delete" small><TrashIcon size={11}/> {rowBusy ? "..." : "Deletar"}</Btn>
                </div>
              </td>
            </tr>
          );
        })}
      </Table>
    </div>
  );

  const CatalogTab = () => (
    <div>
      <div style={{marginBottom:24}}>
        <h1 style={{fontSize:20,fontWeight:700,color:"#111827",marginBottom:4,letterSpacing:"-0.4px"}}>Catalogo de SKUs</h1>
        <p style={{fontSize:13,color:"#6b7280"}}>{catalogData.length} produtos canonicos cadastrados</p>
      </div>
      <Toolbar value={catSearch} onChange={setCatSearch} placeholder="Buscar por nome ou SKU..."
        extra={<select style={SELECT_STYLE} value={catCategory} onChange={e=>setCatCategory(e.target.value)}>{categories.map(c=><option key={c}>{c}</option>)}</select>}
      />
      <Table headers={["","SKU ID","Nome Canonico","Marca","Categoria","Base","Fingerprints"]}>
        {filteredCatalog.map((c,idx)=>{
          const isExpanded = expandedSku===c.sku_id;
          const relatedMappings = mappings.filter(m=>m.sku_id===c.sku_id);
          const catStyle = CAT_COLORS[c.category]||{bg:"#f9fafb",color:"#374151"};
          return (
            <Fragment key={c.sku_id}>
              <tr key={c.sku_id} onClick={()=>setExpandedSku(isExpanded?null:c.sku_id)}
                style={{borderBottom:isExpanded?"none":(idx<filteredCatalog.length-1?"1px solid #f3f4f6":"none"),cursor:"pointer",background:isExpanded?"#f8faff":"#fff"}}>
                <td style={{...TD_STYLE,width:32,paddingRight:0}}>
                  <svg width={14} height={14} viewBox="0 0 24 24" fill="none" stroke="#9ca3af" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"
                    style={{transform:isExpanded?"rotate(90deg)":"rotate(0deg)",transition:"transform 0.18s",display:"block"}}>
                    <path d="M9 18l6-6-6-6"/>
                  </svg>
                </td>
                <td style={{...TD_STYLE,fontFamily:"'DM Mono',monospace",fontSize:12,color:"#2563eb",fontWeight:700}}>{c.sku_id}</td>
                <td style={{...TD_STYLE,fontSize:13,fontWeight:600,color:"#111827"}}>{c.name}</td>
                <td style={{...TD_STYLE,fontSize:12,color:"#6b7280"}}>{c.brand||<span style={{color:"#d1d5db"}}>-</span>}</td>
                <td style={TD_STYLE}><span style={{display:"inline-flex",padding:"2px 8px",borderRadius:99,fontSize:11,fontWeight:600,background:catStyle.bg,color:catStyle.color}}>{c.category}</span></td>
                <td style={TD_STYLE}><BMPill bm={c.base_measure}/></td>
                <td style={{...TD_STYLE,fontFamily:"'DM Mono',monospace",fontSize:12,color:"#6b7280"}}>{relatedMappings.length}</td>
              </tr>
              {isExpanded&&(
                <tr>
                  <td colSpan={7} style={{padding:"0 14px 14px 46px",background:"#f8faff",borderBottom:idx<filteredCatalog.length-1?"1px solid #f3f4f6":"none"}}>
                    <div style={{fontSize:11,fontWeight:600,color:"#6b7280",textTransform:"uppercase",letterSpacing:"0.06em",marginBottom:8}}>Fingerprints mapeados</div>
                    {relatedMappings.length===0 ? (
                      <div style={{fontSize:12,color:"#9ca3af",fontStyle:"italic"}}>Nenhum mapping encontrado</div>
                    ) : (
                      <table style={{width:"100%",borderCollapse:"collapse"}}>
                        <thead><tr>{["Descricao","uCom","Base","Qty","Status"].map(h=><th key={h} style={{textAlign:"left",fontSize:10,fontWeight:600,color:"#9ca3af",padding:"4px 10px",textTransform:"uppercase"}}>{h}</th>)}</tr></thead>
                        <tbody>
                          {relatedMappings.map((m,mi)=>{
                            const mfp = fpParts(m.fingerprint);
                            return (
                              <tr key={mi} style={{borderTop:"1px solid #e5e7eb"}}>
                                <td style={{padding:"6px 10px",fontSize:12,color:"#374151"}}>{mfp.desc}</td>
                                <td style={{padding:"6px 10px"}}><span style={{fontFamily:"'DM Mono',monospace",fontSize:10,background:"#f3f4f6",color:"#6b7280",padding:"1px 5px",borderRadius:4}}>{mfp.ucom||"-"}</span></td>
                                <td style={{padding:"6px 10px"}}>{m.base_measure?<BMPill bm={m.base_measure}/>:"-"}</td>
                                <td style={{padding:"6px 10px",fontFamily:"'DM Mono',monospace",fontSize:12,color:"#374151"}}>{m.qty||"-"}</td>
                                <td style={{padding:"6px 10px"}}><StatusPill status={m.status}/></td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    )}
                  </td>
                </tr>
              )}
            </Fragment>
          );
        })}
      </Table>
    </div>
  );

  return (
    <div style={{fontFamily:"'DM Sans',sans-serif",background:"#f9fafb",minHeight:"100vh",display:"flex"}}>
      <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet"/>
      <div style={{width:220,background:"#fff",borderRight:"1px solid #e5e7eb",display:"flex",flexDirection:"column",position:"fixed",top:0,left:0,height:"100vh"}}>
        <div style={{padding:"20px 20px 16px",borderBottom:"1px solid #f3f4f6"}}>
          <div style={{fontFamily:"'DM Mono',monospace",fontSize:9,letterSpacing:"0.2em",textTransform:"uppercase",color:"#9ca3af",marginBottom:4}}>NFe Pipeline</div>
          <div style={{fontSize:16,fontWeight:800,color:"#111827",letterSpacing:"-0.5px"}}>Gestao</div>
        </div>
        <nav style={{padding:"12px 10px",flex:1}}>
          {[
            {key:"review",  label:"Review",  icon:<AlertIcon size={15}/>,badge:pendingCount},
            {key:"mappings",label:"Mappings", icon:<MapIcon   size={15}/>},
            {key:"catalog", label:"Catalogo", icon:<BookIcon  size={15}/>},
          ].map(item=>{
            const active = tab===item.key;
            return (
              <div key={item.key} onClick={()=>setTab(item.key)}
                style={{display:"flex",alignItems:"center",gap:8,padding:"9px 12px",borderRadius:8,cursor:"pointer",marginBottom:2,fontSize:13,fontWeight:active?600:500,background:active?"#eff6ff":"transparent",color:active?"#1d4ed8":"#6b7280",border:active?"1px solid #dbeafe":"1px solid transparent",transition:"all 0.12s"}}>
                {item.icon}{item.label}
                {item.badge>0&&<span style={{background:"#ef4444",color:"#fff",borderRadius:99,fontSize:10,fontWeight:700,padding:"1px 6px",marginLeft:"auto"}}>{item.badge}</span>}
              </div>
            );
          })}
        </nav>
        <div style={{padding:"14px 20px",borderTop:"1px solid #f3f4f6"}}>
          <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:2}}>
            <span style={{display:"inline-block",width:6,height:6,borderRadius:"50%",background:"#22c55e"}}/>
            <span style={{fontSize:11,color:"#166534",fontWeight:600}}>{loading ? "Carregando..." : (busyKey ? "Salvando..." : "Sistema online")}</span>
          </div>
          <div style={{fontSize:10,color:"#9ca3af",fontFamily:"'DM Mono',monospace"}}>API integrada (leitura e escrita)</div>
        </div>
      </div>
      <main style={{marginLeft:220,padding:"32px 36px",flex:1,minHeight:"100vh"}}>
        {loadError && (
          <div style={{background:"#fff1f2",border:"1px solid #fecaca",borderRadius:10,padding:"10px 12px",fontSize:13,color:"#991b1b",marginBottom:12}}>
            Falha ao carregar API: {loadError}
          </div>
        )}
        {actionError && (
          <div style={{background:"#fff1f2",border:"1px solid #fecaca",borderRadius:10,padding:"10px 12px",fontSize:13,color:"#991b1b",marginBottom:12}}>
            Falha ao salvar: {actionError}
          </div>
        )}
        {loading && (
          <div style={{fontSize:13,color:"#6b7280",marginBottom:12}}>Carregando dados...</div>
        )}
        {tab==="review"&&<ReviewTab/>}
        {tab==="mappings"&&<MappingsTab/>}
        {tab==="catalog"&&<CatalogTab/>}
      </main>
    </div>
  );
}
