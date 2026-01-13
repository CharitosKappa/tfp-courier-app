import os
import io
import re
import zipfile
import hashlib
import datetime as dt
from typing import List, Dict, Optional, Iterable, Tuple

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from pypdf import PdfReader

APP_TITLE = "TFP Courier Cost Monitor"
APP_VERSION = "v7.0 (progress+ETA, zip/non-zip, faster bulk upserts)"

# -----------------------
# Regex / Rules
# -----------------------
LGK_RE = re.compile(r"(LGK/OUT/\d+)(?:_\d+)?", re.IGNORECASE)
RMA_RE = re.compile(r"(RMA\d+)", re.IGNORECASE)
LEGACY_7DIGIT_RE = re.compile(r"^\d{7}$")            # old eshop: exclude from ALL calcs
TRACKING_NUM_RE = re.compile(r"\b\d{9,20}\b")        # generic tracking-like
BOXNOW_LINE_RE = re.compile(
    r"\b(?P<tracking>\d{9,20})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+\w+\s+(?P<amt>\d+,\d{2})\b"
)

# -----------------------
# Helpers
# -----------------------
def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def norm_str(x) -> str:
    return "" if x is None else str(x).strip()

def to_iso_date(x) -> Optional[str]:
    if x is None:
        return None
    ts = pd.to_datetime(x, errors="coerce", dayfirst=True)
    if pd.isna(ts):
        return None
    return ts.date().isoformat()

def safe_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    colmap = {str(c).lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in colmap:
            return colmap[cand.lower()]
    return None

def coerce_ref(x) -> str:
    # handle floats like 1047260.0
    if x is None:
        return ""
    if isinstance(x, float):
        if pd.isna(x):
            return ""
        if float(x).is_integer():
            return str(int(x))
        return str(x)
    s = str(x).strip()
    if re.fullmatch(r"\d+\.0", s):
        return s[:-2]
    return s

def detect_refs(s: str) -> Dict[str, Optional[str]]:
    s = norm_str(coerce_ref(s))
    out = {"lgk": None, "rma": None, "legacy7": None, "tracking": None}

    m = LGK_RE.search(s)
    if m:
        out["lgk"] = m.group(1).upper()

    m = RMA_RE.search(s)
    if m:
        out["rma"] = m.group(1).upper()

    if LEGACY_7DIGIT_RE.fullmatch(s):
        out["legacy7"] = s

    m = TRACKING_NUM_RE.search(s)
    if m:
        out["tracking"] = m.group(0)

    return out

def extract_pdf_text(file_bytes: bytes, max_pages: int = 40) -> str:
    try:
        reader = PdfReader(io.BytesIO(file_bytes))
        parts = []
        for p in reader.pages[:max_pages]:
            parts.append(p.extract_text() or "")
        return "\n".join(parts)
    except Exception:
        return ""

# -----------------------
# DB
# -----------------------
def get_engine() -> Engine:
    db_url = None
    try:
        db_url = st.secrets.get("db", {}).get("url")
    except Exception:
        db_url = None
    if not db_url:
        db_url = os.getenv("DATABASE_URL")
    if db_url:
        return create_engine(db_url, pool_pre_ping=True)
    return create_engine("sqlite:///courier_app.db")

def is_sqlite(engine: Engine) -> bool:
    return engine.url.get_backend_name() == "sqlite"

def fetch_df(engine: Engine, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    with engine.begin() as cx:
        return pd.read_sql(text(sql), cx, params=params or {})

def preload_upload_hashes(engine: Engine) -> set:
    try:
        df = fetch_df(engine, "SELECT file_hash FROM uploads")
        return set(df["file_hash"].astype(str).tolist())
    except Exception:
        return set()

def upsert_upload(engine: Engine, file_hash: str, filename: str, file_type: str) -> None:
    now = dt.datetime.utcnow().isoformat()
    if is_sqlite(engine):
        sql = """
        INSERT OR REPLACE INTO uploads(file_hash, filename, file_type, uploaded_at)
        VALUES(:h,:fn,:ft,:at)
        """
    else:
        sql = """
        INSERT INTO uploads(file_hash, filename, file_type, uploaded_at)
        VALUES(:h,:fn,:ft,:at)
        ON CONFLICT (file_hash) DO UPDATE SET
          filename=EXCLUDED.filename,
          file_type=EXCLUDED.file_type,
          uploaded_at=EXCLUDED.uploaded_at
        """
    with engine.begin() as cx:
        cx.execute(text(sql), {"h": file_hash, "fn": filename, "ft": file_type, "at": now})

def init_db(engine: Engine) -> None:
    stmts = [
        """
        CREATE TABLE IF NOT EXISTS uploads (
          file_hash TEXT PRIMARY KEY,
          filename TEXT,
          file_type TEXT,
          uploaded_at TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS courier_charges (
          row_hash TEXT PRIMARY KEY,
          courier TEXT,
          country TEXT,
          invoice_no TEXT,
          invoice_date TEXT,
          voucher_no TEXT,
          ref_raw TEXT,
          ref_lgk TEXT,
          ref_rma TEXT,
          ref_tracking TEXT,
          amount_net REAL,
          vat_rate REAL,
          amount_gross REAL,
          currency TEXT,
          source_file_hash TEXT,
          excluded_reason TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS odoo_pickings (
          picking_name TEXT PRIMARY KEY,
          done_date TEXT,
          scheduled_date TEXT,
          origin TEXT,
          partner_country TEXT,
          carrier TEXT,
          tracking TEXT,
          source_file_hash TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS odoo_returns (
          picking_name TEXT PRIMARY KEY,
          done_date TEXT,
          scheduled_date TEXT,
          origin TEXT,
          partner_country TEXT,
          carrier TEXT,
          tracking TEXT,
          source_file_hash TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS odoo_sales_courier_fees (
          order_ref TEXT PRIMARY KEY,
          order_date TEXT,
          partner_country TEXT,
          delivery_method TEXT,
          invoiced_qty REAL,
          fee_gross REAL,
          currency TEXT,
          source_file_hash TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS odoo_rma_fees (
          rma_ref TEXT PRIMARY KEY,
          done_date TEXT,
          partner_country TEXT,
          delivery_method TEXT,
          service_fee_gross REAL,
          currency TEXT,
          source_file_hash TEXT
        )
        """,
    ]
    with engine.begin() as cx:
        for s in stmts:
            cx.execute(text(s))

def upsert_many(engine: Engine, table: str, rows: List[dict], pk: str) -> Tuple[int, int]:
    """
    Bulk upsert via executemany.
    """
    if not rows:
        return 0, 0

    cols = list(rows[0].keys())
    col_list = ",".join(cols)
    ph = ",".join([f":{c}" for c in cols])

    if is_sqlite(engine):
        sql = f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({ph})"
    else:
        updates = ",".join([f"{c}=EXCLUDED.{c}" for c in cols if c != pk])
        sql = f"""
        INSERT INTO {table} ({col_list}) VALUES ({ph})
        ON CONFLICT ({pk}) DO UPDATE SET {updates}
        """

    ok, failed = 0, 0
    try:
        with engine.begin() as cx:
            cx.execute(text(sql), rows)
        ok = len(rows)
    except Exception:
        with engine.begin() as cx:
            for r in rows:
                try:
                    cx.execute(text(sql), r)
                    ok += 1
                except Exception:
                    failed += 1
    return ok, failed

# -----------------------
# Odoo Parsers
# -----------------------
def parse_odoo_pickings_orders_xlsx(b: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(b))
    c_ref = safe_col(df, ["Reference"]) or df.columns[0]
    c_done = safe_col(df, ["Date of Transfer"])
    c_sched = safe_col(df, ["Scheduled Date"])
    c_origin = safe_col(df, ["Source Document"])
    c_country = safe_col(df, ["Destination Country"])
    c_carrier = safe_col(df, ["Carrier"])
    c_track = safe_col(df, ["Tracking Reference"])

    out = pd.DataFrame({
        "picking_name": df[c_ref].astype(str).str.strip(),
        "done_date": df[c_done] if c_done else None,
        "scheduled_date": df[c_sched] if c_sched else None,
        "origin": df[c_origin] if c_origin else None,
        "partner_country": df[c_country] if c_country else None,
        "carrier": df[c_carrier] if c_carrier else None,
        "tracking": df[c_track] if c_track else None,
    })
    out["done_date"] = out["done_date"].apply(to_iso_date)
    out["scheduled_date"] = out["scheduled_date"].apply(to_iso_date)
    for c in ["origin", "partner_country", "carrier", "tracking"]:
        out[c] = out[c].apply(lambda x: norm_str(x) or None)
    return out[out["picking_name"].astype(str).str.strip() != ""]

def parse_odoo_pickings_returns_xlsx(b: bytes) -> pd.DataFrame:
    return parse_odoo_pickings_orders_xlsx(b)

def parse_odoo_sales_orders_xlsx(b: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(b))
    c_order = safe_col(df, ["Order Reference"])
    c_date = safe_col(df, ["Order Date"])
    c_line = safe_col(df, ["Order Lines"])
    c_price = safe_col(df, ["Order Lines/Unit Price"])
    c_qty = safe_col(df, ["Order Lines/Invoiced Quantity"])
    c_deliv = safe_col(df, ["Delivery Method"])

    if any(x is None for x in [c_order, c_line, c_price, c_qty]):
        raise KeyError("Sales Orders: missing required columns (Order Reference, Order Lines, Unit Price, Invoiced Quantity).")

    tmp = df[[c_order, c_date, c_line, c_price, c_qty] + ([c_deliv] if c_deliv else [])].copy()
    tmp.columns = ["order_ref", "order_date", "line_name", "unit_price", "invoiced_qty"] + (["delivery_method"] if c_deliv else [])

    name_lc = tmp["line_name"].astype(str).str.lower()
    mask_fee = (
        name_lc.str.contains("courier") |
        name_lc.str.contains("shipping") |
        name_lc.str.contains("μεταφορ") |
        name_lc.str.contains("εξοδα αποστολ") |
        name_lc.str.contains("έξοδα αποστολ")
    )
    tmp = tmp[mask_fee].copy()
    tmp["invoiced_qty"] = pd.to_numeric(tmp["invoiced_qty"], errors="coerce").fillna(0.0)
    tmp = tmp[tmp["invoiced_qty"] > 0].copy()
    tmp["unit_price"] = pd.to_numeric(tmp["unit_price"], errors="coerce").fillna(0.0)
    tmp["fee_gross"] = tmp["unit_price"] * tmp["invoiced_qty"]

    agg = tmp.groupby("order_ref", as_index=False).agg(
        order_date=("order_date", "first"),
        invoiced_qty=("invoiced_qty", "sum"),
        fee_gross=("fee_gross", "sum"),
        delivery_method=("delivery_method", "first") if "delivery_method" in tmp.columns else ("order_ref", "first"),
    )
    agg["partner_country"] = None
    agg["currency"] = "EUR"
    agg["order_date"] = agg["order_date"].apply(to_iso_date)
    agg["delivery_method"] = agg["delivery_method"].apply(lambda x: norm_str(x) or None)
    return agg[["order_ref", "order_date", "partner_country", "delivery_method", "invoiced_qty", "fee_gross", "currency"]]

def parse_odoo_rma_xlsx(b: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(b))
    c_rma = safe_col(df, ["Name"])
    c_fee = safe_col(df, ["Service Cost"])
    c_deliv = safe_col(df, ["Delivery Method"])
    if not c_rma or not c_fee:
        raise KeyError("RMA: missing required columns (Name, Service Cost).")

    out = pd.DataFrame({
        "rma_ref": df[c_rma].astype(str).str.strip(),
        "done_date": None,
        "partner_country": None,
        "delivery_method": df[c_deliv] if c_deliv else None,
        "service_fee_gross": df[c_fee],
    })
    out["service_fee_gross"] = pd.to_numeric(out["service_fee_gross"], errors="coerce").fillna(0.0)
    out = out[out["service_fee_gross"] != 0].copy()
    out["delivery_method"] = out["delivery_method"].apply(lambda x: norm_str(x) or None)
    out["currency"] = "EUR"
    return out[["rma_ref", "done_date", "partner_country", "delivery_method", "service_fee_gross", "currency"]]

# -----------------------
# Courier Parsers
# -----------------------
def parse_acs_gr_xlsx(b: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(b))

    c_voucher = safe_col(df, ["Αριθμός Αποδεικτικού"])
    c_date = safe_col(df, ["Ημ/νια παραλαβής", "Ημ/νια παράδοσης"])
    c_amount = safe_col(df, ["Καθαρή Αξία Τιμολογίου"])
    c_ref1 = safe_col(df, ["Σχετικό 1"])
    c_ref2 = safe_col(df, ["Σχετικό 2"])

    if not c_amount:
        raise KeyError("ACS GR: cannot find net amount column (Καθαρή Αξία Τιμολογίου).")

    if c_ref1 and c_ref2:
        ref_raw = df[c_ref1].where(df[c_ref1].notna(), df[c_ref2])
    elif c_ref1:
        ref_raw = df[c_ref1]
    elif c_ref2:
        ref_raw = df[c_ref2]
    else:
        ref_raw = df[c_voucher] if c_voucher else None

    out = pd.DataFrame({
        "invoice_no": None,
        "invoice_date": df[c_date] if c_date else None,
        "voucher_no": df[c_voucher] if c_voucher else None,
        "ref_raw": ref_raw,
        "amount_net": df[c_amount],
    })

    out["amount_net"] = (out["amount_net"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False))
    out["amount_net"] = pd.to_numeric(out["amount_net"], errors="coerce")
    out = out.dropna(subset=["amount_net"])

    out["ref_raw"] = out["ref_raw"].apply(coerce_ref).astype(str).str.strip()
    out["voucher_no"] = out["voucher_no"].apply(coerce_ref) if "voucher_no" in out.columns else None
    return out

def parse_courier_center_xls(b: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(b), engine="xlrd")

    c_ref = safe_col(df, ["Σχετικό", "Παραστατικό", "Αιτιολογία", "Reference", "Ref", "Notes", "Περιγραφή"]) or df.columns[0]
    c_amount = safe_col(df, ["Ποσό", "Χρέωση", "Amount", "Net", "Αξία", "Καθαρή Αξία"])
    c_date = safe_col(df, ["Ημερομηνία", "Date", "Invoice Date"])
    c_invoice = safe_col(df, ["Invoice", "Invoice No", "Αρ. Παραστατικού", "Αριθμός", "No"])
    c_voucher = safe_col(df, ["Voucher", "Αποδεικτικό", "Αριθμός Αποδεικτικού"])

    if not c_amount:
        numeric_cols = [c for c in df.columns if pd.to_numeric(df[c], errors="coerce").notna().sum() > 0]
        c_amount = numeric_cols[-1] if numeric_cols else df.columns[-1]

    out = pd.DataFrame({
        "invoice_no": df[c_invoice].astype(str).str.strip() if c_invoice else None,
        "invoice_date": df[c_date] if c_date else None,
        "voucher_no": df[c_voucher] if c_voucher else None,
        "ref_raw": df[c_ref],
        "amount_net": df[c_amount],
    })

    out["amount_net"] = pd.to_numeric(out["amount_net"], errors="coerce")
    out = out.dropna(subset=["amount_net"])
    out["ref_raw"] = out["ref_raw"].apply(coerce_ref).astype(str).str.strip()
    out["voucher_no"] = out["voucher_no"].apply(coerce_ref) if "voucher_no" in out.columns else None
    return out

def parse_boxnow_pdf(b: bytes) -> pd.DataFrame:
    t = extract_pdf_text(b, max_pages=40)
    inv_no = None
    m = re.search(r"Αριθμός\s+Παραστατικού\s*:\s*([^\n\r]+)", t, flags=re.IGNORECASE)
    if m:
        inv_no = norm_str(m.group(1)).split()[0]

    rows = []
    for m in BOXNOW_LINE_RE.finditer(t):
        tracking = m.group("tracking")
        date_s = m.group("date")
        amt = float(m.group("amt").replace(".", "").replace(",", "."))
        rows.append({"invoice_no": inv_no, "invoice_date": date_s, "voucher_no": tracking, "ref_raw": tracking, "amount_net": amt})

    if rows:
        return pd.DataFrame(rows).drop_duplicates()

    trackings = sorted(set(re.findall(r"\b\d{9,20}\b", t)))
    out = pd.DataFrame({"ref_raw": trackings})
    out["invoice_no"] = inv_no
    out["invoice_date"] = None
    out["voucher_no"] = None
    out["amount_net"] = None
    return out

def parse_acs_cy_pdf(b: bytes) -> pd.DataFrame:
    t = extract_pdf_text(b, max_pages=30)
    if not t.strip():
        return pd.DataFrame(columns=["invoice_no", "invoice_date", "voucher_no", "ref_raw", "amount_net"])
    refs = re.findall(r"LGK/OUT/\d+(?:_\d+)?|RMA\d+|\b\d{7}\b|\b\d{9,20}\b", t, flags=re.IGNORECASE)
    out = pd.DataFrame({"ref_raw": [r.upper() for r in refs]})
    out["invoice_no"] = None
    out["invoice_date"] = None
    out["voucher_no"] = None
    out["amount_net"] = None
    return out.drop_duplicates()

# -----------------------
# Normalize -> DB rows
# -----------------------
def normalize_charges(df: pd.DataFrame, courier: str, country: str, vat_rate: float, source_file_hash: str, currency: str = "EUR") -> List[dict]:
    rows = []
    vat = float(vat_rate or 0.0)

    for _, r in df.iterrows():
        ref_raw = coerce_ref(r.get("ref_raw"))
        refs = detect_refs(ref_raw)

        excluded_reason = "legacy_7digit_old_eshop" if refs["legacy7"] else None

        amount_net = r.get("amount_net")
        amount_net = float(amount_net) if amount_net is not None and pd.notna(amount_net) else None
        amount_gross = (amount_net * (1.0 + vat)) if amount_net is not None else None

        invoice_no = norm_str(r.get("invoice_no")) or None
        invoice_date = to_iso_date(r.get("invoice_date"))
        voucher_no = coerce_ref(r.get("voucher_no")) or None

        basis = "|".join([
            courier, country,
            invoice_no or "", invoice_date or "", voucher_no or "",
            ref_raw,
            "" if amount_net is None else f"{amount_net:.6f}",
            f"{vat:.6f}",
            "" if amount_gross is None else f"{amount_gross:.6f}",
            currency,
            source_file_hash
        ])
        row_hash = sha256_bytes(basis.encode("utf-8"))

        rows.append({
            "row_hash": row_hash,
            "courier": courier,
            "country": country,
            "invoice_no": invoice_no,
            "invoice_date": invoice_date,
            "voucher_no": voucher_no,
            "ref_raw": ref_raw,
            "ref_lgk": refs["lgk"],
            "ref_rma": refs["rma"],
            "ref_tracking": refs["tracking"],
            "amount_net": amount_net,
            "vat_rate": vat,
            "amount_gross": amount_gross,
            "currency": currency,
            "source_file_hash": source_file_hash,
            "excluded_reason": excluded_reason,
        })

    return rows

# -----------------------
# ZIP + non-ZIP iterator
# -----------------------
def iter_uploaded_files(files) -> Iterable[Tuple[str, bytes, str]]:
    """
    Yields: (inner_filename, bytes, container_name)
    container_name: original upload name (zip or file)
    """
    if not files:
        return
    for f in files:
        raw = f.getvalue()
        name = f.name
        if name.lower().endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(raw), "r") as z:
                for info in z.infolist():
                    if info.is_dir():
                        continue
                    inner = info.filename.split("/")[-1]
                    if not inner:
                        continue
                    yield inner, z.read(info), name
        else:
            yield name, raw, name

# -----------------------
# Progress / ETA
# -----------------------
def progress_update(pb, status_box, metrics_box, done: int, total: int, phase: str, current: str, ok_rows: int, skipped: int, failed: int, start_ts: float):
    elapsed = max(0.001, dt.datetime.utcnow().timestamp() - start_ts)
    pct = int(100 * done / max(1, total))
    pb.progress(min(100, pct), text=f"{phase} — {pct}%")

    rate = done / elapsed
    remaining = max(0, total - done)
    eta = int(remaining / rate) if rate > 0 else None

    status_box.markdown(f"**Now:** {phase}<br/>**File:** `{current}`", unsafe_allow_html=True)
    metrics_box.info(
        f"Elapsed: {int(elapsed)}s | ETA: {('—' if eta is None else str(eta)+'s')} | "
        f"OK rows: {ok_rows} | Skipped files: {skipped} | Failed: {failed}"
    )

# -----------------------
# Streamlit App
# -----------------------
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)
st.caption(f"{APP_VERSION} — DB URL never shown")

engine = get_engine()
init_db(engine)

with st.sidebar:
    st.header("VAT defaults (net->gross)")
    vat_gr = st.selectbox("Greece VAT", [0.24, 0.13, 0.0], index=0)
    vat_cy = st.selectbox("Cyprus VAT", [0.19, 0.05, 0.0], index=0)

    st.header("Dedupe")
    skip_identical = st.toggle("Skip identical files (by hash)", value=True)

    st.header("Performance")
    chunk_size = st.slider("DB chunk size", 500, 20000, 5000, 500)

st.subheader("1) Upload files")
st.write("Upload ZIP (bulk) or normal files. The app handles both.")

c1, c2 = st.columns(2)
with c1:
    st.markdown("**Courier charges**")
    up_boxnow = st.file_uploader("BoxNow PDFs or ZIP", type=["pdf", "zip"], accept_multiple_files=True, key="boxnow")
    up_acsgr = st.file_uploader("ACS GR XLSX or ZIP", type=["xlsx", "zip"], accept_multiple_files=True, key="acsgr")
    up_cc = st.file_uploader("Courier Center XLS or ZIP", type=["xls", "zip"], accept_multiple_files=True, key="cc")
    up_acscy = st.file_uploader("ACS CY PDFs or ZIP", type=["pdf", "zip"], accept_multiple_files=True, key="acscy")

with c2:
    st.markdown("**Odoo exports**")
    up_pick_orders = st.file_uploader("Pickings Orders (.xlsx)", type=["xlsx"], accept_multiple_files=False, key="pick_orders")
    up_pick_returns = st.file_uploader("Pickings Returns (.xlsx)", type=["xlsx"], accept_multiple_files=False, key="pick_returns")
    up_sales = st.file_uploader("Sales Order (.xlsx)", type=["xlsx"], accept_multiple_files=False, key="sales")
    up_rma = st.file_uploader("RMA (.xlsx)", type=["xlsx"], accept_multiple_files=False, key="rma")

st.subheader("2) Process uploads")

pb = st.progress(0, text="Ready")
status_box = st.empty()
metrics_box = st.empty()

if st.button("Process now", type="primary"):
    start_ts = dt.datetime.utcnow().timestamp()
    existing_hashes = preload_upload_hashes(engine)

    ok_rows = 0
    skipped = 0
    failed = 0
    errors: List[str] = []

    work = []

    def add_slot(slot: str, files, ftype: str, parser):
        for fname, b, container in iter_uploaded_files(files):
            work.append((slot, fname, b, container, ftype, parser))

    add_slot("BOXNOW", up_boxnow, "boxnow_pdf", parse_boxnow_pdf)
    add_slot("ACS_GR", up_acsgr, "acs_gr_xlsx", parse_acs_gr_xlsx)
    add_slot("COURIER_CENTER", up_cc, "courier_center_xls", parse_courier_center_xls)
    add_slot("ACS_CY", up_acscy, "acs_cy_pdf", parse_acs_cy_pdf)

    total_steps = max(1, len(work) * 4 + 12)
    done = 0

    def handle_upload(display_name: str, b: bytes, ftype: str) -> Tuple[str, bool]:
        h = sha256_bytes(b)
        already = (h in existing_hashes)
        if (not already) or (not skip_identical):
            upsert_upload(engine, h, display_name, ftype)
            existing_hashes.add(h)
        return h, already

    for slot, fname, b, container, ftype, parser in work:
        display = f"{container} → {fname}"

        try:
            done += 1
            progress_update(pb, status_box, metrics_box, done, total_steps, "Hashing + dedupe", display, ok_rows, skipped, failed, start_ts)
            h, already = handle_upload(f"{container}:{fname}", b, ftype)

            if already and skip_identical:
                skipped += 1
                done += 3
                continue

            done += 1
            progress_update(pb, status_box, metrics_box, done, total_steps, "Parsing", display, ok_rows, skipped, failed, start_ts)
            df = parser(b)

            done += 1
            progress_update(pb, status_box, metrics_box, done, total_steps, "Normalizing", display, ok_rows, skipped, failed, start_ts)
            if slot == "BOXNOW":
                rows = normalize_charges(df, "BOXNOW", "GR", vat_gr, h)
            elif slot == "ACS_GR":
                rows = normalize_charges(df, "ACS_GR", "GR", vat_gr, h)
            elif slot == "COURIER_CENTER":
                rows = normalize_charges(df, "COURIER_CENTER", "GR", vat_gr, h)
            else:
                rows = normalize_charges(df, "ACS_CY", "CY", vat_cy, h)

            done += 1
            progress_update(pb, status_box, metrics_box, done, total_steps, "DB write (bulk)", display, ok_rows, skipped, failed, start_ts)
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i + chunk_size]
                o, f2 = upsert_many(engine, "courier_charges", chunk, "row_hash")
                ok_rows += o
                failed += f2

        except Exception as e:
            failed += 1
            errors.append(f"{slot} {display}: {e}")
            done += 3

    def process_odoo(uploaded_file, ftype: str, parse_fn, table: str, pk: str, label: str):
        nonlocal done, ok_rows, skipped, failed, errors

        if uploaded_file is None:
            return

        try:
            done += 1
            progress_update(pb, status_box, metrics_box, done, total_steps, "Odoo hashing + dedupe", label, ok_rows, skipped, failed, start_ts)
            b = uploaded_file.getvalue()
            h, already = handle_upload(uploaded_file.name, b, ftype)

            if already and skip_identical:
                skipped += 1
                done += 2
                return

            done += 1
            progress_update(pb, status_box, metrics_box, done, total_steps, "Odoo parsing", label, ok_rows, skipped, failed, start_ts)
            df = parse_fn(b)
            df["source_file_hash"] = h
            rows = df.to_dict("records")

            done += 1
            progress_update(pb, status_box, metrics_box, done, total_steps, "Odoo DB write (bulk)", label, ok_rows, skipped, failed, start_ts)
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i + chunk_size]
                o, f2 = upsert_many(engine, table, chunk, pk)
                ok_rows += o
                failed += f2

        except Exception as e:
            failed += 1
            errors.append(f"{label}: {e}")
            done += 2

    process_odoo(up_pick_orders, "odoo_pickings_orders", parse_odoo_pickings_orders_xlsx, "odoo_pickings", "picking_name", "Odoo Pickings Orders")
    process_odoo(up_pick_returns, "odoo_pickings_returns", parse_odoo_pickings_returns_xlsx, "odoo_returns", "picking_name", "Odoo Pickings Returns")
    process_odoo(up_sales, "odoo_sales_orders", parse_odoo_sales_orders_xlsx, "odoo_sales_courier_fees", "order_ref", "Odoo Sales Orders")
    process_odoo(up_rma, "odoo_rma", parse_odoo_rma_xlsx, "odoo_rma_fees", "rma_ref", "Odoo RMA")

    pb.progress(100, text="Completed")
    elapsed = int(dt.datetime.utcnow().timestamp() - start_ts)
    st.success(f"Completed in {elapsed}s | OK rows={ok_rows} | Skipped files={skipped} | Failed={failed}")

    if errors:
        st.error("Errors")
        for e in errors:
            st.write(e)

st.subheader("3) Monitoring")
counts = {
    "uploads": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM uploads")["n"].iloc[0]),
    "courier_charges": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM courier_charges")["n"].iloc[0]),
    "odoo_pickings": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM odoo_pickings")["n"].iloc[0]),
    "odoo_returns": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM odoo_returns")["n"].iloc[0]),
    "odoo_sales_fees": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM odoo_sales_courier_fees")["n"].iloc[0]),
    "odoo_rma_fees": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM odoo_rma_fees")["n"].iloc[0]),
}
st.json(counts)

excluded = int(fetch_df(engine, "SELECT COUNT(*) AS n FROM courier_charges WHERE excluded_reason IS NOT NULL")["n"].iloc[0])
st.write(f"Excluded legacy 7-digit rows (old e-shop): {excluded}")

st.subheader("4) Export")
if st.button("Build Excel export"):
    ch = fetch_df(engine, "SELECT * FROM courier_charges")
    pk = fetch_df(engine, "SELECT * FROM odoo_pickings")
    rt = fetch_df(engine, "SELECT * FROM odoo_returns")
    so = fetch_df(engine, "SELECT * FROM odoo_sales_courier_fees")
    rma = fetch_df(engine, "SELECT * FROM odoo_rma_fees")

    out_path = "tfp_courier_monitor_export.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as xl:
        ch.to_excel(xl, index=False, sheet_name="courier_charges")
        pk.to_excel(xl, index=False, sheet_name="odoo_pickings")
        rt.to_excel(xl, index=False, sheet_name="odoo_returns")
        so.to_excel(xl, index=False, sheet_name="odoo_sales_fees")
        rma.to_excel(xl, index=False, sheet_name="odoo_rma_fees")

    with open(out_path, "rb") as f:
        st.download_button("Download export", data=f, file_name=out_path)
