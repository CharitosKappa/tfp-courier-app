"""
TFP Courier Cost Monitor (Streamlit + Neon Postgres)

What this app does
- Incremental uploads of courier invoices/charges + Odoo exports (pickings, returns, sales orders, RMAs)
- Stores normalized rows in Postgres (Neon) or local SQLite fallback
- Dedupe: file-level (file_hash) + row-level (row_hash) + Odoo upserts by PK
- Excludes legacy old-e-shop references (plain 7-digit numeric) from ALL calculations
- Ledger (signed gross): Charges negative, Revenues positive
- Summaries by week/month/quarter/year and per courier
- Excel export

Security
- DB URL is never printed/shown
- DB URL is only read from Streamlit secrets or env var DATABASE_URL

Known limitations
- Some PDFs (e.g., ACS CY) may be scanned without embedded text; amounts may not be extractable without OCR.
"""

import os
import io
import re
import zipfile
import hashlib
import datetime as dt
from typing import List, Optional, Dict, Tuple, Iterable

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from pypdf import PdfReader


APP_TITLE = "TFP Courier Cost Monitor"
APP_VERSION = "v6.2 (zip-ready, Neon-safe, tracking-aware)"


# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------

LGK_RE = re.compile(r"(LGK/OUT/\d+)(?:_\d+)?", re.IGNORECASE)
RMA_RE = re.compile(r"(RMA\d+)", re.IGNORECASE)

# “Old e-shop” rule: plain 7-digit numeric reference (allow floats like 1047260.0)
LEGACY_7DIGIT_RE = re.compile(r"^\d{7}$")

# Tracking refs often are long numeric strings (e.g. BoxNow)
TRACKING_NUM_RE = re.compile(r"\b\d{9,20}\b")

# BoxNow PDF: attempt line parsing like: 2973923620 4/6/2025 S  1,50 1041232 46
BOXNOW_LINE_RE = re.compile(
    r"\b(?P<tracking>\d{9,20})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+\w+\s+(?P<amt>\d+,\d{2})\b"
)

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def norm_str(x) -> str:
    if x is None:
        return ""
    return str(x).strip()

def to_iso_date(x) -> Optional[str]:
    if x is None:
        return None
    try:
        ts = pd.to_datetime(x, errors="coerce", dayfirst=True)
        if pd.isna(ts):
            return None
        return ts.date().isoformat()
    except Exception:
        return None

def safe_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    colmap = {str(c).lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in colmap:
            return colmap[cand.lower()]
    return None

def _coerce_ref_cell_to_str(x) -> str:
    """
    Courier exports sometimes carry refs as floats (e.g. 1047260.0).
    We convert cleanly so legacy-7digit detection works.
    """
    if x is None:
        return ""
    # pandas may give float for numeric columns
    if isinstance(x, (int,)):
        return str(x)
    if isinstance(x, float):
        if pd.isna(x):
            return ""
        # if it is an integer float, make it int
        if float(x).is_integer():
            return str(int(x))
        return str(x)
    s = str(x).strip()
    # "1047260.0" => "1047260"
    if re.fullmatch(r"\d+\.0", s):
        return s[:-2]
    return s

def detect_refs(s: str) -> Dict[str, Optional[str]]:
    """
    Returns:
      - lgk: base "LGK/OUT/12345" (no suffix)
      - rma: "RMA123"
      - legacy7: 7-digit numeric (old e-shop) when the entire token is 7 digits
      - tracking: long numeric tracking-like token (optional)
    """
    s = _coerce_ref_cell_to_str(s)
    s = norm_str(s)

    out = {"lgk": None, "rma": None, "legacy7": None, "tracking": None}

    m = LGK_RE.search(s)
    if m:
        out["lgk"] = m.group(1).upper()

    m = RMA_RE.search(s)
    if m:
        out["rma"] = m.group(1).upper()

    # legacy: whole cell is exactly 7 digits
    if LEGACY_7DIGIT_RE.fullmatch(s):
        out["legacy7"] = s

    # tracking: for PDFs or courier data where we only have tracking ids
    m = TRACKING_NUM_RE.search(s)
    if m:
        out["tracking"] = m.group(0)

    return out

def extract_pdf_text(file_bytes: bytes, max_pages: int = 30) -> str:
    try:
        reader = PdfReader(io.BytesIO(file_bytes))
        chunks = []
        for p in reader.pages[:max_pages]:
            chunks.append(p.extract_text() or "")
        return "\n".join(chunks)
    except Exception:
        return ""


# -----------------------------------------------------------------------------
# DB
# -----------------------------------------------------------------------------

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

def upload_exists(engine: Engine, file_hash: str) -> bool:
    df = fetch_df(engine, "SELECT 1 AS x FROM uploads WHERE file_hash=:h LIMIT 1", {"h": file_hash})
    return not df.empty

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

def _table_has_column(engine: Engine, table: str, col: str) -> bool:
    if is_sqlite(engine):
        info = fetch_df(engine, f"PRAGMA table_info({table})")
        return col in info["name"].tolist()
    else:
        info = fetch_df(
            engine,
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name=:t
            """,
            {"t": table},
        )
        return col in info["column_name"].tolist()

def _add_column(engine: Engine, table: str, col: str, coltype: str) -> None:
    with engine.begin() as cx:
        cx.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}"))

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

    # Lightweight "migration" for older DBs (if user already ran previous versions)
    # We try to add missing columns if the table exists but was created with older schema.
    for col, coltype in [
        ("voucher_no", "TEXT"),
        ("ref_tracking", "TEXT"),
    ]:
        try:
            if not _table_has_column(engine, "courier_charges", col):
                _add_column(engine, "courier_charges", col, coltype)
        except Exception:
            pass

    for col, coltype in [
        ("delivery_method", "TEXT"),
    ]:
        try:
            if not _table_has_column(engine, "odoo_sales_courier_fees", col):
                _add_column(engine, "odoo_sales_courier_fees", col, coltype)
        except Exception:
            pass
        try:
            if not _table_has_column(engine, "odoo_rma_fees", col):
                _add_column(engine, "odoo_rma_fees", col, coltype)
        except Exception:
            pass

def upsert_many(engine: Engine, table: str, rows: List[dict], pk: str) -> Tuple[int, int]:
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

    ok = 0
    failed = 0
    with engine.begin() as cx:
        for r in rows:
            try:
                cx.execute(text(sql), r)
                ok += 1
            except Exception:
                failed += 1
    return ok, failed


# -----------------------------------------------------------------------------
# Parsers — Odoo (matched to your real exports)
# -----------------------------------------------------------------------------

def parse_odoo_pickings_orders_xlsx(file_bytes: bytes) -> pd.DataFrame:
    """
    Matches your file: Transfer (stock.picking) (order all).xlsx
    Columns:
      Source Document, Reference, Carrier, Tracking Reference, Contact,
      Destination Country, Date of Transfer, Status, Scheduled Date, ...
    """
    df = pd.read_excel(io.BytesIO(file_bytes))

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

    out = out[out["picking_name"].notna() & (out["picking_name"].astype(str).str.strip() != "")]
    return out


def parse_odoo_pickings_returns_xlsx(file_bytes: bytes) -> pd.DataFrame:
    """
    Matches your file: Transfer (stock.picking) (Returns all).xlsx
    Columns include:
      Reference, Carrier, Contact, Date of Transfer, Destination Country,
      Source Document, Tracking Reference, Scheduled Date, ...
    """
    df = pd.read_excel(io.BytesIO(file_bytes))

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

    out = out[out["picking_name"].notna() & (out["picking_name"].astype(str).str.strip() != "")]
    return out


def parse_odoo_sales_orders_xlsx(file_bytes: bytes) -> pd.DataFrame:
    """
    Matches your file: Sales Order (sale.order) all.xlsx
    Columns:
      Customer, Invoice Status, Order Date, Order Reference, Total, Courier State,
      Order Lines/Unit Price, Order Lines, Delivery Method, Delivery Status,
      Order Lines/Invoiced Quantity, ...
    We treat shipping/courier fees as lines where Order Lines contains keywords.
    """
    df = pd.read_excel(io.BytesIO(file_bytes))

    c_order = safe_col(df, ["Order Reference"])
    c_date = safe_col(df, ["Order Date"])
    c_country = safe_col(df, ["Customer"])  # Not ideal; we don't have country here. We'll store customer as "partner_country" fallback.
    c_line = safe_col(df, ["Order Lines"])
    c_price = safe_col(df, ["Order Lines/Unit Price"])
    c_qty = safe_col(df, ["Order Lines/Invoiced Quantity"])
    c_deliv = safe_col(df, ["Delivery Method"])

    required = [c_order, c_line, c_price, c_qty]
    if any(x is None for x in required):
        raise KeyError("Sales Orders: missing required columns (Order Reference, Order Lines, Unit Price, Invoiced Quantity).")

    tmp = df[[c_order, c_date, c_line, c_price, c_qty] + ([c_deliv] if c_deliv else [])].copy()
    tmp.columns = ["order_ref", "order_date", "line_name", "unit_price", "invoiced_qty"] + (["delivery_method"] if c_deliv else [])

    name_lc = tmp["line_name"].astype(str).str.lower()
    mask_fee = (
        name_lc.str.contains("courier") |
        name_lc.str.contains("shipping") |
        name_lc.str.contains("μεταφορ") |
        name_lc.str.contains("έξοδα αποστολ") |
        name_lc.str.contains("εξοδα αποστολ")
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
        delivery_method=(("delivery_method", "first") if "delivery_method" in tmp.columns else ("order_ref", "first")),
    )

    # we don't have a clean country column in this export; store None
    agg["partner_country"] = None
    agg["currency"] = "EUR"

    agg["order_date"] = agg["order_date"].apply(to_iso_date)
    agg["delivery_method"] = agg["delivery_method"].apply(lambda x: norm_str(x) or None)

    return agg[["order_ref", "order_date", "partner_country", "delivery_method", "invoiced_qty", "fee_gross", "currency"]]


def parse_odoo_rma_xlsx(file_bytes: bytes) -> pd.DataFrame:
    """
    Matches your file: sale.order.rma (sale.order.rma) all.xlsx
    Columns:
      Return Picking, Delivery Method, Name (RMAxxxx), Customer, Order,
      Partner Shipping, Refund Amount, Refund Method, Service Cost, State
    Service Cost is your revenue, includes VAT.
    """
    df = pd.read_excel(io.BytesIO(file_bytes))

    c_rma = safe_col(df, ["Name"])
    c_done = None  # your export does not include done date; keep None
    c_country = None
    c_fee = safe_col(df, ["Service Cost"])
    c_deliv = safe_col(df, ["Delivery Method"])

    if not c_rma or not c_fee:
        raise KeyError("RMA: missing required columns (Name, Service Cost).")

    out = pd.DataFrame({
        "rma_ref": df[c_rma].astype(str).str.strip(),
        "done_date": df[c_done] if c_done else None,
        "partner_country": df[c_country] if c_country else None,
        "delivery_method": df[c_deliv] if c_deliv else None,
        "service_fee_gross": df[c_fee],
    })

    out["service_fee_gross"] = pd.to_numeric(out["service_fee_gross"], errors="coerce").fillna(0.0)
    out = out[out["service_fee_gross"] != 0].copy()

    out["done_date"] = out["done_date"].apply(to_iso_date) if "done_date" in out.columns else None
    out["delivery_method"] = out["delivery_method"].apply(lambda x: norm_str(x) or None)
    out["currency"] = "EUR"

    out = out[out["rma_ref"].notna() & (out["rma_ref"].astype(str).str.strip() != "")]
    return out[["rma_ref", "done_date", "partner_country", "delivery_method", "service_fee_gross", "currency"]]


# -----------------------------------------------------------------------------
# Parsers — Courier
# -----------------------------------------------------------------------------

def parse_acs_gr_xlsx(file_bytes: bytes) -> pd.DataFrame:
    """
    Matches your ACS GR files (Greek columns).
    Observed columns include:
      'Αριθμός Αποδεικτικού' (voucher),
      'Ημ/νια παραλαβής' (pickup date),
      'Καθαρή Αξία Τιμολογίου' (net),
      'Σχετικό 1', 'Σχετικό 2' (refs) often numeric or branch_XXXXXXX
    """
    df = pd.read_excel(io.BytesIO(file_bytes))

    c_voucher = safe_col(df, ["Αριθμός Αποδεικτικού"])
    c_date = safe_col(df, ["Ημ/νια παραλαβής", "Ημ/νια παράδοσης"])
    c_amount = safe_col(df, ["Καθαρή Αξία Τιμολογίου"])
    c_ref1 = safe_col(df, ["Σχετικό 1"])
    c_ref2 = safe_col(df, ["Σχετικό 2"])

    if not c_amount:
        raise KeyError("ACS GR: cannot find net amount column (Καθαρή Αξία Τιμολογίου).")

    # Choose best ref available
    ref_raw = None
    if c_ref1 and c_ref2:
        ref_raw = df[c_ref1].where(df[c_ref1].notna(), df[c_ref2])
    elif c_ref1:
        ref_raw = df[c_ref1]
    elif c_ref2:
        ref_raw = df[c_ref2]
    else:
        # fallback to voucher
        ref_raw = df[c_voucher] if c_voucher else None

    out = pd.DataFrame({
        "invoice_no": None,
        "invoice_date": df[c_date] if c_date else None,
        "voucher_no": df[c_voucher] if c_voucher else None,
        "ref_raw": ref_raw,
        "amount_net": df[c_amount],
    })

    out["amount_net"] = (
        out["amount_net"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    out["amount_net"] = pd.to_numeric(out["amount_net"], errors="coerce")
    out = out.dropna(subset=["amount_net"])

    out["ref_raw"] = out["ref_raw"].apply(_coerce_ref_cell_to_str).astype(str).str.strip()
    out["voucher_no"] = out["voucher_no"].apply(_coerce_ref_cell_to_str) if "voucher_no" in out.columns else None

    return out


def parse_courier_center_xls(file_bytes: bytes) -> pd.DataFrame:
    """
    Courier Center legacy .xls. Requires xlrd installed in the environment.
    We keep it robust with multiple candidate column names.
    """
    # NOTE: xlrd must be installed at runtime for .xls support
    df = pd.read_excel(io.BytesIO(file_bytes), engine="xlrd")

    c_ref = safe_col(df, ["Σχετικό", "Παραστατικό", "Αιτιολογία", "Reference", "Ref", "Notes", "Περιγραφή"])
    c_amount = safe_col(df, ["Ποσό", "Χρέωση", "Amount", "Net", "Αξία", "Καθαρή Αξία"])
    c_date = safe_col(df, ["Ημερομηνία", "Date", "Invoice Date"])
    c_invoice = safe_col(df, ["Invoice", "Invoice No", "Αρ. Παραστατικού", "Αριθμός", "No"])
    c_voucher = safe_col(df, ["Voucher", "Αποδεικτικό", "Αριθμός Αποδεικτικού"])

    if not c_ref:
        c_ref = df.columns[0]

    if not c_amount:
        numeric_candidates = []
        for c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            if s.notna().sum() > 0:
                numeric_candidates.append(c)
        c_amount = numeric_candidates[-1] if numeric_candidates else df.columns[-1]

    out = pd.DataFrame({
        "invoice_no": df[c_invoice].astype(str).str.strip() if c_invoice else None,
        "invoice_date": df[c_date] if c_date else None,
        "voucher_no": df[c_voucher] if c_voucher else None,
        "ref_raw": df[c_ref],
        "amount_net": df[c_amount],
    })

    out["amount_net"] = pd.to_numeric(out["amount_net"], errors="coerce")
    out = out.dropna(subset=["amount_net"])
    out["ref_raw"] = out["ref_raw"].apply(_coerce_ref_cell_to_str).astype(str).str.strip()
    out["voucher_no"] = out["voucher_no"].apply(_coerce_ref_cell_to_str) if "voucher_no" in out.columns else None
    return out


def parse_boxnow_pdf(file_bytes: bytes) -> pd.DataFrame:
    """
    BoxNow PDFs often contain per-shipment line items with tracking numbers and amounts.
    We attempt to parse:
      tracking + date + amount (comma decimal)
    If we fail, we still extract tracking numbers.
    """
    text_pdf = extract_pdf_text(file_bytes, max_pages=40)

    # invoice no
    inv_no = None
    m = re.search(r"Αριθμός\s+Παραστατικού\s*:\s*([^\n\r]+)", text_pdf, flags=re.IGNORECASE)
    if m:
        inv_no = norm_str(m.group(1))
        inv_no = inv_no.split()[0]

    rows = []
    for m in BOXNOW_LINE_RE.finditer(text_pdf):
        tracking = m.group("tracking")
        date_s = m.group("date")
        amt_s = m.group("amt")

        amt = float(amt_s.replace(".", "").replace(",", "."))
        rows.append({
            "invoice_no": inv_no,
            "invoice_date": date_s,
            "voucher_no": tracking,     # for BoxNow, tracking is also a voucher-like identifier
            "ref_raw": tracking,        # we store tracking as ref_raw
            "amount_net": amt,
        })

    if rows:
        return pd.DataFrame(rows).drop_duplicates()

    # fallback: just tracking numbers
    trackings = sorted(set(re.findall(r"\b\d{9,20}\b", text_pdf)))
    out = pd.DataFrame({"ref_raw": trackings})
    out["invoice_no"] = inv_no
    out["invoice_date"] = None
    out["voucher_no"] = None
    out["amount_net"] = None
    return out


def parse_acs_cy_pdf(file_bytes: bytes) -> pd.DataFrame:
    """
    ACS CY PDFs in your sample appear scanned (no embedded text).
    We attempt extraction; if empty, return empty DataFrame.
    """
    text_pdf = extract_pdf_text(file_bytes, max_pages=30)
    if not text_pdf.strip():
        return pd.DataFrame(columns=["invoice_no", "invoice_date", "voucher_no", "ref_raw", "amount_net"])

    refs = re.findall(r"LGK/OUT/\d+(?:_\d+)?|RMA\d+|\b\d{7}\b|\b\d{9,20}\b", text_pdf, flags=re.IGNORECASE)
    out = pd.DataFrame({"ref_raw": [r.upper() for r in refs]})
    out["invoice_no"] = None
    out["invoice_date"] = None
    out["voucher_no"] = None
    out["amount_net"] = None
    return out.drop_duplicates()


# -----------------------------------------------------------------------------
# Normalize courier charges -> DB rows
# -----------------------------------------------------------------------------

def normalize_charges(
    df: pd.DataFrame,
    courier: str,
    country_default: str,
    vat_rate: float,
    source_file_hash: str,
    currency: str = "EUR",
) -> List[dict]:
    rows: List[dict] = []

    for _, r in df.iterrows():
        ref_raw = _coerce_ref_cell_to_str(r.get("ref_raw"))
        refs = detect_refs(ref_raw)

        excluded_reason = "legacy_7digit_old_eshop" if refs["legacy7"] else None

        amount_net = r.get("amount_net")
        amount_net = float(amount_net) if amount_net is not None and pd.notna(amount_net) else None

        vat = float(vat_rate or 0.0)
        amount_gross = (amount_net * (1.0 + vat)) if amount_net is not None else None

        invoice_date_iso = to_iso_date(r.get("invoice_date"))
        invoice_no = norm_str(r.get("invoice_no")) or None
        voucher_no = _coerce_ref_cell_to_str(r.get("voucher_no")) or None

        # Row hash basis: include voucher_no to preserve split-charges rows when available
        basis = "|".join([
            courier,
            country_default,
            invoice_no or "",
            invoice_date_iso or "",
            voucher_no or "",
            ref_raw,
            "" if amount_net is None else f"{amount_net:.6f}",
            f"{vat:.6f}",
            "" if amount_gross is None else f"{amount_gross:.6f}",
            currency,
            source_file_hash,
        ])
        row_hash = sha256_bytes(basis.encode("utf-8"))

        rows.append({
            "row_hash": row_hash,
            "courier": courier,
            "country": country_default,
            "invoice_no": invoice_no,
            "invoice_date": invoice_date_iso,
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


# -----------------------------------------------------------------------------
# Ledger + Summaries
# -----------------------------------------------------------------------------

def _map_delivery_method_to_courier(delivery_method: Optional[str]) -> str:
    """
    Heuristic mapping (can be refined later).
    """
    s = (delivery_method or "").lower()
    if "box" in s:
        return "BOXNOW"
    if "acs" in s and "cy" in s:
        return "ACS_CY"
    if "acs" in s:
        return "ACS_GR"
    if "courier center" in s or "center" in s:
        return "COURIER_CENTER"
    return "UNKNOWN"


def build_ledger(engine: Engine) -> pd.DataFrame:
    frames = []

    ch = fetch_df(engine, "SELECT * FROM courier_charges")
    if not ch.empty:
        tmp = ch.copy()
        tmp["entry_type"] = "CHARGE"
        tmp["ref_key"] = (
            tmp["ref_lgk"]
            .fillna(tmp["ref_rma"])
            .fillna(tmp["ref_tracking"])
            .fillna(tmp["ref_raw"])
        )
        tmp["date_exec"] = pd.to_datetime(tmp["invoice_date"], errors="coerce")
        tmp["amount_gross_signed"] = pd.to_numeric(tmp["amount_gross"], errors="coerce") * (-1.0)
        frames.append(tmp[["entry_type", "courier", "country", "ref_key", "date_exec", "amount_gross_signed", "excluded_reason"]])

    so = fetch_df(engine, "SELECT * FROM odoo_sales_courier_fees")
    if not so.empty:
        tmp = so.copy()
        tmp["entry_type"] = "REVENUE_SHIPPING"
        tmp["courier"] = tmp["delivery_method"].apply(_map_delivery_method_to_courier)
        tmp["country"] = tmp["partner_country"]
        tmp["ref_key"] = tmp["order_ref"].astype(str)
        tmp["date_exec"] = pd.to_datetime(tmp["order_date"], errors="coerce")
        tmp["amount_gross_signed"] = pd.to_numeric(tmp["fee_gross"], errors="coerce")
        tmp["excluded_reason"] = None
        frames.append(tmp[["entry_type", "courier", "country", "ref_key", "date_exec", "amount_gross_signed", "excluded_reason"]])

    rma = fetch_df(engine, "SELECT * FROM odoo_rma_fees")
    if not rma.empty:
        tmp = rma.copy()
        tmp["entry_type"] = "REVENUE_RMA_SERVICE"
        tmp["courier"] = tmp["delivery_method"].apply(_map_delivery_method_to_courier)
        tmp["country"] = tmp["partner_country"]
        tmp["ref_key"] = tmp["rma_ref"].astype(str)
        tmp["date_exec"] = pd.to_datetime(tmp["done_date"], errors="coerce")
        tmp["amount_gross_signed"] = pd.to_numeric(tmp["service_fee_gross"], errors="coerce")
        tmp["excluded_reason"] = None
        frames.append(tmp[["entry_type", "courier", "country", "ref_key", "date_exec", "amount_gross_signed", "excluded_reason"]])

    if not frames:
        return pd.DataFrame(columns=["entry_type", "courier", "country", "ref_key", "date_exec", "amount_gross_signed", "excluded_reason"])

    return pd.concat(frames, ignore_index=True)


def summarize_periods(ledger: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    empty = pd.DataFrame(columns=["courier", "period", "charges_gross", "revenues_gross", "net_gross"])

    if ledger.empty:
        return {"weekly": empty, "monthly": empty, "quarterly": empty, "yearly": empty}

    df = ledger.copy()
    df = df[df["date_exec"].notna()].copy()
    if df.empty:
        return {"weekly": empty, "monthly": empty, "quarterly": empty, "yearly": empty}

    # Exclude old-e-shop rows
    ex = df["excluded_reason"].fillna("")
    df = df[~ex.str.contains("legacy_7digit", case=False)].copy()

    df["courier"] = df["courier"].fillna("UNKNOWN")
    df["is_charge"] = df["entry_type"].eq("CHARGE")
    df["charges_signed"] = df["amount_gross_signed"].where(df["is_charge"], 0.0)  # negative values
    df["revenues_signed"] = df["amount_gross_signed"].where(~df["is_charge"], 0.0)

    def agg(freq: str) -> pd.DataFrame:
        tmp = df.set_index("date_exec").copy()
        tmp["period"] = tmp.index.to_period(freq).astype(str)
        g = tmp.groupby(["courier", "period"], as_index=False).agg(
            charges_signed=("charges_signed", "sum"),
            revenues_gross=("revenues_signed", "sum"),
        )
        g["charges_gross"] = g["charges_signed"].abs()
        g["net_gross"] = g["revenues_gross"] - g["charges_gross"]
        return g[["courier", "period", "charges_gross", "revenues_gross", "net_gross"]].sort_values(["courier", "period"])

    return {"weekly": agg("W"), "monthly": agg("M"), "quarterly": agg("Q"), "yearly": agg("Y")}


# -----------------------------------------------------------------------------
# Upload helpers (zip-aware)
# -----------------------------------------------------------------------------

def iter_uploaded_files(uploaded) -> Iterable[Tuple[str, bytes]]:
    """
    Yields (filename, bytes) for:
      - normal uploaded files
      - zip files (recursively extracts file entries)
    """
    if not uploaded:
        return
    for f in uploaded:
        raw = f.getvalue()
        name = f.name

        if name.lower().endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(raw), "r") as z:
                for info in z.infolist():
                    if info.is_dir():
                        continue
                    inner_name = info.filename.split("/")[-1]
                    if not inner_name:
                        continue
                    yield inner_name, z.read(info)
        else:
            yield name, raw


# -----------------------------------------------------------------------------
# Streamlit UI
# -----------------------------------------------------------------------------

st.set_page_config(page_title=APP_TITLE, layout="wide")

def main():
    st.title(APP_TITLE)
    st.caption(f"{APP_VERSION} — DB URL never shown")

    engine = get_engine()
    init_db(engine)

    with st.sidebar:
        st.header("VAT defaults (courier invoices -> gross)")
        vat_gr = st.selectbox("Greece VAT", options=[0.24, 0.13, 0.0], index=0)
        vat_cy = st.selectbox("Cyprus VAT", options=[0.19, 0.05, 0.0], index=0)

        st.header("Dedupe")
        skip_identical = st.toggle("Skip identical files (by file_hash)", value=True)

    st.subheader("1) Upload files (you can also upload ZIPs)")

    left, right = st.columns(2)

    with left:
        st.markdown("**Courier charges**")
        up_boxnow = st.file_uploader("BoxNow PDFs / ZIP", type=["pdf", "zip"], accept_multiple_files=True, key="boxnow")
        up_acsgr = st.file_uploader("ACS GR XLSX / ZIP", type=["xlsx", "zip"], accept_multiple_files=True, key="acsgr")
        up_cc = st.file_uploader("Courier Center XLS / ZIP", type=["xls", "zip"], accept_multiple_files=True, key="cc")
        up_acscy = st.file_uploader("ACS CY PDFs / ZIP", type=["pdf", "zip"], accept_multiple_files=True, key="acscy")

    with right:
        st.markdown("**Odoo exports**")
        up_pick_orders = st.file_uploader("Pickings Orders (.xlsx)", type=["xlsx"], accept_multiple_files=False, key="pick_orders")
        up_pick_returns = st.file_uploader("Pickings Returns (.xlsx)", type=["xlsx"], accept_multiple_files=False, key="pick_returns")
        up_sales = st.file_uploader("Sales Orders all (.xlsx)", type=["xlsx"], accept_multiple_files=False, key="sales")
        up_rma = st.file_uploader("RMA all (.xlsx)", type=["xlsx"], accept_multiple_files=False, key="rma")

    st.subheader("2) Process uploads")

    if st.button("Process now", type="primary"):
        ok = skipped = failed = 0
        errors: List[str] = []

        def handle_file_bytes(filename: str, b: bytes, ftype: str) -> Tuple[str, bool]:
            h = sha256_bytes(b)
            already = upload_exists(engine, h)
            if (not already) or (not skip_identical):
                upsert_upload(engine, h, filename, ftype)
            return h, already

        # Courier: BOXNOW
        for fname, b in iter_uploaded_files(up_boxnow):
            try:
                h, already = handle_file_bytes(fname, b, "boxnow_pdf")
                if already and skip_identical:
                    skipped += 1
                    continue
                df = parse_boxnow_pdf(b)
                rows = normalize_charges(df, "BOXNOW", "GR", vat_gr, h)
                o, f2 = upsert_many(engine, "courier_charges", rows, "row_hash")
                ok += o
                failed += f2
            except Exception as e:
                failed += 1
                errors.append(f"BOXNOW {fname}: {e}")

        # Courier: ACS GR
        for fname, b in iter_uploaded_files(up_acsgr):
            try:
                h, already = handle_file_bytes(fname, b, "acs_gr_xlsx")
                if already and skip_identical:
                    skipped += 1
                    continue
                df = parse_acs_gr_xlsx(b)
                rows = normalize_charges(df, "ACS_GR", "GR", vat_gr, h)
                o, f2 = upsert_many(engine, "courier_charges", rows, "row_hash")
                ok += o
                failed += f2
            except Exception as e:
                failed += 1
                errors.append(f"ACS_GR {fname}: {e}")

        # Courier: COURIER CENTER
        for fname, b in iter_uploaded_files(up_cc):
            try:
                h, already = handle_file_bytes(fname, b, "courier_center_xls")
                if already and skip_identical:
                    skipped += 1
                    continue
                df = parse_courier_center_xls(b)
                rows = normalize_charges(df, "COURIER_CENTER", "GR", vat_gr, h)
                o, f2 = upsert_many(engine, "courier_charges", rows, "row_hash")
                ok += o
                failed += f2
            except Exception as e:
                failed += 1
                errors.append(f"COURIER_CENTER {fname}: {e}")

        # Courier: ACS CY
        for fname, b in iter_uploaded_files(up_acscy):
            try:
                h, already = handle_file_bytes(fname, b, "acs_cy_pdf")
                if already and skip_identical:
                    skipped += 1
                    continue
                df = parse_acs_cy_pdf(b)
                rows = normalize_charges(df, "ACS_CY", "CY", vat_cy, h)
                o, f2 = upsert_many(engine, "courier_charges", rows, "row_hash")
                ok += o
                failed += f2
            except Exception as e:
                failed += 1
                errors.append(f"ACS_CY {fname}: {e}")

        # Odoo: pickings orders
        if up_pick_orders is not None:
            try:
                b = up_pick_orders.getvalue()
                h, already = handle_file_bytes(up_pick_orders.name, b, "odoo_pickings_orders")
                if not (already and skip_identical):
                    df = parse_odoo_pickings_orders_xlsx(b)
                    df["source_file_hash"] = h
                    o, f2 = upsert_many(engine, "odoo_pickings", df.to_dict("records"), "picking_name")
                    ok += o
                    failed += f2
                else:
                    skipped += 1
            except Exception as e:
                failed += 1
                errors.append(f"Odoo pickings orders: {e}")

        # Odoo: pickings returns
        if up_pick_returns is not None:
            try:
                b = up_pick_returns.getvalue()
                h, already = handle_file_bytes(up_pick_returns.name, b, "odoo_pickings_returns")
                if not (already and skip_identical):
                    df = parse_odoo_pickings_returns_xlsx(b)
                    df["source_file_hash"] = h
                    o, f2 = upsert_many(engine, "odoo_returns", df.to_dict("records"), "picking_name")
                    ok += o
                    failed += f2
                else:
                    skipped += 1
            except Exception as e:
                failed += 1
                errors.append(f"Odoo pickings returns: {e}")

        # Odoo: sales
        if up_sales is not None:
            try:
                b = up_sales.getvalue()
                h, already = handle_file_bytes(up_sales.name, b, "odoo_sales_orders")
                if not (already and skip_identical):
                    df = parse_odoo_sales_orders_xlsx(b)
                    df["source_file_hash"] = h
                    o, f2 = upsert_many(engine, "odoo_sales_courier_fees", df.to_dict("records"), "order_ref")
                    ok += o
                    failed += f2
                else:
                    skipped += 1
            except Exception as e:
                failed += 1
                errors.append(f"Odoo sales orders: {e}")

        # Odoo: RMA
        if up_rma is not None:
            try:
                b = up_rma.getvalue()
                h, already = handle_file_bytes(up_rma.name, b, "odoo_rma")
                if not (already and skip_identical):
                    df = parse_odoo_rma_xlsx(b)
                    df["source_file_hash"] = h
                    o, f2 = upsert_many(engine, "odoo_rma_fees", df.to_dict("records"), "rma_ref")
                    ok += o
                    failed += f2
                else:
                    skipped += 1
            except Exception as e:
                failed += 1
                errors.append(f"Odoo RMA: {e}")

        st.success(f"Done. OK rows={ok} | Skipped files={skipped} | Failed={failed}")

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
        "sales_fee_rows": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM odoo_sales_courier_fees")["n"].iloc[0]),
        "rma_fee_rows": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM odoo_rma_fees")["n"].iloc[0]),
    }
    st.json(counts)

    excluded = int(fetch_df(
        engine,
        "SELECT COUNT(*) AS n FROM courier_charges WHERE excluded_reason IS NOT NULL"
    )["n"].iloc[0])
    st.write(f"Excluded legacy 7-digit rows (old e-shop): {excluded}")

    ledger = build_ledger(engine)
    sums = summarize_periods(ledger)

    t1, t2, t3, t4 = st.tabs(["Weekly", "Monthly", "Quarterly", "Yearly"])
    with t1:
        st.dataframe(sums["weekly"], use_container_width=True, height=360)
    with t2:
        st.dataframe(sums["monthly"], use_container_width=True, height=360)
    with t3:
        st.dataframe(sums["quarterly"], use_container_width=True, height=360)
    with t4:
        st.dataframe(sums["yearly"], use_container_width=True, height=360)

    st.subheader("4) Export")

    if st.button("Build Excel export"):
        ch = fetch_df(engine, "SELECT * FROM courier_charges")
        so = fetch_df(engine, "SELECT * FROM odoo_sales_courier_fees")
        rma = fetch_df(engine, "SELECT * FROM odoo_rma_fees")
        pk = fetch_df(engine, "SELECT * FROM odoo_pickings")
        rt = fetch_df(engine, "SELECT * FROM odoo_returns")

        out_path = "tfp_courier_monitor_export.xlsx"
        with pd.ExcelWriter(out_path, engine="openpyxl") as xl:
            ch.to_excel(xl, index=False, sheet_name="courier_charges")
            so.to_excel(xl, index=False, sheet_name="odoo_sales_fees")
            rma.to_excel(xl, index=False, sheet_name="odoo_rma_fees")
            pk.to_excel(xl, index=False, sheet_name="odoo_pickings")
            rt.to_excel(xl, index=False, sheet_name="odoo_returns")

            sums["weekly"].to_excel(xl, index=False, sheet_name="summary_weekly")
            sums["monthly"].to_excel(xl, index=False, sheet_name="summary_monthly")
            sums["quarterly"].to_excel(xl, index=False, sheet_name="summary_quarterly")
            sums["yearly"].to_excel(xl, index=False, sheet_name="summary_yearly")

        with open(out_path, "rb") as f:
            st.download_button("Download export", data=f, file_name=out_path)

main()
