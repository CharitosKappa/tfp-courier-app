
# TFP Courier Cost Monitor (Streamlit) — v7.3
# ------------------------------------------------------------
# What changed vs older versions:
# - Single "Upload everything" zone (ZIP/PDF/XLS/XLSX) with AUTO detection.
# - Progress bar + per-file status + rough ETA so it never "looks stuck".
# - Excludes invoice TOTAL / VAT / summary lines from calculations.
# - Bulk inserts/upserts to Neon (Postgres) for speed.
# - Never prints DB URL; reads from Streamlit secrets or DATABASE_URL.
#
# Notes:
# - ACS CY / BoxNow PDFs: amount extraction is best-effort (text PDFs only).
# - If a file is scanned PDF, refs might still be found; amounts may be missing.

import os
import io
import re
import zipfile
import time
import hashlib
import datetime as dt
from dataclasses import dataclass
from typing import Iterable, List, Dict, Optional, Tuple

import pandas as pd
import streamlit as st

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from pypdf import PdfReader


APP_TITLE = "TFP Courier Cost Monitor"
APP_VERSION = "v7.3 (auto-detect + progress + total/VAT exclusion)"


# -----------------------
# 1) Helpers / regex
# -----------------------

LGK_RE = re.compile(r"(LGK/OUT/\d+)(?:_\d+)?", re.IGNORECASE)
RMA_RE = re.compile(r"(RMA\d+)", re.IGNORECASE)
LEGACY_7DIGIT_EXACT_RE = re.compile(r"^\d{7}$")
TRACKING_NUM_RE = re.compile(r"\b\d{9,20}\b")

COURIER_SUMMARY_KEYWORDS = [
    "ΣΥΝΟΛΟ", "ΓΕΝΙΚΟ ΣΥΝΟΛΟ", "ΣΥΝΟΛΙΚΟ", "ΜΕΡΙΚΟ ΣΥΝΟΛΟ",
    "TOTAL", "GRAND TOTAL", "SUBTOTAL",
    "ΦΠΑ", "FPA", "VAT", "V.A.T", "TAX",
    "ΠΛΗΡΩΤΕΟ", "ΥΠΟΛΟΙΠΟ", "AMOUNT DUE", "PAYABLE",
]

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def norm_str(x) -> str:
    if x is None:
        return ""
    return str(x).strip()

def to_float(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)) and pd.notna(x):
        return float(x)
    s = norm_str(x)
    if not s or s.lower() in ("nan","none","nat"):
        return None
    # accept comma decimals
    s = s.replace(".", "").replace(",", ".") if re.search(r"\d+,\d+", s) else s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def parse_date_any(x) -> Optional[pd.Timestamp]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    try:
        return pd.to_datetime(x, errors="coerce", dayfirst=True)
    except Exception:
        return None

def extract_pdf_text(file_bytes: bytes, max_pages: int = 15) -> str:
    try:
        reader = PdfReader(io.BytesIO(file_bytes))
        chunks = []
        for p in reader.pages[:max_pages]:
            chunks.append(p.extract_text() or "")
        return "\n".join(chunks)
    except Exception:
        return ""

def detect_refs(s: str) -> Dict[str, Optional[str]]:
    s = norm_str(s)
    out = {"lgk": None, "rma": None, "legacy7": None, "tracking": None}
    m = LGK_RE.search(s)
    if m:
        out["lgk"] = m.group(1).upper()
    m = RMA_RE.search(s)
    if m:
        out["rma"] = m.group(1).upper()
    if LEGACY_7DIGIT_EXACT_RE.match(s):
        out["legacy7"] = s
    m = TRACKING_NUM_RE.search(s)
    if m:
        out["tracking"] = m.group(0)
    return out

def looks_like_summary_line(ref_raw: str, ref_desc: str) -> bool:
    text = f"{(ref_raw or '').upper()} {(ref_desc or '').upper()}".strip()
    if not text:
        return False
    if LGK_RE.search(text) or RMA_RE.search(text) or TRACKING_NUM_RE.search(text):
        return False
    return any(k in text for k in COURIER_SUMMARY_KEYWORDS)

def safe_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


# -----------------------
# 2) DB (Neon) + schema
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

def init_db(engine: Engine) -> None:
    """Create tables if missing and perform light schema migrations."""
    ddl = """
    CREATE TABLE IF NOT EXISTS uploads (
      file_hash TEXT PRIMARY KEY,
      filename TEXT,
      detected_type TEXT,
      uploaded_at TEXT
    );

    CREATE TABLE IF NOT EXISTS courier_charges (
      row_hash TEXT PRIMARY KEY,
      courier TEXT,
      country TEXT,
      invoice_no TEXT,
      invoice_date TEXT,
      voucher_no TEXT,
      ref_raw TEXT,
      ref_desc TEXT,
      ref_lgk TEXT,
      ref_rma TEXT,
      tracking_no TEXT,
      amount_net REAL,
      vat_rate REAL,
      amount_gross REAL,
      currency TEXT,
      source_file_hash TEXT,
      excluded_reason TEXT
    );

    CREATE TABLE IF NOT EXISTS odoo_pickings (
      picking_name TEXT PRIMARY KEY,
      picking_type TEXT,
      done_date TEXT,
      scheduled_date TEXT,
      origin TEXT,
      partner_country TEXT,
      carrier TEXT,
      tracking TEXT,
      source_file_hash TEXT
    );

    CREATE TABLE IF NOT EXISTS odoo_sales_courier_fees (
      order_ref TEXT PRIMARY KEY,
      order_date TEXT,
      partner_country TEXT,
      invoiced_qty REAL,
      fee_gross REAL,
      currency TEXT,
      source_file_hash TEXT
    );

    CREATE TABLE IF NOT EXISTS odoo_rma_fees (
      rma_ref TEXT PRIMARY KEY,
      done_date TEXT,
      partner_country TEXT,
      service_fee_gross REAL,
      currency TEXT,
      source_file_hash TEXT
    );
    """

    backend = db_backend(engine)

    with engine.begin() as cx:
        # create tables
        for stmt in ddl.split(";"):
            s = stmt.strip()
            if s:
                cx.execute(text(s))

        # migrations (safe no-ops if columns exist)
        def col_exists(table: str, col: str) -> bool:
            if backend == "sqlite":
                rows = cx.execute(text(f"PRAGMA table_info({table})")).fetchall()
                return any(r[1] == col for r in rows)
            q = """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name=:t AND column_name=:c
            LIMIT 1
            """
            return cx.execute(text(q), {"t": table, "c": col}).fetchone() is not None

        def add_col(table: str, col: str, coltype: str) -> None:
            if col_exists(table, col):
                return
            cx.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}"))

        # courier_charges columns we rely on
        for col, typ in [
            ("voucher_no", "TEXT"),
            ("ref_desc", "TEXT"),
            ("tracking_no", "TEXT"),
            ("excluded_reason", "TEXT"),
        ]:
            add_col("courier_charges", col, typ)

        # odoo_pickings
        add_col("odoo_pickings", "picking_type", "TEXT")


def db_backend(engine: Engine) -> str:
    return engine.url.get_backend_name()

def upsert_upload(engine: Engine, file_hash: str, filename: str, detected_type: str) -> None:
    now = dt.datetime.utcnow().isoformat()
    if db_backend(engine) == "sqlite":
        sql = """INSERT OR REPLACE INTO uploads(file_hash, filename, detected_type, uploaded_at)
                 VALUES(:h,:fn,:dt,:at)"""
    else:
        sql = """INSERT INTO uploads(file_hash, filename, detected_type, uploaded_at)
                 VALUES(:h,:fn,:dt,:at)
                 ON CONFLICT (file_hash) DO UPDATE SET
                 filename=EXCLUDED.filename,
                 detected_type=EXCLUDED.detected_type,
                 uploaded_at=EXCLUDED.uploaded_at"""
    with engine.begin() as cx:
        cx.execute(text(sql), {"h": file_hash, "fn": filename, "dt": detected_type, "at": now})

def fetch_df(engine: Engine, sql: str) -> pd.DataFrame:
    with engine.begin() as cx:
        return pd.read_sql(text(sql), cx)

def upsert_many(engine: Engine, table: str, rows: List[dict], pk: str, chunk: int = 2000) -> Tuple[int, int]:
    if not rows:
        return 0, 0
    ok = 0
    failed = 0
    cols = list(rows[0].keys())
    placeholders = ",".join([f":{c}" for c in cols])
    col_list = ",".join(cols)
    if db_backend(engine) == "sqlite":
        sql = f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})"
    else:
        updates = ",".join([f"{c}=EXCLUDED.{c}" for c in cols if c != pk])
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT ({pk}) DO UPDATE SET {updates}"

    with engine.begin() as cx:
        for i in range(0, len(rows), chunk):
            batch = rows[i:i+chunk]
            try:
                cx.execute(text(sql), batch)
                ok += len(batch)
            except Exception:
                # fallback: per-row to isolate failures
                for r in batch:
                    try:
                        cx.execute(text(sql), r)
                        ok += 1
                    except Exception:
                        failed += 1
    return ok, failed


# -----------------------
# 3) File iterator (ZIP + non-ZIP)
# -----------------------

@dataclass
class InFile:
    name: str
    bytes: bytes

def iter_files(uploaded: List[st.runtime.uploaded_file_manager.UploadedFile]) -> Iterable[InFile]:
    for uf in uploaded or []:
        b = uf.getvalue()
        name = uf.name
        if name.lower().endswith(".zip"):
            try:
                with zipfile.ZipFile(io.BytesIO(b), "r") as z:
                    for info in z.infolist():
                        if info.is_dir():
                            continue
                        inner = info.filename
                        inner_bytes = z.read(info)
                        yield InFile(name=f"{name}:{inner}", bytes=inner_bytes)
            except Exception:
                yield InFile(name=name, bytes=b)
        else:
            yield InFile(name=name, bytes=b)


# -----------------------
# 4) Detectors + parsers
# -----------------------

def detect_xlsx_type(df: pd.DataFrame) -> Optional[str]:
    cols = set([c.strip() for c in df.columns])
    # RMA
    if "Service Cost" in cols and "Name" in cols:
        return "odoo_rma"
    # Sales
    if any(c.startswith("Order Lines/") for c in cols) and "Order Reference" in cols:
        return "odoo_sales"
    # Pickings (orders/returns)
    if "Reference" in cols and ("Tracking Reference" in cols or "Carrier" in cols) and ("Scheduled Date" in cols or "Date of Transfer" in cols):
        return "odoo_pickings"
    # ACS GR
    if "Καθαρή Αξία Τιμολογίου" in cols and ("Αριθμός Αποδεικτικού" in cols or "Σχετικό 1" in cols):
        return "acs_gr_xlsx"
    return None

def parse_odoo_pickings(df: pd.DataFrame, picking_type_guess: str) -> pd.DataFrame:
    c_name = safe_col(df, ["Reference", "Name"]) or df.columns[0]
    c_done = safe_col(df, ["Date of Transfer", "Date Done", "Done Date", "Completed Date"])
    c_sched = safe_col(df, ["Scheduled Date", "Scheduled date"])
    c_origin = safe_col(df, ["Source Document", "Origin"])
    c_country = safe_col(df, ["Destination Country", "Partner/Country", "Partner Country", "Country"])
    c_carrier = safe_col(df, ["Carrier", "Delivery Method"])
    c_track = safe_col(df, ["Tracking Reference", "Tracking", "tracking_reference"])

    out = pd.DataFrame({
        "picking_name": df[c_name].astype(str).str.strip(),
        "picking_type": picking_type_guess,
        "done_date": df[c_done] if c_done else None,
        "scheduled_date": df[c_sched] if c_sched else None,
        "origin": df[c_origin] if c_origin else None,
        "partner_country": df[c_country] if c_country else None,
        "carrier": df[c_carrier] if c_carrier else None,
        "tracking": df[c_track] if c_track else None,
    })
    for c in ["done_date","scheduled_date"]:
        out[c] = pd.to_datetime(out[c], errors="coerce", dayfirst=True).dt.date.astype(str)
        out.loc[out[c].isin(["NaT","None","nan"]), c] = None
    for c in ["origin","partner_country","carrier","tracking"]:
        out[c] = out[c].astype(str).str.strip()
        out.loc[out[c].isin(["nan","None",""]), c] = None
    out = out.dropna(subset=["picking_name"])
    return out

def parse_odoo_sales(df: pd.DataFrame) -> pd.DataFrame:
    c_order = safe_col(df, ["Order Reference"])
    c_date = safe_col(df, ["Order Date"])
    c_country = safe_col(df, ["Partner/Country", "Partner Country", "Country"])
    c_line_name = safe_col(df, ["Order Lines/Name"])
    c_price = safe_col(df, ["Order Lines/Unit Price"])
    c_qty = safe_col(df, ["Order Lines/Invoiced Quantity"])
    c_currency = safe_col(df, ["Currency"])

    if not (c_order and c_line_name and c_price and c_qty):
        raise KeyError("Sales Orders: missing required columns.")
    tmp = df[[c_order, c_date, c_country, c_line_name, c_price, c_qty] + ([c_currency] if c_currency else [])].copy()
    tmp.columns = ["order_ref","order_date","partner_country","line_name","unit_price","invoiced_qty"] + (["currency"] if c_currency else [])
    name_lc = tmp["line_name"].astype(str).str.lower()
    mask_fee = (
        name_lc.str.contains("courier") | name_lc.str.contains("shipping") |
        name_lc.str.contains("μεταφορ") | name_lc.str.contains("έξοδα αποστολ") | name_lc.str.contains("εξοδα αποστολ")
    )
    tmp = tmp[mask_fee].copy()
    tmp["invoiced_qty"] = pd.to_numeric(tmp["invoiced_qty"], errors="coerce").fillna(0.0)
    tmp = tmp[tmp["invoiced_qty"] > 0].copy()
    tmp["unit_price"] = pd.to_numeric(tmp["unit_price"], errors="coerce").fillna(0.0)
    tmp["fee_gross"] = tmp["unit_price"] * tmp["invoiced_qty"]
    agg = tmp.groupby("order_ref", as_index=False).agg(
        order_date=("order_date","first"),
        partner_country=("partner_country","first"),
        invoiced_qty=("invoiced_qty","sum"),
        fee_gross=("fee_gross","sum"),
        currency=(("currency","first") if "currency" in tmp.columns else ("order_ref","size")),
    )
    if "currency" not in agg.columns:
        agg["currency"] = "EUR"
    agg["order_date"] = pd.to_datetime(agg["order_date"], errors="coerce", dayfirst=True).dt.date.astype(str)
    agg.loc[agg["order_date"].isin(["NaT","None","nan"]), "order_date"] = None
    return agg

def parse_odoo_rma(df: pd.DataFrame) -> pd.DataFrame:
    c_rma = safe_col(df, ["Name", "Reference"])
    c_done = safe_col(df, ["Done Date", "Date Done", "Completed Date", "Date"])
    c_country = safe_col(df, ["Partner/Country", "Partner Country", "Country", "Partner Shipping"])
    c_fee = safe_col(df, ["Service Cost", "Return Fee", "Service Fee", "Fee"])
    if not (c_rma and c_fee):
        raise KeyError("RMA: missing required columns.")
    out = pd.DataFrame({
        "rma_ref": df[c_rma].astype(str).str.strip(),
        "done_date": df[c_done] if c_done else None,
        "partner_country": df[c_country] if c_country else None,
        "service_fee_gross": df[c_fee],
    })
    out["service_fee_gross"] = pd.to_numeric(out["service_fee_gross"], errors="coerce").fillna(0.0)
    out = out[out["service_fee_gross"] != 0].copy()
    out["done_date"] = pd.to_datetime(out["done_date"], errors="coerce", dayfirst=True).dt.date.astype(str)
    out.loc[out["done_date"].isin(["NaT","None","nan"]), "done_date"] = None
    out["currency"] = "EUR"
    out = out.dropna(subset=["rma_ref"])
    return out

def parse_acs_gr_xlsx(df: pd.DataFrame) -> pd.DataFrame:
    c_amt = safe_col(df, ["Καθαρή Αξία Τιμολογίου", "Αξία Βασικών Υπηρεσιών", "Αξία"])
    c_ref1 = safe_col(df, ["Σχετικό 1"])
    c_ref2 = safe_col(df, ["Σχετικό 2"])
    c_voucher = safe_col(df, ["Αριθμός Αποδεικτικού"])
    c_date = safe_col(df, ["Ημ/νια παραλαβής", "Ημ/νια παράδοσης", "Ημ/νια παραλαβης", "Ημ/νια"])
    c_desc = safe_col(df, ["Πρόσθετες Υπηρεσίες", "Προορισμός", "Παραλήπτης", "Αποστολέας"])
    if not c_amt:
        raise KeyError("ACS GR: missing amount column.")
    ref_raw = None
    if c_ref1 or c_ref2 or c_voucher:
        # prefer related fields, else voucher
        s1 = df[c_ref1] if c_ref1 else None
        s2 = df[c_ref2] if c_ref2 else None
        ref_raw = (s1.fillna("") if s1 is not None else "").astype(str) if s1 is not None else None
        # build ref_raw with fallback order
        tmp = pd.Series([""] * len(df))
        if c_ref1:
            tmp = df[c_ref1].astype(str).where(df[c_ref1].notna(), "")
        if c_ref2:
            tmp = tmp.where(tmp.str.strip() != "", df[c_ref2].astype(str).where(df[c_ref2].notna(), ""))
        if c_voucher:
            tmp = tmp.where(tmp.str.strip() != "", df[c_voucher].astype(str).where(df[c_voucher].notna(), ""))
        ref_raw = tmp
    else:
        ref_raw = df.iloc[:,0].astype(str)

    out = pd.DataFrame({
        "invoice_date": df[c_date] if c_date else None,
        "invoice_no": None,
        "voucher_no": df[c_voucher] if c_voucher else None,
        "ref_raw": ref_raw.astype(str).str.strip(),
        "ref_desc": df[c_desc] if c_desc else None,
        "amount_net": df[c_amt],
    })
    out["amount_net"] = out["amount_net"].apply(to_float)
    out = out.dropna(subset=["amount_net"])
    return out

def parse_courier_center_xls(df: pd.DataFrame) -> pd.DataFrame:
    c_ref = safe_col(df, ["Σχετικό", "Παραστατικό", "Reference", "Ref"]) or df.columns[0]
    c_desc = safe_col(df, ["Αιτιολογία", "Notes", "Περιγραφή", "Description", "Σχόλια"])
    c_amt = safe_col(df, ["Ποσό", "Χρέωση", "Amount", "Net", "Αξία"])
    c_date = safe_col(df, ["Ημερομηνία", "Date", "Invoice Date"])
    if not c_amt:
        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        c_amt = num_cols[-1] if num_cols else df.columns[-1]
    out = pd.DataFrame({
        "invoice_date": df[c_date] if c_date else None,
        "invoice_no": None,
        "voucher_no": None,
        "ref_raw": df[c_ref].astype(str).str.strip(),
        "ref_desc": df[c_desc] if c_desc else None,
        "amount_net": df[c_amt],
    })
    out["amount_net"] = out["amount_net"].apply(to_float)
    out = out.dropna(subset=["amount_net"])
    return out

def parse_boxnow_pdf(file_bytes: bytes) -> pd.DataFrame:
    text = extract_pdf_text(file_bytes, max_pages=15)
    refs = re.findall(r"LGK/OUT/\d+(?:_\d+)?", text, flags=re.IGNORECASE)
    out = pd.DataFrame({"ref_raw": [r.upper() for r in refs]})
    out["invoice_no"] = None
    out["invoice_date"] = None
    out["voucher_no"] = None
    out["ref_desc"] = None
    out["amount_net"] = None
    return out.drop_duplicates()

def parse_acs_cy_pdf(file_bytes: bytes) -> pd.DataFrame:
    text = extract_pdf_text(file_bytes, max_pages=15)
    refs = re.findall(r"LGK/OUT/\d+(?:_\d+)?|RMA\d+|\b\d{7}\b", text, flags=re.IGNORECASE)
    out = pd.DataFrame({"ref_raw": [r.upper() for r in refs]})
    out["invoice_no"] = None
    out["invoice_date"] = None
    out["voucher_no"] = None
    out["ref_desc"] = None
    out["amount_net"] = None
    return out.drop_duplicates()


# -----------------------
# 5) Normalization (courier charges)
# -----------------------

def normalize_charges(df: pd.DataFrame, courier: str, country_default: str, vat_rate: float, source_file_hash: str, currency: str = "EUR") -> List[dict]:
    rows: List[dict] = []
    vat = float(vat_rate or 0.0)
    for _, r in df.iterrows():
        ref_raw = norm_str(r.get("ref_raw"))
        ref_desc = norm_str(r.get("ref_desc"))
        refs = detect_refs(ref_raw)

        excluded_reason = "legacy_7digit_old_eshop" if refs["legacy7"] else None
        if excluded_reason is None:
            has_shipment_ref = bool(refs.get("lgk") or refs.get("rma") or refs.get("tracking"))
            if (not has_shipment_ref) and looks_like_summary_line(ref_raw, ref_desc):
                excluded_reason = "courier_summary_total_or_vat"

        amount_net = to_float(r.get("amount_net"))
        amount_gross = (amount_net * (1.0 + vat)) if amount_net is not None else None

        inv_dt = parse_date_any(r.get("invoice_date"))
        inv_dt_str = inv_dt.date().isoformat() if inv_dt is not None and pd.notna(inv_dt) else None

        invoice_no = norm_str(r.get("invoice_no")) or None
        voucher_no = norm_str(r.get("voucher_no")) or None

        basis = f"{courier}|{country_default}|{invoice_no}|{voucher_no}|{inv_dt_str}|{ref_raw}|{ref_desc}|{amount_net}|{vat}|{amount_gross}|{currency}|{source_file_hash}"
        row_hash = sha256_bytes(basis.encode("utf-8"))

        rows.append({
            "row_hash": row_hash,
            "courier": courier,
            "country": country_default,
            "invoice_no": invoice_no,
            "invoice_date": inv_dt_str,
            "voucher_no": voucher_no,
            "ref_raw": ref_raw,
            "ref_desc": ref_desc or None,
            "ref_lgk": refs["lgk"],
            "ref_rma": refs["rma"],
            "tracking_no": refs["tracking"],
            "amount_net": amount_net,
            "vat_rate": vat,
            "amount_gross": amount_gross,
            "currency": currency,
            "source_file_hash": source_file_hash,
            "excluded_reason": excluded_reason,
        })
    return rows


# -----------------------
# 6) Ledger + summaries
# -----------------------

def build_ledger(engine: Engine) -> pd.DataFrame:
    frames = []

    ch = fetch_df(engine, "SELECT * FROM courier_charges")
    if not ch.empty:
        ch = ch.copy()
        ch["entry_type"] = "CHARGE"
        ch["ref_key"] = ch["ref_lgk"].fillna(ch["ref_rma"]).fillna(ch["tracking_no"]).fillna(ch["ref_raw"])
        ch["date_exec"] = pd.to_datetime(ch["invoice_date"], errors="coerce")
        ch["amount_gross_signed"] = pd.to_numeric(ch["amount_gross"], errors="coerce") * (-1.0)
        frames.append(ch[["entry_type","courier","country","ref_key","date_exec","amount_gross_signed","excluded_reason"]])

    so = fetch_df(engine, "SELECT * FROM odoo_sales_courier_fees")
    if not so.empty:
        so = so.copy()
        so["entry_type"] = "REVENUE_SHIPPING"
        so["courier"] = "UNKNOWN"
        so["country"] = so["partner_country"]
        so["ref_key"] = so["order_ref"].astype(str)
        so["date_exec"] = pd.to_datetime(so["order_date"], errors="coerce")
        so["amount_gross_signed"] = pd.to_numeric(so["fee_gross"], errors="coerce")
        so["excluded_reason"] = None
        frames.append(so[["entry_type","courier","country","ref_key","date_exec","amount_gross_signed","excluded_reason"]])

    rma = fetch_df(engine, "SELECT * FROM odoo_rma_fees")
    if not rma.empty:
        rma = rma.copy()
        rma["entry_type"] = "REVENUE_RMA_SERVICE"
        rma["courier"] = "UNKNOWN"
        rma["country"] = rma["partner_country"]
        rma["ref_key"] = rma["rma_ref"].astype(str)
        rma["date_exec"] = pd.to_datetime(rma["done_date"], errors="coerce")
        rma["amount_gross_signed"] = pd.to_numeric(rma["service_fee_gross"], errors="coerce")
        rma["excluded_reason"] = None
        frames.append(rma[["entry_type","courier","country","ref_key","date_exec","amount_gross_signed","excluded_reason"]])

    if not frames:
        return pd.DataFrame(columns=["entry_type","courier","country","ref_key","date_exec","amount_gross_signed","excluded_reason"])
    return pd.concat(frames, ignore_index=True)

def summarize_periods(ledger: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    empty = pd.DataFrame(columns=["courier","period","charges_gross","revenues_gross","net_gross"])
    if ledger.empty or ledger["date_exec"].isna().all():
        return {"weekly": empty, "monthly": empty, "quarterly": empty, "yearly": empty}

    df = ledger.copy()
    df = df[~df["excluded_reason"].fillna("").str.contains("legacy_7digit", case=False)]
    df = df[~df["excluded_reason"].fillna("").str.contains("courier_summary_total_or_vat", case=False)]
    df["courier"] = df["courier"].fillna("UNKNOWN")
    df["is_charge"] = df["entry_type"].eq("CHARGE")
    df["charges"] = df["amount_gross_signed"].where(df["is_charge"], 0.0)
    df["revenues"] = df["amount_gross_signed"].where(~df["is_charge"], 0.0)

    def agg(freq: str) -> pd.DataFrame:
        tmp = df.set_index("date_exec").copy()
        tmp["period"] = tmp.index.to_period(freq).astype(str)
        g = tmp.groupby(["courier","period"], as_index=False).agg(
            charges_gross=("charges","sum"),
            revenues_gross=("revenues","sum"),
        )
        g["charges_gross"] = g["charges_gross"].abs()
        g["net_gross"] = g["revenues_gross"] - g["charges_gross"]
        return g.sort_values(["courier","period"])
    return {"weekly": agg("W"), "monthly": agg("M"), "quarterly": agg("Q"), "yearly": agg("Y")}


# -----------------------
# 7) Streamlit UI
# -----------------------

st.set_page_config(page_title=APP_TITLE, layout="wide")

def main():
    st.title(APP_TITLE)
    st.caption(f"{APP_VERSION} — DB URL never shown")

    engine = get_engine()
    init_db(engine)

    with st.sidebar:
        st.header("VAT defaults (courier invoices)")
        vat_gr = st.selectbox("Greece VAT", options=[0.24, 0.13, 0.0], index=0)
        vat_cy = st.selectbox("Cyprus VAT", options=[0.19, 0.05, 0.0], index=0)
        st.header("Processing")
        skip_identical = st.toggle("Skip identical files (file_hash)", value=True)
        st.caption("Tip: If you changed code/logic and want to reprocess, turn this OFF.")

    st.subheader("1) Upload (ZIP / PDF / XLS / XLSX)")
    uploaded = st.file_uploader(
        "Drop everything here (you can mix ZIP and non-ZIP files)",
        type=["zip","pdf","xls","xlsx"],
        accept_multiple_files=True
    )

    st.subheader("2) Process uploads")
    if st.button("Process now", type="primary", disabled=not uploaded):
        # We build a concrete worklist first, so we know total steps.
        work = list(iter_files(uploaded))
        total = len(work)

        progress = st.progress(0)
        status = st.empty()
        details = st.empty()

        ok = skipped = failed = 0
        ok_rows = 0
        errors: List[str] = []

        start = time.time()

        # cache upload hashes in memory for this run
        seen_hashes = set(fetch_df(engine, "SELECT file_hash FROM uploads")["file_hash"].tolist())

        def update_ui(i: int, label: str):
            done = i + 1
            elapsed = time.time() - start
            rate = done / elapsed if elapsed > 0 else 0
            remaining = (total - done) / rate if rate > 0 else 0
            progress.progress(done / max(total, 1))
            status.write(f"Processing {done}/{total}: {label}")
            details.write(f"Elapsed: {elapsed:0.1f}s  |  ETA: {remaining:0.1f}s  |  Rate: {rate:0.2f} files/s")

        for i, inf in enumerate(work):
            update_ui(i, inf.name)

            try:
                h = sha256_bytes(inf.bytes)
                if (h in seen_hashes) and skip_identical:
                    skipped += 1
                    continue

                # Detect by extension first
                name_lc = inf.name.lower()
                detected = None

                if name_lc.endswith(".xlsx"):
                    df = pd.read_excel(io.BytesIO(inf.bytes))
                    detected = detect_xlsx_type(df) or "xlsx_unknown"

                    # Decide picking type guess by filename
                    picking_type_guess = "OUT"
                    if "return" in name_lc or "returns" in name_lc or "επιστρ" in name_lc:
                        picking_type_guess = "RETURN"

                    if detected == "odoo_pickings":
                        out = parse_odoo_pickings(df, picking_type_guess)
                        out["source_file_hash"] = h
                        upsert_upload(engine, h, inf.name, detected)
                        seen_hashes.add(h)
                        o, f2 = upsert_many(engine, "odoo_pickings", out.to_dict("records"), "picking_name")
                        ok_rows += o
                        failed += f2
                        ok += 1
                        continue

                    if detected == "odoo_sales":
                        out = parse_odoo_sales(df)
                        out["source_file_hash"] = h
                        upsert_upload(engine, h, inf.name, detected)
                        seen_hashes.add(h)
                        o, f2 = upsert_many(engine, "odoo_sales_courier_fees", out.to_dict("records"), "order_ref")
                        ok_rows += o
                        failed += f2
                        ok += 1
                        continue

                    if detected == "odoo_rma":
                        out = parse_odoo_rma(df)
                        out["source_file_hash"] = h
                        upsert_upload(engine, h, inf.name, detected)
                        seen_hashes.add(h)
                        o, f2 = upsert_many(engine, "odoo_rma_fees", out.to_dict("records"), "rma_ref")
                        ok_rows += o
                        failed += f2
                        ok += 1
                        continue

                    if detected == "acs_gr_xlsx":
                        raw = parse_acs_gr_xlsx(df)
                        upsert_upload(engine, h, inf.name, detected)
                        seen_hashes.add(h)
                        rows = normalize_charges(raw, "ACS_GR", "GR", vat_gr, h)
                        o, f2 = upsert_many(engine, "courier_charges", rows, "row_hash")
                        ok_rows += o
                        failed += f2
                        ok += 1
                        continue

                    # Unknown xlsx: store upload only (no crash)
                    upsert_upload(engine, h, inf.name, detected)
                    seen_hashes.add(h)
                    ok += 1
                    continue

                if name_lc.endswith(".xls"):
                    # Courier Center xls
                    df = pd.read_excel(io.BytesIO(inf.bytes), engine="xlrd")
                    detected = "courier_center_xls"
                    raw = parse_courier_center_xls(df)
                    upsert_upload(engine, h, inf.name, detected)
                    seen_hashes.add(h)
                    rows = normalize_charges(raw, "COURIER_CENTER", "GR", vat_gr, h)
                    o, f2 = upsert_many(engine, "courier_charges", rows, "row_hash")
                    ok_rows += o
                    failed += f2
                    ok += 1
                    continue

                if name_lc.endswith(".pdf"):
                    # Decide courier by path/name
                    if "acs" in name_lc and ("cy" in name_lc or "cyprus" in name_lc):
                        detected = "acs_cy_pdf"
                        raw = parse_acs_cy_pdf(inf.bytes)
                        upsert_upload(engine, h, inf.name, detected)
                        seen_hashes.add(h)
                        rows = normalize_charges(raw, "ACS_CY", "CY", vat_cy, h)
                        o, f2 = upsert_many(engine, "courier_charges", rows, "row_hash")
                        ok_rows += o
                        failed += f2
                        ok += 1
                        continue

                    if "box" in name_lc or "boxnow" in name_lc:
                        detected = "boxnow_pdf"
                        raw = parse_boxnow_pdf(inf.bytes)
                        upsert_upload(engine, h, inf.name, detected)
                        seen_hashes.add(h)
                        rows = normalize_charges(raw, "BOXNOW", "GR", vat_gr, h)
                        o, f2 = upsert_many(engine, "courier_charges", rows, "row_hash")
                        ok_rows += o
                        failed += f2
                        ok += 1
                        continue

                    # fallback: treat as BoxNow-ish (refs only) but do not crash
                    detected = "pdf_unknown"
                    raw = parse_boxnow_pdf(inf.bytes)
                    upsert_upload(engine, h, inf.name, detected)
                    seen_hashes.add(h)
                    rows = normalize_charges(raw, "UNKNOWN_PDF", "GR", vat_gr, h)
                    o, f2 = upsert_many(engine, "courier_charges", rows, "row_hash")
                    ok_rows += o
                    failed += f2
                    ok += 1
                    continue

                # If we reached here: unknown extension -> record upload
                detected = "unknown"
                upsert_upload(engine, h, inf.name, detected)
                seen_hashes.add(h)
                ok += 1

            except Exception as e:
                failed += 1
                errors.append(f"{inf.name}: {e}")

        st.success(f"Done. Files OK={ok}  Skipped={skipped}  Failed={failed} | Rows upserted OK={ok_rows}")
        if errors:
            with st.expander("Errors"):
                for e in errors:
                    st.write("- " + e)

    st.subheader("3) Monitoring")

    counts = {
        "uploads": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM uploads")["n"].iloc[0]),
        "courier_charges": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM courier_charges")["n"].iloc[0]),
        "odoo_pickings": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM odoo_pickings")["n"].iloc[0]),
        "odoo_sales_fees": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM odoo_sales_courier_fees")["n"].iloc[0]),
        "odoo_rma_fees": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM odoo_rma_fees")["n"].iloc[0]),
    }
    st.json(counts)

    excluded_total = int(fetch_df(engine, "SELECT COUNT(*) AS n FROM courier_charges WHERE excluded_reason IS NOT NULL")["n"].iloc[0])
    excluded_summary = int(fetch_df(engine, "SELECT COUNT(*) AS n FROM courier_charges WHERE excluded_reason='courier_summary_total_or_vat'")["n"].iloc[0])
    excluded_legacy = int(fetch_df(engine, "SELECT COUNT(*) AS n FROM courier_charges WHERE excluded_reason='legacy_7digit_old_eshop'")["n"].iloc[0])

    st.write(f"Excluded rows: {excluded_total}  |  Summary/VAT: {excluded_summary}  |  Legacy 7-digit: {excluded_legacy}")

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

        out_path = "tfp_courier_monitor_export.xlsx"
        with pd.ExcelWriter(out_path, engine="openpyxl") as xl:
            ch.to_excel(xl, index=False, sheet_name="courier_charges")
            pk.to_excel(xl, index=False, sheet_name="odoo_pickings")
            so.to_excel(xl, index=False, sheet_name="odoo_sales_fees")
            rma.to_excel(xl, index=False, sheet_name="odoo_rma_fees")
            sums["weekly"].to_excel(xl, index=False, sheet_name="summary_weekly")
            sums["monthly"].to_excel(xl, index=False, sheet_name="summary_monthly")
            sums["quarterly"].to_excel(xl, index=False, sheet_name="summary_quarterly")
            sums["yearly"].to_excel(xl, index=False, sheet_name="summary_yearly")

        with open(out_path, "rb") as f:
            st.download_button("Download export", data=f, file_name=out_path)

if __name__ == "__main__":
    main()
