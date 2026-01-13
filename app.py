# TFP Courier Cost Monitor v8.0
# Safe UX: explicit upload slots per file type (ZIP or single file per slot) + progress + logs.
# Fixes discussed:
# - Avoid "upload everything mixed" ambiguity: user chooses slot/type.
# - ZIP or non-ZIP supported per slot.
# - Shows progress so it never "looks stuck".
# - Excludes TOTAL/VAT/summary lines from courier charges calculations.
# - Legacy 7-digit refs excluded from ALL calculations.
# - DB URL never printed; Neon Postgres via st.secrets["db"]["url"] or env DATABASE_URL; SQLite fallback.
#
# Run: streamlit run app.py

import os
import io
import re
import zipfile
import hashlib
import datetime as dt
from typing import Optional, List, Dict, Tuple

import pandas as pd
import streamlit as st

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from pypdf import PdfReader


APP_TITLE = "TFP Courier Cost Monitor"
APP_VERSION = "v8.0 — safe slots + zip + progress"


# ---------------------------
# Utilities
# ---------------------------

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def norm_str(x) -> str:
    if x is None:
        return ""
    return str(x).strip()


def parse_date_any(x) -> Optional[pd.Timestamp]:
    if x is None:
        return None
    try:
        return pd.to_datetime(x, errors="coerce", dayfirst=True)
    except Exception:
        return None


def extract_pdf_text(file_bytes: bytes, max_pages: int = 12) -> str:
    try:
        reader = PdfReader(io.BytesIO(file_bytes))
        chunks = []
        for p in reader.pages[:max_pages]:
            chunks.append(p.extract_text() or "")
        return "\n".join(chunks)
    except Exception:
        return ""


# References & exclusions
LEGACY_7DIGIT_RE = re.compile(r"(?<!\d)(\d{7})(?!\d)")
LGK_RE = re.compile(r"(LGK/OUT/\d+)(?:_\d+)?", re.IGNORECASE)
RMA_RE = re.compile(r"(RMA\d+)", re.IGNORECASE)
TRACKING_NUM_RE = re.compile(r"(?<!\d)(\d{9,20})(?!\d)")

COURIER_SUMMARY_KEYWORDS = [
    "ΣΥΝΟΛΟ", "ΓΕΝΙΚΟ ΣΥΝΟΛΟ", "ΣΥΝΟΛΙΚΟ", "ΜΕΡΙΚΟ ΣΥΝΟΛΟ",
    "TOTAL", "GRAND TOTAL", "SUBTOTAL",
    "ΦΠΑ", "FPA", "VAT", "V.A.T", "TAX",
    "ΠΛΗΡΩΤΕΟ", "ΥΠΟΛΟΙΠΟ", "AMOUNT DUE", "PAYABLE",
]


def looks_like_summary_line(ref_raw: str, ref_desc: str) -> bool:
    txt = f"{(ref_raw or '').upper()} {(ref_desc or '').upper()}".strip()
    if not txt:
        return False
    # If there is a shipment-ish ref, it is not a summary line.
    if LGK_RE.search(txt) or RMA_RE.search(txt) or TRACKING_NUM_RE.search(txt):
        return False
    return any(k in txt for k in COURIER_SUMMARY_KEYWORDS)


def detect_refs(s: str) -> Dict[str, Optional[str]]:
    s = norm_str(s)
    out = {"lgk": None, "rma": None, "legacy7": None, "tracking": None}

    m = LGK_RE.search(s)
    if m:
        out["lgk"] = m.group(1).upper()

    m = RMA_RE.search(s)
    if m:
        out["rma"] = m.group(1).upper()

    m = TRACKING_NUM_RE.search(s)
    if m:
        out["tracking"] = m.group(1)

    # legacy7 must be EXACT full cell (your rule)
    m = LEGACY_7DIGIT_RE.fullmatch(s.strip())
    if m:
        out["legacy7"] = m.group(1)

    return out


def safe_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


# ---------------------------
# DB
# ---------------------------

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
    ddl = """
    CREATE TABLE IF NOT EXISTS uploads (
      file_hash TEXT PRIMARY KEY,
      filename TEXT,
      file_type TEXT,
      uploaded_at TEXT
    );

    CREATE TABLE IF NOT EXISTS courier_charges (
      row_hash TEXT PRIMARY KEY,
      courier TEXT,
      country TEXT,
      invoice_no TEXT,
      voucher_no TEXT,
      invoice_date TEXT,
      ref_raw TEXT,
      ref_desc TEXT,
      ref_lgk TEXT,
      ref_rma TEXT,
      ref_tracking TEXT,
      amount_net REAL,
      vat_rate REAL,
      amount_gross REAL,
      currency TEXT,
      source_file_hash TEXT,
      excluded_reason TEXT
    );

    CREATE TABLE IF NOT EXISTS odoo_pickings (
      picking_name TEXT PRIMARY KEY,
      done_date TEXT,
      scheduled_date TEXT,
      origin TEXT,
      partner_country TEXT,
      carrier TEXT,
      tracking TEXT,
      source_file_hash TEXT
    );

    CREATE TABLE IF NOT EXISTS odoo_returns (
      picking_name TEXT PRIMARY KEY,
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
    with engine.begin() as cx:
        for stmt in ddl.split(";"):
            s = stmt.strip()
            if s:
                cx.execute(text(s))


def upsert_upload(engine: Engine, file_hash: str, filename: str, file_type: str) -> None:
    now = dt.datetime.utcnow().isoformat()
    backend = engine.url.get_backend_name()
    with engine.begin() as cx:
        if backend == "sqlite":
            cx.execute(
                text("""INSERT OR REPLACE INTO uploads(file_hash, filename, file_type, uploaded_at)
                        VALUES(:h,:fn,:ft,:at)"""),
                {"h": file_hash, "fn": filename, "ft": file_type, "at": now},
            )
        else:
            cx.execute(
                text("""INSERT INTO uploads(file_hash, filename, file_type, uploaded_at)
                        VALUES(:h,:fn,:ft,:at)
                        ON CONFLICT (file_hash) DO UPDATE SET
                        filename=EXCLUDED.filename,
                        file_type=EXCLUDED.file_type,
                        uploaded_at=EXCLUDED.uploaded_at"""),
                {"h": file_hash, "fn": filename, "ft": file_type, "at": now},
            )


def fetch_df(engine: Engine, sql: str) -> pd.DataFrame:
    with engine.begin() as cx:
        return pd.read_sql(text(sql), cx)


def file_seen(engine: Engine, file_hash: str) -> bool:
    q = f"SELECT 1 FROM uploads WHERE file_hash='{file_hash}' LIMIT 1"
    return not fetch_df(engine, q).empty


def upsert_many(engine: Engine, table: str, rows: List[dict], pk: str) -> Tuple[int, int]:
    if not rows:
        return 0, 0

    backend = engine.url.get_backend_name()
    cols = list(rows[0].keys())
    col_list = ",".join(cols)
    placeholders = ",".join([f":{c}" for c in cols])

    if backend == "sqlite":
        sql = f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})"
        ok = failed = 0
        with engine.begin() as cx:
            for r in rows:
                try:
                    cx.execute(text(sql), r)
                    ok += 1
                except Exception:
                    failed += 1
        return ok, failed

    updates = ",".join([f"{c}=EXCLUDED.{c}" for c in cols if c != pk])
    sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT ({pk}) DO UPDATE SET {updates}"
    ok = failed = 0
    with engine.begin() as cx:
        for r in rows:
            try:
                cx.execute(text(sql), r)
                ok += 1
            except Exception:
                failed += 1
    return ok, failed


# ---------------------------
# ZIP helper
# ---------------------------

def iter_files_from_upload(upload, allowed_ext: List[str]) -> List[Tuple[str, bytes]]:
    """Return list of (filename, bytes) for either:
      - a .zip upload (all matching extensions inside)
      - a single file upload
    """
    name = upload.name
    b = upload.getvalue()

    if name.lower().endswith(".zip"):
        out = []
        with zipfile.ZipFile(io.BytesIO(b)) as z:
            for info in z.infolist():
                if info.is_dir():
                    continue
                fname = info.filename
                ext = os.path.splitext(fname)[1].lower()
                if ext in allowed_ext:
                    out.append((fname, z.read(info)))
        return out

    ext = os.path.splitext(name)[1].lower()
    if ext in allowed_ext:
        return [(name, b)]

    return []


# ---------------------------
# Parsers (Odoo)
# ---------------------------

def parse_odoo_pickings_xlsx(b: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(b))
    c_name = safe_col(df, ["Name", "Transfer", "Picking", "Reference"]) or df.columns[0]
    c_done = safe_col(df, ["Date Done", "Done Date", "Completed Date"])
    c_sched = safe_col(df, ["Scheduled Date", "Scheduled date"])
    c_origin = safe_col(df, ["Origin", "Source Document"])
    c_country = safe_col(df, ["Partner/Country", "Partner Country", "Country"])
    c_carrier = safe_col(df, ["Carrier", "Delivery Method"])
    c_track = safe_col(df, ["Tracking Reference", "Tracking", "tracking_reference"])

    out = pd.DataFrame({
        "picking_name": df[c_name].astype(str).str.strip(),
        "done_date": df[c_done] if c_done else None,
        "scheduled_date": df[c_sched] if c_sched else None,
        "origin": df[c_origin] if c_origin else None,
        "partner_country": df[c_country] if c_country else None,
        "carrier": df[c_carrier] if c_carrier else None,
        "tracking": df[c_track] if c_track else None,
    })

    for c in ["done_date", "scheduled_date"]:
        out[c] = pd.to_datetime(out[c], errors="coerce", dayfirst=True).dt.date.astype(str)
        out.loc[out[c].isin(["NaT", "None", "nan"]), c] = None

    for c in ["origin", "partner_country", "carrier", "tracking"]:
        out[c] = out[c].astype(str).str.strip()
        out.loc[out[c].isin(["nan", "None", ""]), c] = None

    out = out.dropna(subset=["picking_name"])
    return out


def parse_odoo_sales_orders_xlsx(b: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(b))

    c_order = safe_col(df, ["Order Reference", "Name", "Order", "Reference"])
    c_date = safe_col(df, ["Order Date", "Date Order", "Date"])
    c_country = safe_col(df, ["Partner/Country", "Partner Country", "Country"])
    c_line_name = safe_col(df, ["Order Lines/Name", "Order Lines/Product", "Order Lines/Description", "Order Lines"])
    c_price = safe_col(df, ["Order Lines/Unit Price", "Order Lines/Price Unit", "Unit Price"])
    c_qty = safe_col(df, ["Order Lines/Invoiced Quantity", "Invoiced Quantity", "Order Lines/Qty Invoiced"])
    c_currency = safe_col(df, ["Currency", "Pricelist Currency", "Company Currency"])

    if not c_order or not c_line_name or not c_price or not c_qty:
        raise KeyError("Sales Orders export: missing required columns (order_ref/line_name/unit_price/invoiced_qty).")

    cols = [c_order, c_date, c_country, c_line_name, c_price, c_qty] + ([c_currency] if c_currency else [])
    tmp = df[cols].copy()
    tmp.columns = ["order_ref", "order_date", "partner_country", "line_name", "unit_price", "invoiced_qty"] + (["currency"] if c_currency else [])

    name_lc = tmp["line_name"].astype(str).str.lower()
    mask_fee = (
        name_lc.str.contains("courier")
        | name_lc.str.contains("shipping")
        | name_lc.str.contains("μεταφορ")
        | name_lc.str.contains("έξοδα αποστολ")
        | name_lc.str.contains("εξοδα αποστολ")
    )
    tmp = tmp[mask_fee].copy()

    tmp["invoiced_qty"] = pd.to_numeric(tmp["invoiced_qty"], errors="coerce").fillna(0.0)
    tmp = tmp[tmp["invoiced_qty"] > 0].copy()

    tmp["unit_price"] = pd.to_numeric(tmp["unit_price"], errors="coerce").fillna(0.0)
    tmp["fee_gross"] = tmp["unit_price"] * tmp["invoiced_qty"]

    agg = tmp.groupby("order_ref", as_index=False).agg(
        order_date=("order_date", "first"),
        partner_country=("partner_country", "first"),
        invoiced_qty=("invoiced_qty", "sum"),
        fee_gross=("fee_gross", "sum"),
    )
    agg["currency"] = tmp["currency"].iloc[0] if "currency" in tmp.columns and len(tmp) else "EUR"

    agg["order_date"] = pd.to_datetime(agg["order_date"], errors="coerce", dayfirst=True).dt.date.astype(str)
    agg.loc[agg["order_date"].isin(["NaT", "None", "nan"]), "order_date"] = None
    return agg


def parse_odoo_rma_xlsx(b: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(b))

    c_rma = safe_col(df, ["Name", "Reference", "RMA", "Rma"])
    c_done = safe_col(df, ["Done Date", "Date Done", "Completed Date", "Date"])
    c_country = safe_col(df, ["Partner/Country", "Partner Country", "Country"])
    c_fee = safe_col(df, ["Service Cost", "Service cost", "Return Fee", "Service Fee", "Fee"])

    if not c_rma or not c_fee:
        raise KeyError("RMA export: missing required columns (rma_ref/service_fee).")

    out = pd.DataFrame({
        "rma_ref": df[c_rma].astype(str).str.strip(),
        "done_date": df[c_done] if c_done else None,
        "partner_country": df[c_country] if c_country else None,
        "service_fee_gross": df[c_fee],
    })

    out["service_fee_gross"] = pd.to_numeric(out["service_fee_gross"], errors="coerce").fillna(0.0)
    out = out[out["service_fee_gross"] != 0].copy()

    out["done_date"] = pd.to_datetime(out["done_date"], errors="coerce", dayfirst=True).dt.date.astype(str)
    out.loc[out["done_date"].isin(["NaT", "None", "nan"]), "done_date"] = None

    out["currency"] = "EUR"
    out = out.dropna(subset=["rma_ref"])
    return out


# ---------------------------
# Parsers (Couriers)
# ---------------------------

def parse_acs_gr_xlsx(b: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(b))

    c_date = safe_col(df, ["Ημερομηνία", "Date", "Invoice Date", "Ημ/νία"])
    c_amount = safe_col(df, ["Χρέωση", "Ποσό", "Amount", "Net", "Αξία"])
    c_ref1 = safe_col(df, ["Σχετικό", "Reference", "Ref"])
    c_ref2 = safe_col(df, ["Σχετικό 2"])
    c_desc = safe_col(df, ["Αιτιολογία", "Περιγραφή", "Σχόλια", "Notes", "Description"])

    if not c_amount:
        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        c_amount = num_cols[-1] if num_cols else df.columns[-1]

    if not c_ref1 and not c_ref2:
        c_ref1 = df.columns[0]

    if c_ref1 and c_ref2:
        ref_raw = df[c_ref1].astype(str).str.strip() + " " + df[c_ref2].astype(str).str.strip()
    else:
        ref_raw = df[c_ref1].astype(str).str.strip() if c_ref1 else df[c_ref2].astype(str).str.strip()

    out = pd.DataFrame({
        "invoice_date": df[c_date] if c_date else None,
        "invoice_no": None,
        "voucher_no": None,
        "ref_raw": ref_raw,
        "ref_desc": df[c_desc] if c_desc else None,
        "amount_net": df[c_amount],
    })

    out["amount_net"] = pd.to_numeric(out["amount_net"], errors="coerce")
    out = out.dropna(subset=["amount_net"])
    return out


def parse_courier_center_xls(b: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(b), engine="xlrd")

    c_date = safe_col(df, ["Ημερομηνία", "Date", "Invoice Date"])
    c_amount = safe_col(df, ["Χρέωση", "Ποσό", "Amount", "Net", "Αξία"])
    c_ref = safe_col(df, ["Σχετικό", "Παραστατικό", "Reference", "Ref"]) or df.columns[0]
    c_desc = safe_col(df, ["Αιτιολογία", "Notes", "Περιγραφή", "Description", "Σχόλια"])

    if not c_amount:
        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        c_amount = num_cols[-1] if num_cols else df.columns[-1]

    out = pd.DataFrame({
        "invoice_date": df[c_date] if c_date else None,
        "invoice_no": None,
        "voucher_no": None,
        "ref_raw": df[c_ref],
        "ref_desc": df[c_desc] if c_desc else None,
        "amount_net": df[c_amount],
    })

    out["amount_net"] = pd.to_numeric(out["amount_net"], errors="coerce")
    out = out.dropna(subset=["amount_net"])
    return out


def parse_boxnow_pdf(b: bytes) -> pd.DataFrame:
    text = extract_pdf_text(b, max_pages=20)
    refs = re.findall(r"LGK/OUT/\d+(?:_\d+)?", text, flags=re.IGNORECASE)
    out = pd.DataFrame({
        "invoice_date": None,
        "invoice_no": None,
        "voucher_no": None,
        "ref_raw": [r.upper() for r in refs],
        "ref_desc": None,
        "amount_net": None,  # limitation
    })
    return out.drop_duplicates(subset=["ref_raw"])


def parse_acs_cy_pdf(b: bytes) -> pd.DataFrame:
    text = extract_pdf_text(b, max_pages=20)
    refs = re.findall(r"LGK/OUT/\d+(?:_\d+)?|RMA\d+|\b\d{7}\b", text, flags=re.IGNORECASE)
    out = pd.DataFrame({
        "invoice_date": None,
        "invoice_no": None,
        "voucher_no": None,
        "ref_raw": [r.upper() for r in refs],
        "ref_desc": None,
        "amount_net": None,  # limitation
    })
    return out.drop_duplicates(subset=["ref_raw"])


# ---------------------------
# Normalize to DB rows
# ---------------------------

def normalize_charges(
    df: pd.DataFrame,
    courier: str,
    country_default: str,
    vat_rate: float,
    source_file_hash: str,
    currency: str = "EUR",
) -> List[dict]:
    rows = []
    for _, r in df.iterrows():
        ref_raw = norm_str(r.get("ref_raw"))
        ref_desc = norm_str(r.get("ref_desc"))
        refs = detect_refs(ref_raw)

        excluded_reason = "legacy_7digit_old_eshop" if refs["legacy7"] else None

        # Exclude totals/VAT summary lines unless they contain a shipment identifier
        if excluded_reason is None:
            has_shipment_ref = bool(refs.get("lgk") or refs.get("rma") or refs.get("tracking"))
            if (not has_shipment_ref) and looks_like_summary_line(ref_raw, ref_desc):
                excluded_reason = "courier_summary_total_or_vat"

        amount_net = r.get("amount_net")
        amount_net = float(amount_net) if amount_net is not None and pd.notna(amount_net) else None

        vat = float(vat_rate) if vat_rate is not None else 0.0
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
            "voucher_no": voucher_no,
            "invoice_date": inv_dt_str,
            "ref_raw": ref_raw,
            "ref_desc": ref_desc or None,
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


# ---------------------------
# Ledger & summaries
# ---------------------------

def build_ledger(engine: Engine) -> pd.DataFrame:
    frames = []

    ch = fetch_df(engine, "SELECT * FROM courier_charges")
    if not ch.empty:
        ch = ch.copy()
        ch["entry_type"] = "CHARGE"
        ch["courier"] = ch["courier"].fillna("UNKNOWN")
        ch["ref_key"] = ch["ref_lgk"].fillna(ch["ref_rma"]).fillna(ch["ref_tracking"]).fillna(ch["ref_raw"])
        ch["date_exec"] = pd.to_datetime(ch["invoice_date"], errors="coerce")
        ch["amount_gross_signed"] = pd.to_numeric(ch["amount_gross"], errors="coerce") * (-1.0)
        frames.append(ch[["entry_type", "courier", "country", "ref_key", "date_exec", "amount_gross_signed", "excluded_reason"]])

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
        frames.append(so[["entry_type", "courier", "country", "ref_key", "date_exec", "amount_gross_signed", "excluded_reason"]])

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
        frames.append(rma[["entry_type", "courier", "country", "ref_key", "date_exec", "amount_gross_signed", "excluded_reason"]])

    if not frames:
        return pd.DataFrame(columns=["entry_type", "courier", "country", "ref_key", "date_exec", "amount_gross_signed", "excluded_reason"])

    return pd.concat(frames, ignore_index=True)


def summarize_periods(ledger: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    empty = pd.DataFrame(columns=["courier", "period", "charges_gross", "revenues_gross", "net_gross"])
    if ledger.empty or ledger["date_exec"].isna().all():
        return {"weekly": empty, "monthly": empty, "quarterly": empty, "yearly": empty}

    df = ledger.copy()
    df = df[~df["excluded_reason"].fillna("").str.contains("legacy_7digit|summary_total_or_vat", case=False)].copy()

    df["courier"] = df["courier"].fillna("UNKNOWN")
    df["is_charge"] = df["entry_type"].eq("CHARGE")
    df["charges"] = df["amount_gross_signed"].where(df["is_charge"], 0.0)
    df["revenues"] = df["amount_gross_signed"].where(~df["is_charge"], 0.0)

    def agg(freq: str) -> pd.DataFrame:
        tmp = df.set_index("date_exec").copy()
        tmp["period"] = tmp.index.to_period(freq).astype(str)
        g = tmp.groupby(["courier", "period"], as_index=False).agg(
            charges_gross=("charges", "sum"),
            revenues_gross=("revenues", "sum"),
        )
        g["charges_gross"] = g["charges_gross"].abs()
        g["net_gross"] = g["revenues_gross"] - g["charges_gross"]
        return g.sort_values(["courier", "period"])

    return {"weekly": agg("W"), "monthly": agg("M"), "quarterly": agg("Q"), "yearly": agg("Y")}


# ---------------------------
# UI
# ---------------------------

st.set_page_config(page_title=APP_TITLE, layout="wide")


def main():
    st.title(APP_TITLE)
    st.caption(f"{APP_VERSION} — DB URL never shown")

    engine = get_engine()
    init_db(engine)

    with st.sidebar:
        st.header("VAT defaults (courier invoices net → gross)")
        vat_gr = st.selectbox("Greece VAT", options=[0.24, 0.13, 0.0], index=0)
        vat_cy = st.selectbox("Cyprus VAT", options=[0.19, 0.05, 0.0], index=0)

        st.header("Upload safety")
        skip_identical = st.toggle("Skip identical files (file_hash)", value=True)

        st.header("BoxNow country")
        boxnow_country = st.selectbox("Default BoxNow country", options=["GR", "CY"], index=0)

    st.subheader("Upload — one file per slot (ZIP or single file)")
    od_tab, co_tab = st.tabs(["Odoo uploads", "Courier uploads"])

    with od_tab:
        st.write("Upload exactly the correct export in each slot. ZIP accepted.")
        od_pick = st.file_uploader("Odoo Pickings Orders (.xlsx or .zip)", type=["xlsx", "zip"], accept_multiple_files=False)
        od_ret  = st.file_uploader("Odoo Pickings Returns (.xlsx or .zip)", type=["xlsx", "zip"], accept_multiple_files=False)
        od_sales = st.file_uploader("Odoo Sales Orders (.xlsx or .zip)", type=["xlsx", "zip"], accept_multiple_files=False)
        od_rma = st.file_uploader("Odoo RMA (.xlsx or .zip)", type=["xlsx", "zip"], accept_multiple_files=False)

        if st.button("Process Odoo", type="primary"):
            logs: List[str] = []
            prog = st.progress(0, text="Starting…")
            steps = 4
            done_steps = 0

            def bump(msg: str):
                nonlocal done_steps
                done_steps += 1
                prog.progress(min(done_steps / steps, 1.0), text=msg)

            # Pickings orders
            try:
                if od_pick:
                    files = iter_files_from_upload(od_pick, allowed_ext=[".xlsx"])
                    if not files:
                        logs.append("Pickings Orders: no .xlsx found inside upload.")
                    for fname, b in files[:1]:
                        h = sha256_bytes(b)
                        if (not file_seen(engine, h)) or (not skip_identical):
                            upsert_upload(engine, h, fname, "odoo_pickings_orders")
                            df = parse_odoo_pickings_xlsx(b)
                            df["source_file_hash"] = h
                            ok, fail = upsert_many(engine, "odoo_pickings", df.to_dict("records"), "picking_name")
                            logs.append(f"Pickings Orders: {fname} → ok={ok}, failed={fail}, rows={len(df)}")
                        else:
                            logs.append(f"Pickings Orders: {fname} skipped (identical)")
                else:
                    logs.append("Pickings Orders: (no file)")
            except Exception as e:
                logs.append(f"Pickings Orders ERROR: {e}")
            bump("Processed Pickings Orders")

            # Returns
            try:
                if od_ret:
                    files = iter_files_from_upload(od_ret, allowed_ext=[".xlsx"])
                    if not files:
                        logs.append("Returns: no .xlsx found inside upload.")
                    for fname, b in files[:1]:
                        h = sha256_bytes(b)
                        if (not file_seen(engine, h)) or (not skip_identical):
                            upsert_upload(engine, h, fname, "odoo_pickings_returns")
                            df = parse_odoo_pickings_xlsx(b)
                            df["source_file_hash"] = h
                            ok, fail = upsert_many(engine, "odoo_returns", df.to_dict("records"), "picking_name")
                            logs.append(f"Returns: {fname} → ok={ok}, failed={fail}, rows={len(df)}")
                        else:
                            logs.append(f"Returns: {fname} skipped (identical)")
                else:
                    logs.append("Returns: (no file)")
            except Exception as e:
                logs.append(f"Returns ERROR: {e}")
            bump("Processed Returns")

            # Sales
            try:
                if od_sales:
                    files = iter_files_from_upload(od_sales, allowed_ext=[".xlsx"])
                    if not files:
                        logs.append("Sales: no .xlsx found inside upload.")
                    for fname, b in files[:1]:
                        h = sha256_bytes(b)
                        if (not file_seen(engine, h)) or (not skip_identical):
                            upsert_upload(engine, h, fname, "odoo_sales_orders")
                            df = parse_odoo_sales_orders_xlsx(b)
                            df["source_file_hash"] = h
                            ok, fail = upsert_many(engine, "odoo_sales_courier_fees", df.to_dict("records"), "order_ref")
                            logs.append(f"Sales: {fname} → ok={ok}, failed={fail}, orders={len(df)}")
                        else:
                            logs.append(f"Sales: {fname} skipped (identical)")
                else:
                    logs.append("Sales: (no file)")
            except Exception as e:
                logs.append(f"Sales ERROR: {e}")
            bump("Processed Sales")

            # RMA
            try:
                if od_rma:
                    files = iter_files_from_upload(od_rma, allowed_ext=[".xlsx"])
                    if not files:
                        logs.append("RMA: no .xlsx found inside upload.")
                    for fname, b in files[:1]:
                        h = sha256_bytes(b)
                        if (not file_seen(engine, h)) or (not skip_identical):
                            upsert_upload(engine, h, fname, "odoo_rma")
                            df = parse_odoo_rma_xlsx(b)
                            df["source_file_hash"] = h
                            ok, fail = upsert_many(engine, "odoo_rma_fees", df.to_dict("records"), "rma_ref")
                            logs.append(f"RMA: {fname} → ok={ok}, failed={fail}, rmas={len(df)}")
                        else:
                            logs.append(f"RMA: {fname} skipped (identical)")
                else:
                    logs.append("RMA: (no file)")
            except Exception as e:
                logs.append(f"RMA ERROR: {e}")
            bump("Processed RMA")

            st.success("Odoo processing finished.")
            st.text("\n".join(logs))

    with co_tab:
        st.write("Upload each courier source in its slot. ZIP accepted.")
        up_acsgr = st.file_uploader("ACS GR (.xlsx or .zip)", type=["xlsx", "zip"], accept_multiple_files=False, key="acsgr")
        up_cc = st.file_uploader("Courier Center (.xls or .zip)", type=["xls", "zip"], accept_multiple_files=False, key="cc")
        up_boxnow = st.file_uploader("BoxNow (.pdf or .zip)", type=["pdf", "zip"], accept_multiple_files=False, key="boxnow")
        up_acscy = st.file_uploader("ACS CY (.pdf or .zip)", type=["pdf", "zip"], accept_multiple_files=False, key="acscy")

        if st.button("Process Couriers", type="primary"):
            logs: List[str] = []
            prog = st.progress(0, text="Starting…")
            steps = 4
            done_steps = 0

            def bump(msg: str):
                nonlocal done_steps
                done_steps += 1
                prog.progress(min(done_steps / steps, 1.0), text=msg)

            # ACS GR
            try:
                if up_acsgr:
                    files = iter_files_from_upload(up_acsgr, allowed_ext=[".xlsx"])
                    if not files:
                        logs.append("ACS GR: no .xlsx found inside upload.")
                    for fname, b in files:
                        h = sha256_bytes(b)
                        if file_seen(engine, h) and skip_identical:
                            logs.append(f"ACS GR: {fname} skipped (identical)")
                            continue
                        upsert_upload(engine, h, fname, "acs_gr_xlsx")
                        df = parse_acs_gr_xlsx(b)
                        rows = normalize_charges(df, "ACS_GR", "GR", vat_gr, h)
                        ok, fail = upsert_many(engine, "courier_charges", rows, "row_hash")
                        logs.append(f"ACS GR: {fname} → ok={ok}, failed={fail}, lines={len(rows)}")
                else:
                    logs.append("ACS GR: (no file)")
            except Exception as e:
                logs.append(f"ACS GR ERROR: {e}")
            bump("Processed ACS GR")

            # Courier Center
            try:
                if up_cc:
                    files = iter_files_from_upload(up_cc, allowed_ext=[".xls"])
                    if not files:
                        logs.append("Courier Center: no .xls found inside upload.")
                    for fname, b in files:
                        h = sha256_bytes(b)
                        if file_seen(engine, h) and skip_identical:
                            logs.append(f"Courier Center: {fname} skipped (identical)")
                            continue
                        upsert_upload(engine, h, fname, "courier_center_xls")
                        df = parse_courier_center_xls(b)
                        rows = normalize_charges(df, "COURIER_CENTER", "GR", vat_gr, h)
                        ok, fail = upsert_many(engine, "courier_charges", rows, "row_hash")
                        logs.append(f"Courier Center: {fname} → ok={ok}, failed={fail}, lines={len(rows)}")
                else:
                    logs.append("Courier Center: (no file)")
            except Exception as e:
                logs.append(f"Courier Center ERROR: {e}")
            bump("Processed Courier Center")

            # BoxNow
            try:
                if up_boxnow:
                    files = iter_files_from_upload(up_boxnow, allowed_ext=[".pdf"])
                    if not files:
                        logs.append("BoxNow: no .pdf found inside upload.")
                    for fname, b in files:
                        h = sha256_bytes(b)
                        if file_seen(engine, h) and skip_identical:
                            logs.append(f"BoxNow: {fname} skipped (identical)")
                            continue
                        upsert_upload(engine, h, fname, "boxnow_pdf")
                        df = parse_boxnow_pdf(b)
                        vat = vat_gr if boxnow_country == "GR" else vat_cy
                        rows = normalize_charges(df, "BOXNOW", boxnow_country, vat, h)
                        ok, fail = upsert_many(engine, "courier_charges", rows, "row_hash")
                        logs.append(f"BoxNow: {fname} → ok={ok}, failed={fail}, refs={len(rows)}")
                else:
                    logs.append("BoxNow: (no file)")
            except Exception as e:
                logs.append(f"BoxNow ERROR: {e}")
            bump("Processed BoxNow")

            # ACS CY
            try:
                if up_acscy:
                    files = iter_files_from_upload(up_acscy, allowed_ext=[".pdf"])
                    if not files:
                        logs.append("ACS CY: no .pdf found inside upload.")
                    for fname, b in files:
                        h = sha256_bytes(b)
                        if file_seen(engine, h) and skip_identical:
                            logs.append(f"ACS CY: {fname} skipped (identical)")
                            continue
                        upsert_upload(engine, h, fname, "acs_cy_pdf")
                        df = parse_acs_cy_pdf(b)
                        rows = normalize_charges(df, "ACS_CY", "CY", vat_cy, h)
                        ok, fail = upsert_many(engine, "courier_charges", rows, "row_hash")
                        logs.append(f"ACS CY: {fname} → ok={ok}, failed={fail}, refs={len(rows)}")
                else:
                    logs.append("ACS CY: (no file)")
            except Exception as e:
                logs.append(f"ACS CY ERROR: {e}")
            bump("Processed ACS CY")

            st.success("Courier processing finished.")
            st.text("\n".join(logs))

    st.subheader("Monitoring")
    counts = {
        "uploads": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM uploads")["n"].iloc[0]),
        "courier_charges": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM courier_charges")["n"].iloc[0]),
        "odoo_pickings": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM odoo_pickings")["n"].iloc[0]),
        "odoo_returns": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM odoo_returns")["n"].iloc[0]),
        "odoo_sales_fees": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM odoo_sales_courier_fees")["n"].iloc[0]),
        "odoo_rma_fees": int(fetch_df(engine, "SELECT COUNT(*) AS n FROM odoo_rma_fees")["n"].iloc[0]),
    }
    st.json(counts)

    excluded_legacy = int(fetch_df(engine, "SELECT COUNT(*) AS n FROM courier_charges WHERE excluded_reason='legacy_7digit_old_eshop'")["n"].iloc[0])
    excluded_summary = int(fetch_df(engine, "SELECT COUNT(*) AS n FROM courier_charges WHERE excluded_reason='courier_summary_total_or_vat'")["n"].iloc[0])
    st.write(f"Excluded legacy 7-digit: {excluded_legacy} | Excluded totals/VAT lines: {excluded_summary}")

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

    st.subheader("Export")
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


if __name__ == "__main__":
    main()
