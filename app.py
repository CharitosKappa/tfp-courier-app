# app.py
# TFP Courier Reconciliation – Streamlit app (v4)
# - Incremental uploads into DB (Neon Postgres via Secrets / env, or SQLite fallback)
# - Courier invoices (charges): BoxNow PDF, ACS GR XLSX, Courier Center XLS, ACS CY PDF (text-based)
# - Odoo exports: stock.picking Orders/Returns, sale.order (courier fees revenue), sale.order.rma (service cost revenue)
# - Matching + summaries + Excel export
#
# SECURITY: DATABASE_URL is NEVER shown in UI. Provide it via Streamlit Secrets or environment variable.

from __future__ import annotations

import io
import os
import re
import hashlib
import sqlite3
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd
import numpy as np
import streamlit as st

# Optional deps
try:
    import pdfplumber  # type: ignore
    PDFPLUMBER_OK = True
except Exception:
    PDFPLUMBER_OK = False

try:
    import sqlalchemy as sa  # type: ignore
    SQLA_OK = True
except Exception:
    SQLA_OK = False


# ----------------------------
# App constants / helpers
# ----------------------------

APP_TITLE = "TFP Courier Reconciliation (v4)"
DB_SQLITE_DEFAULT = "tfp_courier_recon.sqlite"

LGK_RE = re.compile(r"(LGK/OUT/\d+(?:_\d+)?)", re.IGNORECASE)
RMA_RE = re.compile(r"(RMA\d+)", re.IGNORECASE)
LEGACY_7DIGIT_TOKEN_RE = re.compile(r"(?<!\d)(\d{7})(?!\d)")

# Heuristic: courier fee lines in sales order
DEFAULT_COURIER_FEE_REGEX = r"(courier|shipping|delivery|μεταφορ|μεταφορικ|αντικαταβολ|cod|boxnow|acs|courier\s*center)"


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def norm_text(x: Any) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    return str(x).strip()


def norm_amount(x: Any) -> Optional[float]:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return None
    # Greek style "1.234,56"
    s = s.replace(" ", "")
    if re.fullmatch(r"-?\d{1,3}(\.\d{3})*,\d{2}", s):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def norm_date(x: Any) -> pd.Timestamp:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return pd.NaT
    if isinstance(x, pd.Timestamp):
        return x
    try:
        return pd.to_datetime(x, dayfirst=True, errors="coerce")
    except Exception:
        return pd.NaT


def extract_lgk(text: str) -> Optional[str]:
    m = LGK_RE.search(text or "")
    return m.group(1).upper() if m else None


def extract_rma(text: str) -> Optional[str]:
    m = RMA_RE.search(text or "")
    return m.group(1).upper() if m else None


def legacy_7digit_in_text(text: str) -> Optional[str]:
    """Returns a 7-digit legacy order ref (old eshop) if present, else None."""
    if not text:
        return None
    s = str(text).strip()
    if re.fullmatch(r"\d{7}", s):
        return s
    m = LEGACY_7DIGIT_TOKEN_RE.search(s)
    return m.group(1) if m else None


def lgk_base(lgk_out: Optional[str]) -> Optional[str]:
    if not lgk_out:
        return None
    # LGK/OUT/22831_2 -> LGK/OUT/22831
    return re.sub(r"(_\d+)$", "", lgk_out.upper()).strip()


def compute_charge_exclusion(ref_blob: str, lgk: Optional[str], rma: Optional[str]) -> Tuple[int, Optional[str]]:
    """Exclude when a simple 7-digit numeric legacy ref exists AND we don't also have LGK/RMA."""
    token = legacy_7digit_in_text(ref_blob)
    if token and not lgk and not rma:
        return 1, f"legacy_eshop_ref_{token}"
    return 0, None


def courier_normalize(raw: str) -> str:
    s = (raw or "").strip().lower()
    if not s:
        return "UNKNOWN"
    if "box" in s:
        return "BOXNOW"
    if "acs" in s and "cy" in s:
        return "ACS_CY"
    if "acs" in s:
        return "ACS_GR"
    if "courier center" in s or "couriercenter" in s or "c.center" in s:
        return "COURIER_CENTER"
    if "geniki" in s or "γενικη" in s:
        return "GENIKI"
    return raw.strip().upper().replace(" ", "_")


def week_start(d: pd.Timestamp) -> pd.Timestamp:
    if pd.isna(d):
        return pd.NaT
    dd = pd.to_datetime(d).normalize()
    return dd - pd.to_timedelta(dd.weekday(), unit="D")


def quarter_label(d: pd.Timestamp) -> str:
    if pd.isna(d):
        return ""
    y = d.year
    q = (d.month - 1) // 3 + 1
    return f"{y}-Q{q}"


# ----------------------------
# Safe Excel reader (xls/xlsx)
# ----------------------------

def safe_read_excel(b: bytes, filename: str) -> pd.DataFrame:
    """Reads .xlsx/.xls with best effort."""
    name = (filename or "").lower()
    bio = io.BytesIO(b)
    if name.endswith(".xls"):
        # xlrd required
        try:
            return pd.read_excel(bio, engine="xlrd")
        except Exception:
            return pd.read_excel(bio)
    return pd.read_excel(bio)


# ----------------------------
# DB layer
# ----------------------------

@dataclass
class DB:
    mode: str  # "sqlite" | "postgres"
    sqlite_path: Optional[str] = None
    pg_url: Optional[str] = None

    def __post_init__(self):
        self._sqlite_conn: Optional[sqlite3.Connection] = None
        self._engine: Optional[Any] = None

    def sqlite(self) -> sqlite3.Connection:
        if self._sqlite_conn is None:
            assert self.sqlite_path
            self._sqlite_conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
        return self._sqlite_conn

    def engine(self):
        if not SQLA_OK:
            raise RuntimeError("sqlalchemy is required for postgres mode")
        if self._engine is None:
            assert self.pg_url
            self._engine = sa.create_engine(self.pg_url, pool_pre_ping=True, future=True)
        return self._engine

    def init_schema(self):
        if self.mode == "sqlite":
            con = self.sqlite()
            c = con.cursor()

            c.execute("""
                CREATE TABLE IF NOT EXISTS ingestions(
                    file_hash TEXT PRIMARY KEY,
                    filename TEXT,
                    kind TEXT,
                    rows_written INTEGER,
                    ingested_at TEXT
                )
            """)

            c.execute("""
                CREATE TABLE IF NOT EXISTS charges_raw(
                    row_key TEXT PRIMARY KEY,
                    courier TEXT, country TEXT, tracking TEXT,
                    lgk_out TEXT, lgk_base TEXT, rma TEXT,
                    ref_raw TEXT, ref_kind TEXT,
                    is_excluded INTEGER DEFAULT 0,
                    exclude_reason TEXT,
                    exec_date TEXT,
                    charge_net REAL,
                    vat_rate REAL, vat_amount REAL, charge_gross REAL, vat_source TEXT,
                    note TEXT,
                    source_file TEXT, file_hash TEXT,
                    updated_at TEXT
                )
            """)

            c.execute("""
                CREATE TABLE IF NOT EXISTS pickings_raw(
                    row_key TEXT PRIMARY KEY,
                    picking_type TEXT,  -- orders/returns
                    source_document TEXT,
                    lgk_out TEXT, lgk_base TEXT,
                    origin TEXT,
                    reference TEXT,
                    carrier TEXT,
                    tracking TEXT,
                    exec_date TEXT,
                    state TEXT,
                    source_file TEXT, file_hash TEXT,
                    updated_at TEXT
                )
            """)

            c.execute("""
                CREATE TABLE IF NOT EXISTS sales_fees_raw(
                    row_key TEXT PRIMARY KEY,
                    order_ref TEXT,
                    line_name TEXT,
                    courier TEXT,
                    country TEXT,
                    exec_date TEXT,
                    amount_gross REAL,
                    vat_included INTEGER,
                    source_file TEXT, file_hash TEXT,
                    updated_at TEXT
                )
            """)

            c.execute("""
                CREATE TABLE IF NOT EXISTS rma_fees_raw(
                    row_key TEXT PRIMARY KEY,
                    rma TEXT,
                    order_ref TEXT,
                    courier TEXT,
                    country TEXT,
                    exec_date TEXT,
                    service_cost_gross REAL,
                    vat_included INTEGER,
                    state TEXT,
                    source_file TEXT, file_hash TEXT,
                    updated_at TEXT
                )
            """)

            # Safe migrations (in case DB exists from older versions)
            for stmt in [
                "ALTER TABLE charges_raw ADD COLUMN ref_raw TEXT",
                "ALTER TABLE charges_raw ADD COLUMN ref_kind TEXT",
                "ALTER TABLE charges_raw ADD COLUMN is_excluded INTEGER DEFAULT 0",
                "ALTER TABLE charges_raw ADD COLUMN exclude_reason TEXT",
            ]:
                try:
                    c.execute(stmt)
                except Exception:
                    pass

            con.commit()
            return

        # Postgres
        eng = self.engine()
        with eng.begin() as conn:
            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS ingestions(
                    file_hash TEXT PRIMARY KEY,
                    filename TEXT,
                    kind TEXT,
                    rows_written INTEGER,
                    ingested_at TEXT
                )
            """))

            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS charges_raw(
                    row_key TEXT PRIMARY KEY,
                    courier TEXT, country TEXT, tracking TEXT,
                    lgk_out TEXT, lgk_base TEXT, rma TEXT,
                    ref_raw TEXT, ref_kind TEXT,
                    is_excluded INTEGER DEFAULT 0,
                    exclude_reason TEXT,
                    exec_date TIMESTAMP NULL,
                    charge_net DOUBLE PRECISION NULL,
                    vat_rate DOUBLE PRECISION NULL,
                    vat_amount DOUBLE PRECISION NULL,
                    charge_gross DOUBLE PRECISION NULL,
                    vat_source TEXT,
                    note TEXT,
                    source_file TEXT, file_hash TEXT,
                    updated_at TEXT
                )
            """))

            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS pickings_raw(
                    row_key TEXT PRIMARY KEY,
                    picking_type TEXT,
                    source_document TEXT,
                    lgk_out TEXT, lgk_base TEXT,
                    origin TEXT,
                    reference TEXT,
                    carrier TEXT,
                    tracking TEXT,
                    exec_date TIMESTAMP NULL,
                    state TEXT,
                    source_file TEXT, file_hash TEXT,
                    updated_at TEXT
                )
            """))

            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS sales_fees_raw(
                    row_key TEXT PRIMARY KEY,
                    order_ref TEXT,
                    line_name TEXT,
                    courier TEXT,
                    country TEXT,
                    exec_date TIMESTAMP NULL,
                    amount_gross DOUBLE PRECISION NULL,
                    vat_included INTEGER,
                    source_file TEXT, file_hash TEXT,
                    updated_at TEXT
                )
            """))

            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS rma_fees_raw(
                    row_key TEXT PRIMARY KEY,
                    rma TEXT,
                    order_ref TEXT,
                    courier TEXT,
                    country TEXT,
                    exec_date TIMESTAMP NULL,
                    service_cost_gross DOUBLE PRECISION NULL,
                    vat_included INTEGER,
                    state TEXT,
                    source_file TEXT, file_hash TEXT,
                    updated_at TEXT
                )
            """))

            # Safe migrations
            try:
                conn.execute(sa.text("ALTER TABLE charges_raw ADD COLUMN IF NOT EXISTS ref_raw TEXT"))
                conn.execute(sa.text("ALTER TABLE charges_raw ADD COLUMN IF NOT EXISTS ref_kind TEXT"))
                conn.execute(sa.text("ALTER TABLE charges_raw ADD COLUMN IF NOT EXISTS is_excluded INTEGER DEFAULT 0"))
                conn.execute(sa.text("ALTER TABLE charges_raw ADD COLUMN IF NOT EXISTS exclude_reason TEXT"))
            except Exception:
                pass

    def ingested(self, file_hash: str) -> bool:
        if self.mode == "sqlite":
            c = self.sqlite().cursor()
            c.execute("SELECT 1 FROM ingestions WHERE file_hash=? LIMIT 1", (file_hash,))
            return c.fetchone() is not None
        with self.engine().connect() as conn:
            r = conn.execute(sa.text("SELECT 1 FROM ingestions WHERE file_hash=:h LIMIT 1"), {"h": file_hash}).fetchone()
            return r is not None

    def record_ingestion(self, file_hash: str, filename: str, kind: str, rows_written: int):
        now = pd.Timestamp.now().isoformat()
        if self.mode == "sqlite":
            c = self.sqlite().cursor()
            c.execute("""INSERT OR REPLACE INTO ingestions(file_hash, filename, kind, rows_written, ingested_at)
                         VALUES (?,?,?,?,?)""", (file_hash, filename, kind, int(rows_written), now))
            self.sqlite().commit()
            return
        with self.engine().begin() as conn:
            conn.execute(sa.text("""
                INSERT INTO ingestions(file_hash, filename, kind, rows_written, ingested_at)
                VALUES (:h,:f,:k,:r,:t)
                ON CONFLICT (file_hash) DO UPDATE
                SET filename=EXCLUDED.filename, kind=EXCLUDED.kind, rows_written=EXCLUDED.rows_written, ingested_at=EXCLUDED.ingested_at
            """), {"h": file_hash, "f": filename, "k": kind, "r": int(rows_written), "t": now})

    # ---- Upserts / Inserts

    def insert_charges(self, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        df = df.copy()
        df["updated_at"] = pd.Timestamp.now().isoformat()

        # Ensure expected cols exist
        for col in ["tracking","lgk_out","lgk_base","rma","ref_raw","ref_kind","exclude_reason","note","vat_source","country","courier"]:
            if col not in df.columns:
                df[col] = None
        if "is_excluded" not in df.columns:
            df["is_excluded"] = 0

        # Build stable row_key
        def rk(r):
            if r.get("tracking"):
                return f"CHARGE|{str(r.get('courier')).upper()}|TRK|{str(r.get('tracking')).strip().upper()}|{str(r.get('exec_date'))[:10]}|{float(r.get('charge_net') or 0):.2f}"
            if r.get("lgk_out"):
                return f"CHARGE|{str(r.get('courier')).upper()}|LGK|{str(r.get('lgk_out')).strip().upper()}|{str(r.get('exec_date'))[:10]}|{float(r.get('charge_net') or 0):.2f}"
            if r.get("rma"):
                return f"CHARGE|{str(r.get('courier')).upper()}|RMA|{str(r.get('rma')).strip().upper()}|{str(r.get('exec_date'))[:10]}|{float(r.get('charge_net') or 0):.2f}"
            if r.get("ref_raw"):
                return f"CHARGE|{str(r.get('courier')).upper()}|REF|{str(r.get('ref_raw')).strip().upper()}|{str(r.get('exec_date'))[:10]}|{float(r.get('charge_net') or 0):.2f}"
            return None

        df["row_key"] = df.apply(rk, axis=1)
        df = df[df["row_key"].notna()].copy()

        cols = ["row_key","courier","country","tracking","lgk_out","lgk_base","rma",
                "ref_raw","ref_kind","is_excluded","exclude_reason",
                "exec_date","charge_net","vat_rate","vat_amount","charge_gross","vat_source",
                "note","source_file","file_hash","updated_at"]

        df = df[cols].copy()

        if self.mode == "sqlite":
            con = self.sqlite()
            c = con.cursor()
            rows = [tuple(df.iloc[i].tolist()) for i in range(len(df))]
            c.executemany("""
                INSERT OR REPLACE INTO charges_raw(
                    row_key,courier,country,tracking,lgk_out,lgk_base,rma,ref_raw,ref_kind,is_excluded,exclude_reason,
                    exec_date,charge_net,vat_rate,vat_amount,charge_gross,vat_source,note,source_file,file_hash,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, rows)
            con.commit()
            return len(rows)

        with self.engine().begin() as conn:
            for _, r in df.iterrows():
                conn.execute(sa.text("""
                    INSERT INTO charges_raw(
                        row_key,courier,country,tracking,lgk_out,lgk_base,rma,ref_raw,ref_kind,is_excluded,exclude_reason,
                        exec_date,charge_net,vat_rate,vat_amount,charge_gross,vat_source,note,source_file,file_hash,updated_at
                    ) VALUES (
                        :row_key,:courier,:country,:tracking,:lgk_out,:lgk_base,:rma,:ref_raw,:ref_kind,:is_excluded,:exclude_reason,
                        :exec_date,:charge_net,:vat_rate,:vat_amount,:charge_gross,:vat_source,:note,:source_file,:file_hash,:updated_at
                    )
                    ON CONFLICT (row_key) DO UPDATE SET
                        courier=EXCLUDED.courier,
                        country=EXCLUDED.country,
                        tracking=EXCLUDED.tracking,
                        lgk_out=EXCLUDED.lgk_out,
                        lgk_base=EXCLUDED.lgk_base,
                        rma=EXCLUDED.rma,
                        ref_raw=EXCLUDED.ref_raw,
                        ref_kind=EXCLUDED.ref_kind,
                        is_excluded=EXCLUDED.is_excluded,
                        exclude_reason=EXCLUDED.exclude_reason,
                        exec_date=EXCLUDED.exec_date,
                        charge_net=EXCLUDED.charge_net,
                        vat_rate=EXCLUDED.vat_rate,
                        vat_amount=EXCLUDED.vat_amount,
                        charge_gross=EXCLUDED.charge_gross,
                        vat_source=EXCLUDED.vat_source,
                        note=EXCLUDED.note,
                        source_file=EXCLUDED.source_file,
                        file_hash=EXCLUDED.file_hash,
                        updated_at=EXCLUDED.updated_at
                """), r.to_dict())
        return len(df)

    def upsert_pickings(self, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        df = df.copy()
        df["updated_at"] = pd.Timestamp.now().isoformat()

        def rk(r):
            key = norm_text(r.get("source_document")) or norm_text(r.get("reference")) or norm_text(r.get("tracking"))
            return f"PICK|{norm_text(r.get('picking_type')).upper()}|{key.upper()}"

        df["row_key"] = df.apply(rk, axis=1)
        cols = ["row_key","picking_type","source_document","lgk_out","lgk_base","origin","reference","carrier","tracking","exec_date","state","source_file","file_hash","updated_at"]
        df = df[cols].copy()

        if self.mode == "sqlite":
            con = self.sqlite()
            c = con.cursor()
            rows = [tuple(df.iloc[i].tolist()) for i in range(len(df))]
            c.executemany("""
                INSERT OR REPLACE INTO pickings_raw(
                    row_key,picking_type,source_document,lgk_out,lgk_base,origin,reference,carrier,tracking,exec_date,state,source_file,file_hash,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, rows)
            con.commit()
            return len(rows)

        with self.engine().begin() as conn:
            for _, r in df.iterrows():
                conn.execute(sa.text("""
                    INSERT INTO pickings_raw(
                        row_key,picking_type,source_document,lgk_out,lgk_base,origin,reference,carrier,tracking,exec_date,state,source_file,file_hash,updated_at
                    ) VALUES (
                        :row_key,:picking_type,:source_document,:lgk_out,:lgk_base,:origin,:reference,:carrier,:tracking,:exec_date,:state,:source_file,:file_hash,:updated_at
                    )
                    ON CONFLICT (row_key) DO UPDATE SET
                        picking_type=EXCLUDED.picking_type,
                        source_document=EXCLUDED.source_document,
                        lgk_out=EXCLUDED.lgk_out,
                        lgk_base=EXCLUDED.lgk_base,
                        origin=EXCLUDED.origin,
                        reference=EXCLUDED.reference,
                        carrier=EXCLUDED.carrier,
                        tracking=EXCLUDED.tracking,
                        exec_date=EXCLUDED.exec_date,
                        state=EXCLUDED.state,
                        source_file=EXCLUDED.source_file,
                        file_hash=EXCLUDED.file_hash,
                        updated_at=EXCLUDED.updated_at
                """), r.to_dict())
        return len(df)

    def upsert_sales_fees(self, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        df = df.copy()
        df["updated_at"] = pd.Timestamp.now().isoformat()

        def rk(r):
            return f"SALEFEE|{norm_text(r.get('order_ref')).upper()}|{hashlib.md5(norm_text(r.get('line_name')).encode('utf-8')).hexdigest()}|{float(r.get('amount_gross') or 0):.2f}"

        df["row_key"] = df.apply(rk, axis=1)
        cols = ["row_key","order_ref","line_name","courier","country","exec_date","amount_gross","vat_included","source_file","file_hash","updated_at"]
        df = df[cols].copy()

        if self.mode == "sqlite":
            con = self.sqlite()
            c = con.cursor()
            rows = [tuple(df.iloc[i].tolist()) for i in range(len(df))]
            c.executemany("""
                INSERT OR REPLACE INTO sales_fees_raw(
                    row_key,order_ref,line_name,courier,country,exec_date,amount_gross,vat_included,source_file,file_hash,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, rows)
            con.commit()
            return len(rows)

        with self.engine().begin() as conn:
            for _, r in df.iterrows():
                conn.execute(sa.text("""
                    INSERT INTO sales_fees_raw(
                        row_key,order_ref,line_name,courier,country,exec_date,amount_gross,vat_included,source_file,file_hash,updated_at
                    ) VALUES (
                        :row_key,:order_ref,:line_name,:courier,:country,:exec_date,:amount_gross,:vat_included,:source_file,:file_hash,:updated_at
                    )
                    ON CONFLICT (row_key) DO UPDATE SET
                        order_ref=EXCLUDED.order_ref,
                        line_name=EXCLUDED.line_name,
                        courier=EXCLUDED.courier,
                        country=EXCLUDED.country,
                        exec_date=EXCLUDED.exec_date,
                        amount_gross=EXCLUDED.amount_gross,
                        vat_included=EXCLUDED.vat_included,
                        source_file=EXCLUDED.source_file,
                        file_hash=EXCLUDED.file_hash,
                        updated_at=EXCLUDED.updated_at
                """), r.to_dict())
        return len(df)

    def upsert_rma_fees(self, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        df = df.copy()
        df["updated_at"] = pd.Timestamp.now().isoformat()

        def rk(r):
            rma = norm_text(r.get("rma")).upper()
            return f"RMAFEE|{rma}|{float(r.get('service_cost_gross') or 0):.2f}|{norm_text(r.get('exec_date'))[:10]}"

        df["row_key"] = df.apply(rk, axis=1)
        cols = ["row_key","rma","order_ref","courier","country","exec_date","service_cost_gross","vat_included","state","source_file","file_hash","updated_at"]
        df = df[cols].copy()

        if self.mode == "sqlite":
            con = self.sqlite()
            c = con.cursor()
            rows = [tuple(df.iloc[i].tolist()) for i in range(len(df))]
            c.executemany("""
                INSERT OR REPLACE INTO rma_fees_raw(
                    row_key,rma,order_ref,courier,country,exec_date,service_cost_gross,vat_included,state,source_file,file_hash,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, rows)
            con.commit()
            return len(rows)

        with self.engine().begin() as conn:
            for _, r in df.iterrows():
                conn.execute(sa.text("""
                    INSERT INTO rma_fees_raw(
                        row_key,rma,order_ref,courier,country,exec_date,service_cost_gross,vat_included,state,source_file,file_hash,updated_at
                    ) VALUES (
                        :row_key,:rma,:order_ref,:courier,:country,:exec_date,:service_cost_gross,:vat_included,:state,:source_file,:file_hash,:updated_at
                    )
                    ON CONFLICT (row_key) DO UPDATE SET
                        rma=EXCLUDED.rma,
                        order_ref=EXCLUDED.order_ref,
                        courier=EXCLUDED.courier,
                        country=EXCLUDED.country,
                        exec_date=EXCLUDED.exec_date,
                        service_cost_gross=EXCLUDED.service_cost_gross,
                        vat_included=EXCLUDED.vat_included,
                        state=EXCLUDED.state,
                        source_file=EXCLUDED.source_file,
                        file_hash=EXCLUDED.file_hash,
                        updated_at=EXCLUDED.updated_at
                """), r.to_dict())
        return len(df)

    # ---- Loaders

    def load_all(self) -> Dict[str, pd.DataFrame]:
        if self.mode == "sqlite":
            con = self.sqlite()
            charges = pd.read_sql_query("SELECT * FROM charges_raw", con)
            pickings = pd.read_sql_query("SELECT * FROM pickings_raw", con)
            sales = pd.read_sql_query("SELECT * FROM sales_fees_raw", con)
            rma = pd.read_sql_query("SELECT * FROM rma_fees_raw", con)
        else:
            eng = self.engine()
            with eng.connect() as conn:
                charges = pd.read_sql(sa.text("SELECT * FROM charges_raw"), conn)
                pickings = pd.read_sql(sa.text("SELECT * FROM pickings_raw"), conn)
                sales = pd.read_sql(sa.text("SELECT * FROM sales_fees_raw"), conn)
                rma = pd.read_sql(sa.text("SELECT * FROM rma_fees_raw"), conn)

        # Normalize datetimes
        for df, col in [(charges,"exec_date"), (pickings,"exec_date"), (sales,"exec_date"), (rma,"exec_date")]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        return {"charges": charges, "pickings": pickings, "sales": sales, "rma": rma}


# ----------------------------
# Parsers: Courier charges
# ----------------------------

def parse_boxnow_pdf(filename: str, b: bytes, country: str, file_hash: str) -> pd.DataFrame:
    """BoxNow PDF: supports LGK/OUT refs and legacy 7-digit refs.
    - Aggregates LGK/OUT repeated (split parcel) into one row (sum cost, note).
    - Legacy 7-digit refs are ingested but excluded from calculations.
    """
    if not PDFPLUMBER_OK:
        raise RuntimeError("pdfplumber missing. For BoxNow PDFs we need pdfplumber.")
    text = ""
    with pdfplumber.open(io.BytesIO(b)) as pdf:
        for page in pdf.pages:
            text += "\n" + (page.extract_text() or "")

    rows = []
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines() if ln.strip()]
    for ln in lines:
        # amount last
        ma = re.findall(r"(-?\d{1,3}(?:\.\d{3})*(?:,\d{2})|-?\d+(?:,\d{2}))\b", ln)
        amt = norm_amount(ma[-1]) if ma else None
        if amt is None:
            continue
        # voucher/tracking: a long numeric token
        mv = re.search(r"\b(\d{8,12})\b", ln)
        voucher = mv.group(1) if mv else None
        # date
        md = re.search(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b", ln)
        d = pd.to_datetime(md.group(1), dayfirst=True, errors="coerce") if md else pd.NaT

        lgk = extract_lgk(ln)
        rma = extract_rma(ln)
        legacy = legacy_7digit_in_text(ln)

        ref_raw = lgk or rma or legacy or None
        ref_kind = "LGK_OUT" if lgk else ("RMA" if rma else ("LEGACY_7DIGIT" if legacy else None))

        is_excluded, excl_reason = compute_charge_exclusion(ln, lgk, rma)

        if not voucher and not ref_raw:
            continue

        rows.append({
            "courier": "BOXNOW",
            "country": country,
            "tracking": voucher,
            "exec_date": d,
            "lgk_out": lgk,
            "lgk_base": lgk_base(lgk) if lgk else None,
            "rma": rma,
            "ref_raw": ref_raw,
            "ref_kind": ref_kind,
            "is_excluded": int(is_excluded),
            "exclude_reason": excl_reason,
            "charge_net": amt,
            "vat_rate": None, "vat_amount": None, "charge_gross": None, "vat_source": None,
            "note": None,
            "source_file": filename,
            "file_hash": file_hash
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Aggregate only LGK/OUT
    has_lgk = df["lgk_out"].notna() & (df["lgk_out"].astype(str).str.strip() != "")
    df_lgk = df[has_lgk].copy()
    df_other = df[~has_lgk].copy()

    out_parts = []
    if not df_lgk.empty:
        out_lgk = df_lgk.groupby("lgk_out", dropna=False).agg(
            courier=("courier","first"),
            country=("country","first"),
            tracking=("tracking", lambda s: ",".join(sorted(set([str(x) for x in s.dropna()])))),
            exec_date=("exec_date","min"),
            lgk_out=("lgk_out","first"),
            lgk_base=("lgk_base","first"),
            rma=("rma","first"),
            ref_raw=("ref_raw","first"),
            ref_kind=("ref_kind","first"),
            is_excluded=("is_excluded","max"),
            exclude_reason=("exclude_reason","first"),
            charge_net=("charge_net","sum"),
            parcel_lines=("tracking","count"),
            source_file=("source_file","first"),
            file_hash=("file_hash","first"),
        ).reset_index(drop=True)
        out_lgk["note"] = out_lgk["parcel_lines"].apply(lambda n: "Split parcel (multiple vouchers)" if n and n > 1 else "")
        out_lgk["vat_rate"] = None
        out_lgk["vat_amount"] = None
        out_lgk["charge_gross"] = None
        out_lgk["vat_source"] = None
        out_parts.append(out_lgk.drop(columns=["parcel_lines"]))

    if not df_other.empty:
        df_other["note"] = df_other.apply(lambda r: "Legacy 7-digit ref (excluded)" if int(r.get("is_excluded") or 0)==1 else "", axis=1)
        out_parts.append(df_other)

    return pd.concat(out_parts, ignore_index=True)


def parse_acs_gr_xlsx(filename: str, b: bytes, country: str, file_hash: str) -> pd.DataFrame:
    df = safe_read_excel(b, filename)
    if df.empty:
        return df
    df = df.rename(columns={c: str(c).strip() for c in df.columns})

    tracking_col = next((c for c in df.columns if "Αριθμός Αποδεικτικού" in c or "Αριθμος Αποδεικτικου" in c), None)
    pick_date_col = next((c for c in df.columns if "Ημερομηνία Παραλαβής" in c or "Ημερομηνια Παραλαβης" in c), None)
    deliv_date_col = next((c for c in df.columns if "Ημερομηνία Παράδοσης" in c or "Ημερομηνια Παραδοσης" in c), None)
    net_col = next((c for c in df.columns if "Καθαρή Αξία" in c or "Καθαρη Αξια" in c), None)

    rel_cols = [c for c in df.columns if "Σχετικό" in c or "Σχετικο" in c or "ΣΧΕΤΙΚ" in c]

    rows = []
    for _, r in df.iterrows():
        tracking = norm_text(r.get(tracking_col)) if tracking_col else ""
        rels = [norm_text(r.get(c)) for c in rel_cols]
        rels = [x for x in rels if x]
        ref_text = " ".join([tracking] + rels).strip()

        lgk = extract_lgk(ref_text)
        rma = extract_rma(ref_text)
        legacy = legacy_7digit_in_text(ref_text)

        ref_raw = lgk or rma or legacy or None
        ref_kind = "LGK_OUT" if lgk else ("RMA" if rma else ("LEGACY_7DIGIT" if legacy else None))
        is_excluded, excl_reason = compute_charge_exclusion(ref_text, lgk, rma)

        exec_dt = norm_date(r.get(deliv_date_col)) if deliv_date_col else pd.NaT
        if pd.isna(exec_dt):
            exec_dt = norm_date(r.get(pick_date_col)) if pick_date_col else pd.NaT

        charge_net = norm_amount(r.get(net_col)) if net_col else None
        if not tracking and not ref_raw and charge_net is None:
            continue

        note = None
        if is_excluded:
            note = "Legacy 7-digit ref (excluded)"
        elif rels:
            note = "rels: " + " | ".join(rels[:3])

        rows.append({
            "courier":"ACS_GR","country":country,"tracking":tracking or None,
            "exec_date":exec_dt,"lgk_out":lgk,"lgk_base":lgk_base(lgk) if lgk else None,
            "rma":rma,
            "ref_raw":ref_raw,"ref_kind":ref_kind,
            "is_excluded":int(is_excluded),
            "exclude_reason":excl_reason,
            "charge_net":charge_net,
            "vat_rate":None,"vat_amount":None,"charge_gross":None,"vat_source":None,
            "note":note,"source_file":filename,"file_hash":file_hash
        })
    return pd.DataFrame(rows)


def parse_courier_center_xls(filename: str, b: bytes, country: str, file_hash: str) -> pd.DataFrame:
    """Courier Center .xls. LGK/RMA often in Σχετικό columns."""
    df = safe_read_excel(b, filename)
    if df.empty:
        return df
    df = df.rename(columns={c: str(c).strip() for c in df.columns})

    tracking_col = next((c for c in df.columns if "Αποδεικ" in c or "ΑΠΟΔΕΙΚ" in c), None)
    date_col = next((c for c in df.columns if "Ημερομην" in c), None)
    net_col = next((c for c in df.columns if "Χωρίς" in c or "Χωρις" in c), None)
    vat_amt_col = next((c for c in df.columns if "Ποσό ΦΠΑ" in c or "Ποσο ΦΠΑ" in c), None)
    vat_rate_col = next((c for c in df.columns if "% ΦΠΑ" in c or "ΦΠΑ %" in c), None)
    gross_col = next((c for c in df.columns if "Συνολική" in c or "Συνολικη" in c), None)
    rel_cols = [c for c in df.columns if "Σχετικ" in c or "ΣΧΕΤΙΚ" in c]

    rows = []
    for _, r in df.iterrows():
        tracking = norm_text(r.get(tracking_col)) if tracking_col else ""
        rels = [norm_text(r.get(c)) for c in rel_cols]
        rels = [x for x in rels if x]
        ref_text = " ".join([tracking] + rels).strip()

        lgk = extract_lgk(ref_text)
        rma = extract_rma(ref_text)
        legacy = legacy_7digit_in_text(ref_text)

        ref_raw = lgk or rma or legacy or None
        ref_kind = "LGK_OUT" if lgk else ("RMA" if rma else ("LEGACY_7DIGIT" if legacy else None))
        is_excluded, excl_reason = compute_charge_exclusion(ref_text, lgk, rma)

        exec_dt = norm_date(r.get(date_col)) if date_col else pd.NaT
        net = norm_amount(r.get(net_col)) if net_col else None
        vat_amt = norm_amount(r.get(vat_amt_col)) if vat_amt_col else None
        vr = norm_amount(r.get(vat_rate_col)) if vat_rate_col else None
        vat_rate = (vr/100.0) if vr is not None and vr > 1 else vr
        gross = norm_amount(r.get(gross_col)) if gross_col else None

        if not tracking and not ref_raw and net is None and gross is None:
            continue

        note = None
        if is_excluded:
            note = "Legacy 7-digit ref (excluded)"
        elif rels:
            note = "rels: " + " | ".join(rels[:3])

        rows.append({
            "courier":"COURIER_CENTER","country":country,"tracking":tracking or None,
            "exec_date":exec_dt,
            "lgk_out":lgk,
            "lgk_base":lgk_base(lgk) if lgk else None,
            "rma":rma,
            "ref_raw":ref_raw,"ref_kind":ref_kind,
            "is_excluded":int(is_excluded),
            "exclude_reason":excl_reason,
            "charge_net":net,
            "vat_rate":vat_rate,"vat_amount":vat_amt,"charge_gross":gross,
            "vat_source":"INVOICE_LINE" if (vat_rate is not None or vat_amt is not None or gross is not None) else None,
            "note":note,"source_file":filename,"file_hash":file_hash
        })
    return pd.DataFrame(rows)


def parse_acs_cy_pdf(filename: str, b: bytes, country: str, file_hash: str) -> pd.DataFrame:
    """ACS CY PDF parser – ONLY works for text-based PDFs. Scanned PDFs will error."""
    if not PDFPLUMBER_OK:
        raise RuntimeError("pdfplumber missing. For ACS CY PDFs we need pdfplumber.")
    text = ""
    with pdfplumber.open(io.BytesIO(b)) as pdf:
        for page in pdf.pages:
            text += "\n" + (page.extract_text() or "")
    if not text.strip():
        raise RuntimeError("ACS CY PDF seems scanned/no-text. Please export XLSX/CSV or use a text-based PDF.")

    rows = []
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines() if ln.strip()]
    for ln in lines:
        # a shipment number
        ms = re.search(r"\b(\d{6,12})\b", ln)
        if not ms:
            continue
        shipment = ms.group(1)
        md = re.search(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b", ln)
        d = pd.to_datetime(md.group(1), dayfirst=True, errors="coerce") if md else pd.NaT
        ma = re.findall(r"(-?\d{1,3}(?:\.\d{3})*(?:,\d{2})|-?\d+(?:,\d{2}))\b", ln)
        amt = norm_amount(ma[-1]) if ma else None
        if amt is None:
            continue

        lgk = extract_lgk(ln)
        rma = extract_rma(ln)
        legacy = legacy_7digit_in_text(ln)
        ref_raw = lgk or rma or legacy or None
        ref_kind = "LGK_OUT" if lgk else ("RMA" if rma else ("LEGACY_7DIGIT" if legacy else None))
        is_excluded, excl_reason = compute_charge_exclusion(ln, lgk, rma)

        rows.append({
            "courier":"ACS_CY","country":"CY","tracking":shipment,
            "exec_date":d,
            "lgk_out":lgk,
            "lgk_base":lgk_base(lgk) if lgk else None,
            "rma":rma,
            "ref_raw":ref_raw,"ref_kind":ref_kind,
            "is_excluded":int(is_excluded),
            "exclude_reason":excl_reason,
            "charge_net":amt,
            "vat_rate":None,"vat_amount":None,"charge_gross":None,"vat_source":None,
            "note":("Legacy 7-digit ref (excluded)" if is_excluded else None),
            "source_file":filename,"file_hash":file_hash
        })
    return pd.DataFrame(rows)


# ----------------------------
# Parsers: Odoo pickings + Sales + RMA
# ----------------------------

def parse_odoo_pickings(filename: str, b: bytes, picking_type: str, file_hash: str) -> pd.DataFrame:
    df = safe_read_excel(b, filename)
    if df.empty:
        return df
    df = df.rename(columns={c: str(c).strip() for c in df.columns})

    col_source = next((c for c in df.columns if c.lower() in ["source document","source_document","source"] or "Source Document" in c), None)
    col_ref = next((c for c in df.columns if c.lower() == "reference" or c == "Reference"), None)
    col_origin = next((c for c in df.columns if c.lower() == "origin" or c == "Origin"), None)
    col_carrier = next((c for c in df.columns if "Carrier" in c), None)
    col_tracking = next((c for c in df.columns if "Tracking" in c), None)
    col_date = next((c for c in df.columns if "Date of Transfer" in c or "Scheduled Date" in c or "Completion Date" in c), None)
    col_state = next((c for c in df.columns if c.lower() == "state" or c == "State"), None)

    rows = []
    for _, r in df.iterrows():
        source_document = norm_text(r.get(col_source)) if col_source else ""
        reference = norm_text(r.get(col_ref)) if col_ref else ""
        origin = norm_text(r.get(col_origin)) if col_origin else ""
        carrier = norm_text(r.get(col_carrier)) if col_carrier else ""
        tracking = norm_text(r.get(col_tracking)) if col_tracking else ""
        exec_dt = norm_date(r.get(col_date)) if col_date else pd.NaT
        state = norm_text(r.get(col_state)) if col_state else ""

        blob = " ".join([source_document, reference, origin, tracking]).strip()
        lgk = extract_lgk(blob)
        rma = extract_rma(blob)

        rows.append({
            "picking_type": picking_type,
            "source_document": source_document or None,
            "lgk_out": lgk,
            "lgk_base": lgk_base(lgk) if lgk else None,
            "origin": origin or None,
            "reference": reference or None,
            "carrier": carrier or None,
            "tracking": tracking or None,
            "exec_date": exec_dt,
            "state": state or None,
            "source_file": filename,
            "file_hash": file_hash
        })
    return pd.DataFrame(rows)


def parse_odoo_sales_all(filename: str, b: bytes, file_hash: str, *, country_for_vat: str, fee_regex: str) -> pd.DataFrame:
    df = safe_read_excel(b, filename)
    if df.empty:
        return df
    df = df.rename(columns={c: str(c).strip() for c in df.columns})

    required_any = ["Order Reference", "Order Lines", "Order Lines/Unit Price"]
    missing = [c for c in required_any if c not in df.columns]
    if missing:
        # Sometimes header starts lower
        try:
            tmp = pd.read_excel(io.BytesIO(b), header=None)
            header_row = None
            for i in range(min(30, len(tmp))):
                row_vals = set([str(x).strip() for x in tmp.iloc[i].tolist() if pd.notna(x)])
                if set(required_any).issubset(row_vals):
                    header_row = i
                    break
            if header_row is not None:
                df = pd.read_excel(io.BytesIO(b), header=header_row)
                df = df.rename(columns={c: str(c).strip() for c in df.columns})
                missing = [c for c in required_any if c not in df.columns]
        except Exception:
            pass
    if missing:
        raise RuntimeError(f"Sales Orders: λείπουν βασικές στήλες: {', '.join(missing)}")

    col_order = "Order Reference"
    col_line = "Order Lines"
    col_unit = "Order Lines/Unit Price"
    col_invq = next((c for c in df.columns if "Invoiced Quantity" in c), None)
    col_date = next((c for c in df.columns if c.lower() in ["order date","date order","confirmation date"] or "Order Date" in c), None)
    col_country = next((c for c in df.columns if "Country" in c and "Invoice" in c or c.lower()=="country"), None)
    col_carrier = next((c for c in df.columns if "Delivery Carrier" in c or "Carrier" in c), None)

    fee_re = re.compile(fee_regex, re.IGNORECASE)

    rows = []
    for _, r in df.iterrows():
        order_ref = norm_text(r.get(col_order))
        line_name = norm_text(r.get(col_line))
        if not order_ref or not line_name:
            continue

        # only courier fee lines
        if not fee_re.search(line_name):
            continue

        invq = norm_amount(r.get(col_invq)) if col_invq else None
        if invq is not None and invq == 0:
            # Your rule: zeros mean not collected / returned -> ignore
            continue
        if invq is None:
            invq = 1.0

        unit = norm_amount(r.get(col_unit)) or 0.0
        amount_gross = float(unit) * float(invq)

        exec_dt = norm_date(r.get(col_date)) if col_date else pd.NaT
        country = norm_text(r.get(col_country)) if col_country else country_for_vat
        courier_raw = norm_text(r.get(col_carrier)) if col_carrier else ""
        courier = courier_normalize(courier_raw) if courier_raw else courier_normalize(line_name)

        rows.append({
            "order_ref": order_ref,
            "line_name": line_name,
            "courier": courier,
            "country": country if country else country_for_vat,
            "exec_date": exec_dt,
            "amount_gross": amount_gross,
            "vat_included": 1,
            "source_file": filename,
            "file_hash": file_hash
        })

    return pd.DataFrame(rows)


def parse_odoo_rma(filename: str, b: bytes, file_hash: str, *, country_for_vat: str) -> pd.DataFrame:
    df = safe_read_excel(b, filename)
    if df.empty:
        return df
    df = df.rename(columns={c: str(c).strip() for c in df.columns})

    col_rma = next((c for c in df.columns if c.lower() == "name" or "RMA" in c), None)
    col_state = next((c for c in df.columns if c.lower() == "state" or "State" in c), None)
    col_cost = next((c for c in df.columns if "Service Cost" in c or "service cost" in c.lower()), None)
    col_date = next((c for c in df.columns if "Date" in c and "Create" in c or "Create Date" in c or "Completion" in c), None)
    col_order = next((c for c in df.columns if "Order" in c and "Reference" in c), None)
    col_carrier = next((c for c in df.columns if "Carrier" in c or "Delivery" in c), None)
    col_country = next((c for c in df.columns if "Country" in c), None)

    rows = []
    for _, r in df.iterrows():
        rma = norm_text(r.get(col_rma))
        if not rma:
            continue
        service_cost = norm_amount(r.get(col_cost)) if col_cost else None
        if service_cost is None or service_cost == 0:
            continue  # nothing to count
        state = norm_text(r.get(col_state)) if col_state else ""
        exec_dt = norm_date(r.get(col_date)) if col_date else pd.NaT
        order_ref = norm_text(r.get(col_order)) if col_order else ""
        country = norm_text(r.get(col_country)) if col_country else country_for_vat
        courier_raw = norm_text(r.get(col_carrier)) if col_carrier else ""
        courier = courier_normalize(courier_raw) if courier_raw else "UNKNOWN"

        rows.append({
            "rma": rma.upper(),
            "order_ref": order_ref or None,
            "courier": courier,
            "country": country if country else country_for_vat,
            "exec_date": exec_dt,
            "service_cost_gross": float(service_cost),
            "vat_included": 1,
            "state": state or None,
            "source_file": filename,
            "file_hash": file_hash
        })
    return pd.DataFrame(rows)


# ----------------------------
# VAT + Ledger + Matching
# ----------------------------

def ensure_charge_gross(charges: pd.DataFrame, default_country: str, vat_gr: float, vat_cy: float) -> pd.DataFrame:
    """Courier charges often come as net. Ensure gross using VAT if missing."""
    ch = charges.copy()
    if ch.empty:
        return ch

    # Normalize numeric cols
    for c in ["charge_net","vat_rate","vat_amount","charge_gross"]:
        if c in ch.columns:
            ch[c] = pd.to_numeric(ch[c], errors="coerce")

    def default_vat_for_country(ctry: str) -> float:
        c = (ctry or default_country or "GR").upper()
        if c in ["CY", "CYP", "CYPRUS"]:
            return vat_cy
        return vat_gr

    # If gross present, trust it
    mask_need = ch["charge_gross"].isna() & ch["charge_net"].notna()
    if mask_need.any():
        for i in ch[mask_need].index:
            ctry = norm_text(ch.at[i, "country"]) or default_country
            vr = ch.at[i, "vat_rate"]
            if pd.isna(vr) or vr is None:
                vr = default_vat_for_country(ctry)
                ch.at[i, "vat_rate"] = vr
                ch.at[i, "vat_source"] = ch.at[i, "vat_source"] or "DEFAULT"
            net = ch.at[i, "charge_net"]
            vat_amt = net * float(vr)
            ch.at[i, "vat_amount"] = vat_amt
            ch.at[i, "charge_gross"] = net + vat_amt

    return ch


def build_ledger(data: Dict[str, pd.DataFrame], default_country: str, vat_gr: float, vat_cy: float) -> Dict[str, pd.DataFrame]:
    charges = data["charges"].copy()
    pickings = data["pickings"].copy()
    sales = data["sales"].copy()
    rma = data["rma"].copy()

    # Excluded (legacy) bucket
    excluded = charges.copy()
    if not excluded.empty and "is_excluded" in excluded.columns:
        excluded = excluded[excluded["is_excluded"].fillna(0).astype(int) == 1].copy()
    else:
        excluded = excluded.iloc[0:0].copy()

    # Keep only non-excluded for calculations
    if not charges.empty and "is_excluded" in charges.columns:
        charges = charges[charges["is_excluded"].fillna(0).astype(int) != 1].copy()

    # Ensure gross for charges
    charges = ensure_charge_gross(charges, default_country, vat_gr, vat_cy)

    # Build mapping from pickings to sales orders (Origin typically equals sales order reference)
    pick_map = pickings.copy()
    for col in ["origin","reference","source_document","lgk_out","lgk_base","tracking"]:
        if col in pick_map.columns:
            pick_map[col] = pick_map[col].astype(str)
    # minimal map: order_ref -> lgk_base, lgk_out
    map_rows = []
    if not pick_map.empty:
        for _, r in pick_map.iterrows():
            origin = norm_text(r.get("origin"))
            if origin:
                map_rows.append({"order_ref": origin, "lgk_base": r.get("lgk_base"), "lgk_out": r.get("lgk_out")})
    map_df = pd.DataFrame(map_rows).dropna()
    if not map_df.empty:
        map_df = map_df.drop_duplicates(subset=["order_ref"], keep="first")

    # Attach lgk_base to sales and rma (for matching)
    if not sales.empty and not map_df.empty:
        sales = sales.merge(map_df, how="left", on="order_ref")
    else:
        sales["lgk_base"] = np.nan
        sales["lgk_out"] = np.nan

    if not rma.empty and not map_df.empty and "order_ref" in rma.columns:
        rma = rma.merge(map_df, how="left", on="order_ref")
    else:
        rma["lgk_base"] = np.nan
        rma["lgk_out"] = np.nan

    # Charges references
    charges["lgk_base"] = charges["lgk_base"].fillna(charges["lgk_out"].map(lgk_base))
    charges["ref_for_match"] = charges["lgk_out"].fillna(charges["lgk_base"]).fillna(charges.get("rma"))

    # Create unified ledger
    led_rows = []

    for _, r in charges.iterrows():
        led_rows.append({
            "entry_type": "CHARGE",
            "courier": norm_text(r.get("courier")) or "UNKNOWN",
            "country": norm_text(r.get("country")) or default_country,
            "exec_date": r.get("exec_date"),
            "amount_gross": float(r.get("charge_gross") or 0.0),
            "amount_net": float(r.get("charge_net") or 0.0) if pd.notna(r.get("charge_net")) else np.nan,
            "ref_kind": r.get("ref_kind") if "ref_kind" in r else None,
            "ref_raw": r.get("ref_raw") if "ref_raw" in r else None,
            "lgk_out": r.get("lgk_out"),
            "lgk_base": r.get("lgk_base"),
            "rma": r.get("rma"),
            "tracking": r.get("tracking"),
            "source": r.get("source_file"),
        })

    for _, r in sales.iterrows():
        led_rows.append({
            "entry_type": "REVENUE",
            "courier": norm_text(r.get("courier")) or "UNKNOWN",
            "country": norm_text(r.get("country")) or default_country,
            "exec_date": r.get("exec_date"),
            "amount_gross": float(r.get("amount_gross") or 0.0),
            "amount_net": np.nan,
            "ref_kind": "ORDER",
            "ref_raw": r.get("order_ref"),
            "lgk_out": r.get("lgk_out"),
            "lgk_base": r.get("lgk_base"),
            "rma": None,
            "tracking": None,
            "source": r.get("source_file"),
        })

    for _, r in rma.iterrows():
        led_rows.append({
            "entry_type": "RMA_REVENUE",
            "courier": norm_text(r.get("courier")) or "UNKNOWN",
            "country": norm_text(r.get("country")) or default_country,
            "exec_date": r.get("exec_date"),
            "amount_gross": float(r.get("service_cost_gross") or 0.0),
            "amount_net": np.nan,
            "ref_kind": "RMA",
            "ref_raw": r.get("rma"),
            "lgk_out": r.get("lgk_out"),
            "lgk_base": r.get("lgk_base"),
            "rma": r.get("rma"),
            "tracking": None,
            "source": r.get("source_file"),
        })


    ledger = pd.DataFrame(led_rows)

    # If no rows yet (e.g., DB empty), return empty outputs safely
    if ledger.empty:
        return {
            "ledger": ledger,
            "excluded": excluded,
            "matched": pd.DataFrame(),
            "unmatched_charges": pd.DataFrame(),
            "unmatched_revenues": pd.DataFrame(),
            "weekly": pd.DataFrame(),
            "monthly": pd.DataFrame(),
            "quarterly": pd.DataFrame(),
            "yearly": pd.DataFrame(),
            "charges": charges,
            "sales": sales,
            "rma": rma,
            "pickings": pickings,
        }

    if not ledger.empty:
        ledger["exec_date"] = pd.to_datetime(ledger["exec_date"], errors="coerce")
        ledger["week"] = ledger["exec_date"].apply(week_start)
        ledger["month"] = ledger["exec_date"].dt.to_period("M").astype(str)
        ledger["quarter"] = ledger["exec_date"].apply(quarter_label)
        ledger["year"] = ledger["exec_date"].dt.year.astype("Int64")

    # Matching (charge ↔ revenue) – conservative
    matched = []
    unmatched_charges = []
    unmatched_revenues = []

    # Split charges and revenues
    ch = ledger[ledger["entry_type"] == "CHARGE"].copy()
    rev = ledger[ledger["entry_type"].isin(["REVENUE","RMA_REVENUE"])].copy()

    # Build quick index for revenues by lgk_out / lgk_base / rma
    rev_idx_lgk_out = {}
    rev_idx_lgk_base = {}
    rev_idx_rma = {}

    for i, r in rev.iterrows():
        if pd.notna(r.get("lgk_out")) and norm_text(r.get("lgk_out")):
            rev_idx_lgk_out.setdefault(norm_text(r.get("lgk_out")).upper(), []).append(i)
        if pd.notna(r.get("lgk_base")) and norm_text(r.get("lgk_base")):
            rev_idx_lgk_base.setdefault(norm_text(r.get("lgk_base")).upper(), []).append(i)
        if pd.notna(r.get("rma")) and norm_text(r.get("rma")):
            rev_idx_rma.setdefault(norm_text(r.get("rma")).upper(), []).append(i)

    used_rev = set()

    for ci, cr in ch.iterrows():
        best = None
        best_conf = 0.0

        lgk_out_v = norm_text(cr.get("lgk_out")).upper()
        lgk_base_v = norm_text(cr.get("lgk_base")).upper()
        rma_v = norm_text(cr.get("rma")).upper()

        candidates = []
        if lgk_out_v and lgk_out_v in rev_idx_lgk_out:
            candidates += [(ri, 1.0, "lgk_out") for ri in rev_idx_lgk_out[lgk_out_v]]
        if lgk_base_v and lgk_base_v in rev_idx_lgk_base:
            candidates += [(ri, 0.95, "lgk_base") for ri in rev_idx_lgk_base[lgk_base_v]]
        if rma_v and rma_v in rev_idx_rma:
            candidates += [(ri, 1.0, "rma") for ri in rev_idx_rma[rma_v]]

        # Choose first unused highest confidence
        for ri, conf, why in sorted(candidates, key=lambda x: -x[1]):
            if ri in used_rev:
                continue
            best = (ri, why)
            best_conf = conf
            break

        if best is None:
            unmatched_charges.append(cr.to_dict())
            continue

        ri, why = best
        rr = rev.loc[ri]
        used_rev.add(ri)

        matched.append({
            **{f"charge_{k}": v for k, v in cr.to_dict().items()},
            **{f"rev_{k}": v for k, v in rr.to_dict().items()},
            "match_confidence": best_conf,
            "match_reason": why
        })

    # Unmatched revenues = those not used
    for ri, rr in rev.iterrows():
        if ri not in used_rev:
            unmatched_revenues.append(rr.to_dict())

    matched_df = pd.DataFrame(matched)
    unmatched_ch_df = pd.DataFrame(unmatched_charges)
    unmatched_rev_df = pd.DataFrame(unmatched_revenues)

    # Summaries
    def summary_by(period_col: str) -> pd.DataFrame:
        if ledger.empty:
            return pd.DataFrame()
        tmp = ledger.copy()
        tmp["sign"] = tmp["entry_type"].apply(lambda t: -1 if t=="CHARGE" else 1)
        tmp["net_gross"] = tmp["amount_gross"] * tmp["sign"]
        g = tmp.groupby(["courier", period_col], dropna=False).agg(
            charges_gross=("amount_gross", lambda s: float(tmp.loc[s.index][tmp.loc[s.index]["entry_type"]=="CHARGE"]["amount_gross"].sum())),
            revenues_gross=("amount_gross", lambda s: float(tmp.loc[s.index][tmp.loc[s.index]["entry_type"]!="CHARGE"]["amount_gross"].sum())),
            net_gross=("net_gross","sum"),
            rows=("entry_type","count"),
        ).reset_index()
        return g.sort_values(["courier", period_col])

    weekly = summary_by("week")
    monthly = summary_by("month")
    quarterly = summary_by("quarter")
    yearly = summary_by("year")

    return {
        "ledger": ledger,
        "excluded": excluded,
        "matched": matched_df,
        "unmatched_charges": unmatched_ch_df,
        "unmatched_revenues": unmatched_rev_df,
        "weekly": weekly,
        "monthly": monthly,
        "quarterly": quarterly,
        "yearly": yearly,
        "charges": charges,
        "sales": sales,
        "rma": rma,
        "pickings": pickings,
    }


def export_excel(tables: Dict[str, pd.DataFrame]) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as xw:
        order = [
            ("Ledger", "ledger"),
            ("Matched", "matched"),
            ("Unmatched Charges", "unmatched_charges"),
            ("Unmatched Revenues", "unmatched_revenues"),
            ("Excluded (Legacy)", "excluded"),
            ("Weekly", "weekly"),
            ("Monthly", "monthly"),
            ("Quarterly", "quarterly"),
            ("Yearly", "yearly"),
            ("Charges (clean)", "charges"),
            ("Sales Fees", "sales"),
            ("RMA Fees", "rma"),
            ("Pickings", "pickings"),
        ]
        for sheet, key in order:
            df = tables.get(key)
            if df is None:
                continue
            if df.empty:
                pd.DataFrame({"empty": []}).to_excel(xw, sheet_name=sheet, index=False)
            else:
                df.to_excel(xw, sheet_name=sheet, index=False)
    return bio.getvalue()


# ----------------------------
# Streamlit UI
# ----------------------------

def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    with st.sidebar:
        st.header("Database")
        db_mode = st.selectbox("DB mode", ["postgres (Neon via Secrets)", "sqlite (local file)"])

        if db_mode.startswith("postgres"):
            # IMPORTANT: Never show DB URL in UI.
            pg_url = None
            try:
                pg_url = st.secrets.get("DATABASE_URL")
            except Exception:
                pg_url = None
            pg_url = pg_url or os.getenv("DATABASE_URL")
            if not pg_url:
                st.warning("DATABASE_URL δεν βρέθηκε στα Secrets. Θα δουλέψουμε με SQLite local.")
                db = DB(mode="sqlite", sqlite_path=DB_SQLITE_DEFAULT)
            else:
                db = DB(mode="postgres", pg_url=pg_url)
        else:
            sqlite_path = st.text_input("SQLite file", DB_SQLITE_DEFAULT)
            db = DB(mode="sqlite", sqlite_path=sqlite_path)

        if st.button("Init / Migrate DB (safe)"):
            db.init_schema()
            st.success("DB ready.")

        st.header("Ingestion settings")
        skip_identical = st.toggle("Skip identical files (file_hash)", value=True)
        default_country = st.selectbox("Default country (when missing)", ["GR", "CY"], index=0)
        vat_gr = st.selectbox("VAT rate GR", [0.24, 0.13, 0.06], index=0)
        vat_cy = st.selectbox("VAT rate CY", [0.19, 0.05], index=0)
        fee_regex = st.text_input("Courier fee line regex (Sales Orders)", DEFAULT_COURIER_FEE_REGEX)

    st.subheader("1) Upload files (slots)")
    c1, c2 = st.columns(2)

    with c1:
        st.markdown("### Courier invoices (cost)")
        boxnow_pdfs = st.file_uploader("BoxNow invoices (PDF)", type=["pdf"], accept_multiple_files=True, key="boxnow_pdf")
        acs_gr_xlsx = st.file_uploader("ACS GR invoices (XLSX)", type=["xlsx"], accept_multiple_files=True, key="acs_gr_xlsx")
        courier_center_xls = st.file_uploader("Courier Center invoices (XLS)", type=["xls"], accept_multiple_files=True, key="courier_center_xls")
        acs_cy_pdf = st.file_uploader("ACS CY invoices (PDF - text-based)", type=["pdf"], accept_multiple_files=True, key="acs_cy_pdf")

    with c2:
        st.markdown("### Odoo exports (matching + revenues)")
        odoo_pick_orders = st.file_uploader("Odoo Pickings Orders (XLSX)", type=["xlsx"], accept_multiple_files=True, key="odoo_pick_orders")
        odoo_pick_returns = st.file_uploader("Odoo Pickings Returns (XLSX)", type=["xlsx"], accept_multiple_files=True, key="odoo_pick_returns")
        odoo_sales_all = st.file_uploader("Odoo Sales Orders all (XLSX)", type=["xlsx"], accept_multiple_files=True, key="odoo_sales_all")
        odoo_rma = st.file_uploader("Odoo RMA all (XLSX)", type=["xlsx"], accept_multiple_files=True, key="odoo_rma")

    st.caption("Uploads can be incremental (one file today, another in 10 days). The DB keeps history. Overlaps in Odoo are handled via upserts.")

    if st.button("Process uploads"):
        db.init_schema()
        results = []

        def ingest_many(kind: str, uploads, country_override: Optional[str]=None):
            if not uploads:
                return
            for upl in uploads:
                b = upl.read()
                fhash = sha256_bytes(b)
                if skip_identical and db.ingested(fhash):
                    results.append((upl.name, kind, "SKIPPED (already ingested)"))
                    continue

                try:
                    rows_written = 0
                    if kind == "boxnow_pdf":
                        d = parse_boxnow_pdf(upl.name, b, country_override or default_country, fhash)
                        rows_written = db.insert_charges(d)
                    elif kind == "acs_gr_xlsx":
                        d = parse_acs_gr_xlsx(upl.name, b, country_override or default_country, fhash)
                        rows_written = db.insert_charges(d)
                    elif kind == "courier_center_xls":
                        d = parse_courier_center_xls(upl.name, b, country_override or default_country, fhash)
                        rows_written = db.insert_charges(d)
                    elif kind == "acs_cy_pdf":
                        d = parse_acs_cy_pdf(upl.name, b, "CY", fhash)
                        rows_written = db.insert_charges(d)

                    elif kind == "odoo_pick_orders":
                        d = parse_odoo_pickings(upl.name, b, "orders", fhash)
                        rows_written = db.upsert_pickings(d)
                    elif kind == "odoo_pick_returns":
                        d = parse_odoo_pickings(upl.name, b, "returns", fhash)
                        rows_written = db.upsert_pickings(d)
                    elif kind == "odoo_sales_all":
                        d = parse_odoo_sales_all(upl.name, b, fhash, country_for_vat=default_country, fee_regex=fee_regex)
                        rows_written = db.upsert_sales_fees(d)
                    elif kind == "odoo_rma":
                        d = parse_odoo_rma(upl.name, b, fhash, country_for_vat=default_country)
                        rows_written = db.upsert_rma_fees(d)

                    db.record_ingestion(fhash, upl.name, kind, int(rows_written))
                    results.append((upl.name, kind, f"OK (rows written: {rows_written})"))
                except Exception as e:
                    results.append((upl.name, kind, f"ERROR: {e}"))

        ingest_many("boxnow_pdf", boxnow_pdfs or [])
        ingest_many("acs_gr_xlsx", acs_gr_xlsx or [])
        ingest_many("courier_center_xls", courier_center_xls or [])
        ingest_many("acs_cy_pdf", acs_cy_pdf or [])
        ingest_many("odoo_pick_orders", odoo_pick_orders or [])
        ingest_many("odoo_pick_returns", odoo_pick_returns or [])
        ingest_many("odoo_sales_all", odoo_sales_all or [])
        ingest_many("odoo_rma", odoo_rma or [])

        if results:
            st.subheader("Ingestion results")
            st.dataframe(pd.DataFrame(results, columns=["file","kind","status"]), use_container_width=True)
        else:
            st.info("No files uploaded.")

    st.subheader("2) Monitoring & Export")
    db.init_schema()
    data = db.load_all()

    counts = {
        "charges_rows": len(data["charges"]),
        "pickings_rows": len(data["pickings"]),
        "sales_fee_rows": len(data["sales"]),
        "rma_fee_rows": len(data["rma"]),
        "excluded_legacy_rows": int((data["charges"].get("is_excluded", pd.Series(dtype=int)).fillna(0).astype(int) == 1).sum()) if not data["charges"].empty else 0,
    }
    st.dataframe(pd.DataFrame([counts]), use_container_width=True)

    tables = build_ledger(data, default_country=default_country, vat_gr=vat_gr, vat_cy=vat_cy)

    c3, c4 = st.columns(2)
    with c3:
        st.markdown("### Weekly net (gross) per courier")
        if tables["weekly"].empty:
            st.info("No weekly data yet.")
        else:
            st.dataframe(tables["weekly"], use_container_width=True)
    with c4:
        st.markdown("### Excluded (Legacy 7-digit)")
        if tables["excluded"].empty:
            st.info("No excluded legacy rows.")
        else:
            show_cols = [c for c in ["courier","country","exec_date","tracking","ref_raw","exclude_reason","charge_net","charge_gross","source_file"] if c in tables["excluded"].columns]
            st.dataframe(tables["excluded"][show_cols], use_container_width=True)

    st.markdown("### Matched vs Unmatched")
    c5, c6 = st.columns(2)
    with c5:
        st.markdown("Matched (conservative, >=95% when base match)")
        st.dataframe(tables["matched"].head(200), use_container_width=True)
    with c6:
        st.markdown("Unmatched Charges / Revenues")
        st.write("Unmatched Charges")
        st.dataframe(tables["unmatched_charges"].head(200), use_container_width=True)
        st.write("Unmatched Revenues")
        st.dataframe(tables["unmatched_revenues"].head(200), use_container_width=True)

    st.markdown("### Download Excel export")
    xbytes = export_excel(tables)
    st.download_button("Download TFP_Courier_Recon.xlsx", data=xbytes, file_name="TFP_Courier_Recon.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


if __name__ == "__main__":
    main()