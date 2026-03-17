"""
engine.py — DataPrep Pro v2
File loading, basic cleaning, numeric coercion, CSV export.
Mapping / derivation / FinAnalyst Pro output removed — those live in FinAnalyst Pro.
"""

from __future__ import annotations

import io
import re
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# ── File loading ──────────────────────────────────────────────────────────────

def load_file(file_obj) -> Tuple[Optional[pd.DataFrame], Optional[str], List[str], List[str]]:
    """Load CSV or Excel. Returns (df, error_msg, sheet_names, rename_log)."""
    try:
        name = getattr(file_obj, "name", "").lower()
        if name.endswith(".csv"):
            for enc in ("utf-8", "latin-1", "cp1252"):
                try:
                    file_obj.seek(0)
                    df = pd.read_csv(file_obj, encoding=enc, thousands=",")
                    df, renames = deduplicate_columns(df)
                    return df, None, [], renames
                except UnicodeDecodeError:
                    continue
            return None, "Could not decode CSV — try saving as UTF-8.", [], []
        elif name.endswith((".xlsx", ".xls")):
            file_obj.seek(0)
            xl  = pd.ExcelFile(file_obj)
            sheets = xl.sheet_names
            df  = xl.parse(sheets[0])
            df, renames = deduplicate_columns(df)
            return df, None, sheets, renames
        else:
            return None, "Unsupported file type. Upload .csv, .xlsx, or .xls.", [], []
    except Exception as e:
        return None, f"Error reading file: {e}", [], []


def load_excel_sheet(file_obj, sheet_name: str) -> Optional[pd.DataFrame]:
    try:
        file_obj.seek(0)
        df = pd.ExcelFile(file_obj).parse(sheet_name)
        df, _ = deduplicate_columns(df)
        return df
    except Exception:
        return None


def load_google_sheet(url: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    try:
        import urllib.request
        if "/edit" in url or "/pub" in url:
            gid_match = re.search(r"gid=(\d+)", url)
            gid = gid_match.group(1) if gid_match else "0"
            sheet_id = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
            if not sheet_id:
                return None, "Could not parse Google Sheets URL."
            csv_url = (f"https://docs.google.com/spreadsheets/d/"
                       f"{sheet_id.group(1)}/export?format=csv&gid={gid}")
        elif "export?format=csv" in url:
            csv_url = url
        else:
            return None, "Share via File → Share → Publish to web → CSV."
        with urllib.request.urlopen(csv_url, timeout=15) as resp:
            content = resp.read().decode("utf-8")
        df = pd.read_csv(io.StringIO(content))
        df, _ = deduplicate_columns(df)
        return df, None
    except Exception as e:
        return None, f"Failed to fetch sheet: {e}"


# ── Column deduplication ──────────────────────────────────────────────────────

def deduplicate_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """Fix duplicate / blank / Unnamed column names. Returns (fixed_df, rename_log)."""
    raw = [str(c).strip() for c in df.columns]
    renamed: List[str] = []

    # Step 1 — normalise pandas auto-suffix (.1 → _1)
    step1 = []
    for c in raw:
        n = re.sub(r"\.([0-9]+)$", r"_\1", c)
        if n != c:
            renamed.append(f'pandas-renamed "{c}" → "{n}"')
        step1.append(n)

    # Step 2 — blank / Unnamed: N
    step2 = []
    for i, c in enumerate(step1):
        if c == "" or re.match(r"^unnamed:\s*\d+$", c, re.IGNORECASE):
            new = f"Column_{i + 1}"
            renamed.append(f'blank/unnamed → "{new}"')
            step2.append(new)
        else:
            step2.append(c)

    # Step 3 — true duplicates
    seen: dict = {}
    step3 = []
    for c in step2:
        if c in seen:
            seen[c] += 1
            new = f"{c}_{seen[c]}"
            renamed.append(f'duplicate "{c}" → "{new}"')
            step3.append(new)
        else:
            seen[c] = 0
            step3.append(c)

    df = df.copy()
    df.columns = step3
    return df, renamed


def basic_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Strip column names, deduplicate, drop fully-empty rows/cols."""
    try:
        df = df.copy()
        raw_cols = [str(c).strip() for c in df.columns]
        cleaned  = []
        for i, c in enumerate(raw_cols):
            cleaned.append(f"Column_{i+1}" if (c == "" or c.lower().startswith("unnamed:")) else c)
        seen = {}
        deduped = []
        for c in cleaned:
            if c in seen:
                seen[c] += 1
                deduped.append(f"{c}_{seen[c]}")
            else:
                seen[c] = 0
                deduped.append(c)
        df.columns = deduped
        df = df.dropna(how="all", axis=1).dropna(how="all", axis=0).reset_index(drop=True)
        return df
    except Exception:
        try:
            df.columns = [f"Col_{i}" for i in range(len(df.columns))]
        except Exception:
            pass
        return df.reset_index(drop=True)


def coerce_numeric(series: pd.Series) -> pd.Series:
    """Convert a series to float64, handling currency/thousands/accounting formats."""
    try:
        if pd.api.types.is_numeric_dtype(series):
            return pd.to_numeric(series, errors="coerce")
        s = series.apply(
            lambda x: "" if x is None or (isinstance(x, float) and np.isnan(x)) else str(x)
        ).str.strip()
        null_strings = {"", "nan", "none", "null", "n/a", "na", "#n/a", "-", "—", "–"}
        s = s.apply(lambda x: np.nan if x.lower() in null_strings else x)

        def _clean(x):
            if not isinstance(x, str):
                return x
            x = re.sub(r"[$€£¥\s]", "", x)
            x = re.sub(r",(?=\d{3})", "", x)
            x = re.sub(r"\(([0-9.,]+)\)", r"-\1", x)
            x = x.rstrip("%").strip()
            return x if x else np.nan

        return pd.to_numeric(s.apply(_clean), errors="coerce")
    except Exception:
        return pd.to_numeric(series, errors="coerce")


# ── Export ────────────────────────────────────────────────────────────────────

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_csv(buf, index=False, encoding="utf-8")
    return buf.getvalue()
