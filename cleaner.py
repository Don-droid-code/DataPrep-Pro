"""
cleaner.py — DataPrep Pro v2
The 7 Pillars of Data Cleaning.

Each public function returns (cleaned_df, log_entry_dict).
All operations are non-destructive: they receive a df, return a new df.
The app stores the log list in session state for the Cleaning Report.
"""

from __future__ import annotations

import re
import json
import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from engine import coerce_numeric


# ── Log helpers ───────────────────────────────────────────────────────────────

def _log(pillar: int, title: str, detail: str, rows_before: int, rows_after: int,
         cols_before: int, cols_after: int) -> Dict:
    return {
        "ts":           datetime.datetime.now().strftime("%H:%M:%S"),
        "pillar":       pillar,
        "title":        title,
        "detail":       detail,
        "rows_before":  rows_before,
        "rows_after":   rows_after,
        "cols_before":  cols_before,
        "cols_after":   cols_after,
        "rows_delta":   rows_before - rows_after,
        "cols_delta":   cols_before - cols_after,
    }


# ══════════════════════════════════════════════════════════════════════════════
# PILLAR 1 — DUPLICATES
# ══════════════════════════════════════════════════════════════════════════════

def detect_duplicates(df: pd.DataFrame, subset: Optional[List[str]] = None
                      ) -> Dict[str, Any]:
    """
    Return a dict describing duplicate rows.
    subset: columns to consider (None = all columns).
    """
    dup_mask = df.duplicated(subset=subset, keep=False)
    dup_df   = df[dup_mask].copy()
    dup_df["__dup_group__"] = df[dup_mask].groupby(
        list(df.columns if subset is None else subset)
    ).ngroup()
    n_dup_rows = int(df.duplicated(subset=subset, keep="first").sum())

    return {
        "n_exact_dup_rows":  n_dup_rows,          # rows that would be dropped
        "n_dup_in_groups":   int(dup_mask.sum()),  # total rows in dup groups
        "dup_preview":       dup_df.head(20),      # for UI display
        "mask":              dup_mask,
    }


def drop_duplicates(df: pd.DataFrame, subset: Optional[List[str]] = None,
                    keep: str = "first") -> Tuple[pd.DataFrame, Dict]:
    rb, cb = len(df), len(df.columns)
    cleaned = df.drop_duplicates(subset=subset, keep=keep).reset_index(drop=True)
    dropped = rb - len(cleaned)
    entry = _log(1, "Drop Duplicates",
                 f"Kept '{keep}' occurrence; dropped {dropped} duplicate row(s)"
                 + (f" on subset: {subset}" if subset else ""),
                 rb, len(cleaned), cb, len(cleaned.columns))
    return cleaned, entry


# ══════════════════════════════════════════════════════════════════════════════
# PILLAR 2 — MISSING VALUES
# ══════════════════════════════════════════════════════════════════════════════

def missing_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Return a summary DataFrame of missing values per column."""
    total = len(df)
    rows = []
    for col in df.columns:
        n_miss = int(df[col].isna().sum())
        rows.append({
            "column":    col,
            "missing":   n_miss,
            "pct":       round(n_miss / max(total, 1) * 100, 1),
            "dtype":     str(df[col].dtype),
            "sample":    _sample(df[col]),
        })
    return pd.DataFrame(rows).sort_values("pct", ascending=False).reset_index(drop=True)


def _sample(s: pd.Series, n: int = 3) -> str:
    vals = s.dropna().head(n).tolist()
    return ", ".join(str(v)[:20] for v in vals) if vals else "—"


def fill_missing(df: pd.DataFrame, strategy: str, columns: List[str],
                 custom_value: Any = None) -> Tuple[pd.DataFrame, Dict]:
    """
    strategy: "drop_rows" | "mean" | "median" | "mode" | "custom" | "ffill" | "bfill"
    columns:  list of column names to apply to ([] = all)
    """
    rb, cb = len(df), len(df.columns)
    cleaned = df.copy()
    cols = columns if columns else list(df.columns)
    total_filled = 0

    if strategy == "drop_rows":
        mask = cleaned[cols].isna().any(axis=1)
        total_filled = int(mask.sum())
        cleaned = cleaned[~mask].reset_index(drop=True)
        detail = f"Dropped {total_filled} row(s) with missing values in {len(cols)} column(s)"
    else:
        for col in cols:
            if col not in cleaned.columns:
                continue
            n_before = int(cleaned[col].isna().sum())
            if strategy == "mean":
                num = coerce_numeric(cleaned[col])
                cleaned[col] = cleaned[col].where(cleaned[col].notna(), num.mean())
            elif strategy == "median":
                num = coerce_numeric(cleaned[col])
                cleaned[col] = cleaned[col].where(cleaned[col].notna(), num.median())
            elif strategy == "mode":
                mode_val = cleaned[col].mode()
                if len(mode_val):
                    cleaned[col] = cleaned[col].fillna(mode_val.iloc[0])
            elif strategy == "custom":
                cleaned[col] = cleaned[col].fillna(custom_value)
            elif strategy == "ffill":
                cleaned[col] = cleaned[col].ffill()
            elif strategy == "bfill":
                cleaned[col] = cleaned[col].bfill()
            total_filled += n_before - int(cleaned[col].isna().sum())
        detail = (f"Strategy '{strategy}': filled {total_filled} missing value(s) "
                  f"across {len(cols)} column(s)")

    entry = _log(2, f"Fill Missing — {strategy}", detail, rb, len(cleaned), cb, len(cleaned.columns))
    return cleaned, entry


def drop_high_missing_cols(df: pd.DataFrame, threshold_pct: float = 70.0
                           ) -> Tuple[pd.DataFrame, Dict]:
    rb, cb = len(df), len(df.columns)
    pcts = df.isna().mean() * 100
    drop_cols = pcts[pcts >= threshold_pct].index.tolist()
    cleaned = df.drop(columns=drop_cols)
    entry = _log(2, "Drop High-Missing Columns",
                 f"Dropped {len(drop_cols)} column(s) with ≥{threshold_pct}% missing: "
                 + (", ".join(drop_cols[:10]) + (" …" if len(drop_cols) > 10 else "") if drop_cols else "none"),
                 rb, len(cleaned), cb, len(cleaned.columns))
    return cleaned, entry


# ══════════════════════════════════════════════════════════════════════════════
# PILLAR 3 — DATA TYPE DETECTION & COERCION
# ══════════════════════════════════════════════════════════════════════════════

_DATE_PATTERNS = [
    re.compile(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}$"),
    re.compile(r"^\d{1,2}[-/]\d{1,2}[-/]\d{4}$"),
    re.compile(r"^\d{4}$"),
    re.compile(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[\s\-]\d{4}$", re.I),
]

def detect_types(df: pd.DataFrame) -> List[Dict]:
    """
    Auto-detect the semantic type of each column.
    Returns list of {column, detected_type, current_dtype, pct_parseable, sample, suggestion}
    """
    result = []
    for col in df.columns:
        s   = df[col]
        det = _detect_one(s)
        result.append({
            "column":        col,
            "detected_type": det["type"],
            "current_dtype": str(s.dtype),
            "pct_parseable": det["pct"],
            "sample":        _sample(s),
            "suggestion":    det["suggestion"],
        })
    return result


def _detect_one(s: pd.Series) -> Dict:
    non_null = s.dropna()
    n = len(non_null)
    if n == 0:
        return {"type": "empty", "pct": 0.0, "suggestion": "Drop column (fully empty)"}

    # Already numeric?
    if pd.api.types.is_numeric_dtype(s):
        return {"type": "numeric", "pct": 100.0, "suggestion": "Already numeric"}

    # Try numeric coercion
    coerced = coerce_numeric(non_null)
    pct_num = coerced.notna().mean() * 100
    if pct_num >= 80:
        has_currency = bool(non_null.astype(str).str.contains(r"[$€£¥]", na=False).any())
        has_pct      = bool(non_null.astype(str).str.rstrip().str.endswith("%").any())
        if has_pct:
            return {"type": "percentage", "pct": pct_num, "suggestion": "Convert to numeric (strip %)"}
        if has_currency:
            return {"type": "currency",   "pct": pct_num, "suggestion": "Convert to numeric (strip symbol)"}
        return {"type": "numeric",        "pct": pct_num, "suggestion": "Convert to numeric"}

    # Try date
    sample_strs = non_null.astype(str).head(50)
    date_hits   = sum(1 for v in sample_strs if any(p.match(v.strip()) for p in _DATE_PATTERNS))
    pct_date    = date_hits / max(len(sample_strs), 1) * 100
    if pct_date >= 60:
        return {"type": "date", "pct": pct_date, "suggestion": "Convert to datetime (YYYY-MM-DD)"}

    # Text / categorical
    n_unique = int(s.nunique(dropna=True))
    if n_unique <= 20:
        return {"type": "categorical", "pct": 100.0, "suggestion": "Low cardinality — consider encoding"}
    return {"type": "text", "pct": 100.0, "suggestion": "Free text — no conversion needed"}


def coerce_column_type(df: pd.DataFrame, column: str, target_type: str,
                       date_format: Optional[str] = None) -> Tuple[pd.DataFrame, Dict]:
    """
    Coerce a column to target_type: 'numeric' | 'date' | 'text' | 'percentage'
    """
    rb, cb = len(df), len(df.columns)
    cleaned = df.copy()
    before_nulls = int(cleaned[column].isna().sum())

    if target_type == "numeric":
        cleaned[column] = coerce_numeric(cleaned[column])
    elif target_type == "percentage":
        s = cleaned[column].astype(str).str.rstrip("%").str.strip()
        cleaned[column] = pd.to_numeric(s, errors="coerce") / 100
    elif target_type == "date":
        if date_format:
            cleaned[column] = pd.to_datetime(cleaned[column], format=date_format, errors="coerce")
        else:
            cleaned[column] = pd.to_datetime(cleaned[column], infer_datetime_format=True, errors="coerce")
        cleaned[column] = cleaned[column].dt.strftime("%Y-%m-%d")
    elif target_type == "text":
        cleaned[column] = cleaned[column].astype(str).replace("nan", "")

    after_nulls  = int(cleaned[column].isna().sum())
    parse_fails  = max(0, after_nulls - before_nulls)
    entry = _log(3, f"Coerce Type → {target_type}",
                 f"Column '{column}': {parse_fails} value(s) could not be parsed → NaN",
                 rb, len(cleaned), cb, len(cleaned.columns))
    return cleaned, entry


# ══════════════════════════════════════════════════════════════════════════════
# PILLAR 4 — OUTLIERS
# ══════════════════════════════════════════════════════════════════════════════

def detect_outliers(df: pd.DataFrame, method: str = "iqr",
                    zscore_threshold: float = 3.0,
                    iqr_multiplier:   float = 1.5) -> Dict[str, Any]:
    """
    Returns {column → {n_outliers, lower_bound, upper_bound, outlier_values}}
    for all numeric columns with outliers detected.
    """
    result = {}
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    for col in num_cols:
        s = df[col].dropna()
        if len(s) < 4:
            continue
        if method == "iqr":
            q1, q3 = s.quantile(0.25), s.quantile(0.75)
            iqr    = q3 - q1
            lower  = q1 - iqr_multiplier * iqr
            upper  = q3 + iqr_multiplier * iqr
            mask   = (df[col] < lower) | (df[col] > upper)
        else:  # z-score
            mean, std = s.mean(), s.std()
            if std == 0:
                continue
            zs    = (df[col] - mean).abs() / std
            mask  = zs > zscore_threshold
            lower = mean - zscore_threshold * std
            upper = mean + zscore_threshold * std

        n_out = int(mask.sum())
        if n_out > 0:
            result[col] = {
                "n_outliers":    n_out,
                "pct_outliers":  round(n_out / len(df) * 100, 1),
                "lower_bound":   round(float(lower), 4),
                "upper_bound":   round(float(upper), 4),
                "outlier_values": df.loc[mask, col].dropna().head(10).tolist(),
                "method":        method,
            }
    return result


def handle_outliers(df: pd.DataFrame, columns: List[str], action: str,
                    method: str = "iqr", iqr_multiplier: float = 1.5,
                    zscore_threshold: float = 3.0) -> Tuple[pd.DataFrame, Dict]:
    """
    action: 'keep' | 'drop' | 'cap' | 'flag'
    """
    rb, cb = len(df), len(df.columns)
    cleaned = df.copy()
    total_affected = 0

    for col in columns:
        if col not in cleaned.columns:
            continue
        s = cleaned[col].dropna()
        if len(s) < 4:
            continue

        if method == "iqr":
            q1, q3  = s.quantile(0.25), s.quantile(0.75)
            iqr     = q3 - q1
            lower   = q1 - iqr_multiplier * iqr
            upper   = q3 + iqr_multiplier * iqr
        else:
            mean, std = s.mean(), s.std()
            lower = mean - zscore_threshold * std
            upper = mean + zscore_threshold * std

        mask = (cleaned[col] < lower) | (cleaned[col] > upper)
        total_affected += int(mask.sum())

        if action == "drop":
            cleaned = cleaned[~mask]
        elif action == "cap":
            cleaned[col] = cleaned[col].clip(lower=lower, upper=upper)
        elif action == "flag":
            flag_col = f"{col}__outlier"
            cleaned[flag_col] = mask.astype(int)

    if action == "drop":
        cleaned = cleaned.reset_index(drop=True)

    entry = _log(4, f"Outliers — {action}",
                 f"Method: {method}; {action}ped {total_affected} outlier(s) across {len(columns)} column(s)",
                 rb, len(cleaned), cb, len(cleaned.columns))
    return cleaned, entry


# ══════════════════════════════════════════════════════════════════════════════
# PILLAR 5 — FINANCIAL-SPECIFIC CLEANING
# ══════════════════════════════════════════════════════════════════════════════

def detect_currency_columns(df: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Detect which columns contain currency symbols, and which currencies.
    Returns {column: [detected_currencies]}.
    """
    CURRENCY_RE = re.compile(r"[$€£¥₹₩₽]|USD|EUR|GBP|CHF|JPY|INR", re.I)
    result = {}
    for col in df.select_dtypes(include="object").columns:
        hits = df[col].astype(str).str.findall(CURRENCY_RE).explode().dropna()
        hits = hits[hits != ""]
        if len(hits):
            result[col] = list(hits.unique()[:5])
    return result


def standardise_column_names(df: pd.DataFrame,
                              rename_map: Optional[Dict[str, str]] = None
                              ) -> Tuple[pd.DataFrame, Dict]:
    """
    Rename columns to canonical financial names using the SYNONYMS dict.
    rename_map: user-supplied overrides {old_name: new_name} (takes precedence).
    Auto-builds suggestions from SYNONYMS if rename_map is None.
    """
    from synonyms import SYNONYMS, CANONICAL_NAMES

    rb, cb = len(df), len(df.columns)
    cleaned = df.copy()

    if rename_map is None:
        # Auto-detect
        rename_map = {}
        col_lower  = {c.lower().strip(): c for c in cleaned.columns}
        used_targets: set = set()

        for canonical, aliases in SYNONYMS.items():
            if canonical in used_targets:
                continue
            for alias in aliases:
                alias_clean = re.sub(r"[_\-\s]+", " ", alias.lower()).strip()
                for src_low, src_orig in col_lower.items():
                    src_clean = re.sub(r"[_\-\s]+", " ", src_low).strip()
                    if src_clean == alias_clean and src_orig not in rename_map:
                        rename_map[src_orig] = canonical
                        used_targets.add(canonical)
                        break
                if canonical in used_targets:
                    break

    # Apply (skip if no change or target col already exists)
    final_map = {}
    for old, new in rename_map.items():
        if old in cleaned.columns and old != new and new not in cleaned.columns:
            final_map[old] = new

    cleaned = cleaned.rename(columns=final_map)

    renames_str = "; ".join(f'"{o}" → "{n}"' for o, n in list(final_map.items())[:10])
    if len(final_map) > 10:
        renames_str += f" … (+{len(final_map)-10} more)"

    entry = _log(5, "Standardise Column Names",
                 f"{len(final_map)} column(s) renamed: {renames_str or 'none'}",
                 rb, len(cleaned), cb, len(cleaned.columns))
    return cleaned, entry


def drop_empty_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    rb, cb = len(df), len(df.columns)
    empty  = [c for c in df.columns if df[c].isna().all()]
    cleaned = df.drop(columns=empty)
    entry = _log(5, "Drop Empty Columns",
                 f"Removed {len(empty)} fully-empty column(s)"
                 + (": " + ", ".join(empty[:10]) if empty else ""),
                 rb, len(cleaned), cb, len(cleaned.columns))
    return cleaned, entry


def convert_currency(df: pd.DataFrame, column: str,
                     from_symbol: str, rate: float) -> Tuple[pd.DataFrame, Dict]:
    """Multiply a numeric column by rate (currency conversion)."""
    rb, cb = len(df), len(df.columns)
    cleaned = df.copy()
    cleaned[column] = coerce_numeric(cleaned[column]) * rate
    entry = _log(5, "Currency Conversion",
                 f"Column '{column}': ×{rate} ({from_symbol} → base currency)",
                 rb, len(cleaned), cb, len(cleaned.columns))
    return cleaned, entry


# ══════════════════════════════════════════════════════════════════════════════
# PILLAR 6 — STANDARDISATION
# ══════════════════════════════════════════════════════════════════════════════

def standardise_dates(df: pd.DataFrame, columns: List[str],
                      input_format: Optional[str] = None,
                      output_format: str = "%Y-%m-%d") -> Tuple[pd.DataFrame, Dict]:
    rb, cb = len(df), len(df.columns)
    cleaned = df.copy()
    converted, failed = 0, 0

    for col in columns:
        if col not in cleaned.columns:
            continue
        before_nulls = int(cleaned[col].isna().sum())
        if input_format:
            parsed = pd.to_datetime(cleaned[col], format=input_format, errors="coerce")
        else:
            parsed = pd.to_datetime(cleaned[col], infer_datetime_format=True, errors="coerce")
        after_nulls  = int(parsed.isna().sum())
        failed      += max(0, after_nulls - before_nulls)
        converted   += int(parsed.notna().sum())
        cleaned[col] = parsed.dt.strftime(output_format)

    entry = _log(6, "Standardise Dates",
                 f"Converted {converted} date value(s) to {output_format}; {failed} parse failures",
                 rb, len(cleaned), cb, len(cleaned.columns))
    return cleaned, entry


def standardise_numbers(df: pd.DataFrame, columns: List[str]) -> Tuple[pd.DataFrame, Dict]:
    """Strip thousand separators and normalise decimal to dot for numeric cols."""
    rb, cb = len(df), len(df.columns)
    cleaned = df.copy()
    fixed = 0
    for col in columns:
        if col not in cleaned.columns:
            continue
        cleaned[col] = coerce_numeric(cleaned[col])
        fixed += 1
    entry = _log(6, "Standardise Numbers",
                 f"Coerced {fixed} column(s) to clean float64 (no separators, dot decimal)",
                 rb, len(cleaned), cb, len(cleaned.columns))
    return cleaned, entry


def standardise_text_case(df: pd.DataFrame, columns: List[str],
                           case: str = "title") -> Tuple[pd.DataFrame, Dict]:
    """case: 'title' | 'upper' | 'lower' | 'strip'"""
    rb, cb = len(df), len(df.columns)
    cleaned = df.copy()
    for col in columns:
        if col not in cleaned.columns:
            continue
        s = cleaned[col].astype(str)
        if case == "title":
            cleaned[col] = s.str.title()
        elif case == "upper":
            cleaned[col] = s.str.upper()
        elif case == "lower":
            cleaned[col] = s.str.lower()
        elif case == "strip":
            cleaned[col] = s.str.strip()
    entry = _log(6, f"Standardise Text Case ({case})",
                 f"Applied '{case}' case to {len(columns)} column(s)",
                 rb, len(cleaned), cb, len(cleaned.columns))
    return cleaned, entry


# ══════════════════════════════════════════════════════════════════════════════
# AUTO-CLEAN — applies safe defaults in one pass
# ══════════════════════════════════════════════════════════════════════════════

def auto_clean(df: pd.DataFrame, fill_strategy: str = "mean",
               missing_threshold: float = 70.0) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    One-click safe cleaning:
    1. Drop fully-empty columns
    2. Drop exact duplicates
    3. Drop columns with > threshold % missing
    4. Fill remaining missing with fill_strategy
    5. Standardise financial column names
    6. Flag (don't drop) numeric outliers
    Returns (cleaned_df, log_entries).
    """
    log = []
    cleaned = df.copy()

    cleaned, e = drop_empty_columns(cleaned)
    log.append(e)

    cleaned, e = drop_duplicates(cleaned, keep="first")
    log.append(e)

    cleaned, e = drop_high_missing_cols(cleaned, threshold_pct=missing_threshold)
    log.append(e)

    num_cols = cleaned.select_dtypes(include=[np.number]).columns.tolist()
    if num_cols:
        cleaned, e = fill_missing(cleaned, fill_strategy, num_cols)
        log.append(e)

    cleaned, e = standardise_column_names(cleaned)
    log.append(e)

    # Flag outliers in numeric cols (do not drop)
    num_cols_now = cleaned.select_dtypes(include=[np.number]).columns.tolist()
    if num_cols_now:
        outliers = detect_outliers(cleaned, method="iqr")
        flagged_cols = list(outliers.keys())
        if flagged_cols:
            cleaned, e = handle_outliers(cleaned, flagged_cols, action="flag")
            log.append(e)

    return cleaned, log


# ══════════════════════════════════════════════════════════════════════════════
# CLEANING REPORT
# ══════════════════════════════════════════════════════════════════════════════

def build_report_html(original_df: pd.DataFrame, cleaned_df: pd.DataFrame,
                      log: List[Dict]) -> str:
    """Generate a self-contained HTML cleaning report."""
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    orig_missing  = int(original_df.isna().sum().sum())
    clean_missing = int(cleaned_df.isna().sum().sum())
    orig_dup      = int(original_df.duplicated().sum())
    clean_dup     = int(cleaned_df.duplicated().sum())

    steps_html = ""
    for i, e in enumerate(log, 1):
        dr = e["rows_delta"]
        dc = e["cols_delta"]
        row_badge = (f'<span style="color:#ff6060">−{dr} rows</span>' if dr > 0
                     else f'<span style="color:#00c8a8">no row change</span>')
        col_badge = (f'<span style="color:#f0a020">−{dc} cols</span>' if dc > 0
                     else f'<span style="color:#00c8a8">no col change</span>')
        steps_html += f"""
        <div style="border-left:3px solid #00c8a8;padding:10px 16px;margin-bottom:10px;background:#0d1e2c;border-radius:0 4px 4px 0">
          <div style="font-family:monospace;font-size:11px;color:#4a7088">Step {i} · {e['ts']}</div>
          <div style="font-weight:700;color:#e8f4fb;margin:4px 0">{e['title']}</div>
          <div style="font-size:12px;color:#7ab8d4">{e['detail']}</div>
          <div style="font-size:11px;margin-top:6px">{row_badge} &nbsp; {col_badge}</div>
        </div>"""

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>DataPrep Pro — Cleaning Report</title>
<style>
  body {{font-family:'Segoe UI',sans-serif;background:#080f14;color:#c9d8e3;margin:0;padding:32px;}}
  h1 {{font-family:monospace;background:linear-gradient(135deg,#00c8a8,#0084d4);
       -webkit-background-clip:text;-webkit-text-fill-color:transparent;font-size:28px;margin-bottom:4px}}
  h2 {{font-family:monospace;font-size:14px;letter-spacing:.1em;color:#4a7088;
       border-bottom:1px solid #1a3347;padding-bottom:8px;margin-top:32px}}
  .stat-grid {{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:16px 0}}
  .stat {{background:#0d1e2c;border:1px solid #1a3347;border-radius:6px;
          padding:14px;text-align:center}}
  .stat-num {{font-family:monospace;font-size:28px;font-weight:700;color:#00c8a8}}
  .stat-lbl {{font-size:10px;letter-spacing:.1em;color:#4a7088;margin-top:4px}}
  .delta-pos {{color:#00c8a8}} .delta-neg {{color:#ff6060}} .delta-neu {{color:#4a7088}}
</style></head><body>
<h1>⚙ DataPrep Pro — Cleaning Report</h1>
<div style="font-size:12px;color:#4a7088">Generated {ts}</div>

<h2>DATASET SUMMARY</h2>
<div class="stat-grid">
  <div class="stat"><div class="stat-num">{len(original_df):,}</div><div class="stat-lbl">ORIGINAL ROWS</div></div>
  <div class="stat"><div class="stat-num">{len(cleaned_df):,}</div><div class="stat-lbl">CLEAN ROWS</div></div>
  <div class="stat"><div class="stat-num">{len(original_df.columns)}</div><div class="stat-lbl">ORIGINAL COLS</div></div>
  <div class="stat"><div class="stat-num">{len(cleaned_df.columns)}</div><div class="stat-lbl">CLEAN COLS</div></div>
</div>
<div class="stat-grid">
  <div class="stat"><div class="stat-num" style="color:{'#00c8a8' if orig_missing==0 else '#f0a020'}">{orig_missing:,}</div><div class="stat-lbl">MISSING (BEFORE)</div></div>
  <div class="stat"><div class="stat-num" style="color:{'#00c8a8' if clean_missing==0 else '#f0a020'}">{clean_missing:,}</div><div class="stat-lbl">MISSING (AFTER)</div></div>
  <div class="stat"><div class="stat-num" style="color:{'#00c8a8' if orig_dup==0 else '#ff6060'}">{orig_dup}</div><div class="stat-lbl">DUPLICATES (BEFORE)</div></div>
  <div class="stat"><div class="stat-num">{clean_dup}</div><div class="stat-lbl">DUPLICATES (AFTER)</div></div>
</div>

<h2>CLEANING LOG ({len(log)} operations)</h2>
{steps_html if steps_html else '<p style="color:#4a7088">No operations recorded.</p>'}

<h2>COLUMN OVERVIEW (after cleaning)</h2>
<table style="width:100%;border-collapse:collapse;font-size:12px">
  <tr style="background:#0d1e2c;font-family:monospace;font-size:10px;letter-spacing:.05em;color:#4a7088">
    <th style="padding:8px;text-align:left">COLUMN</th>
    <th style="padding:8px;text-align:left">DTYPE</th>
    <th style="padding:8px;text-align:right">MISSING %</th>
    <th style="padding:8px;text-align:right">UNIQUE</th>
  </tr>
  {''.join(
    f'<tr style="border-bottom:1px solid #1a3347">'
    f'<td style="padding:7px 8px;color:#e8f4fb">{col}</td>'
    f'<td style="padding:7px 8px;font-family:monospace;color:#4db8ff">{str(cleaned_df[col].dtype)}</td>'
    f'<td style="padding:7px 8px;text-align:right;color:{"#ff6060" if (p:=round(cleaned_df[col].isna().mean()*100,1))>30 else "#00c8a8"}">{p}%</td>'
    f'<td style="padding:7px 8px;text-align:right;color:#7ab8d4">{cleaned_df[col].nunique()}</td></tr>'
    for col in cleaned_df.columns
  )}
</table>
</body></html>"""
    return html
