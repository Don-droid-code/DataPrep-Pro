"""
profiler.py — DataPrep Pro
Comprehensive column profiling engine. Zero external dependencies beyond pandas/numpy.

Returns structured dicts consumed by the Streamlit UI in render_profile().
Analyses first SAMPLE_ROWS rows if dataset is large, for performance.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

SAMPLE_ROWS = 1_000          # cap for large datasets
HIGH_MISSING_THRESHOLD = 30  # % → red flag
LOW_CARD_THRESHOLD = 10      # unique values → low-cardinality flag
IQR_MULTIPLIER = 3.0         # outlier sensitivity (higher = fewer outliers flagged)

# ── Regex patterns for type-sniffing ─────────────────────────────────────────
_DATE_PATTERNS = [
    r"^\d{4}[-/]\d{2}[-/]\d{2}$",          # 2023-01-15
    r"^\d{2}[-/]\d{2}[-/]\d{4}$",          # 15-01-2023
    r"^\d{4}$",                             # bare year: 2023
    r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[\s\-]\d{4}$",
]
_DATE_RE = [re.compile(p, re.IGNORECASE) for p in _DATE_PATTERNS]


# ─────────────────────────────────────────────────────────────────────────────
# Dataset-level overview
# ─────────────────────────────────────────────────────────────────────────────

def dataset_overview(df: pd.DataFrame) -> Dict[str, Any]:
    """Return a dict of dataset-level metrics."""
    n = len(df)
    sample_df = df.head(SAMPLE_ROWS) if n > SAMPLE_ROWS else df
    sampled = n > SAMPLE_ROWS

    try:
        mem_bytes = df.memory_usage(deep=True).sum()
        mem_str = _fmt_bytes(mem_bytes)
    except Exception:
        mem_str = "—"

    try:
        dup_rows = int(sample_df.duplicated().sum())
        if sampled:
            # Extrapolate estimate
            dup_rows = round(dup_rows * n / SAMPLE_ROWS)
    except Exception:
        dup_rows = 0

    total_cells = n * len(df.columns)
    missing_cells = int(df.isna().sum().sum()) if total_cells > 0 else 0
    completeness = round((1 - missing_cells / max(total_cells, 1)) * 100, 1)

    return {
        "rows":          n,
        "columns":       len(df.columns),
        "memory":        mem_str,
        "dup_rows":      dup_rows,
        "missing_cells": missing_cells,
        "total_cells":   total_cells,
        "completeness":  completeness,
        "sampled":       sampled,
        "sample_rows":   SAMPLE_ROWS,
    }


def _fmt_bytes(b: int) -> str:
    if b >= 1_073_741_824: return f"{b/1_073_741_824:.1f} GB"
    if b >= 1_048_576:     return f"{b/1_048_576:.1f} MB"
    if b >= 1_024:         return f"{b/1_024:.1f} KB"
    return f"{b} B"


# ─────────────────────────────────────────────────────────────────────────────
# Per-column statistics
# ─────────────────────────────────────────────────────────────────────────────

def profile_column(col_name: str, series: pd.Series) -> Dict[str, Any]:
    """
    Compute rich statistics for one column.
    Never raises — wraps everything in try/except.
    """
    n = len(series)

    # ── Basic null stats ──────────────────────────────────────────────────────
    try:
        n_null = int(series.isna().sum())
    except Exception:
        n_null = sum(1 for v in series if v is None or v != v)
    pct_null = round(n_null / max(n, 1) * 100, 1)

    # ── Unique count ──────────────────────────────────────────────────────────
    try:
        n_unique = int(series.nunique(dropna=True))
    except Exception:
        n_unique = 0

    # ── Dtype detection ───────────────────────────────────────────────────────
    raw_dtype = str(series.dtype)
    detected_type, type_confidence = _detect_type(series)

    # ── Numeric stats ─────────────────────────────────────────────────────────
    num_stats: Dict[str, Any] = {}
    outlier_count = 0
    if detected_type in ("integer", "float"):
        num_series = pd.to_numeric(series, errors="coerce").dropna()
        if len(num_series) > 0:
            try:
                num_stats = {
                    "min":    _fmt_num(num_series.min()),
                    "max":    _fmt_num(num_series.max()),
                    "mean":   _fmt_num(num_series.mean()),
                    "median": _fmt_num(num_series.median()),
                    "std":    _fmt_num(num_series.std()),
                }
                outlier_count = _count_outliers(num_series)
            except Exception:
                pass
    elif detected_type == "numeric_string":
        # Parse currency/comma numbers
        cleaned = _clean_numeric_str(series).dropna()
        if len(cleaned) > 0:
            try:
                num_stats = {
                    "min":    _fmt_num(cleaned.min()),
                    "max":    _fmt_num(cleaned.max()),
                    "mean":   _fmt_num(cleaned.mean()),
                    "median": _fmt_num(cleaned.median()),
                    "std":    _fmt_num(cleaned.std()),
                }
                outlier_count = _count_outliers(cleaned)
            except Exception:
                pass

    # ── Sample values ─────────────────────────────────────────────────────────
    try:
        sample_vals = series.dropna().head(3).tolist()
        sample = ", ".join(str(v)[:22] for v in sample_vals) if sample_vals else "—"
    except Exception:
        sample = "—"

    # ── Issues / flags ────────────────────────────────────────────────────────
    issues = _detect_issues(
        col_name, series, detected_type, pct_null, n_unique, n, outlier_count, type_confidence
    )

    # ── Quality flag ──────────────────────────────────────────────────────────
    quality = _quality_flag(pct_null, issues)

    return {
        "column":          col_name,
        "raw_dtype":       raw_dtype,
        "detected_type":   detected_type,
        "type_confidence": type_confidence,
        "n_missing":       n_null,
        "pct_missing":     pct_null,
        "n_unique":        n_unique,
        "num_stats":       num_stats,
        "sample":          sample,
        "outlier_count":   outlier_count,
        "issues":          issues,
        "quality":         quality,   # "green" | "yellow" | "red"
    }


def _detect_type(series: pd.Series) -> Tuple[str, str]:
    """
    Detect semantic type. Returns (type_label, confidence).
    confidence: "high" | "medium" | "low"
    """
    try:
        if pd.api.types.is_bool_dtype(series):
            return "boolean", "high"

        if pd.api.types.is_integer_dtype(series):
            return "integer", "high"

        if pd.api.types.is_float_dtype(series):
            return "float", "high"

        if pd.api.types.is_datetime64_any_dtype(series):
            return "date", "high"

        # Object column — sniff content
        non_null = series.dropna()
        if len(non_null) == 0:
            return "empty", "high"

        sample = non_null.head(min(200, len(non_null)))

        # Try direct numeric conversion
        num = pd.to_numeric(sample, errors="coerce")
        numeric_ratio = num.notna().mean()
        if numeric_ratio >= 0.95:
            int_ratio = (num.dropna() % 1 == 0).mean()
            return ("integer" if int_ratio >= 0.99 else "float"), "high"
        if numeric_ratio >= 0.70:
            return "numeric_string", "medium"

        # Try cleaned numeric conversion (handles $, commas, parentheses)
        s_clean = sample.astype(str).str.strip()
        s_clean = s_clean.str.replace(r"[$€£¥,\s]", "", regex=True)
        s_clean = s_clean.str.replace(r"\(([0-9.,]+)\)", r"-\1", regex=True)
        s_clean = s_clean.str.rstrip("%")
        num_clean = pd.to_numeric(s_clean, errors="coerce")
        clean_ratio = num_clean.notna().mean()
        if clean_ratio >= 0.90:
            return "numeric_string", "high"
        if clean_ratio >= 0.60:
            return "numeric_string", "medium"

        # Date sniff
        str_sample = sample.astype(str)
        date_matches = sum(
            1 for v in str_sample
            if any(pat.match(v.strip()) for pat in _DATE_RE)
        )
        if date_matches / max(len(str_sample), 1) >= 0.70:
            return "date_string", "medium"

        return "string", "high"

    except Exception:
        return "unknown", "low"


def _clean_numeric_str(series: pd.Series) -> pd.Series:
    """Strip currency / thousands separators and convert to float."""
    s = series.astype(str).str.strip()
    s = s.str.replace(r"[$€£¥,\s]", "", regex=True)
    s = s.str.replace(r"\(([0-9.,]+)\)", r"-\1", regex=True)
    s = s.str.rstrip("%")
    return pd.to_numeric(s, errors="coerce")


def _count_outliers(series: pd.Series) -> int:
    """IQR-based outlier count. Returns 0 on any error."""
    try:
        if len(series) < 4:
            return 0
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            return 0
        lo = q1 - IQR_MULTIPLIER * iqr
        hi = q3 + IQR_MULTIPLIER * iqr
        return int(((series < lo) | (series > hi)).sum())
    except Exception:
        return 0


def _fmt_num(v) -> str:
    """Format a number for display."""
    try:
        if pd.isna(v):
            return "—"
        if abs(v) >= 1_000_000_000:
            return f"{v/1e9:.2f}B"
        if abs(v) >= 1_000_000:
            return f"{v/1e6:.2f}M"
        if abs(v) >= 1_000:
            return f"{v:,.0f}"
        if v == int(v):
            return f"{int(v):,}"
        return f"{v:,.2f}"
    except Exception:
        return str(v)


def _detect_issues(
    col_name: str,
    series: pd.Series,
    detected_type: str,
    pct_null: float,
    n_unique: int,
    n_total: int,
    outlier_count: int,
    type_confidence: str,
) -> List[Dict[str, str]]:
    """Return list of issue dicts: {severity, code, label}."""
    issues = []

    # High missingness
    if pct_null > HIGH_MISSING_THRESHOLD:
        sev = "red" if pct_null >= 70 else "yellow"
        issues.append({
            "severity": sev,
            "code":     "high_missing",
            "label":    f"{pct_null:.0f}% missing",
        })

    # Mixed types (numeric_string with medium confidence)
    if detected_type == "numeric_string" and type_confidence == "medium":
        issues.append({
            "severity": "yellow",
            "code":     "mixed_types",
            "label":    "Mixed types (numbers + text)",
        })

    # Potential date stored as string
    if detected_type == "date_string":
        issues.append({
            "severity": "yellow",
            "code":     "date_as_string",
            "label":    "Dates stored as text",
        })

    # Low cardinality (likely categorical)
    if (detected_type in ("string", "integer", "numeric_string")
            and 1 < n_unique <= LOW_CARD_THRESHOLD
            and n_total > 20):
        issues.append({
            "severity": "yellow",
            "code":     "low_cardinality",
            "label":    f"Low cardinality ({n_unique} values)",
        })

    # Outliers
    if outlier_count > 0:
        ratio = outlier_count / max(n_total, 1) * 100
        issues.append({
            "severity": "yellow",
            "code":     "outliers",
            "label":    f"{outlier_count} outlier{'s' if outlier_count != 1 else ''} ({ratio:.1f}%)",
        })

    # Constant column
    if n_unique <= 1 and n_total > 1:
        issues.append({
            "severity": "yellow",
            "code":     "constant",
            "label":    "Constant column (no variation)",
        })

    return issues


def _quality_flag(pct_null: float, issues: List[Dict]) -> str:
    if any(i["severity"] == "red" for i in issues):
        return "red"
    if issues:
        return "yellow"
    if pct_null > 5:
        return "yellow"
    return "green"


# ─────────────────────────────────────────────────────────────────────────────
# Full profile (all columns)
# ─────────────────────────────────────────────────────────────────────────────

def full_profile(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Run the complete profiling pipeline.
    Returns a dict with keys: overview, columns, quality_score, suggestions.
    Safe to cache in st.session_state.
    """
    sample_df = df.head(SAMPLE_ROWS) if len(df) > SAMPLE_ROWS else df

    overview = dataset_overview(df)

    col_profiles = []
    for col in sample_df.columns:
        try:
            p = profile_column(col, sample_df[col])
            col_profiles.append(p)
        except Exception as e:
            col_profiles.append({
                "column":          str(col),
                "raw_dtype":       "unknown",
                "detected_type":   "unknown",
                "type_confidence": "low",
                "n_missing":       0,
                "pct_missing":     0.0,
                "n_unique":        0,
                "num_stats":       {},
                "sample":          "—",
                "outlier_count":   0,
                "issues":          [{"severity": "yellow", "code": "error",
                                     "label": f"Profile error: {e}"}],
                "quality":         "yellow",
            })

    quality_score = _global_quality_score(df, col_profiles)
    suggestions   = _generate_suggestions(col_profiles, df)

    return {
        "overview":      overview,
        "columns":       col_profiles,
        "quality_score": quality_score,
        "suggestions":   suggestions,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Global quality score
# ─────────────────────────────────────────────────────────────────────────────

def _global_quality_score(df: pd.DataFrame, col_profiles: List[Dict]) -> Dict[str, Any]:
    """
    Composite 0-100 score from three sub-scores.
    Returns score dict with breakdown.
    """
    n_cols = max(len(col_profiles), 1)

    # 1. Completeness (0–40 pts): penalise missing values
    avg_null = sum(p["pct_missing"] for p in col_profiles) / n_cols
    completeness_score = max(0.0, 40.0 - avg_null * 0.6)

    # 2. Type consistency (0–35 pts): columns with high-confidence types
    high_conf = sum(1 for p in col_profiles if p["type_confidence"] == "high")
    type_score = (high_conf / n_cols) * 35.0

    # 3. No issues (0–25 pts): penalise columns with red/yellow flags
    red_cols    = sum(1 for p in col_profiles if p["quality"] == "red")
    yellow_cols = sum(1 for p in col_profiles if p["quality"] == "yellow")
    issue_penalty = (red_cols * 5 + yellow_cols * 1.5) / n_cols * 25
    issue_score = max(0.0, 25.0 - issue_penalty)

    total = int(completeness_score + type_score + issue_score)
    total = max(0, min(100, total))

    label = "Excellent" if total >= 85 else "Good" if total >= 65 else "Fair" if total >= 40 else "Poor"
    color = "#00c8a8" if total >= 85 else "#4db8ff" if total >= 65 else "#f0a020" if total >= 40 else "#ff6060"

    return {
        "score":              total,
        "label":              label,
        "color":              color,
        "completeness_score": round(completeness_score, 1),
        "type_score":         round(type_score, 1),
        "issue_score":        round(issue_score, 1),
        "red_cols":           red_cols,
        "yellow_cols":        yellow_cols,
        "green_cols":         sum(1 for p in col_profiles if p["quality"] == "green"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Auto-fix suggestions
# ─────────────────────────────────────────────────────────────────────────────

def _generate_suggestions(col_profiles: List[Dict], df: pd.DataFrame) -> List[Dict]:
    """Generate ranked, actionable cleaning suggestions."""
    suggestions = []

    for p in col_profiles:
        col = p["column"]
        for issue in p["issues"]:
            code = issue["code"]

            if code == "high_missing":
                pct = p["pct_missing"]
                if pct > 70:
                    suggestions.append({
                        "priority": "high",
                        "icon":     "🗑️",
                        "column":   col,
                        "action":   f'Drop column "{col}" ({pct:.0f}% missing)',
                        "detail":   "More than 70% of values are missing — this column adds little value.",
                        "code":     "drop_col",
                    })
                else:
                    suggestions.append({
                        "priority": "medium",
                        "icon":     "🔢",
                        "column":   col,
                        "action":   f'Fill missing values in "{col}" ({pct:.0f}% missing)',
                        "detail":   "Suggest filling with 0 for financial data, or mean/median for ratios.",
                        "code":     "fill_missing",
                    })

            elif code == "mixed_types":
                suggestions.append({
                    "priority": "high",
                    "icon":     "🔄",
                    "column":   col,
                    "action":   f'Convert "{col}" to numeric',
                    "detail":   "Column contains mostly numbers but has text/symbols (e.g. $, commas). Strip formatting and convert.",
                    "code":     "convert_numeric",
                })

            elif code == "date_as_string":
                suggestions.append({
                    "priority": "medium",
                    "icon":     "📅",
                    "column":   col,
                    "action":   f'Convert "{col}" to date type',
                    "detail":   "Values look like dates stored as plain text. Converting enables time-series analysis.",
                    "code":     "convert_date",
                })

            elif code == "outliers":
                suggestions.append({
                    "priority": "low",
                    "icon":     "⚠️",
                    "column":   col,
                    "action":   f'Review outliers in "{col}" ({issue["label"]})',
                    "detail":   f"IQR method detected {p['outlier_count']} extreme value(s). Verify they are not data-entry errors.",
                    "code":     "review_outliers",
                })

            elif code == "constant":
                suggestions.append({
                    "priority": "low",
                    "icon":     "⊘",
                    "column":   col,
                    "action":   f'Consider dropping constant column "{col}"',
                    "detail":   "This column has only one unique value — it carries no information for analysis.",
                    "code":     "drop_constant",
                })

    # Sort: high → medium → low
    order = {"high": 0, "medium": 1, "low": 2}
    suggestions.sort(key=lambda x: order.get(x["priority"], 3))
    return suggestions
