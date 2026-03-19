"""
app.py — DataPrep Pro v2
Standalone financial data cleaning tool.
7 cleaning pillars: Duplicates · Missing · Types · Outliers · Finance · Standardise · Report
"""

import streamlit as st
import pandas as pd
import numpy as np
import sys, os, json, io

sys.path.insert(0, os.path.dirname(__file__))

from engine import (
    load_file, load_excel_sheet, load_google_sheet,
    basic_clean, deduplicate_columns, coerce_numeric, to_csv_bytes,
)
from profiler import full_profile
from cleaner import (
    detect_duplicates, drop_duplicates,
    missing_summary, fill_missing, drop_high_missing_cols,
    detect_types, coerce_column_type,
    detect_outliers, handle_outliers,
    detect_currency_columns, standardise_column_names,
    drop_empty_columns, convert_currency,
    standardise_dates, standardise_numbers, standardise_text_case,
    auto_clean, build_report_html,
)

st.set_page_config(page_title="DataPrep Pro", page_icon="⚙️",
                   layout="wide", initial_sidebar_state="expanded")

# ── CSS ───────────────────────────────────────────────────────────────────────
CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600;700&family=IBM+Plex+Sans:wght@300;400;500;600;700&display=swap');
html,body,[class*="css"]{font-family:'IBM Plex Sans',sans-serif!important;background:#080f14!important;color:#c9d8e3!important}
.stApp{background:#080f14}
[data-testid="stSidebar"]{background:#0c1821!important;border-right:1px solid #1a2e3d}
h1,h2,h3{font-family:'IBM Plex Mono',monospace!important}
.dp-card{background:#0d1e2c;border:1px solid #1a3347;border-radius:6px;padding:20px 24px;margin-bottom:16px;position:relative}
.dp-card::before{content:'';position:absolute;top:0;left:0;width:3px;height:100%;background:linear-gradient(180deg,#00c8a8,#0084d4);border-radius:6px 0 0 6px}
.step-header{display:flex;align-items:center;gap:14px;margin-bottom:20px;padding-bottom:12px;border-bottom:1px solid #1a3347}
.step-number{font-family:'IBM Plex Mono',monospace;font-size:11px;font-weight:700;color:#00c8a8;background:rgba(0,200,168,.1);border:1px solid rgba(0,200,168,.6);padding:3px 10px;border-radius:3px;letter-spacing:.15em}
.step-title{font-family:'IBM Plex Mono',monospace;font-size:18px;font-weight:700;color:#e8f4fb;letter-spacing:-.01em}
.step-desc{font-size:12px;color:#5a8099;margin-top:2px}
.dp-info{background:rgba(0,132,212,.08);border:1px solid rgba(0,132,212,.25);border-radius:4px;padding:10px 14px;font-size:12px;color:#7ab8d4;margin-bottom:12px}
.dp-warn{background:rgba(255,160,0,.06);border:1px solid rgba(255,160,0,.5);border-radius:4px;padding:10px 14px;font-size:12px;color:#c8922a;margin-bottom:12px}
.dp-success{background:rgba(0,200,168,.06);border:1px solid rgba(0,200,168,.5);border-radius:4px;padding:10px 14px;font-size:12px;color:#00c8a8;margin-bottom:12px}
.stButton>button{font-family:'IBM Plex Mono',monospace!important;font-weight:600!important;font-size:12px!important;letter-spacing:.06em!important;background:linear-gradient(135deg,#004a38,#003a60)!important;color:#00c8a8!important;border:1px solid rgba(0,200,168,.35)!important;border-radius:4px!important;padding:8px 20px!important;transition:all .2s!important}
.stButton>button:hover{background:linear-gradient(135deg,#005a48,#004a75)!important;border-color:#00c8a8!important}
.stDownloadButton>button{background:linear-gradient(135deg,#003a60,#002a48)!important;color:#64a0ff!important;border-color:rgba(100,160,255,.35)!important}
.stSelectbox>div>div{background:#0a1820!important;border:1px solid #1a3347!important;border-radius:4px!important;color:#c9d8e3!important;font-size:12px!important}
.stTextInput>div>div>input{background:#0a1820!important;border:1px solid #1a3347!important;color:#c9d8e3!important;font-size:12px!important;border-radius:4px!important}
div[data-testid="stMetric"]{background:#0d1e2c!important;border:1px solid #1a3347!important;border-radius:6px!important;padding:14px!important}
div[data-testid="stMetricValue"]{font-family:'IBM Plex Mono',monospace!important;color:#00c8a8!important}
.stTabs [data-baseweb="tab-list"]{background:#0c1821!important;border:1px solid #1a2e3d!important;border-radius:6px!important;padding:4px!important;gap:2px!important}
.stTabs [data-baseweb="tab"]{font-family:'IBM Plex Mono',monospace!important;font-size:11px!important;font-weight:600!important;letter-spacing:.06em!important;border-radius:4px!important;padding:7px 18px!important;color:#4a7088!important}
.stTabs [aria-selected="true"]{background:linear-gradient(135deg,#004a38,#003a60)!important;color:#00c8a8!important}
.streamlit-expanderHeader{font-family:'IBM Plex Mono',monospace!important;font-size:12px!important;color:#5a8099!important;background:#0a1820!important}
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-track{background:#080f14}
::-webkit-scrollbar-thumb{background:#1a3347;border-radius:3px}
.stCheckbox>label{font-size:12px!important;color:#7ab8d4!important}
.stProgress>div>div{background:#00c8a8!important}
.nav-btn-done button,.nav-btn-done .stButton>button{background:transparent!important;border:1px solid #1a4a3a!important;color:#00c8a8!important;font-family:'IBM Plex Mono',monospace!important;font-size:10px!important;letter-spacing:.06em!important;border-radius:3px!important;min-height:28px!important;width:100%!important}
.nav-btn-done button:hover,.nav-btn-done .stButton>button:hover{background:rgba(0,200,168,.10)!important;border-color:#00c8a8!important}
.nav-btn-active button,.nav-btn-active .stButton>button{background:rgba(0,200,168,.13)!important;border:1px solid #00c8a8!important;color:#00c8a8!important;font-family:'IBM Plex Mono',monospace!important;font-size:10px!important;font-weight:700!important;letter-spacing:.06em!important;border-radius:3px!important;min-height:28px!important;width:100%!important}
.nav-btn-future button,.nav-btn-future .stButton>button{background:transparent!important;border:1px solid #152535!important;color:#2a4a5e!important;font-family:'IBM Plex Mono',monospace!important;font-size:10px!important;letter-spacing:.06em!important;border-radius:3px!important;min-height:28px!important;width:100%!important;cursor:not-allowed!important}
.nav-footer{margin-top:28px;border-top:1px solid #0d1e2c;padding-top:16px}
footer{visibility:hidden}#MainMenu{visibility:hidden}

/* ── Compact nav selectbox ── */
div[data-testid="stSelectbox"] > label { display: none !important; }

/* ── Quality banner progress bar transition ── */
.quality-bar-fill { transition: width 0.4s ease; }

/* ── Tighter spacing on metric tiles ── */
.metric-tile { min-width: 100px; }
</style>"""


# ── UI helpers ────────────────────────────────────────────────────────────────

def step_header(num, title, desc=""):
    st.markdown(
        f'<div class="step-header"><span class="step-number">STEP {num}</span>'
        f'<div><div class="step-title">{title}</div>'
        f'{"<div class=step-desc>"+desc+"</div>" if desc else ""}</div></div>',
        unsafe_allow_html=True)

def info_box(msg, kind="info"):
    cls = {"info":"dp-info","warn":"dp-warn","success":"dp-success"}.get(kind,"dp-info")
    st.markdown(f'<div class="{cls}">{msg}</div>', unsafe_allow_html=True)

def metric_tile(label, value, color="#e8f4fb"):
    st.markdown(
        f'<div style="background:#0d1e2c;border:1px solid #1a3347;border-radius:6px;'
        f'padding:12px 16px;text-align:center">'
        f'<div style="font-family:IBM Plex Mono;font-size:22px;font-weight:700;color:{color}">{value}</div>'
        f'<div style="font-size:10px;color:#4a7088;letter-spacing:.1em">{label}</div></div>',
        unsafe_allow_html=True)

def get_state(k, d=None): return st.session_state.get(k, d)
def set_state(k, v):      st.session_state[k] = v


# ── Navigation ────────────────────────────────────────────────────────────────

ALL_STEPS = [
    (1,  "IMPORT"),
    (2, "FILE REVIEW"),
    (3, "MERGE"),
    (4, "RESOLVE DUPES"),
    (5, "PROFILE"),
    (6, "DUPLICATES"),
    (7, "MISSING"),
    (8, "TYPES"),
    (9, "OUTLIERS"),
    (10, "FINANCE"),
    (11, "STANDARDISE"),
    (12, "REPORT"),
]
# Expert-mode steps live in a separate flat list (not mixed with quick-clean steps)
EXPERT_STEPS = [
    (20, "E·UPLOAD"),
    (21, "E·MERGE"),
    (22, "E·EXPORT"),
]
EXPERT_STEP_ORDER = {sn: i for i, (sn, _) in enumerate(EXPERT_STEPS)}
STEP_ORDER = {sn: i for i, (sn, _) in enumerate(ALL_STEPS)}
MULTI_STEPS = {3, 2, 4}


def _visible_steps(active):
    multi     = get_state("_multi_file_mode")
    has_dupes = bool(get_state("_dup_groups"))
    steps = ALL_STEPS[:]
    if not multi and active not in MULTI_STEPS:
        steps = [(n, l) for n, l in steps if n not in MULTI_STEPS]
    else:
        if not has_dupes and active not in (4,):
            steps = [(n, l) for n, l in steps if n != 16]

    return steps

def _highest_reached():
    return get_state("_highest_step") or get_state("step") or 1

def _mark_reached(s):
    cur = _highest_reached()   # returns a step NUMBER
    # Compare via STEP_ORDER (list positions) — step numbers are non-sequential
    if STEP_ORDER.get(s, 99) >= STEP_ORDER.get(cur, 0):
        set_state("_highest_step", s)

def _prev_step(active):
    nums = [n for n, _ in _visible_steps(active)]
    try:
        i = nums.index(active)
        return nums[i-1] if i > 0 else active
    except ValueError:
        return active

def _next_step(active):
    nums = [n for n, _ in _visible_steps(active)]
    try:
        i = nums.index(active)
        result = nums[i+1] if i < len(nums)-1 else active
    except ValueError:
        result = active

    return result


def pipeline_bar(active):
    """Compact two-row nav: step counter + dropdown jump on top, dot-trail below."""
    if active in (20, 21, 22):
        _expert_pipeline_bar(active); return
    steps      = _visible_steps(active)
    highest_ov = STEP_ORDER.get(_highest_reached(), 0)
    _mark_reached(active)

    # Step index / total
    nums   = [sn for sn, _ in steps]
    labels = {sn: lbl for sn, lbl in steps}
    try:
        idx = nums.index(active) + 1
    except ValueError:
        idx = 1
    total = len(nums)
    active_lbl = labels.get(active, "")

    # Reachable steps for dropdown (done + current)
    reachable = [(sn, lbl) for sn, lbl in steps
                 if STEP_ORDER.get(sn, 99) <= highest_ov or sn == active]

    # ── Row 1: counter + label + dropdown ────────────────────────────────────
    r1a, r1b, r1c = st.columns([5, 8, 3])
    with r1a:
        score_num = ""
        qs = get_state("_last_quality_score")
        score_html = (
            f'<span style="font-family:IBM Plex Mono;font-size:11px;color:{qs["color"]};'
            f'background:rgba(0,200,168,.08);border:1px solid rgba(0,200,168,.5);'
            f'border-radius:3px;padding:2px 8px;margin-left:6px">Q:{qs["score"]}</span>'
            if qs else ""
        )
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:8px;padding-top:2px">'
            f'<div style="font-family:IBM Plex Mono;font-size:11px;color:#4a7088">'
            f'STEP <span style="color:#00c8a8;font-weight:700">{idx}</span>/{total}</div>'
            f'{score_html}</div>',
            unsafe_allow_html=True)
    with r1b:
        st.markdown(
            f'<div style="font-family:IBM Plex Mono;font-size:13px;font-weight:700;'
            f'color:#e8f4fb;padding-top:2px">◆ {active_lbl}</div>',
            unsafe_allow_html=True)
    with r1c:
        if len(reachable) > 1:
            opts      = [sn for sn, _ in reachable]
            opt_lbls  = {sn: f"{'✓ ' if STEP_ORDER.get(sn,99)<STEP_ORDER.get(active,0) else '◆ ' if sn==active else ''}{lbl}"
                         for sn, lbl in reachable}
            cur_idx   = opts.index(active) if active in opts else 0
            # Key includes active step — stale widget value from a different step
            # can never survive a step transition and cause a redirect loop.
            chosen_sn = st.selectbox("Jump to step", opts,
                                     index=cur_idx,
                                     format_func=opt_lbls.get,
                                     key=f"nav_dropdown_{active}",
                                     label_visibility="collapsed")
            if chosen_sn != active:

                if "nav_dropdown" in st.session_state:
                    del st.session_state["nav_dropdown"]
                set_state("step", chosen_sn); st.rerun()

    # ── Row 2: dot progress trail ─────────────────────────────────────────────
    dots = []
    for sn, lbl in steps:
        sn_ov = STEP_ORDER.get(sn, 99)
        if sn == active:
            dots.append(f'<span title="{lbl}" style="color:#00c8a8;font-size:16px">◆</span>')
        elif sn_ov <= highest_ov:
            dots.append(f'<span title="{lbl}" style="color:#1a5a40;font-size:12px">●</span>')
        else:
            dots.append(f'<span title="{lbl}" style="color:#1a2e3d;font-size:12px">○</span>')
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:4px;padding:4px 0 10px">'
        + " ".join(dots)
        + '</div>',
        unsafe_allow_html=True)


def nav_footer(active, *, back_label="← Back", next_label="Next →",
               next_step=None, back_step=None,
               next_action=None, next_disabled=False,
               show_back=True, show_next=True):

    st.markdown('<div class="nav-footer">', unsafe_allow_html=True)
    c1, _, c2 = st.columns([5, 9, 2])
    auto_back = back_step if back_step is not None else _prev_step(active)
    auto_next = next_step if next_step is not None else _next_step(active)

    with c1:
        if show_back and active != 1 and auto_back != active:
            if st.button(back_label, key=f"back_{active}"):
                if "nav_dropdown" in st.session_state:
                    del st.session_state["nav_dropdown"]
                set_state("step", auto_back); st.rerun()
    with c2:
        if show_next:
            if st.button(next_label, key=f"next_{active}",
                         type="primary", disabled=next_disabled):
                # Clear the dropdown widget so it doesn't redirect back on rerun
                if "nav_dropdown" in st.session_state:
                    del st.session_state["nav_dropdown"]
                if next_action:
                    next_action()
                else:
                    set_state("step", auto_next); st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)


def reset_pipeline():
    keep = {"step"}  # nothing to keep — full reset
    keys_to_del = [k for k in st.session_state if k not in keep]
    for k in keys_to_del:
        del st.session_state[k]

def append_log(entry):
    log = get_state("_clean_log") or []
    log.append(entry)
    set_state("_clean_log", log)

def apply_and_store(new_df, entry):
    """Store cleaned df + append log entry + mark clean_df."""
    set_state("clean_df", new_df)
    append_log(entry)
    # Invalidate profile cache
    set_state("_profile_cache", None)
    set_state("_profile_sig", None)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — IMPORT  (kept from v1, stripped of mapping references)
# ══════════════════════════════════════════════════════════════════════════════

def render_import():
    step_header("1", "IMPORT DATA", "Upload files or paste a Google Sheets URL")

    # ── Mode selector (top of the page) ──────────────────────────────────────
    st.markdown(
        '<div style="background:linear-gradient(135deg,#0a1f2e,#081828);border:1px solid #1a3347;'
        'border-radius:8px;padding:16px 20px;margin-bottom:18px">',
        unsafe_allow_html=True)
    mc1, mc2 = st.columns(2)
    with mc1:
        st.markdown(
            '<div style="font-family:IBM Plex Mono;font-size:11px;font-weight:700;'
            'color:#00c8a8;letter-spacing:.12em;margin-bottom:4px">⚡ QUICK CLEAN</div>'
            '<div style="font-size:11px;color:#4a7088">Merge first, then clean globally.<br>'
            'Good for fast jobs and beginners.</div>',
            unsafe_allow_html=True)
    with mc2:
        st.markdown(
            '<div style="font-family:IBM Plex Mono;font-size:11px;font-weight:700;'
            'color:#4db8ff;letter-spacing:.12em;margin-bottom:4px">🔬 EXPERT MODE</div>'
            '<div style="font-size:11px;color:#4a7088">Clean each file individually,<br>'
            'then control the merge precisely.</div>',
            unsafe_allow_html=True)
    app_mode = st.radio("app_mode", ["⚡ Quick Clean", "🔬 Expert Mode"],
                         horizontal=True, label_visibility="collapsed",
                         key="app_mode_picker")
    st.markdown("</div>", unsafe_allow_html=True)

    if app_mode == "🔬 Expert Mode":
        _render_expert_entry()
        return

    mode = st.radio("source",
                    ["📁 Single File", "📂 Multiple Files", "🌐 Google Sheets"],
                    horizontal=True, label_visibility="collapsed")
    raw_df = None

    if mode == "📁 Single File":
        up = st.file_uploader("Drop file here", type=["csv","xlsx","xls"],
                              label_visibility="collapsed")
        if up:
            with st.spinner("Loading…"):
                df, err, sheets, renames = load_file(up)
            if err:
                st.error(err)
            else:
                if len(sheets) > 1:
                    sel = st.selectbox("Select worksheet", sheets)
                    if sel != sheets[0]:
                        up.seek(0); df = load_excel_sheet(up, sel)
                raw_df, post_ren = deduplicate_columns(basic_clean(df))
                all_ren = renames + post_ren
                info_box(f"✓ Loaded <strong>{len(raw_df):,} rows × {len(raw_df.columns)} columns</strong> — {up.name}", "success")
                if all_ren:
                    info_box(f"⚠ <strong>Columns auto-renamed:</strong> {', '.join(all_ren[:6])}", "warn")

    elif mode == "📂 Multiple Files":
        info_box("Upload 2+ files. You'll preview each and choose how to merge them.")
        ups = st.file_uploader("Files", type=["csv","xlsx","xls"],
                               accept_multiple_files=True, label_visibility="collapsed")
        if ups and len(ups) >= 2:
            entries = []
            all_ok  = True
            with st.spinner(f"Loading {len(ups)} files…"):
                for u in ups:
                    u.seek(0); size = len(u.read()); u.seek(0)
                    df, err, _, renames = load_file(u)
                    if err:
                        st.error(f"{u.name}: {err}"); all_ok = False; break
                    df_c, post_ren = deduplicate_columns(basic_clean(df))
                    entries.append({"name":u.name,"size":size,"rows":len(df_c),
                                    "cols":len(df_c.columns),"df":df_c,
                                    "renames":renames+post_ren,"selected":True})
            if all_ok:
                set_state("_file_entries", entries)
                set_state("_multi_file_mode", True)
                info_box(f"✓ Loaded <strong>{len(entries)} files</strong>. Review and select files to merge.", "success")
                if st.button("→ Review Files", type="primary"):
                    set_state("step", 2); st.rerun()
        elif ups and len(ups) == 1:
            info_box("Upload at least 2 files to use multi-file mode.", "warn")

    elif mode == "🌐 Google Sheets":
        info_box("Share via <strong>File → Share → Publish to web → CSV</strong>")
        url = st.text_input("Google Sheets URL",
                            placeholder="https://docs.google.com/spreadsheets/d/…",
                            label_visibility="collapsed")
        if st.button("⚙ Fetch Sheet") and url:
            with st.spinner("Fetching…"):
                df, err = load_google_sheet(url)
            if err:
                st.error(err)
            elif df is not None:
                raw_df = basic_clean(df)
                info_box(f"✓ Fetched <strong>{len(raw_df):,} rows × {len(raw_df.columns)} columns</strong>", "success")
                set_state("raw_df", raw_df)

    if raw_df is not None:
        set_state("raw_df", raw_df)
        set_state("clean_df", raw_df.copy())
        set_state("_original_df", raw_df.copy())
        set_state("_clean_log", [])

    if get_state("raw_df") is not None:
        nav_footer(1, next_label="→ Profile Data",
                   next_action=lambda: (set_state("step",5), st.rerun()))


# ══════════════════════════════════════════════════════════════════════════════
# STEPS 3, 2, 16 — MULTI-FILE MERGE  (kept verbatim from v1)
# ══════════════════════════════════════════════════════════════════════════════

def _common_columns(entries):
    sel = [e for e in entries if e["selected"]]
    if not sel: return []
    common = set(sel[0]["df"].columns)
    for e in sel[1:]: common &= set(e["df"].columns)
    return sorted(common)

def _all_columns(entries):
    sel = [e for e in entries if e["selected"]]
    counts = {}
    for e in sel:
        for c in e["df"].columns:
            counts[c] = counts.get(c, 0) + 1
    return counts

def _execute_concat(entries, axis):
    sel = [e for e in entries if e["selected"]]
    return pd.concat([e["df"] for e in sel], axis=axis, ignore_index=(axis==0))

def _execute_join(entries, join_keys, join_type, conflict_strat):
    sel = [e for e in entries if e["selected"]]
    if not sel: return pd.DataFrame()
    merged = sel[0]["df"].copy()
    for i, entry in enumerate(sel[1:], 1):
        other   = entry["df"].copy()
        l_cols  = [c for c in merged.columns if c not in join_keys]
        r_cols  = [c for c in other.columns  if c not in join_keys]
        overlap = set(l_cols) & set(r_cols)
        if conflict_strat == "suffixes" or not overlap:
            sfx = (f"_f{i-1}", f"_f{i}")
            merged = merged.merge(other, on=join_keys, how=join_type,
                                  suffixes=sfx)
        elif conflict_strat == "left_only":
            other   = other.drop(columns=[c for c in overlap])
            merged  = merged.merge(other, on=join_keys, how=join_type)
        else:
            merged_cols = [c for c in merged.columns if c not in overlap]
            merged  = merged[merged_cols].merge(other, on=join_keys, how=join_type)
    return merged

def _merge_preview(entries, mode, join_keys, join_type, conflict_strat, concat_axis,
                   preview_rows=100):
    """Run merge on a small slice per file — keeps preview fast on large datasets."""
    try:
        sliced = [{**e, "df": e["df"].head(preview_rows)} for e in entries]
        if mode == "concat":
            result = _execute_concat(sliced, concat_axis)
        else:
            result = _execute_join(sliced, join_keys, join_type, conflict_strat)
        return result, None
    except Exception as e:
        return pd.DataFrame(), str(e)

def render_merge_settings():
    step_header("1b", "MERGE SETTINGS", "Choose how to combine your selected files")
    entries = get_state("_file_entries")
    if not entries:
        info_box("No files loaded — go back to File Review.", "warn")
        nav_footer(3, back_label="← Back to File Review", back_step=2, show_next=False)
        return
    selected = [e for e in entries if e["selected"]]
    saved = get_state("_merge_config") or {}

    st.markdown('<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.12em;margin-bottom:10px">MERGE TYPE</div>', unsafe_allow_html=True)
    mode = st.radio("merge_mode", ["concat","join"],
                    format_func=lambda x: ("🔹 Concatenate (stack rows)" if x=="concat" else "🔸 Join (combine columns by key)"),
                    index=0 if saved.get("mode","concat")=="concat" else 1,
                    label_visibility="collapsed")

    concat_axis = 0; join_keys = []; join_type = "left"; conflict_strat = "suffixes"

    if mode == "concat":
        concat_axis = 0 if st.radio("axis",["Vertical (stack rows)","Horizontal (stack columns)"],
                                     label_visibility="collapsed") == "Vertical (stack rows)" else 1
    else:
        common = _common_columns(selected)
        all_c  = _all_columns(selected)
        join_keys = st.multiselect("Join keys", list(all_c.keys()), default=common[:2], max_selections=7)
        join_type = st.radio("Join type",["inner","left","right","outer"], horizontal=True,
                             label_visibility="collapsed")
        if [c for c in all_c if c not in join_keys and all_c[c] > 1]:
            conflict_strat = st.radio("Conflicting columns",["suffixes","left_only","right_only"],
                                      horizontal=True, label_visibility="collapsed")

    # Live preview (sliced — shows shape of final merge using first 100 rows per file)
    st.markdown("---")
    try:
        prev_df, prev_err = _merge_preview(selected, mode, join_keys, join_type,
                                           conflict_strat, concat_axis)
        if prev_err:
            info_box(f"⚠ Preview error: {prev_err}", "warn")
        elif len(prev_df) > 0:
            # Compute full expected shape without building the full DataFrame
            if mode == "concat":
                total_rows = sum(len(e["df"]) for e in selected)
                all_cols   = set()
                for e in selected: all_cols.update(e["df"].columns)
                total_cols = len(all_cols)
            else:
                total_rows = max(len(e["df"]) for e in selected)
                total_cols = len(prev_df.columns)
            st.markdown(
                f'<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;'
                f'letter-spacing:.1em;margin-bottom:6px">'
                f'PREVIEW (first 100 rows/file) — full result will be ~{total_rows:,} rows × {total_cols} cols</div>',
                unsafe_allow_html=True)
            st.dataframe(prev_df.head(10), use_container_width=True, height=180)
    except Exception as e:
        info_box(f"⚠ Could not render preview: {e}", "warn")

    ready = (mode == "concat") or (mode == "join" and len(join_keys) >= 1)
    if not ready:
        info_box("Select at least one join key to continue.", "warn")

    # ── Apply merge button ────────────────────────────────────────────────────
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    b1, _, b2 = st.columns([5, 9, 2])
    with b1:
        if st.button("← Back to File Review", key="merge_back"):
            set_state("step", 2); st.rerun()
    with b2:
        if st.button("→ Apply & Merge", key="merge_apply",
                     type="primary", disabled=not ready):
            with st.spinner("Merging files… please wait"):
                try:
                    set_state("_merge_config", {"mode":mode,"concat_axis":concat_axis,
                                                "join_keys":join_keys,"join_type":join_type,
                                                "conflict_strategy":conflict_strat})
                    if mode == "concat":
                        merged = _execute_concat(selected, concat_axis)
                    else:
                        merged = _execute_join(selected, join_keys, join_type, conflict_strat)
                    merged, renames = deduplicate_columns(merged)
                    set_state("raw_df",       merged)
                    set_state("clean_df",     merged.copy())
                    set_state("_original_df", merged.copy())
                    set_state("_clean_log",   [])
                    set_state("_merge_renames", renames)
                    set_state("step", 5)
                    st.rerun()
                except Exception as e:
                    st.error(f"Merge failed: {e}")


def _fmt_size(b):
    if b >= 1_048_576: return f"{b/1_048_576:.1f} MB"
    if b >= 1_024:     return f"{b/1_024:.1f} KB"
    return f"{b} B"

def _dtype_color(dt):
    if "int" in dt or "float" in dt: return "#4db8ff"
    if "date" in dt or "time" in dt: return "#a78bfa"
    return "#7ab8d4"

def render_file_review():
    step_header("1b", "FILE REVIEW & SELECTION", "Preview files and choose which to merge")
    entries = get_state("_file_entries")
    if not entries:
        info_box("No files loaded — go back to Import.", "warn")
        nav_footer(2, back_label="← Back to Import", back_step=1, show_next=False)
        return

    info_box(f"<strong>{len(entries)} files loaded.</strong> Uncheck to exclude. Click ▶ to preview.")

    updated = []
    for i, entry in enumerate(entries):

        h1, h2, h3 = st.columns([0.6, 7, 0.8])
        with h1:
            sel = st.checkbox("Select file", value=entry["selected"], key=f"file_sel_{i}", label_visibility="collapsed")
        with h2:
            ext = entry["name"].rsplit(".",1)[-1].upper()
            icon = "📊" if ext in ("XLSX","XLS") else "📄"
            rnm  = f' <span style="color:#f0a020;font-size:10px">({len(entry["renames"])} renamed)</span>' if entry["renames"] else ""
            st.markdown(
                f'<div style="background:#0d1e2c;border:1px solid #1a3347;border-radius:5px;padding:10px 14px">'
                f'<div style="font-family:IBM Plex Mono;font-size:12px;font-weight:700;color:#e8f4fb">'
                f'{icon} {entry["name"]}{rnm}</div>'
                f'<div style="font-size:11px;color:#4a7088;margin-top:4px">'
                f'{entry["rows"]:,} rows · {entry["cols"]} cols · {_fmt_size(entry["size"])}</div></div>',
                unsafe_allow_html=True)
        with h3:
            exp_key = f"_expand_{i}"
            if st.button("▶ Preview" if not get_state(exp_key) else "▲ Close",
                         key=f"exp_btn_{i}", width="stretch"):
                set_state(exp_key, not get_state(exp_key, False)); st.rerun()
        if get_state(f"_expand_{i}"):
            tab1, tab2 = st.tabs(["Data","Columns"])
            with tab1:
                st.dataframe(entry["df"].head(10), use_container_width=True, height=200)
            with tab2:
                rows = [{"Column":c,"Type":str(entry["df"][c].dtype),
                         "Missing %":f'{entry["df"][c].isna().mean()*100:.1f}%',
                         "Sample":str(entry["df"][c].dropna().iloc[0]) if entry["df"][c].dropna().size else "—"}
                        for c in entry["df"].columns]
                st.dataframe(pd.DataFrame(rows), use_container_width=True, height=240)
        updated.append({**entry, "selected": sel})
    set_state("_file_entries", updated)

    n_sel = sum(1 for e in updated if e["selected"])

    if n_sel >= 2:
        info_box(f"<strong>{n_sel} files selected</strong>", "success")
    else:
        info_box("Select at least 2 files to continue.", "warn")

    nav_footer(2, back_label="← Back to Import", back_step=1,
               next_label="→ Configure Merge", next_step=3, next_disabled=(n_sel < 5))



def _find_year_col(df):
    for c in df.columns:
        cl = c.lower()
        if "year" in cl or "yr" in cl or "period" in cl or "fy" in cl:
            return c
    return None

def _raw_merge(entries):
    sel = [e for e in entries if e["selected"]]
    merged = sel[0]["df"].copy()
    for fi, entry in enumerate(sel[1:], 1):
        other = entry["df"].copy()
        ya, yb = _find_year_col(merged), _find_year_col(other)
        overlap = set(merged.columns) & set(other.columns) - ({ya} if ya else set())
        for c in overlap:
            other = other.rename(columns={c: f"{c}__FILE{fi}__"})
        if ya and yb and ya != yb:
            other = other.rename(columns={yb: ya})
        if ya and ya in merged.columns and ya in other.columns:
            merged = merged.merge(other, on=ya, how="outer")
        else:
            merged = pd.concat([merged, other], axis=1)
    return merged

def _detect_dup_groups(entries, merged_raw):
    FILE_TAG = re.compile(r"__FILE\d+__$")
    base_to_versions = {}
    for col in merged_raw.columns:
        base = FILE_TAG.sub("", col)
        if base != col:
            base_to_versions.setdefault(base, [col])
            if base in merged_raw.columns:
                base_to_versions[base].insert(0, base)
    groups = []
    for base, versions in base_to_versions.items():
        if len(versions) < 2:
            continue
        ver_info = []
        for v in versions:
            fi = re.search(r"__FILE(\d+)__$", v)
            fname = (entries[int(fi.group(1))]["name"] if fi and int(fi.group(1)) < len(entries)
                     else entries[0]["name"])
            sample = str(merged_raw[v].dropna().head(6).tolist())
            ver_info.append({"col_name":v,"file_name":fname,"sample":sample})
        groups.append({"base_name":base,"versions":ver_info})
    return groups

def _apply_dup_choices(merged_raw, dup_groups, choices):
    drop_cols = []
    for grp in dup_groups:
        base    = grp["base_name"]
        versions= grp["versions"]
        choice  = choices.get(base, "keep_first")
        cols    = [v["col_name"] for v in versions]
        if choice == "keep_first":
            winner = cols[0]
        elif choice.startswith("keep_"):
            idx = int(choice.split("_")[1]) - 1
            winner = cols[idx] if idx < len(cols) else cols[0]
        elif choice == "keep_all":
            continue
        else:  # drop_all
            drop_cols.extend(cols)
            continue
        rename_to = base if base not in merged_raw.columns else winner
        merged_raw = merged_raw.rename(columns={winner: rename_to})
        drop_cols.extend([c for c in cols if c != winner])
    return merged_raw.drop(columns=[c for c in drop_cols if c in merged_raw.columns])

def _finish_merge(df, renames):
    set_state("raw_df",       df)
    set_state("clean_df",     df.copy())
    set_state("_original_df", df.copy())
    set_state("_clean_log",   [])
    set_state("_merge_renames", renames)
    set_state("step", 5); st.rerun()

def render_resolve_dupes():
    step_header("1c", "RESOLVE DUPLICATE COLUMNS", "Choose which version of conflicting columns to keep")
    merged_raw = get_state("_merged_raw")
    dup_groups = get_state("_dup_groups") or []
    if not merged_raw or not dup_groups:
        info_box("No duplicate data found.", "info")
        nav_footer(4, show_back=False, next_label="→ Continue to Profile", next_step=5)
        return

    choices = get_state("_dup_choices") or {}
    for grp in dup_groups:
        base = grp["base_name"]; versions = grp["versions"]
        st.markdown(f'<div class="dp-card"><strong style="color:#e8f4fb">{base}</strong>'
                    f' <span style="font-size:11px;color:#4a7088">— {len(versions)} versions</span></div>',
                    unsafe_allow_html=True)
        opts = ["keep_first"] + [f"keep_{i+1}" for i in range(1, len(versions))] + ["keep_all","drop_all"]
        fmt  = {"keep_first":"Keep first","keep_all":"Keep all (rename)","drop_all":"Drop all",
                **{f"keep_{i+1}": f"Keep version {i+1} ({versions[i]['file_name']})" for i in range(1, len(versions))}}
        choices[base] = st.radio(f"choice_{base}", opts,
                                  format_func=fmt.get, key=f"dup_radio_{base}",
                                  label_visibility="collapsed")
    set_state("_dup_choices", choices)

    def _apply():
        final = _apply_dup_choices(get_state("_merged_raw"), dup_groups, choices)
        set_state("_merged_raw", None); set_state("_dup_groups", None); set_state("_dup_choices", None)
        _finish_merge(final, [])

    nav_footer(4, back_label="← Back to Merge", back_step=3,
               next_label="✅ Apply & Continue", next_action=_apply)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — PROFILE  (updated: no FinAnalyst targets, just data quality)
# ══════════════════════════════════════════════════════════════════════════════

def _score_bar(label, val, max_val, color):
    w = int(val / max_val * 100) if max_val else 0
    return (f'<div style="margin-bottom:8px"><div style="display:flex;justify-content:space-between;'
            f'font-size:10px;color:#4a7088;margin-bottom:3px"><span>{label}</span>'
            f'<span style="color:{color}">{val:.0f}/{max_val}</span></div>'
            f'<div style="background:#0d1e2c;border-radius:2px;height:4px">'
            f'<div style="width:{w}%;background:{color};height:100%;border-radius:2px"></div></div></div>')

def _quality_dot(q):
    return {"green":"🟢","yellow":"🟡","red":"🔴"}.get(q,"⬜")


def _compute_quick_quality(df):
    """Return a minimal quality dict (score, label, color, completeness, dup_rows)."""
    try:
        n = len(df)
        total_cells = n * len(df.columns)
        missing = int(df.isna().sum().sum())
        completeness = round((1 - missing / max(total_cells, 1)) * 100, 1)
        dup_rows = int(df.duplicated().sum())
        # Simple score: 60% completeness weight + 40% no-dups weight
        comp_score = completeness * 0.7
        dup_score  = max(0, 30 * (1 - dup_rows / max(n, 1)))
        score = min(100, int(comp_score + dup_score))
        color = "#00c8a8" if score >= 75 else "#f0a020" if score >= 45 else "#ff6060"
        label = "Good" if score >= 75 else "Fair" if score >= 45 else "Poor"
        return {"score": score, "label": label, "color": color,
                "completeness": completeness, "dup_rows": dup_rows,
                "rows": n, "cols": len(df.columns), "missing": missing}
    except Exception:
        return None


def quality_banner(df, title="DATASET STATUS"):
    """Render a compact quality bar shown on every pillar step."""
    q = _compute_quick_quality(df)
    if q is None:
        return
    set_state("_last_quality_score", q)   # pipeline_bar reads this
    bar_color = q["color"]
    bar_w     = q["score"]
    st.markdown(
        f'<div style="background:#0a1820;border:1px solid #1a3347;border-radius:6px;'
        f'padding:10px 16px;margin-bottom:14px;display:flex;align-items:center;gap:20px;flex-wrap:wrap">'
        f'<div style="text-align:center;min-width:64px">'
        f'<div style="font-family:IBM Plex Mono;font-size:28px;font-weight:700;color:{bar_color};line-height:1">{q["score"]}</div>'
        f'<div style="font-size:9px;color:#4a7088;letter-spacing:.1em">{title}</div>'
        f'<div style="font-size:10px;color:{bar_color}">{q["label"]}</div>'
        f'</div>'
        f'<div style="flex:1;min-width:200px">'
        f'<div style="background:#0d1e2c;border-radius:3px;height:6px;margin-bottom:8px">'
        f'<div style="width:{bar_w}%;background:{bar_color};height:100%;border-radius:3px;transition:width .4s"></div></div>'
        f'<div style="display:flex;gap:16px;flex-wrap:wrap">'
        + "".join([
            f'<span style="font-size:11px"><span style="color:#4a7088">{lbl} </span>'
            f'<span style="font-family:IBM Plex Mono;color:{vc}">{vv}</span></span>'
            for lbl, vv, vc in [
                ("Rows",         f'{q["rows"]:,}',         "#e8f4fb"),
                ("Cols",         str(q["cols"]),              "#e8f4fb"),
                ("Missing",      f'{q["missing"]:,}',       "#ff6060" if q["missing"] else "#00c8a8"),
                ("Completeness", f'{q["completeness"]}%',   "#00c8a8" if q["completeness"]>=90 else "#f0a020"),
                ("Dup rows",     f'{q["dup_rows"]:,}',      "#ff6060" if q["dup_rows"] else "#00c8a8"),
            ]
        ])
        + '</div></div></div>',
        unsafe_allow_html=True)

def render_profile():
    step_header("2", "DATA PROFILE", "Quality diagnostic of your dataset")

    df = (lambda a, b: a if a is not None else b)(get_state("clean_df"), get_state("raw_df"))
    if df is None:
        info_box("No data loaded.", "warn"); return

    # Dedup safety
    df, late_ren = deduplicate_columns(df)
    if late_ren:
        set_state("clean_df", df)
        info_box(f"⚠ <strong>Columns auto-renamed:</strong> {', '.join(late_ren[:6])}", "warn")

    # Compute/cache
    sig = f"{list(df.columns)}_{df.shape}"
    if get_state("_profile_cache") is None or get_state("_profile_sig") != sig:
        with st.spinner("⚙ Analysing…"):
            prof = full_profile(df)
        set_state("_profile_cache", prof); set_state("_profile_sig", sig)
    else:
        prof = get_state("_profile_cache")

    ov = prof["overview"]; qs = prof["quality_score"]; cols_p = prof["columns"]; sugs = prof["suggestions"]
    sc = qs["color"]

    # Score header
    st.markdown(
        f'<div style="background:linear-gradient(135deg,#0a1f2e,#081828);border:1px solid #1a3347;'
        f'border-radius:8px;padding:20px 28px;display:flex;gap:32px;align-items:center;margin-bottom:20px;flex-wrap:wrap">'
        f'<div style="text-align:center;min-width:100px">'
        f'<div style="font-family:IBM Plex Mono;font-size:52px;font-weight:700;color:{sc};line-height:1">{qs["score"]}</div>'
        f'<div style="font-size:10px;letter-spacing:.15em;color:#4a7088;margin-top:4px">QUALITY SCORE</div>'
        f'<div style="font-size:12px;color:{sc};font-weight:600;margin-top:2px">{qs["label"]}</div></div>'
        f'<div style="flex:1;display:grid;grid-template-columns:repeat(6,1fr);gap:12px;min-width:300px">'
        + "".join([
            f'<div style="background:#0d1e2c;border:1px solid #1a3347;border-radius:6px;padding:10px 14px;text-align:center">'
            f'<div style="font-family:IBM Plex Mono;font-size:20px;font-weight:700;color:{vc}">{vv}</div>'
            f'<div style="font-size:10px;color:#4a7088;letter-spacing:.1em">{lbl}</div></div>'
            for lbl, vv, vc in [
                ("ROWS", f'{ov["rows"]:,}', "#e8f4fb"),
                ("COLS", ov["columns"], "#e8f4fb"),
                ("MEMORY", ov["memory"], "#e8f4fb"),
                ("DUP ROWS", f'{ov["dup_rows"]:,}', "#ff6060" if ov["dup_rows"]>0 else "#00c8a8"),
                ("COMPLETENESS", f'{ov["completeness"]}%', "#00c8a8"),
                ("COL HEALTH", f'<span style="color:#00c8a8">●{qs["green_cols"]}</span>'
                               f'<span style="color:#f0a020;margin:0 4px">●{qs["yellow_cols"]}</span>'
                               f'<span style="color:#ff6060">●{qs["red_cols"]}</span>', "#e8f4fb"),
            ]
        ])
        + f'</div><div style="min-width:160px">'
        f'<div style="font-size:10px;color:#4a7088;letter-spacing:.1em;margin-bottom:8px">SCORE BREAKDOWN</div>'
        f'{_score_bar("Completeness", qs["completeness_score"], 40, "#00c8a8")}'
        f'{_score_bar("Type quality",  qs["type_score"],         35, "#4db8ff")}'
        f'{_score_bar("Clean columns", qs["issue_score"],         25, "#a78bfa")}'
        f'</div></div>',
        unsafe_allow_html=True)

    if ov.get("sampled"):
        info_box(f"⚡ Large dataset — profiled first {ov['sample_rows']:,} rows for speed.", "info")

    # ══════════════════════════════════════════════════════════════════════════
    # AUTO-CLEAN — configuration screen + before/after report
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown(
        '<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;'
        'letter-spacing:.12em;margin-bottom:12px">⚡ AUTO-CLEAN FINANCIAL DATA</div>',
        unsafe_allow_html=True)

    ac_result = get_state("_autoclean_result")

    # ── Config panel (always shown; collapses after run) ──────────────────────
    cfg_expanded = ac_result is None   # collapse after first run
    with st.expander("⚙ Configure auto-clean", expanded=cfg_expanded):

        # Row 1: strategy + threshold
        ac1, ac2 = st.columns(2)
        with ac1:
            ac_fill = st.selectbox(
                "Fill strategy for numeric missing values",
                ["mean", "median", "mode", "zero", "Leave as NaN"],
                key="ac_fill_strategy",
                help="Applied to numeric columns after dropping high-missing ones")
        with ac2:
            ac_thresh = st.slider(
                "Drop columns with more than X% missing",
                min_value=10, max_value=100, value=70, step=5,
                key="ac_thresh",
                help="Columns above this threshold are dropped entirely")

        # Row 2: toggles
        t1, t2, t3 = st.columns(3)
        with t1:
            ac_standardise = st.checkbox(
                "Standardise financial column names", value=True,
                key="ac_standardise",
                help="Renames aliases like 'Ventes'→'Revenue', 'dette nette'→'Net Debt'")
        with t2:
            ac_drop_dupes = st.checkbox(
                "Remove exact duplicate rows", value=True,
                key="ac_drop_dupes")
        with t3:
            ac_keepcopy = st.checkbox(
                "Keep backup before cleaning", value=False,
                key="ac_keepcopy",
                help="Saves a copy of the dataset as it is right now")

        # Summary of what will happen
        steps_preview = []
        steps_preview.append(f"Drop 100% empty columns")
        steps_preview.append(f"Drop columns with >{ac_thresh}% missing")
        if ac_drop_dupes:
            steps_preview.append("Remove exact duplicate rows")
        if ac_fill != "Leave as NaN":
            steps_preview.append(f"Fill remaining missing → {ac_fill}")
        if ac_standardise:
            steps_preview.append("Standardise financial column names")
        steps_preview.append("Flag IQR outliers in numeric columns")

        info_box(
            "<strong>Will perform:</strong> " +
            " · ".join(f"({i+1}) {s}" for i, s in enumerate(steps_preview)),
            "info")

        if st.button("⚡ Run Auto-Clean Now", type="primary", width="stretch"):
            raw = (lambda a, b: a if a is not None else b)(get_state("clean_df"), get_state("raw_df"))
            if ac_keepcopy:
                set_state("_autoclean_backup", raw.copy())

            fill_s = "custom" if ac_fill == "zero" else (
                     "mode"   if ac_fill == "Leave as NaN" else ac_fill)
            orig_rows = len(raw)
            orig_cols = len(raw.columns)
            orig_miss = int(raw.isna().sum().sum())
            orig_dup  = int(raw.duplicated().sum())

            # Compute per-column missing% snapshot for before/after chart
            orig_col_miss = (raw.isna().mean() * 100).round(1).to_dict()

            with st.spinner("Auto-cleaning… please wait"):
                from cleaner import (drop_empty_columns, drop_duplicates as _dd,
                                     drop_high_missing_cols, fill_missing as _fm,
                                     handle_outliers, detect_outliers,
                                     standardise_column_names)
                import numpy as _np
                log_entries = []
                cleaned = raw.copy()

                cleaned, e = drop_empty_columns(cleaned)
                log_entries.append(e)

                cleaned, e = drop_high_missing_cols(cleaned, float(ac_thresh))
                log_entries.append(e)

                if ac_drop_dupes:
                    cleaned, e = _dd(cleaned, keep="first")
                    log_entries.append(e)

                if ac_fill != "Leave as NaN":
                    num_c = cleaned.select_dtypes(include=[_np.number]).columns.tolist()
                    if num_c:
                        cleaned, e = _fm(cleaned, fill_s, num_c)
                        log_entries.append(e)

                if ac_standardise:
                    cleaned, e = standardise_column_names(cleaned)
                    log_entries.append(e)

                out_det = detect_outliers(cleaned, method="iqr")
                if out_det:
                    cleaned, e = handle_outliers(cleaned, list(out_det.keys()), action="flag")
                    log_entries.append(e)

            set_state("clean_df", cleaned)
            cur_log = get_state("_clean_log") or []
            set_state("_clean_log", cur_log + log_entries)
            set_state("_profile_cache", None)
            set_state("_profile_sig", None)

            new_miss = int(cleaned.isna().sum().sum())
            new_col_miss = (cleaned.isna().mean() * 100).round(1).to_dict()

            # Identify dropped columns for the report
            dropped_col_names = [c for c in raw.columns if c not in cleaned.columns]

            set_state("_autoclean_result", {
                "ops":              len(log_entries),
                "rows_before":      orig_rows,
                "rows_after":       len(cleaned),
                "cols_before":      orig_cols,
                "cols_after":       len(cleaned.columns),
                "miss_before":      orig_miss,
                "miss_after":       new_miss,
                "dup_before":       orig_dup,
                "dropped_cols":     dropped_col_names,
                "orig_col_miss":    orig_col_miss,
                "new_col_miss":     new_col_miss,
                "log":              log_entries,
            })
            st.rerun()

    # ── Before/After report (shown after run) ─────────────────────────────────
    if ac_result:
        st.markdown(
            '<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;'
            'letter-spacing:.12em;margin:18px 0 12px">📊 AUTO-CLEAN REPORT</div>',
            unsafe_allow_html=True)

        # KPI row
        rows_removed = ac_result["rows_before"] - ac_result["rows_after"]
        cols_removed = ac_result["cols_before"] - ac_result["cols_after"]
        miss_before  = ac_result["miss_before"]
        miss_after   = ac_result["miss_after"]
        miss_pct_reduction = (
            round((miss_before - miss_after) / max(miss_before, 1) * 100, 1)
            if miss_before else 0)

        k1, k2, k3, k4, k5 = st.columns(5)
        with k1: metric_tile("ROWS REMOVED",   f"{rows_removed:,}",
                              "#f0a020" if rows_removed else "#00c8a8")
        with k2: metric_tile("COLS REMOVED",   f"{cols_removed:,}",
                              "#f0a020" if cols_removed else "#00c8a8")
        with k3: metric_tile("MISSING BEFORE", f"{miss_before:,}", "#ff6060")
        with k4: metric_tile("MISSING AFTER",  f"{miss_after:,}",
                              "#00c8a8" if miss_after == 0 else "#f0a020")
        with k5: metric_tile("MISSING ↓",      f"{miss_pct_reduction}%", "#00c8a8")

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # ── Before/After visualisation ─────────────────────────────────────────
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            import matplotlib.patches as mpatches

            orig_cm = ac_result.get("orig_col_miss", {})
            new_cm  = ac_result.get("new_col_miss",  {})

            # Only show columns that still exist in cleaned dataset
            common_cols = [c for c in orig_cm if c in new_cm]
            # Filter to columns that actually changed (or had missing values)
            changed = [(c, orig_cm[c], new_cm[c])
                       for c in common_cols if orig_cm[c] > 0 or new_cm[c] > 0]
            changed.sort(key=lambda x: x[1], reverse=True)
            changed = changed[:20]  # cap at 20 for readability

            if changed:
                labels = [c[:22] + "…" if len(c) > 22 else c for c, _, _ in changed]
                before_vals = [b for _, b, _ in changed]
                after_vals  = [a for _, _, a in changed]
                n = len(changed)
                y = list(range(n))

                fig, ax = plt.subplots(figsize=(9, max(3, n * 0.38)))
                fig.patch.set_facecolor("#080f14")
                ax.set_facecolor("#0a1820")

                bar_h = 0.35
                bars_before = ax.barh([i + bar_h/2 for i in y], before_vals,
                                      height=bar_h, color="#ff6060", alpha=0.85,
                                      label="Before")
                bars_after  = ax.barh([i - bar_h/2 for i in y], after_vals,
                                      height=bar_h, color="#00c8a8", alpha=0.85,
                                      label="After")

                ax.set_yticks(y)
                ax.set_yticklabels(labels, fontsize=8.5, color="#b0cfe0")
                ax.set_xlabel("% Missing", color="#4a7088", fontsize=9)
                ax.set_xlim(0, 105)
                ax.tick_params(colors="#4a7088", labelsize=8)
                for spine in ax.spines.values():
                    spine.set_edgecolor("#1a3347")
                ax.axvline(70, color="#f0a020", linewidth=0.8,
                           linestyle="--", alpha=0.6, label="Drop threshold")
                ax.legend(handles=[
                    mpatches.Patch(color="#ff6060", label="Before"),
                    mpatches.Patch(color="#00c8a8", label="After"),
                    mpatches.Patch(color="#f0a020", label=f"Drop threshold ({ac_result.get('ops',0)} ops)"),
                ], facecolor="#0d1e2c", edgecolor="#1a3347",
                   labelcolor="#b0cfe0", fontsize=8, loc="lower right")

                ax.set_title("Missing values per column — Before vs After",
                             color="#7ab8d4", fontsize=10, pad=10)
                plt.tight_layout()
                st.pyplot(fig, use_container_width=True)
                plt.close(fig)
        except Exception as chart_err:
            info_box(f"Chart unavailable: {chart_err}", "warn")

        # ── Dropped columns list ───────────────────────────────────────────────
        dropped = ac_result.get("dropped_cols", [])
        if dropped:
            with st.expander(f"🗑 Dropped columns ({len(dropped)})"):
                cols_per_row = 3
                for row_start in range(0, len(dropped), cols_per_row):
                    chunk = dropped[row_start:row_start + cols_per_row]
                    dcols = st.columns(cols_per_row)
                    for j, col_name in enumerate(chunk):
                        pct = ac_result["orig_col_miss"].get(col_name, 0)
                        with dcols[j]:
                            st.markdown(
                                f'<div style="background:#1a0a0a;border:1px solid #3a1010;'
                                f'border-radius:4px;padding:6px 10px;margin-bottom:6px;font-size:11px">'
                                f'<span style="color:#ff6060">✕</span> '
                                f'<span style="color:#e8f4fb">{col_name[:30]}</span>'
                                f'<span style="color:#4a7088;font-size:10px"> ({pct:.0f}% missing)</span></div>',
                                unsafe_allow_html=True)

        # ── Operations log ─────────────────────────────────────────────────────
        with st.expander(f"📋 Operations log ({ac_result['ops']} steps)"):
            for i, e in enumerate(ac_result["log"], 1):
                dr = f" <span style='color:#ff6060'>−{e['rows_delta']} rows</span>" if e["rows_delta"] else ""
                dc = f" <span style='color:#f0a020'>−{e['cols_delta']} cols</span>" if e["cols_delta"] else ""
                st.markdown(
                    f'<div style="padding:7px 0;border-bottom:1px solid #0d1e2c;font-size:12px">'
                    f'<span style="font-family:IBM Plex Mono;color:#00c8a8;font-size:10px">#{i:02d}</span>  '
                    f'<strong style="color:#e8f4fb">{e["title"]}</strong>  '
                    f'<span style="color:#4a7088">{e["detail"][:90]}</span>'
                    f'{dr}{dc}</div>',
                    unsafe_allow_html=True)

        if get_state("_autoclean_backup") is not None:
            info_box("💾 Backup saved. Run ⟳ Reset in the sidebar to restore it.", "info")

        if st.button("↺ Reset auto-clean & reconfigure", key="ac_reset"):
            set_state("_autoclean_result", None)
            set_state("clean_df", get_state("raw_df"))
            set_state("_clean_log", [])
            set_state("_profile_cache", None)
            st.rerun()

    # ── Actionable Suggestions ───────────────────────────────────────────────
    if sugs:
        st.markdown("---")
        st.markdown(
            '<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;'
            'letter-spacing:.12em;margin-bottom:10px">💡 SMART SUGGESTIONS</div>',
            unsafe_allow_html=True)

        dismissed = get_state("_dismissed_sugs") or set()
        df_live = (lambda a,b: a if a is not None else b)(get_state("clean_df"), get_state("raw_df"))

        for si, sug in enumerate(sugs[:8]):
            sug_id = f"{sug['column']}_{sug.get('code',si)}"
            if sug_id in dismissed:
                continue
            pri   = sug.get("priority", "low")
            icon  = sug.get("icon", "💡")
            col   = sug['column']
            act   = sug.get("action", "")
            det   = sug.get("detail", "")
            code  = sug.get("code", "")
            kind  = "warn" if pri == "high" else "info"
            border = "#f0a020" if pri == "high" else "#4db8ff"

            st.markdown(
                f'<div style="background:#0a1820;border:1px solid {border};border-radius:5px;'
                f'padding:10px 14px;margin-bottom:8px">',
                unsafe_allow_html=True)

            sc1, sc2 = st.columns([8, 2])
            with sc1:
                st.markdown(
                    f'<div style="font-size:12px">{icon} <strong style="color:#e8f4fb">{col}</strong>'
                    f' <span style="color:#4a7088">·</span> {act}'
                    f'<br><span style="font-size:11px;color:#4a7088">{det}</span></div>',
                    unsafe_allow_html=True)
            with sc2:
                b1, b2, b3 = st.columns(3)

                # Review button — shows a preview expander
                with b1:
                    if st.button("👁", key=f"sug_review_{si}", help="Review what will change"):
                        cur = get_state("_sug_review") or set()
                        if sug_id in cur: cur.discard(sug_id)
                        else: cur.add(sug_id)
                        set_state("_sug_review", cur); st.rerun()

                # Apply button
                with b2:
                    if st.button("✅", key=f"sug_apply_{si}", help="Apply this fix automatically"):
                        if df_live is not None:
                            try:
                                new_df = df_live.copy()
                                entry  = None
                                if code == "high_missing":
                                    new_df, entry = drop_high_missing_cols(df_live, 70.0)
                                elif code == "fill_numeric":
                                    num_c = [col] if col in df_live.columns else []
                                    if num_c:
                                        new_df, entry = fill_missing(df_live, "median", num_c)
                                elif code == "convert_numeric":
                                    new_df, entry = coerce_column_type(df_live, col, "numeric")
                                elif code == "drop_empty":
                                    new_df, entry = drop_empty_columns(df_live)
                                elif code == "standardise_names":
                                    new_df, entry = standardise_column_names(df_live)
                                elif code == "outliers":
                                    new_df, entry = handle_outliers(df_live, [col], "flag")
                                if entry:
                                    apply_and_store(new_df, entry)
                                    d2 = dismissed | {sug_id}
                                    set_state("_dismissed_sugs", d2)
                                    info_box(f"✅ Applied: {entry['detail'][:80]}", "success")
                                    st.rerun()
                            except Exception as ex:
                                st.error(f"Could not apply: {ex}")

                # Ignore button
                with b3:
                    if st.button("✕", key=f"sug_ignore_{si}", help="Dismiss this suggestion"):
                        d2 = dismissed | {sug_id}
                        set_state("_dismissed_sugs", d2); st.rerun()

            st.markdown("</div>", unsafe_allow_html=True)

            # Review panel — inline diff preview
            reviewing = sug_id in (get_state("_sug_review") or set())
            if reviewing and df_live is not None:
                with st.expander(f"👁 Preview: what will change for «{col}»", expanded=True):
                    try:
                        prev_new = df_live.copy()
                        if code == "high_missing":
                            prev_new, _ = drop_high_missing_cols(df_live, 70.0)
                            st.markdown(f"**Columns remaining:** {len(prev_new.columns)} (was {len(df_live.columns)})")
                            dropped = [c for c in df_live.columns if c not in prev_new.columns]
                            st.caption(f"Would drop: {dropped[:10]}")
                        elif code in ("fill_numeric", "convert_numeric"):
                            before_miss = int(df_live[col].isna().sum())
                            if code == "fill_numeric":
                                prev_new, _ = fill_missing(df_live, "median", [col])
                            else:
                                prev_new, _ = coerce_column_type(df_live, col, "numeric")
                            after_miss = int(prev_new[col].isna().sum())
                            ra, rb2, rc = st.columns(3)
                            with ra: metric_tile("BEFORE (NaN)", str(before_miss), "#ff6060")
                            with rb2: metric_tile("AFTER (NaN)", str(after_miss), "#00c8a8")
                            with rc: metric_tile("FILLED", str(before_miss - after_miss), "#4db8ff")
                            st.dataframe(prev_new[[col]].head(8), use_container_width=True, height=180)
                        elif code == "outliers":
                            out = detect_outliers(df_live, method="iqr")
                            info = out.get(col, {})
                            st.markdown(f"**Outliers:** {info.get('n_outliers',0)} rows  "
                                        f"**Bounds:** {info.get('lower_bound',''):.2f} → {info.get('upper_bound',''):.2f}")
                            st.caption(f"Sample: {info.get('outlier_values','')[:5]}")
                        elif code == "standardise_names":
                            prev_new, _ = standardise_column_names(df_live)
                            changed = [(o, n) for o, n in zip(df_live.columns, prev_new.columns) if o != n]
                            if changed:
                                st.dataframe(
                                    pd.DataFrame(changed, columns=["Before","After"]),
                                    use_container_width=True, height=160)
                            else:
                                st.caption("No column names would change.")
                    except Exception as ex:
                        st.caption(f"Preview unavailable: {ex}")

    # Column table
    st.markdown("---")
    search = st.text_input("🔍 Filter columns", placeholder="Type column name…",
                           key="prof_search", label_visibility="collapsed")
    show_cols = [c for c in cols_p if not search or search.lower() in c["column"].lower()]

    for col_info in show_cols:
        q = col_info.get("quality","green")
        dot = _quality_dot(q)
        miss_color = "#ff6060" if col_info["pct_missing"] > 30 else \
                     "#f0a020" if col_info["pct_missing"] > 5  else "#00c8a8"
        with st.expander(f'{dot} {col_info["column"]}  ·  {col_info["detected_type"]}  ·  {col_info["pct_missing"]}% missing'):
            ca, cb_, cc = st.columns(3)
            with ca:
                st.markdown(f'**Type:** `{col_info["detected_type"]}`  \n**dtype:** `{col_info["raw_dtype"]}`')
            with cb_:
                st.markdown(f'**Missing:** <span style="color:{miss_color}">{col_info["pct_missing"]}%</span>  \n**Unique:** {col_info["n_unique"]}', unsafe_allow_html=True)
            with cc:
                st.markdown(f'**Sample:** `{col_info["sample"]}`')
            if col_info.get("issues"):
                for iss in col_info["issues"][:3]:
                    info_box(f'⚠ {iss["label"]}', "warn")

    _back_to = 14 if get_state("_multi_file_mode") else 1
    nav_footer(5, back_label="← Back", back_step=_back_to, next_label="→ Clean: Duplicates")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — DUPLICATES
# ══════════════════════════════════════════════════════════════════════════════

def render_duplicates():
    step_header("3", "DUPLICATES", "Detect and remove duplicate rows")
    df = (lambda a, b: a if a is not None else b)(get_state("clean_df"), get_state("raw_df"))
    if df is None: info_box("No data loaded.", "warn"); return

    # ── Quality banner (always visible) ──────────────────────────────────────
    quality_banner(df)

    info   = detect_duplicates(df)
    n_dup  = info["n_exact_dup_rows"]

    if n_dup == 0:
        info_box("✅ No exact duplicate rows detected. Dataset is clean on this pillar.", "success")
    else:
        info_box(f"⚠ <strong>{n_dup} duplicate rows</strong> found ({n_dup/len(df)*100:.1f}% of data).", "warn")

        with st.expander("👁  Preview duplicate rows (up to 20 shown)"):
            st.dataframe(info["dup_preview"], use_container_width=True, height=220)

        st.markdown("---")
        st.markdown('<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.12em;margin-bottom:10px">DEDUPLICATION OPTIONS</div>', unsafe_allow_html=True)
        d1, d2 = st.columns(2)
        with d1:
            keep = st.radio("Which occurrence to keep",
                            ["first","last","none (drop all)"],
                            key="dup_keep", label_visibility="visible")
        with d2:
            subset_cols = st.multiselect("Consider only these columns (blank = all)",
                                         list(df.columns), key="dup_subset")

        keep_val   = False if keep == "none (drop all)" else keep
        subset_val = subset_cols if subset_cols else None

        sim_info   = detect_duplicates(df, subset=subset_val)
        n_would_drop = sim_info["n_exact_dup_rows"]
        st.caption(f"With these settings: {n_would_drop} rows would be dropped → {len(df)-n_would_drop:,} rows remaining")

        # ── Preview after cleanup ─────────────────────────────────────────────
        if st.button("👁  Preview after cleanup"):
            sim_df, _ = drop_duplicates(df, subset=subset_val, keep=keep_val)
            st.dataframe(sim_df.head(2), use_container_width=True, height=220)
            pa, pb, pc = st.columns(3)
            with pa: metric_tile("ROWS BEFORE", f"{len(df):,}")
            with pb: metric_tile("ROWS AFTER",  f"{len(sim_df):,}", "#00c8a8")
            with pc: metric_tile("REMOVED",     f"{len(df)-len(sim_df):,}", "#ff6060" if len(df)!=len(sim_df) else "#00c8a8")

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        # ── Apply ─────────────────────────────────────────────────────────────
        if st.button("🗑  Apply Deduplication", type="primary"):
            with st.spinner("Deduplicating…"):
                new_df, entry = drop_duplicates(df, subset=subset_val, keep=keep_val)
            apply_and_store(new_df, entry)
            # Store result summary for the confirmation banner
            set_state("_dup_result", {
                "removed":  entry["rows_delta"],
                "before":   len(df),
                "after":    len(new_df),
            })
            st.rerun()

    # ── Post-apply confirmation + before/after ────────────────────────────────
    result = get_state("_dup_result")
    if result:
        st.markdown("---")
        st.markdown('<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.12em;margin-bottom:10px">RESULT</div>', unsafe_allow_html=True)
        ra, rb, rc = st.columns(3)
        with ra: metric_tile("ROWS BEFORE", f'{result["before"]:,}')
        with rb: metric_tile("ROWS AFTER",  f'{result["after"]:,}', "#00c8a8")
        with rc: metric_tile("REMOVED",     f'{result["removed"]:,}', "#ff6060" if result["removed"] else "#00c8a8")
        info_box(f'✅ Deduplication complete. <strong>{result["removed"]} row(s) removed</strong>. ' +
                 f'Dataset now has <strong>{result["after"]:,} rows</strong>. ' +
                 'Click <em>→ Missing Values</em> when ready.', "success")

    log = get_state("_clean_log") or []
    dup_ops = [e for e in log if e["pillar"] == 1]
    if dup_ops and not result:
        info_box(f"✅ <strong>{len(dup_ops)} dedup operation(s) applied</strong> this session.", "success")

    nav_footer(6, next_label="→ Missing Values")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — MISSING VALUES
# ══════════════════════════════════════════════════════════════════════════════

def render_missing():
    step_header("4", "MISSING VALUES", "Detect and handle missing data")
    df = (lambda a, b: a if a is not None else b)(get_state("clean_df"), get_state("raw_df"))
    if df is None: info_box("No data.", "warn"); return
    quality_banner(df)

    ms = missing_summary(df)
    total_missing = int(ms["missing"].sum())
    n_clean_cols  = int((ms["missing"] == 0).sum())
    worst = ms[ms["pct"] > 0].head(8)

    m1,m2,m3,m4 = st.columns(4)
    with m1: metric_tile("TOTAL CELLS MISSING", f"{total_missing:,}", "#ff6060" if total_missing else "#00c8a8")
    with m2: metric_tile("COMPLETENESS", f'{(1-df.isna().mean().mean())*100:.1f}%')
    with m3: metric_tile("CLEAN COLUMNS", f"{n_clean_cols}/{len(df.columns)}", "#00c8a8")
    with m4: metric_tile("COLUMNS >30% MISS", f"{int((ms['pct']>30).sum())}", "#ff6060" if (ms['pct']>30).any() else "#00c8a8")

    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    if total_missing == 0:
        info_box("✅ No missing values detected.", "success")
    else:
        # Missing heatmap (bar chart substitute using st.progress)
        st.markdown('<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.12em;margin-bottom:10px">MISSING % BY COLUMN (top 20)</div>', unsafe_allow_html=True)
        top20 = ms.head(20)
        for _, row in top20.iterrows():
            if row["pct"] == 0: break
            color = "#ff6060" if row["pct"] > 30 else "#f0a020" if row["pct"] > 10 else "#4db8ff"
            st.markdown(
                f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:6px">'
                f'<div style="font-family:IBM Plex Mono;font-size:11px;color:#c9d8e3;min-width:180px;'
                f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="{row["column"]}">'
                f'{row["column"]}</div>'
                f'<div style="flex:1;background:#0d1e2c;border-radius:2px;height:10px">'
                f'<div style="width:{row["pct"]}%;background:{color};height:100%;border-radius:2px"></div></div>'
                f'<div style="font-family:IBM Plex Mono;font-size:11px;color:{color};min-width:45px;text-align:right">'
                f'{row["pct"]:.1f}%</div></div>',
                unsafe_allow_html=True)

        with st.expander("📋 Full missing values table"):
            st.dataframe(ms, use_container_width=True, height=300)

        st.markdown("---")
        st.markdown('<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.12em;margin-bottom:10px">HANDLING STRATEGY</div>', unsafe_allow_html=True)

        tab_drop, tab_fill, tab_dropcol = st.tabs(["Drop High-Missing Cols", "Fill Missing Values", "Drop Rows"])

        with tab_drop:
            thresh = st.slider("Drop columns with more than X% missing", 10, 100, 70, key="mv_thresh")
            to_drop = ms[ms["pct"] >= thresh]["column"].tolist()
            st.caption(f"{len(to_drop)} column(s) would be dropped: {', '.join(to_drop[:8])}{'…' if len(to_drop)>8 else ''}")
            if st.button("🗑 Drop These Columns", key="mv_drop_cols"):
                new_df, entry = drop_high_missing_cols(df, float(thresh))
                apply_and_store(new_df, entry)
                info_box(f"✅ Dropped {entry['cols_delta']} column(s).", "success"); st.rerun()

        with tab_fill:
            f1, f2 = st.columns(2)
            with f1:
                strategy = st.selectbox("Fill strategy",
                                        ["mean","median","mode","custom","ffill","bfill"],
                                        key="mv_strategy")
            with f2:
                custom_val = st.text_input("Custom value (if strategy = custom)", "0",
                                           key="mv_custom") if strategy == "custom" else None
            target_cols = st.multiselect("Apply to columns (blank = all with missing)",
                                         [c for c in df.columns if df[c].isna().any()],
                                         key="mv_cols")
            cv = float(custom_val) if (strategy == "custom" and custom_val) else (custom_val or 0)
            if st.button("✅ Apply Fill Strategy", key="mv_apply_fill"):
                cols_to_fill = target_cols or [c for c in df.columns if df[c].isna().any()]
                new_df, entry = fill_missing(df, strategy, cols_to_fill, custom_value=cv)
                apply_and_store(new_df, entry)
                info_box(f"✅ {entry['detail']}", "success"); st.rerun()

        with tab_dropcol:
            drop_col_list = st.multiselect("Select columns to drop rows on (blank = any missing)",
                                           list(df.columns), key="mv_droprows_cols")
            n_would_drop = int(df[drop_col_list if drop_col_list else list(df.columns)].isna().any(axis=1).sum())
            st.caption(f"{n_would_drop} rows would be dropped")
            if st.button("🗑 Drop Rows with Missing Values", key="mv_droprows"):
                new_df, entry = fill_missing(df, "drop_rows", drop_col_list)
                apply_and_store(new_df, entry)
                info_box(f"✅ {entry['detail']}", "success"); st.rerun()

    nav_footer(7, next_label="→ Data Types")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — DATA TYPES
# ══════════════════════════════════════════════════════════════════════════════

def render_types():
    step_header("5", "DATA TYPE DETECTION", "Verify and correct column types")
    df = (lambda a, b: a if a is not None else b)(get_state("clean_df"), get_state("raw_df"))
    if df is None: info_box("No data.", "warn"); return
    quality_banner(df)

    type_info = detect_types(df)

    # Summary pills
    type_counts = {}
    for t in type_info:
        type_counts[t["detected_type"]] = type_counts.get(t["detected_type"], 0) + 1
    pills = "".join(
        f'<span style="font-family:IBM Plex Mono;font-size:10px;background:#0d1e2c;border:1px solid #1a3347;'
        f'border-radius:3px;padding:3px 10px;margin-right:6px;color:#7ab8d4">'
        f'{tp}: {cnt}</span>'
        for tp, cnt in sorted(type_counts.items(), key=lambda x: -x[1])
    )
    st.markdown(f'<div style="margin-bottom:16px">{pills}</div>', unsafe_allow_html=True)

    search = st.text_input("🔍 Filter columns", key="types_search", label_visibility="collapsed",
                           placeholder="Type to filter…")
    filtered = [t for t in type_info if not search or search.lower() in t["column"].lower()]

    for info in filtered:
        col  = info["column"]
        dtyp = info["detected_type"]
        icon = {"numeric":"🔢","date":"📅","currency":"💰","percentage":"📊",
                "categorical":"🏷","text":"📝","empty":"⬜"}.get(dtyp, "❓")
        sugg_color = "#00c8a8" if "Already" in info["suggestion"] else "#f0a020"

        with st.expander(f'{icon} {col}  ·  detected: {dtyp}  ·  current: {info["current_dtype"]}'):
            ca, cb_, cc, cd = st.columns([5, 5, 5, 1])
            with ca:
                st.markdown(f'**Current dtype:** `{info["current_dtype"]}`  \n**Detected:** `{dtyp}`')
            with cb_:
                st.markdown(f'**Sample:** `{info["sample"]}`')
            with cc:
                st.markdown(f'<span style="color:{sugg_color}">💡 {info["suggestion"]}</span>',
                            unsafe_allow_html=True)
            with cd:
                target_type = st.selectbox(
                    "Convert to", ["(keep)","numeric","date","text","percentage"],
                    key=f"type_sel_{col}", label_visibility="collapsed")
                if target_type != "(keep)":
                    if st.button("Apply", key=f"type_apply_{col}"):
                        new_df, entry = coerce_column_type(df, col, target_type)
                        apply_and_store(new_df, entry)
                        info_box(f"✅ '{col}' → {target_type}", "success"); st.rerun()

    nav_footer(8, next_label="→ Outliers")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — OUTLIERS
# ══════════════════════════════════════════════════════════════════════════════

def render_outliers():
    step_header("6", "OUTLIER DETECTION", "Find and handle statistical outliers")
    df = (lambda a, b: a if a is not None else b)(get_state("clean_df"), get_state("raw_df"))
    if df is None: info_box("No data.", "warn"); return
    quality_banner(df)

    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if not num_cols:
        info_box("No numeric columns found — convert columns to numeric first (Step 8).", "warn")
        nav_footer(9); return

    cfg1, cfg2 = st.columns(2)
    with cfg1:
        method = st.radio("Detection method", ["iqr","zscore"], horizontal=True,
                          format_func=lambda x: "IQR (Interquartile Range)" if x=="iqr" else "Z-Score",
                          key="out_method")
    with cfg2:
        if method == "iqr":
            iqr_mult = st.slider("IQR multiplier (higher = fewer outliers)", 1.0, 4.0, 1.8, 0.1, key="out_iqr")
            zsc = 3.0
        else:
            zsc = st.slider("Z-score threshold", 1.8, 5.0, 3.0, 0.1, key="out_zsc")
            iqr_mult = 1.5

    with st.spinner("Detecting outliers…"):
        outliers = detect_outliers(df, method=method, zscore_threshold=zsc, iqr_multiplier=iqr_mult)

    if not outliers:
        info_box("✅ No outliers detected with current settings.", "success")
    else:
        m1, m2 = st.columns(2)
        with m1: metric_tile("COLUMNS WITH OUTLIERS", str(len(outliers)), "#f0a020")
        with m2:
            total_out = sum(v["n_outliers"] for v in outliers.values())
            metric_tile("TOTAL OUTLIER ROWS", f"{total_out:,}", "#f0a020")

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        for col, info in list(outliers.items())[:20]:
            with st.expander(f'⚠ {col} — {info["n_outliers"]} outliers ({info["pct_outliers"]}%)'):
                oa, ob = st.columns(2)
                with oa:
                    st.markdown(
                        f'**Bounds:** `{info["lower_bound"]:,.2f}` → `{info["upper_bound"]:,.2f}`  \n'
                        f'**Sample outliers:** `{info["outlier_values"][:5]}`')
                try:
                    import matplotlib
                    matplotlib.use("Agg")
                    import matplotlib.pyplot as plt
                    fig, ax = plt.subplots(figsize=(7, 1.8))
                    fig.patch.set_facecolor("#0d1e2c")
                    ax.set_facecolor("#0d1e2c")
                    clean_s = df[col].dropna()
                    ax.boxplot(clean_s, vert=False, patch_artist=True,
                               boxprops=dict(facecolor="#004a38", color="#00c8a8"),
                               medianprops=dict(color="#00c8a8"),
                               whiskerprops=dict(color="#4a7088"),
                               flierprops=dict(marker="o", color="#ff6060", markersize=6),
                               capprops=dict(color="#4a7088"))
                    ax.tick_params(colors="#4a7088", labelsize=10)
                    for sp in ax.spines.values(): sp.set_color("#1a3347")
                    with ob:
                        st.pyplot(fig, use_container_width=True)
                    plt.close(fig)
                except Exception:
                    pass

        st.markdown("---")
        st.markdown('<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.12em;margin-bottom:10px">ACTION</div>', unsafe_allow_html=True)
        a1, a2 = st.columns(2)
        with a1:
            action = st.radio("What to do with outliers",
                              ["keep","cap","drop","flag"],
                              format_func={"keep":"Keep (no change)","cap":"Cap to bounds",
                                           "drop":"Drop rows","flag":"Flag (add __outlier column)"}.get,
                              key="out_action")
        with a2:
            target_out_cols = st.multiselect("Columns to apply to",
                                             list(outliers.keys()),
                                             default=list(outliers.keys())[:5],
                                             key="out_cols")
        if action != "keep":
            if st.button(f"✅ Apply: {action.title()} Outliers", type="primary"):
                new_df, entry = handle_outliers(df, target_out_cols, action,
                                               method=method, iqr_multiplier=iqr_mult,
                                               zscore_threshold=zsc)
                apply_and_store(new_df, entry)
                info_box(f"✅ {entry['detail']}", "success"); st.rerun()

    nav_footer(9, next_label="→ Financial Cleaning")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 7 — FINANCIAL-SPECIFIC CLEANING
# ══════════════════════════════════════════════════════════════════════════════

def render_finance():
    step_header("7", "FINANCIAL CLEANING", "Currency, empty columns, and financial name standardisation")
    df = (lambda a, b: a if a is not None else b)(get_state("clean_df"), get_state("raw_df"))
    if df is None: info_box("No data.", "warn"); return
    quality_banner(df)

    tab_names, tab_currency, tab_empty = st.tabs(["📛 Column Names", "💱 Currency", "🗑 Empty Columns"])

    with tab_names:
        info_box("Auto-detect common financial column names and rename to canonical English equivalents (e.g. 'Ventes' → 'Revenue', 'dette nette' → 'Net Debt').")
        from synonyms import SYNONYMS, CANONICAL_NAMES
        import re as _re

        # Build suggestion map
        col_lower = {c.lower().strip(): c for c in df.columns}
        auto_map  = {}
        used_targets: set = set()
        for canonical, aliases in SYNONYMS.items():
            if canonical in used_targets: continue
            for alias in aliases:
                alias_c = _re.sub(r"[_\-\s]+", " ", alias.lower()).strip()
                for src_low, src_orig in col_lower.items():
                    src_c = _re.sub(r"[_\-\s]+", " ", src_low).strip()
                    if src_c == alias_c and src_orig not in auto_map and src_orig != canonical:
                        auto_map[src_orig] = canonical
                        used_targets.add(canonical)
                        break
                if canonical in used_targets: break

        if not auto_map:
            info_box("✅ No standardisation suggestions — column names already look canonical.", "success")
        else:
            st.markdown(f'<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.12em;margin-bottom:10px">{len(auto_map)} SUGGESTED RENAMES</div>', unsafe_allow_html=True)

        # Let user confirm/override each suggestion + add manual ones
        confirmed_map = {}
        for old_name, suggested_new in auto_map.items():
            r1, r2, r3 = st.columns([6, 6, 1])
            with r1:
                st.markdown(f'<div style="padding-top:8px;font-family:IBM Plex Mono;font-size:11px;color:#e8f4fb">{old_name}</div>', unsafe_allow_html=True)
            with r2:
                new_name = st.text_input(f"→", value=suggested_new,
                                         key=f"fin_rename_{old_name}", label_visibility="collapsed")
            with r3:
                include = st.checkbox("Include column", value=True, key=f"fin_inc_{old_name}",
                                      label_visibility="collapsed")
            if include and new_name and new_name != old_name:
                confirmed_map[old_name] = new_name

        st.markdown("---")
        st.markdown('<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.12em;margin-bottom:8px">MANUAL RENAME</div>', unsafe_allow_html=True)
        m1, m2 = st.columns(2)
        with m1:
            manual_old = st.selectbox("Column to rename", [""] + list(df.columns), key="fin_man_old",
                                      label_visibility="collapsed")
        with m2:
            manual_new = st.text_input("New name", key="fin_man_new", label_visibility="collapsed",
                                       placeholder="New column name…")
        if manual_old and manual_new and manual_old != manual_new:
            confirmed_map[manual_old] = manual_new

        if confirmed_map:
            if st.button("✅ Apply Renames", type="primary"):
                new_df, entry = standardise_column_names(df, rename_map=confirmed_map)
                apply_and_store(new_df, entry)
                info_box(f"✅ {entry['detail']}", "success"); st.rerun()

    with tab_currency:
        curr_cols = detect_currency_columns(df)
        if not curr_cols:
            info_box("✅ No currency symbols detected in data.", "success")
        else:
            info_box(f"Detected currency symbols in {len(curr_cols)} column(s).")
            for col, symbols in curr_cols.items():
                st.markdown(f'`{col}` — symbols found: {", ".join(symbols)}')

            st.markdown("---")
            cc1, cc2, cc3 = st.columns(3)
            with cc1:
                curr_col = st.selectbox("Column to convert", list(curr_cols.keys()), key="curr_col")
            with cc2:
                from_sym = st.text_input("From symbol/currency", value=list(curr_cols.get(curr_col, ["$"]))[0], key="curr_from")
            with cc3:
                rate = st.number_input("Exchange rate (multiply by)", min_value=0.0001, value=1.0,
                                       format="%.4f", key="curr_rate")
            if st.button("✅ Apply Currency Conversion"):
                new_df, entry = convert_currency(df, curr_col, from_sym, rate)
                apply_and_store(new_df, entry)
                info_box(f"✅ {entry['detail']}", "success"); st.rerun()

    with tab_empty:
        empty_cols = [c for c in df.columns if df[c].isna().all()]
        if not empty_cols:
            info_box("✅ No fully-empty columns found.", "success")
        else:
            info_box(f"<strong>{len(empty_cols)} fully-empty column(s)</strong>: {', '.join(empty_cols)}", "warn")
            if st.button("🗑 Drop All Empty Columns", type="primary"):
                new_df, entry = drop_empty_columns(df)
                apply_and_store(new_df, entry)
                info_box(f"✅ {entry['detail']}", "success"); st.rerun()

    nav_footer(10, next_label="→ Standardisation")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 8 — STANDARDISATION
# ══════════════════════════════════════════════════════════════════════════════

def render_standardise():
    step_header("8", "STANDARDISATION", "Consistent formats for dates, numbers, and text")
    df = (lambda a, b: a if a is not None else b)(get_state("clean_df"), get_state("raw_df"))
    if df is None: info_box("No data.", "warn"); return
    quality_banner(df)

    tab_dates, tab_nums, tab_text = st.tabs(["📅 Dates", "🔢 Numbers", "🔤 Text Case"])

    with tab_dates:
        # Auto-detect date columns
        type_info = detect_types(df)
        likely_dates = [t["column"] for t in type_info if t["detected_type"] == "date"]
        date_cols = st.multiselect("Date columns to standardise",
                                   list(df.columns), default=likely_dates, key="std_date_cols")
        d1, d2 = st.columns(2)
        with d1:
            in_fmt = st.text_input("Input format (leave blank to auto-detect)",
                                   placeholder="%d/%m/%Y", key="std_date_in")
        with d2:
            out_fmt = st.text_input("Output format", value="%Y-%m-%d", key="std_date_out")
        if date_cols:
            if st.button("✅ Standardise Dates", type="primary"):
                new_df, entry = standardise_dates(df, date_cols,
                                                   input_format=in_fmt or None, output_format=out_fmt)
                apply_and_store(new_df, entry)
                info_box(f"✅ {entry['detail']}", "success"); st.rerun()

    with tab_nums:
        num_cols = df.select_dtypes(include="object").columns.tolist()
        likely_num = [t["column"] for t in detect_types(df)
                      if t["detected_type"] in ("numeric","currency","percentage") and t["column"] in num_cols]
        num_sel = st.multiselect("Columns to coerce to clean numeric",
                                 num_cols, default=likely_num, key="std_num_cols")
        if num_sel:
            st.caption("Removes thousand separators, currency symbols, percentage signs. Converts to float64.")
            if st.button("✅ Standardise Numbers", type="primary"):
                new_df, entry = standardise_numbers(df, num_sel)
                apply_and_store(new_df, entry)
                info_box(f"✅ {entry['detail']}", "success"); st.rerun()

    with tab_text:
        text_cols = df.select_dtypes(include="object").columns.tolist()
        txt_sel = st.multiselect("Text columns", text_cols, key="std_text_cols")
        case = st.radio("Case style", ["title","upper","lower","strip"],
                        horizontal=True, key="std_case")
        if txt_sel:
            if st.button("✅ Apply Text Case", type="primary"):
                new_df, entry = standardise_text_case(df, txt_sel, case)
                apply_and_store(new_df, entry)
                info_box(f"✅ {entry['detail']}", "success"); st.rerun()

    nav_footer(11, next_label="→ Cleaning Report")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 9 — CLEANING REPORT
# ══════════════════════════════════════════════════════════════════════════════

def render_report():
    step_header("9", "CLEANING REPORT", "Summary, log, and export")
    df_clean   = (lambda a, b: a if a is not None else b)(get_state("clean_df"), get_state("raw_df"))
    df_orig    = get_state("_original_df") if get_state("_original_df") is not None else df_clean
    clean_log  = get_state("_clean_log") or []

    if df_clean is None:
        info_box("No data loaded.", "warn"); return

    # ── Summary stats ─────────────────────────────────────────────────────────
    row_delta = len(df_orig) - len(df_clean)
    col_delta = len(df_orig.columns) - len(df_clean.columns)
    miss_orig = int(df_orig.isna().sum().sum())
    miss_cln  = int(df_clean.isna().sum().sum())

    st.markdown('<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.12em;margin-bottom:12px">BEFORE / AFTER COMPARISON</div>', unsafe_allow_html=True)
    m1,m2,m3,m4,m5,m6 = st.columns(6)
    with m1: metric_tile("ROWS BEFORE",  f"{len(df_orig):,}")
    with m2: metric_tile("ROWS AFTER",   f"{len(df_clean):,}", "#00c8a8" if row_delta>=0 else "#ff6060")
    with m3: metric_tile("COLS BEFORE",  f"{len(df_orig.columns)}")
    with m4: metric_tile("COLS AFTER",   f"{len(df_clean.columns)}", "#00c8a8" if col_delta>=0 else "#ff6060")
    with m5: metric_tile("MISSING BEFORE", f"{miss_orig:,}", "#ff6060" if miss_orig else "#00c8a8")
    with m6: metric_tile("MISSING AFTER",  f"{miss_cln:,}",  "#00c8a8" if miss_cln==0 else "#f0a020")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # ── Operation log ─────────────────────────────────────────────────────────
    if clean_log:
        st.markdown(f'<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.12em;margin-bottom:10px">CLEANING LOG — {len(clean_log)} OPERATIONS</div>', unsafe_allow_html=True)
        for i, entry in enumerate(clean_log, 1):
            dr, dc = entry["rows_delta"], entry["cols_delta"]
            row_txt = f'−{dr} rows' if dr > 0 else "no row change"
            col_txt = f'−{dc} cols' if dc > 0 else "no col change"
            row_c   = "#ff6060" if dr > 0 else "#4a7088"
            col_c   = "#f0a020" if dc > 0 else "#4a7088"
            st.markdown(
                f'<div style="border-left:3px solid #00c8a8;padding:10px 16px;margin-bottom:8px;'
                f'background:#0d1e2c;border-radius:0 4px 4px 0">'
                f'<div style="font-family:IBM Plex Mono;font-size:10px;color:#4a7088">Step {i} · {entry["ts"]}</div>'
                f'<div style="font-weight:700;color:#e8f4fb;margin:3px 0">{entry["title"]}</div>'
                f'<div style="font-size:12px;color:#7ab8d4">{entry["detail"]}</div>'
                f'<div style="font-size:11px;margin-top:5px">'
                f'<span style="color:{row_c}">{row_txt}</span> &nbsp; '
                f'<span style="color:{col_c}">{col_txt}</span></div></div>',
                unsafe_allow_html=True)
    else:
        info_box("No cleaning operations recorded yet — go through the pillar steps above.", "info")

    # ── Column overview ───────────────────────────────────────────────────────
    with st.expander("📋 Column overview (after cleaning)"):
        rows = [{"Column":c, "Dtype":str(df_clean[c].dtype),
                 "Missing %":f'{df_clean[c].isna().mean()*100:.1f}%',
                 "Unique":int(df_clean[c].nunique())}
                for c in df_clean.columns]
        st.dataframe(pd.DataFrame(rows), use_container_width=True, height=260)

    # ── Preview ───────────────────────────────────────────────────────────────
    with st.expander("👁 Preview cleaned data (first 10 rows)"):
        st.dataframe(df_clean.head(10), use_container_width=True, height=240)

    # ── Export ────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown('<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.12em;margin-bottom:12px">EXPORT</div>', unsafe_allow_html=True)

    ex1, ex2, ex3 = st.columns(3)
    with ex1:
        st.markdown('<div class="dp-card">', unsafe_allow_html=True)
        st.markdown("**📄 Cleaned CSV**")
        st.caption("Ready to load into FinAnalyst Pro or any analysis tool.")
        csv_bytes = to_csv_bytes(df_clean)
        st.download_button("⬇ Download cleaned_data.csv", data=csv_bytes,
                           file_name="dataprep_cleaned.csv", mime="text/csv",
                           width="stretch")
        st.markdown(f'<div style="font-size:11px;color:#2a4a5e;margin-top:6px">'
                    f'{len(df_clean):,} rows · {len(df_clean.columns)} cols · {len(csv_bytes)/1024:.1f} KB</div>',
                    unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with ex2:
        st.markdown('<div class="dp-card">', unsafe_allow_html=True)
        st.markdown("**📊 Cleaning Report HTML**")
        st.caption("Self-contained HTML report with log, stats, and column overview.")
        try:
            html_report = build_report_html(df_orig, df_clean, clean_log)
            st.download_button("⬇ Download report.html",
                               data=html_report.encode("utf-8"),
                               file_name="dataprep_report.html",
                               mime="text/html", width="stretch")
        except Exception as e:
            st.error(f"Report error: {e}")
        st.markdown("</div>", unsafe_allow_html=True)

    with ex3:
        st.markdown('<div class="dp-card">', unsafe_allow_html=True)
        st.markdown("**📋 Cleaning Log JSON**")
        st.caption("Machine-readable log of every operation performed.")
        log_json = json.dumps(clean_log, indent=5, default=str)
        st.download_button("⬇ Download cleaning_log.json",
                           data=log_json.encode("utf-8"),
                           file_name="dataprep_log.json",
                           mime="application/json", width="stretch")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("---")
    if st.button("⟳ Start over with a new file"):
        reset_pipeline(); st.rerun()

    nav_footer(12, show_next=False)


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

def render_sidebar():
    with st.sidebar:
        st.markdown(
            '<div style="padding:20px 0 16px;text-align:center">'
            '<div style="font-family:IBM Plex Mono;font-size:26px;font-weight:700;'
            'background:linear-gradient(135deg,#00c8a8,#0084d4);'
            '-webkit-background-clip:text;-webkit-text-fill-color:transparent">⚙ DataPrep Pro</div>'
            '<div style="font-size:10px;color:#2a4a5e;letter-spacing:.2em;margin-top:4px">DATA CLEANING SUITE v2</div>'
            '</div>',
            unsafe_allow_html=True)
        st.markdown('<div style="border-top:1px solid #1a2e3d;margin-bottom:16px"></div>', unsafe_allow_html=True)

        # Dataset summary
        df = (lambda a, b: a if a is not None else b)(get_state("clean_df"), get_state("raw_df"))
        if df is not None:
            orig = get_state("_original_df")
            st.markdown('<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.1em;margin-bottom:8px">DATASET</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div style="font-size:12px;line-height:2;color:#7ab8d4">'
                f'<span style="color:#4a7088">Rows</span> {len(df):,}'
                + (f' <span style="color:#00c8a8;font-size:10px">(−{len(orig)-len(df):,})</span>' if orig is not None and len(orig)!=len(df) else "")
                + f'<br><span style="color:#4a7088">Cols</span> {len(df.columns)}'
                + (f' <span style="color:#f0a020;font-size:10px">(−{len(orig.columns)-len(df.columns)})</span>' if orig is not None and len(orig.columns)!=len(df.columns) else "")
                + f'<br><span style="color:#4a7088">Missing</span> {int(df.isna().sum().sum()):,}</span></div>',
                unsafe_allow_html=True)

            # Pillar completion dots
            st.markdown('<div style="border-top:1px solid #1a2e3d;margin:12px 0 8px"></div>', unsafe_allow_html=True)
            st.markdown('<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.1em;margin-bottom:8px">CLEANING PILLARS</div>', unsafe_allow_html=True)
            log = get_state("_clean_log") or []
            pillars_done = {e["pillar"] for e in log}
            pillar_names = {1:"Duplicates",2:"Missing Values",3:"Types",
                           4:"Outliers",5:"Finance",6:"Standardisation"}
            for num, name in pillar_names.items():
                dot   = "●" if num in pillars_done else "○"
                color = "#00c8a8" if num in pillars_done else "#1a3347"
                st.markdown(
                    f'<div style="font-family:IBM Plex Mono;font-size:11px;color:{color};padding:2px 0">'
                    f'{dot} {num}. {name}</div>',
                    unsafe_allow_html=True)
        else:
            st.markdown('<div style="font-size:12px;color:#2a4a5e;padding:8px 0">No data loaded</div>', unsafe_allow_html=True)

        st.markdown('<div style="border-top:1px solid #1a2e3d;margin:16px 0"></div>', unsafe_allow_html=True)

        # ── Expert glossary (shown when in Expert mode step 20) ───────────────
        step_now = get_state("step") or 1
        if step_now == 20:
            st.markdown(
                '<div style="font-family:IBM Plex Mono;font-size:10px;color:#4db8ff;'
                'letter-spacing:.12em;margin-bottom:8px">🔬 GLOSSARY</div>',
                unsafe_allow_html=True)
            glossary = [
                ("IQR",     "#f0a020", "Interquartile Range. Flags values below Q1−1.5×IQR or above Q3+1.5×IQR. Robust to extremes."),
                ("Z-Score", "#f0a020", "Flags values more than N std-devs from the mean. Sensitive to very large outliers."),
                ("Flag",    "#4db8ff", "Adds a boolean column marking each outlier row. Original values are unchanged."),
                ("Cap",     "#4db8ff", "Replaces outlier values with the boundary value. No rows are removed."),
                ("Drop",    "#ff6060", "Removes rows containing outlier values. Use carefully."),
                ("Mean",    "#00c8a8", "Replace missing with the arithmetic average. Sensitive to outliers."),
                ("Median",  "#00c8a8", "Replace missing with the middle value. More robust than mean."),
                ("Mode",    "#00c8a8", "Replace with the most frequent value. Works for numeric or categorical."),
                ("Zero",    "#00c8a8", "Replace missing with 0. Only use when 0 is meaningful (e.g. no sales = 0)."),
            ]
            for term, color, desc in glossary:
                st.markdown(
                    f'<div style="margin-bottom:7px">'
                    f'<span style="font-family:IBM Plex Mono;font-size:10px;font-weight:700;color:{color}">{term}</span>'
                    f'<div style="font-size:10px;color:#4a7088;line-height:1.4;margin-top:1px">{desc}</div></div>',
                    unsafe_allow_html=True)
            st.markdown('<div style="border-top:1px solid #1a2e3d;margin:12px 0 10px"></div>', unsafe_allow_html=True)

        if st.button("⟳ Reset", width="stretch"):
            reset_pipeline(); st.rerun()
        st.markdown(
            '<div style="font-size:10px;color:#1a3347;line-height:1.6;margin-top:12px">'
            'DataPrep Pro v2.0<br>Standalone data cleaning tool<br>'
            'Output: clean CSV for any tool</div>',
            unsafe_allow_html=True)




# ══════════════════════════════════════════════════════════════════════════════
# EXPERT MODE — _expert_pipeline_bar
# ══════════════════════════════════════════════════════════════════════════════

def _expert_pipeline_bar(active):
    steps = EXPERT_STEPS
    labels = {sn: lbl for sn, lbl in steps}
    order  = EXPERT_STEP_ORDER
    nums   = [sn for sn, _ in steps]
    idx    = nums.index(active) + 1 if active in nums else 1
    total  = len(nums)
    lbl    = labels.get(active, "")

    r1, r2 = st.columns([6, 4])
    with r1:
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:10px;padding-top:2px">'
            f'<div style="font-family:IBM Plex Mono;font-size:11px;color:#4db8ff">'
            f'🔬 EXPERT · STEP <span style="color:#4db8ff;font-weight:700">{idx}</span>/{total}</div>'
            f'<div style="font-family:IBM Plex Mono;font-size:13px;font-weight:700;color:#e8f4fb">◆ {lbl}</div>'
            f'</div>',
            unsafe_allow_html=True)

    # Dot trail
    dots = []
    high = get_state("_expert_highest") or 20
    for sn, slbl in steps:
        if sn == active:
            dots.append(f'<span title="{slbl}" style="color:#4db8ff;font-size:16px">◆</span>')
        elif EXPERT_STEP_ORDER.get(sn, 99) < EXPERT_STEP_ORDER.get(active, 0) or sn <= high:
            dots.append(f'<span title="{slbl}" style="color:#1a3a5a;font-size:12px">●</span>')
        else:
            dots.append(f'<span title="{slbl}" style="color:#1a2e3d;font-size:12px">○</span>')
    st.markdown(
        f'<div style="display:flex;gap:8px;padding:4px 0 10px">'
        + " ".join(dots) + '</div>',
        unsafe_allow_html=True)

    # Back to Quick Clean
    if st.button("← Back to mode selection", key="expert_to_home"):
        set_state("step", 1); st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# EXPERT MODE — entry helper (rendered inside render_import)
# ══════════════════════════════════════════════════════════════════════════════

def _render_expert_entry():
    """File uploader shown on step 1 when Expert mode is selected."""
    st.markdown(
        '<div style="font-family:IBM Plex Mono;font-size:10px;color:#4db8ff;'
        'letter-spacing:.12em;margin-bottom:10px">🔬 EXPERT MODE — STEP 1: UPLOAD FILES</div>',
        unsafe_allow_html=True)
    info_box("Upload 2 or more files. You will clean each one individually before deciding how to merge them.", "info")

    ups = st.file_uploader("Files", type=["csv","xlsx","xls"],
                            accept_multiple_files=True, label_visibility="collapsed",
                            key="expert_uploader")
    if ups and len(ups) >= 1:
        entries = []
        all_ok  = True
        with st.spinner(f"Loading {len(ups)} file(s)…"):
            for u in ups:
                u.seek(0); size = len(u.read()); u.seek(0)
                df, err, _, renames = load_file(u)
                if err:
                    st.error(f"{u.name}: {err}"); all_ok = False; break
                df_c, post_ren = deduplicate_columns(basic_clean(df))
                # Each file gets its own clean_df and log
                entries.append({
                    "name":    u.name,
                    "size":    size,
                    "rows":    len(df_c),
                    "cols":    len(df_c.columns),
                    "df":      df_c,          # current (cleaned) state
                    "raw_df":  df_c.copy(),   # original — never mutated
                    "renames": renames + post_ren,
                    "log":     [],
                    "selected": True,
                })
        if all_ok and entries:
            set_state("_expert_entries", entries)
            set_state("_expert_highest", 20)
            info_box(f"✓ Loaded <strong>{len(entries)} file(s)</strong>. Click below to start individual cleaning.", "success")
            if st.button("→ Start Expert Cleaning", type="primary"):
                set_state("step", 20); st.rerun()
    elif ups and len(ups) == 0:
        pass
    elif not ups:
        info_box("Upload at least one file to continue in Expert mode.", "warn")


# ══════════════════════════════════════════════════════════════════════════════
# EXPERT MODE — Step 20: Per-file cleaning  (redesigned)
# ══════════════════════════════════════════════════════════════════════════════

# ── Glossary tooltips ─────────────────────────────────────────────────────────
_TIPS = {
    "iqr":    "IQR (Interquartile Range): flags values below Q1-1.5xIQR or above Q3+1.5xIQR. Robust to extreme values.",
    "zscore": "Z-Score: flags values more than N standard deviations from the mean. Sensitive to extreme outliers.",
    "flag":   "Flag: adds a new boolean column marking each outlier row. Original values are kept unchanged.",
    "cap":    "Cap (Winsorize): replaces outlier values with the nearest boundary value. No rows are removed.",
    "drop":   "Drop: removes entire rows that contain an outlier. Use carefully -- may lose valid data.",
    "mean":   "Mean: replace each missing value with the arithmetic average of non-missing values in that column.",
    "median": "Median: replace with the middle value. More robust than mean when the column has outliers.",
    "mode":   "Mode: replace with the most frequent value. Works for numeric or categorical columns.",
    "zero":   "Zero: replace all missing values with 0. Only use when 0 is a meaningful value (e.g., no sales = 0).",
}

def _tip(key):
    return _TIPS.get(key, "")


def _preview_actions(df, actions):
    """
    Simulate all checked actions on a copy of df.
    Returns (preview_df, list_of_change_summaries).
    """
    import numpy as _np
    preview = df.copy()
    summaries = []

    if actions.get("drop_empty"):
        empty = [c for c in preview.columns if preview[c].isna().all()]
        if empty:
            preview, _ = drop_empty_columns(preview)
            label = ", ".join(empty[:4]) + ("..." if len(empty) > 4 else "")
            summaries.append(f"Drop {len(empty)} fully-empty column(s): {label}")
        else:
            summaries.append("Drop empty columns -- none found, no change")

    if actions.get("drop_high_missing"):
        thresh = actions["drop_hm_thresh"]
        pcts = preview.isna().mean() * 100
        cols = pcts[pcts >= thresh].index.tolist()
        if cols:
            preview, _ = drop_high_missing_cols(preview, float(thresh))
            label = ", ".join(cols[:4]) + ("..." if len(cols) > 4 else "")
            summaries.append(f"Drop >={thresh}% missing: {len(cols)} column(s) -- {label}")
        else:
            summaries.append(f"Drop >={thresh}% missing -- no columns qualify, no change")

    if actions.get("remove_dupes"):
        n_dup = int(preview.duplicated().sum())
        if n_dup:
            preview, _ = drop_duplicates(preview, keep="first")
            summaries.append(f"Remove duplicates -- {n_dup} row(s) removed, {len(preview):,} rows remain")
        else:
            summaries.append("Remove duplicates -- none found, no change")

    if actions.get("fill_missing"):
        strat = actions["fill_strat"]
        if strat != "Leave as NaN":
            cols_sel = actions.get("fill_cols") or preview.select_dtypes(include=_np.number).columns.tolist()
            total_miss_before = int(preview[cols_sel].isna().sum().sum()) if cols_sel else 0
            if total_miss_before:
                preview, _ = fill_missing(preview, strat, cols_sel)
                total_miss_after = int(preview[cols_sel].isna().sum().sum()) if cols_sel else 0
                filled = total_miss_before - total_miss_after
                per_col = [(c, int(df[c].isna().sum())) for c in cols_sel if c in df.columns and df[c].isna().any()][:4]
                per_col_str = "  |  ".join(f"{c}: {n}" for c, n in per_col)
                summaries.append(f"Fill missing ({strat}) -- {filled} value(s) filled across {len(cols_sel)} col(s)\n   {per_col_str}")
            else:
                summaries.append(f"Fill missing ({strat}) -- no missing values in selected columns")

    if actions.get("handle_outliers"):
        method = actions["out_method"]
        out_action = actions["out_action"]
        outliers = detect_outliers(preview, method=method)
        total_out = sum(v["n_outliers"] for v in outliers.values())
        if outliers:
            preview, _ = handle_outliers(preview, list(outliers.keys()), out_action, method=method)
            cols_str = ", ".join(list(outliers.keys())[:4]) + ("..." if len(outliers) > 4 else "")
            summaries.append(f"Outliers ({method.upper()}) -> {out_action} -- {total_out:,} outlier(s) in {len(outliers)} col(s): {cols_str}")
        else:
            summaries.append(f"Outliers ({method.upper()}) -- none detected, no change")

    if actions.get("std_names"):
        orig_cols = set(preview.columns)
        preview, _ = standardise_column_names(preview)
        renamed = {o: n for o, n in zip(df.columns, preview.columns) if o != n}
        if renamed:
            items = list(renamed.items())[:3]
            summaries.append("Standardise names -- " + str(len(renamed)) + " renamed: " +
                             ", ".join(f'"{o}"->"{n}"' for o, n in items) +
                             ("..." if len(renamed) > 3 else ""))
        else:
            summaries.append("Standardise names -- no financial aliases matched")

    return preview, summaries


def _collect_actions(key_prefix, df):
    """Read all checkbox/widget states for this file, return an actions dict."""
    return {
        "drop_empty":       get_state(f"{key_prefix}_cb_empty")    or False,
        "drop_high_missing":get_state(f"{key_prefix}_cb_hm")       or False,
        "drop_hm_thresh":   get_state(f"{key_prefix}_hm_thresh")   or 70,
        "remove_dupes":     get_state(f"{key_prefix}_cb_dupes")    or False,
        "fill_missing":     get_state(f"{key_prefix}_cb_fill")     or False,
        "fill_strat":       get_state(f"{key_prefix}_fill_strat")  or "median",
        "fill_cols":        get_state(f"{key_prefix}_fill_cols")   or [],
        "handle_outliers":  get_state(f"{key_prefix}_cb_out")      or False,
        "out_method":       get_state(f"{key_prefix}_out_method")  or "iqr",
        "out_action":       get_state(f"{key_prefix}_out_action")  or "flag",
        "std_names":        get_state(f"{key_prefix}_cb_std")      or False,
    }


def render_expert_upload():
    """Expert Step 20: one-file-at-a-time with checkboxes + Apply + before/after."""
    step_header("E·1", "PER-FILE CLEANING",
                "Select and apply cleaning actions to each file individually")

    entries = get_state("_expert_entries")
    if not entries:
        info_box("No files loaded. Go back to Import.", "warn")
        if st.button("Back to Import"):
            set_state("step", 1); st.rerun()
        return

    import numpy as _np

    n_files    = len(entries)
    cur_i      = get_state("_expert_cur_file") or 0
    cur_i      = max(0, min(cur_i, n_files - 1))
    entry      = entries[cur_i]
    df         = entry["df"]
    key_prefix = f"ef_{cur_i}"
    done_count = sum(1 for e in entries if e["log"])

    # ── File header bar ───────────────────────────────────────────────────────
    dots_html = "".join(
        f'<span title="{entries[j]["name"]}" style="cursor:default;'
        f'font-size:{"18" if j==cur_i else "12"}px;'
        f'color:{"#4db8ff" if j==cur_i else "#00c8a8" if entries[j]["log"] else "#1a3347"}">'
        f'{"&#9670;" if j==cur_i else "&#9679;" if entries[j]["log"] else "&#9675;"}</span>'
        for j in range(n_files)
    )
    st.markdown(
        f'<div style="background:linear-gradient(135deg,#0a1f2e,#071420);border:1px solid #1a3347;'
        f'border-radius:8px;padding:14px 20px;margin-bottom:16px;display:flex;align-items:center;gap:16px;flex-wrap:wrap">'
        f'<div style="flex:1">'
        f'<div style="font-family:IBM Plex Mono;font-size:10px;color:#4db8ff;letter-spacing:.12em;margin-bottom:3px">EXPERT MODE</div>'
        f'<div style="font-family:IBM Plex Mono;font-size:16px;font-weight:700;color:#e8f4fb">'
        f'File {cur_i+1} / {n_files} &ndash; <span style="color:#4db8ff">{entry["name"]}</span></div>'
        f'<div style="font-size:11px;color:#4a7088;margin-top:2px">'
        f'{entry["rows"]:,} rows &middot; {entry["cols"]} cols &middot; {done_count}/{n_files} files cleaned</div>'
        f'</div>'
        f'<div style="display:flex;gap:5px;align-items:center">{dots_html}</div>'
        f'</div>',
        unsafe_allow_html=True)

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab_clean, tab_preview, tab_stats = st.tabs(["🧹 Clean", "👁 Preview", "📊 Stats"])

    with tab_preview:
        st.dataframe(df.head(12), use_container_width=True, height=220)

    with tab_stats:
        miss_total = int(df.isna().sum().sum())
        n_dup_stat = int(df.duplicated().sum())
        s1, s2, s3, s4 = st.columns(4)
        with s1: metric_tile("ROWS",       f"{len(df):,}")
        with s2: metric_tile("COLS",       str(len(df.columns)))
        with s3: metric_tile("MISSING",    f"{miss_total:,}",    "#ff6060" if miss_total else "#00c8a8")
        with s4: metric_tile("DUPLICATES", f"{n_dup_stat:,}",    "#ff6060" if n_dup_stat else "#00c8a8")

        miss_cols_stat = sorted(
            [(c, round(df[c].isna().mean()*100, 1)) for c in df.columns if df[c].isna().any()],
            key=lambda x: -x[1])
        if miss_cols_stat:
            st.markdown('<div style="font-size:11px;color:#4a7088;margin:10px 0 6px">Missing values by column (top 10):</div>', unsafe_allow_html=True)
            for col, pct in miss_cols_stat[:10]:
                bar_c = "#ff6060" if pct > 30 else "#f0a020" if pct > 5 else "#4db8ff"
                st.markdown(
                    f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">'
                    f'<div style="font-size:11px;color:#c9d8e3;min-width:160px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="{col}">{col}</div>'
                    f'<div style="flex:1;background:#0d1e2c;height:8px;border-radius:2px">'
                    f'<div style="width:{pct}%;background:{bar_c};height:100%;border-radius:2px"></div></div>'
                    f'<div style="font-size:11px;color:{bar_c};min-width:42px;text-align:right">{pct}%</div></div>',
                    unsafe_allow_html=True)

    with tab_clean:

        # ── Section header ────────────────────────────────────────────────────
        st.markdown(
            '<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;'
            'letter-spacing:.12em;margin-bottom:12px">SELECT CLEANING ACTIONS</div>',
            unsafe_allow_html=True)

        n_dup   = int(df.duplicated().sum())
        n_empty = sum(1 for c in df.columns if df[c].isna().all())
        n_miss  = int(df.isna().sum().sum())
        num_c   = df.select_dtypes(include="number").columns.tolist()

        # ── 1. Drop empty columns ─────────────────────────────────────────────
        cb_empty = st.checkbox(
            f"Drop fully-empty columns" + (f" ({n_empty} detected)" if n_empty else " (none detected)"),
            key=f"{key_prefix}_cb_empty",
            disabled=(n_empty == 0),
            help="Removes columns where every single value is missing (NaN). Safe to always apply.")

        # ── 2. Drop high-missing columns ──────────────────────────────────────
        cb_hm = st.checkbox(
            "Drop columns above missing threshold",
            key=f"{key_prefix}_cb_hm",
            help="Drops any column where the percentage of missing values exceeds the threshold you set.")
        if cb_hm:
            hm_thresh = st.slider("Drop columns with more than X% missing",
                                   10, 100, 70, key=f"{key_prefix}_hm_thresh",
                                   help="Columns at or above this missing % will be dropped.")
            pcts_hm = df.isna().mean() * 100
            would_drop_hm = pcts_hm[pcts_hm >= hm_thresh].index.tolist()
            label_hm = ", ".join(would_drop_hm[:4]) + ("..." if len(would_drop_hm) > 4 else "")
            if would_drop_hm:
                st.caption(f"  Would drop {len(would_drop_hm)} column(s): {label_hm}")
            else:
                st.caption(f"  No columns exceed {hm_thresh}% missing.")

        # ── 3. Remove duplicates ──────────────────────────────────────────────
        cb_dupes = st.checkbox(
            f"Remove duplicate rows" + (f" ({n_dup:,} detected)" if n_dup else " (none detected)"),
            key=f"{key_prefix}_cb_dupes",
            disabled=(n_dup == 0),
            help="Keeps the first occurrence of each duplicate row and removes the rest.")

        # ── 4. Fill missing values ────────────────────────────────────────────
        cb_fill = st.checkbox(
            f"Fill missing values" + (f" ({n_miss:,} total)" if n_miss else " (none)"),
            key=f"{key_prefix}_cb_fill",
            disabled=(n_miss == 0 or not num_c),
            help="Replace NaN values in numeric columns using the chosen strategy.")
        if cb_fill:
            fc1, fc2 = st.columns(2)
            with fc1:
                strat_labels = {
                    "mean":         "Mean -- arithmetic average",
                    "median":       "Median -- middle value (robust to outliers)",
                    "mode":         "Mode -- most frequent value",
                    "zero":         "Zero -- fill with 0",
                    "Leave as NaN": "Leave as NaN (no fill)",
                }
                fill_strat = st.selectbox(
                    "Strategy",
                    list(strat_labels.keys()),
                    key=f"{key_prefix}_fill_strat",
                    format_func=strat_labels.get)
            with fc2:
                fill_cols_sel = st.multiselect(
                    "Columns (blank = all numeric)",
                    num_c, key=f"{key_prefix}_fill_cols",
                    help="Leave blank to apply to all numeric columns, or pick specific ones.")
            if fill_strat in _TIPS:
                st.markdown(
                    f'<div style="font-size:11px;color:#4a7088;padding:4px 0 2px">'
                    f'<strong style="color:#7ab8d4">Tip:</strong> {_tip(fill_strat)}</div>',
                    unsafe_allow_html=True)

        # ── 5. Outlier handling ───────────────────────────────────────────────
        cb_out = st.checkbox(
            "Detect and handle outliers",
            key=f"{key_prefix}_cb_out",
            disabled=(not num_c),
            help="Statistically identifies values that are unusually high or low compared to the rest of the column.")
        if cb_out:
            oc1, oc2 = st.columns(2)
            with oc1:
                out_method = st.radio(
                    "Detection method",
                    ["iqr", "zscore"], horizontal=True,
                    format_func=lambda x: "IQR" if x == "iqr" else "Z-Score",
                    key=f"{key_prefix}_out_method",
                    help="IQR = quartile-based, Z-Score = mean-based.")
                st.markdown(
                    f'<div style="font-size:11px;color:#4a7088;padding:2px 0">'
                    f'<strong style="color:#7ab8d4">Tip:</strong> {_tip(out_method)}</div>',
                    unsafe_allow_html=True)
            with oc2:
                action_labels = {
                    "flag": "Flag -- add marker column",
                    "cap":  "Cap -- clamp to boundary",
                    "drop": "Drop -- remove outlier rows",
                }
                out_action = st.radio(
                    "Action",
                    ["flag", "cap", "drop"], horizontal=True,
                    format_func=action_labels.get,
                    key=f"{key_prefix}_out_action")
                st.markdown(
                    f'<div style="font-size:11px;color:#4a7088;padding:2px 0">'
                    f'<strong style="color:#7ab8d4">Tip:</strong> {_tip(out_action)}</div>',
                    unsafe_allow_html=True)

        # ── 6. Standardise column names ───────────────────────────────────────
        cb_std = st.checkbox(
            "Standardise financial column names",
            key=f"{key_prefix}_cb_std",
            help="Renames common financial aliases to canonical English -- e.g. 'CA'->Revenue, 'dette nette'->Net Debt, 'Ventes'->Revenue.")

        # ─────────────────────────────────────────────────────────────────────
        actions    = _collect_actions(key_prefix, df)
        n_checked  = sum([actions["drop_empty"], actions["drop_high_missing"],
                          actions["remove_dupes"], actions["fill_missing"],
                          actions["handle_outliers"], actions["std_names"]])
        any_checked = n_checked > 0

        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        st.markdown("---")

        # ── Before/After Preview panel ────────────────────────────────────────
        if any_checked:
            with st.expander("Preview changes before applying", expanded=True):
                try:
                    preview_df, summaries = _preview_actions(df, actions)

                    st.markdown(
                        '<div style="font-family:IBM Plex Mono;font-size:10px;color:#4db8ff;'
                        'letter-spacing:.12em;margin-bottom:8px">WHAT WILL HAPPEN</div>',
                        unsafe_allow_html=True)
                    for s in summaries:
                        st.markdown(
                            f'<div style="font-size:12px;padding:5px 0;border-bottom:1px solid #0d1e2c;color:#c9d8e3">'
                            f'&bull; {s}</div>',
                            unsafe_allow_html=True)

                    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
                    pc1, pc2, pc3, pc4 = st.columns(4)
                    with pc1: metric_tile("ROWS BEFORE", f"{len(df):,}")
                    with pc2: metric_tile("ROWS AFTER",  f"{len(preview_df):,}",
                                           "#00c8a8" if len(preview_df) <= len(df) else "#ff6060")
                    with pc3: metric_tile("COLS BEFORE", str(len(df.columns)))
                    with pc4: metric_tile("COLS AFTER",  str(len(preview_df.columns)),
                                           "#00c8a8" if len(preview_df.columns) <= len(df.columns) else "#ff6060")

                    st.markdown('<div style="font-size:11px;color:#4a7088;margin:8px 0 4px">First 5 rows after cleaning:</div>', unsafe_allow_html=True)
                    st.dataframe(preview_df.head(5), use_container_width=True, height=160)

                except Exception as prev_err:
                    st.warning(f"Preview unavailable: {prev_err}")

        # ── Action buttons ────────────────────────────────────────────────────
        btn1, btn2, btn3 = st.columns([3, 3, 3])

        # ── Apply button row ──────────────────────────────────────────────────
        apply_label = (
            f"✅  Apply {n_checked} selected action(s) to this file"
            if any_checked else "No actions selected — tick a checkbox above"
        )
        if st.button(apply_label,
                     type="primary" if any_checked else "secondary",
                     disabled=not any_checked,
                     key=f"{key_prefix}_apply"):
            new_df  = df.copy()
            new_log = list(entry["log"])   # preserve previous ops on this file

            if actions["drop_empty"]:
                empty = [c for c in new_df.columns if new_df[c].isna().all()]
                if empty:
                    new_df, log_e = drop_empty_columns(new_df)
                    new_log.append(log_e)

            if actions["drop_high_missing"]:
                new_df, log_e = drop_high_missing_cols(new_df, float(actions["drop_hm_thresh"]))
                new_log.append(log_e)

            if actions["remove_dupes"]:
                if int(new_df.duplicated().sum()):
                    new_df, log_e = drop_duplicates(new_df, keep="first")
                    new_log.append(log_e)

            if actions["fill_missing"] and actions["fill_strat"] != "Leave as NaN":
                cols_to_fill = actions["fill_cols"] or new_df.select_dtypes(include=_np.number).columns.tolist()
                if cols_to_fill:
                    new_df, log_e = fill_missing(new_df, actions["fill_strat"], cols_to_fill)
                    new_log.append(log_e)

            if actions["handle_outliers"]:
                outliers_found = detect_outliers(new_df, method=actions["out_method"])
                if outliers_found:
                    new_df, log_e = handle_outliers(
                        new_df, list(outliers_found.keys()),
                        actions["out_action"], method=actions["out_method"])
                    new_log.append(log_e)

            if actions["std_names"]:
                new_df, log_e = standardise_column_names(new_df)
                new_log.append(log_e)

            entries[cur_i]["df"]   = new_df
            entries[cur_i]["rows"] = len(new_df)
            entries[cur_i]["cols"] = len(new_df.columns)
            entries[cur_i]["log"]  = new_log
            set_state("_expert_entries", entries)
            # Stay on same file — user navigates manually
            st.rerun()

        # ── Summary card — always shown when ops exist for this file ──────────
        if entry["log"]:
            ops = entry["log"]
            st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
            rows_html = ""
            for op in ops:
                dr = f" <span style='color:#ff6060'>−{op['rows_delta']} rows</span>" if op["rows_delta"] else ""
                dc = f" <span style='color:#f0a020'>−{op['cols_delta']} cols</span>" if op["cols_delta"] else ""
                raw_row = entry["raw_df"]
                raw_miss = int(raw_row.isna().sum().sum()) if "raw_df" in entry else 0
                rows_html += (
                    f'<div style="font-size:12px;padding:5px 0;border-bottom:1px solid #0d2215;color:#c9d8e3">'
                    f'<span style="font-family:IBM Plex Mono;color:#00c8a8;font-size:10px">#{ops.index(op)+1}</span>  '
                    f'<strong style="color:#e8f4fb">{op["title"]}</strong>  '
                    f'<span style="color:#4a7088">{op["detail"][:75]}</span>{dr}{dc}</div>'
                )
            # Compute cumulative before/after
            raw_df = entry.get("raw_df", df)
            cum_rows_removed = len(raw_df) - len(entry["df"])
            cum_cols_removed = len(raw_df.columns) - len(entry["df"].columns)
            cum_miss_filled  = int(raw_df.isna().sum().sum()) - int(entry["df"].isna().sum().sum())
            st.markdown(
                f'<div style="background:rgba(0,200,168,.06);border:1px solid rgba(0,200,168,.3);'
                f'border-radius:6px;padding:14px 18px;margin-top:2px">'
                f'<div style="font-family:IBM Plex Mono;font-size:10px;color:#00c8a8;'
                f'letter-spacing:.12em;margin-bottom:10px">'
                f'✓ FILE {cur_i+1} CLEANED — {len(ops)} OPERATION(S)</div>'
                f'<div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:10px">'
                + (f'<span style="font-size:11px"><span style="color:#4a7088">Rows removed </span><span style="color:#ff6060;font-family:IBM Plex Mono">{cum_rows_removed:+d}</span></span>' if cum_rows_removed else "")
                + (f'<span style="font-size:11px"><span style="color:#4a7088">Cols removed </span><span style="color:#f0a020;font-family:IBM Plex Mono">{cum_cols_removed:+d}</span></span>' if cum_cols_removed else "")
                + (f'<span style="font-size:11px"><span style="color:#4a7088">Missing filled </span><span style="color:#00c8a8;font-family:IBM Plex Mono">{cum_miss_filled:+d}</span></span>' if cum_miss_filled else "")
                + f'</div>{rows_html}'
                + '<div style="font-size:11px;color:#4a7088;margin-top:8px">'
                  'You can apply more actions above, or navigate to the next file.</div>'
                + '</div>',
                unsafe_allow_html=True)

        # ── Revert button (only when ops exist) ──────────────────────────────
        if entry["log"]:
            if st.button("↺  Revert — undo all changes to this file",
                          key=f"{key_prefix}_revert"):
                raw = entry.get("raw_df")
                if raw is not None:
                    entries[cur_i]["df"]   = raw.copy()
                    entries[cur_i]["rows"] = len(raw)
                    entries[cur_i]["cols"] = len(raw.columns)
                    entries[cur_i]["log"]  = []
                    set_state("_expert_entries", entries)
                    st.rerun()

        # ── File navigation row ───────────────────────────────────────────────
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        nav1, nav2, nav3 = st.columns([3, 4, 3])
        with nav1:
            if cur_i > 0:
                if st.button(f"← File {cur_i} / {n_files}", key=f"{key_prefix}_prev"):
                    set_state("_expert_cur_file", cur_i - 1); st.rerun()
        with nav2:
            if st.button("← Back to Import", key=f"{key_prefix}_back"):
                set_state("step", 1); st.rerun()
        with nav3:
            if cur_i + 1 < n_files:
                if st.button(f"File {cur_i+2} / {n_files} →", key=f"{key_prefix}_next",
                             type="primary"):
                    set_state("_expert_cur_file", cur_i + 1); st.rerun()

        # ── Proceed to merge (always visible once on last file or all cleaned) ─
        n_cleaned = sum(1 for e in entries if e["log"])
        ready_msg = (f"<strong>{n_cleaned}/{n_files} files cleaned.</strong> "
                     + ("All files ready — proceed when you are." if n_cleaned == n_files
                        else "Uncleaned files will be merged as-is."))
        st.markdown("---")
        info_box(ready_msg, "success" if n_cleaned == n_files else "info")
        if st.button("→ Column Preview & Merge", type="primary", key="expert_to_merge"):
            set_state("_expert_highest", 21)
            set_state("step", 21); st.rerun()

        # ── All files done -- proceed button ──────────────────────────────────
        # (kept empty — merged into the block above)

        # ── Jump to any file ──────────────────────────────────────────────────
        with st.expander("📋  All files overview — jump to any file"):
            for j, e in enumerate(entries):
                n_ops  = len(e["log"])
                status = "✅ Cleaned" if e["log"] else ("◆ Current" if j == cur_i else "○ Pending")
                color  = "#00c8a8" if e["log"] else ("#4db8ff" if j == cur_i else "#4a7088")
                ja, jb = st.columns([7, 2])
                with ja:
                    ops_txt = f" · {n_ops} op(s)" if n_ops else ""
                    st.markdown(
                        f'<div style="font-size:12px;color:{color};padding:3px 0">'
                        f'<span style="font-family:IBM Plex Mono">{j+1:02d}.</span> '
                        f'<strong>{e["name"]}</strong>'
                        f'<span style="color:#4a7088;font-size:10px"> {e["rows"]:,} rows · {e["cols"]} cols{ops_txt}</span>  '
                        f'<span style="font-size:10px;color:{color}">{status}</span></div>',
                        unsafe_allow_html=True)
                with jb:
                    if j != cur_i:
                        if st.button(f"Go →", key=f"jump_{j}", use_container_width=True):
                            set_state("_expert_cur_file", j); st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# EXPERT MODE — Step 21: Column preview & merge decision
# ══════════════════════════════════════════════════════════════════════════════

def render_expert_merge():
    """Expert Step 21 — cross-file column analysis + merge decision."""
    step_header("E·2", "EXPERT — COLUMN PREVIEW & MERGE",
                "Review common columns and choose your merge strategy")

    entries = get_state("_expert_entries")
    if not entries:
        if st.button("← Back"): set_state("step", 20); st.rerun()
        return

    selected = [e for e in entries if e["selected"]]

    # ── Column presence table ─────────────────────────────────────────────────
    st.markdown('<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.12em;margin-bottom:10px">COLUMN PRESENCE ACROSS FILES</div>', unsafe_allow_html=True)

    all_col_counts = _all_columns(selected)
    n_files = len(selected)
    # Sort: universal columns first, then partial
    universal = sorted([c for c, cnt in all_col_counts.items() if cnt == n_files])
    partial   = sorted([c for c, cnt in all_col_counts.items() if cnt < n_files])

    # Build presence table
    rows = []
    for col in universal + partial:
        row = {"Column": col, "In all files": "✅" if col in universal else ""}
        for e in selected:
            row[e["name"][:18]] = "✓" if col in e["df"].columns else "—"
        rows.append(row)

    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, height=min(400, 40 + len(rows)*35))

    info_box(
        f"<strong>{len(universal)} universal column(s)</strong> present in all {n_files} files — "
        f"good candidates for join keys. "
        f"<strong>{len(partial)} partial column(s)</strong> appear in only some files.",
        "info")

    # ── Merge strategy ────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown('<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.12em;margin-bottom:10px">MERGE STRATEGY</div>', unsafe_allow_html=True)

    merge_mode = st.radio(
        "merge_strategy",
        ["concat","join"],
        format_func=lambda x:
            "🔹 Vertical Concatenate — stack rows (same structure per file)"
            if x=="concat" else
            "🔸 Horizontal Join — combine columns using a common key",
        label_visibility="collapsed",
        key="expert_merge_mode")

    concat_axis = 0
    join_keys   = []
    join_type   = "left"

    if merge_mode == "concat":
        concat_axis = 0 if st.radio(
            "concat_axis",
            ["Vertical (stack rows)", "Horizontal (stack columns)"],
            label_visibility="collapsed", key="expert_concat_axis"
        ) == "Vertical (stack rows)" else 1

    else:  # join
        join_keys = st.multiselect(
            "Join key column(s) — must exist in all files",
            universal,
            default=universal[:1],
            key="expert_join_keys")
        join_type = st.radio("Join type", ["inner","left","right","outer"],
                              horizontal=True, key="expert_join_type",
                              label_visibility="collapsed")
        if not join_keys:
            info_box("Select at least one join key to continue.", "warn")

    # ── Live preview ──────────────────────────────────────────────────────────
    ready = (merge_mode == "concat") or (merge_mode == "join" and len(join_keys) >= 1)
    if ready:
        try:
            if merge_mode == "concat":
                prev = _execute_concat(selected, concat_axis)
            else:
                prev = _execute_join(selected, join_keys, join_type, "suffixes")
            st.markdown("---")
            st.markdown(
                f'<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.1em;margin-bottom:6px">'
                f'MERGE PREVIEW — {len(prev):,} rows × {len(prev.columns)} cols</div>',
                unsafe_allow_html=True)
            st.dataframe(prev.head(8), use_container_width=True, height=180)
        except Exception as ex:
            info_box(f"⚠ Preview error: {ex}", "warn")

    # Save merge config for step 22
    set_state("_expert_merge_config", {
        "mode": merge_mode, "concat_axis": concat_axis,
        "join_keys": join_keys, "join_type": join_type,
    })

    bc1, _, bc2 = st.columns([3, 6, 3])
    with bc1:
        if st.button("← Back to File Cleaning"):
            set_state("step", 20); st.rerun()
    with bc2:
        if st.button("→ Apply Merge & Export", type="primary", disabled=not ready):
            # Execute the real merge
            try:
                if merge_mode == "concat":
                    merged = _execute_concat(selected, concat_axis)
                else:
                    merged = _execute_join(selected, join_keys, join_type, "suffixes")
                merged, renames = deduplicate_columns(merged)
                set_state("_expert_merged_df", merged)
                set_state("_expert_merge_renames", renames)
                set_state("_expert_highest", 22)
                set_state("step", 22); st.rerun()
            except Exception as ex:
                st.error(f"Merge failed: {ex}")


# ══════════════════════════════════════════════════════════════════════════════
# EXPERT MODE — Step 22: Final merged view + export
# ══════════════════════════════════════════════════════════════════════════════

def render_expert_export():
    """Expert Step 22 — final merged result + export options."""
    step_header("E·3", "EXPERT — FINAL MERGE & EXPORT",
                "Review the merged result and export your files")

    entries      = get_state("_expert_entries") or []
    merged_df    = get_state("_expert_merged_df")
    renames      = get_state("_expert_merge_renames") or []
    merge_config = get_state("_expert_merge_config") or {}

    if merged_df is None:
        info_box("No merged data found. Go back to the merge step.", "warn")
        if st.button("← Back"): set_state("step", 21); st.rerun()
        return

    if renames:
        info_box(f"⚠ <strong>{len(renames)} column(s) auto-renamed</strong> during merge to avoid duplicates: "
                 + ", ".join(renames[:6]) + ("…" if len(renames) > 6 else ""), "warn")

    # ── Merged dataset summary ────────────────────────────────────────────────
    st.markdown('<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.12em;margin-bottom:10px">MERGED DATASET</div>', unsafe_allow_html=True)
    m1, m2, m3, m4 = st.columns(4)
    with m1: metric_tile("ROWS",          f"{len(merged_df):,}")
    with m2: metric_tile("COLS",          str(len(merged_df.columns)))
    with m3: metric_tile("MISSING",       f"{int(merged_df.isna().sum().sum()):,}",
                          "#ff6060" if merged_df.isna().any().any() else "#00c8a8")
    with m4: metric_tile("FILES MERGED",  str(len(entries)))

    st.dataframe(merged_df.head(10), use_container_width=True, height=220)

    # ── Export options ────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown('<div style="font-family:IBM Plex Mono;font-size:10px;color:#2a4a5e;letter-spacing:.12em;margin-bottom:12px">EXPORT</div>', unsafe_allow_html=True)

    ex1, ex2, ex3 = st.columns(3)

    with ex1:
        st.markdown('<div class="dp-card">', unsafe_allow_html=True)
        st.markdown("**📄 Merged file**")
        st.caption("The combined and merged dataset.")
        csv_merged = to_csv_bytes(merged_df)
        st.download_button("⬇ Download merged.csv",
                            data=csv_merged,
                            file_name="expert_merged.csv",
                            mime="text/csv",
                            width="stretch")
        st.markdown(f'<div style="font-size:11px;color:#2a4a5e;margin-top:6px">{len(merged_df):,} rows · {len(merged_df.columns)} cols · {len(csv_merged)/1024:.1f} KB</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with ex2:
        st.markdown('<div class="dp-card">', unsafe_allow_html=True)
        st.markdown("**📂 Individual cleaned files**")
        st.caption("Each file as cleaned in Step 1.")
        for e in entries:
            csv_e = to_csv_bytes(e["df"])
            clean_name = e["name"].rsplit(".",1)[0] + "_cleaned.csv"
            st.download_button(
                f"⬇ {e['name'][:28]}",
                data=csv_e,
                file_name=clean_name,
                mime="text/csv",
                key=f"dl_expert_{e['name']}",
                width="stretch")
        st.markdown("</div>", unsafe_allow_html=True)

    with ex3:
        st.markdown('<div class="dp-card">', unsafe_allow_html=True)
        st.markdown("**📋 Cleaning log**")
        st.caption("All operations applied per file.")
        all_logs = []
        for e in entries:
            for op in e["log"]:
                all_logs.append({**op, "file": e["name"]})
        if all_logs:
            log_json = json.dumps(all_logs, indent=2, default=str)
            st.download_button("⬇ Download expert_log.json",
                                data=log_json.encode("utf-8"),
                                file_name="expert_log.json",
                                mime="application/json",
                                width="stretch")
            st.caption(f"{len(all_logs)} total operation(s) across {len(entries)} file(s)")
        else:
            st.caption("No cleaning operations were applied.")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("---")
    bc1, _, bc2 = st.columns([3, 6, 3])
    with bc1:
        if st.button("← Back to Merge"):
            set_state("step", 21); st.rerun()
    with bc2:
        if st.button("⟳ Start Over", type="primary"):
            for key in ["_expert_entries","_expert_merged_df","_expert_merge_renames",
                        "_expert_merge_config","_expert_highest","step"]:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

import re  # needed by resolve_dupes helpers

def main():
    st.markdown(CSS, unsafe_allow_html=True)
    render_sidebar()
    step = get_state("step") or 1
    pipeline_bar(step)

    renames = get_state("_merge_renames")
    if renames and step == 5:
        info_box(f"⚠ <strong>Columns renamed after merge:</strong> {', '.join(renames[:10])}", "warn")
        set_state("_merge_renames", None)


    if   step == 1:  render_import()
    elif step == 2: render_file_review()
    elif step == 3:
        render_merge_settings()
    elif step == 4: render_resolve_dupes()
    elif step == 5:  render_profile()
    elif step == 6:  render_duplicates()
    elif step == 7:  render_missing()
    elif step == 8:  render_types()
    elif step == 9:  render_outliers()
    elif step == 10:  render_finance()
    elif step == 11:  render_standardise()
    elif step == 12:  render_report()
    # ── Expert mode ──────────────────────────────────────────────────────────
    elif step == 20: render_expert_upload()
    elif step == 21: render_expert_merge()
    elif step == 22: render_expert_export()


if __name__ == "__main__":
    main()
