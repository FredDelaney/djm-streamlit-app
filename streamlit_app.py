import streamlit as st
import pandas as pd
import numpy as np
import pytz
import re, requests, math
from datetime import datetime
from rapidfuzz import process as fuzz

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# -------------------- APP CONFIG & THEME --------------------
st.set_page_config(
    page_title="DJM — Scouting & Transfers",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Minimal modern theming via CSS
st.markdown("""
<style>
/* Global */
:root { --accent:#2f80ed; --bg:#0b0f1a; --card:#11172a; --muted:#9aa4b2; --good:#00d084; --warn:#f2c94c; --bad:#ff6b6b; }
body, .stApp { background: linear-gradient(180deg, #0b0f1a 0%, #0d1224 100%); color:#e6eefc; }

/* Headings */
h1, h2, h3 { letter-spacing:.2px; }

/* Cards */
.djm-card { background:var(--card); border-radius:16px; padding:18px 18px 14px; box-shadow: 0 10px 24px rgba(0,0,0,.25); border:1px solid rgba(255,255,255,.05); }
.djm-kpi { display:flex; gap:10px; align-items:baseline }
.djm-kpi .big { font-size:38px; font-weight:800; }
.djm-kpi .label { color:var(--muted); font-size:13px; text-transform:uppercase; letter-spacing:0.4px; }

/* Buttons */
.stButton>button { border-radius:12px; padding:8px 14px; font-weight:600; }

/* Inputs */
.stTextInput>div>div>input, .stSelectbox>div>div>select, .stNumberInput>div>div>input, .stFileUploader, .stMultiSelect>div>div { background:#0f1426; border:1px solid rgba(255,255,255,.08); border-radius:10px; }

/* Tables */
[data-testid="stDataFrame"] { border-radius:12px; overflow:hidden; border:1px solid rgba(255,255,255,.08); }
</style>
""", unsafe_allow_html=True)

# -------------------- SETTINGS --------------------
SHEET_NAME = st.secrets.get("sheet_name", "DJM_Input")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

PLAYERS_HEADERS = [
    "player_id","player_name","player_qid","dob","age","citizenships",
    "height_cm","positions","current_club","shirt_number",
    "tm_url","tm_id"
]

RATINGS_HEADERS = [
    "tm_id","player_name","position_group","age",
    "overall_now","overall_5yr","uncert_low","uncert_high",
    "minutes_90","availability","role_fit","market_signal",
    "updated_at"
]

RAW_MATCHES_HEADERS = [
    "tm_id","player_name","date","competition","opponent","minutes",
    "shots","xg","xa","key_passes","dribbles_won","progressive_passes",
    "progressive_carries","tackles_won","interceptions","aerials_won","passes",
    "passes_accurate","duels_won","touches","position"
]

# -------------------- GOOGLE SHEETS I/O --------------------
@st.cache_resource(show_spinner=False)
def connect_gsheet():
    creds_info = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME)

def get_or_create_ws(ss, name, headers=None):
    try:
        ws = ss.worksheet(name)
    except Exception:
        ws = ss.add_worksheet(title=name, rows=1000, cols=max(20, len(headers or [])))
        if headers:
            ws.update('A1', [headers])
    return ws

def read_tab(ss, tab):
    try:
        ws = ss.worksheet(tab)
    except Exception:
        return None
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    if df is None or df.empty:
        return None
    df = df.dropna(how="all").reset_index(drop=True)
    # numeric coercion for known fields
    for c in df.columns:
        if c in {"p_move","p_make_it","contract_months_left","buyer_need_index",
                 "role_fit","media_rumor_score","scarcity_index","injury_days_pct",
                 "availability_pct","adj_minutes","role_percentile"}:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def read_sheet_as_df(ss, name):
    try:
        ws = ss.worksheet(name)
    except Exception:
        return pd.DataFrame()
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    if df is None:
        return pd.DataFrame()
    return df.dropna(how="all")

def write_df_to_sheet(ss, name, df):
    ws = get_or_create_ws(ss, name, headers=list(df.columns))
    ws.clear()
    set_with_dataframe(ws, df, row=1, col=1, include_index=False, include_column_header=True)

# -------------------- UTILS --------------------
def percent(x):
    try:
        return f"{100*float(x):.1f}%"
    except Exception:
        return ""

def parse_tm_id(url_or_id: str):
    if not url_or_id:
        return None
    s = str(url_or_id).strip()
    m = re.search(r"/spieler/(\d+)", s)
    if m: return m.group(1)
    m = re.fullmatch(r"\d{3,}", s)
    if m: return m.group(0)
    nums = re.findall(r"\d{3,}", s)
    return nums[-1] if nums else None

def fuzzy_pick(options, query, limit=5, score_cutoff=65):
    if not options: return []
    res = fuzz.extract(query, options, limit=limit, score_cutoff=score_cutoff)
    return [r[0] for r in res]

def now_ts():
    tz = pytz.timezone("Europe/Rome")
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M %Z")

# -------------------- DATA LAYER: UPSERTS --------------------
def upsert_players(ss, df_in: pd.DataFrame) -> tuple[int,int]:
    _ = get_or_create_ws(ss, "players", headers=PLAYERS_HEADERS)
    existing = read_sheet_as_df(ss, "players")
    if existing.empty:
        existing = pd.DataFrame(columns=PLAYERS_HEADERS)

    df = df_in.copy()
    for c in PLAYERS_HEADERS:
        if c not in df.columns:
            df[c] = pd.NA

    df["tm_id"] = df.apply(
        lambda r: r["tm_id"] if pd.notna(r["tm_id"]) and str(r["tm_id"]).strip() != ""
        else parse_tm_id(str(r.get("tm_url",""))),
        axis=1
    )
    for c in ["player_id","player_name","player_qid","dob","tm_url","tm_id"]:
        df[c] = df[c].astype(str).str.strip().replace({"None":"","nan":""})

    existing = existing.reindex(columns=PLAYERS_HEADERS).fillna("")
    ex_by_tm = {str(t): i for i,t in enumerate(existing["tm_id"].astype(str)) if t}
    ex_by_name_dob = {(str(n).lower(),str(d)): i
                      for i,(n,d) in enumerate(zip(existing["player_name"].astype(str),
                                                   existing["dob"].astype(str))) if n}

    inserts, updates = [], 0
    for _, r in df.iterrows():
        tm_id = str(r["tm_id"]) if pd.notna(r["tm_id"]) else ""
        idx = ex_by_tm.get(tm_id) if tm_id else None
        if idx is None:
            idx = ex_by_name_dob.get((str(r["player_name"]).lower().strip(), str(r["dob"]).strip()))
        merged = {h: "" for h in PLAYERS_HEADERS}
        for h in PLAYERS_HEADERS:
            incoming = r[h] if h in r.index else ""
            if idx is None:
                merged[h] = "" if pd.isna(incoming) else str(incoming)
            else:
                prev = existing.iat[idx, existing.columns.get_loc(h)] if h in existing.columns else ""
                merged[h] = str(incoming) if (pd.notna(incoming) and str(incoming)!="") else str(prev)
        if idx is None: inserts.append(merged)
        else:
            for h in PLAYERS_HEADERS:
                existing.iat[idx, existing.columns.get_loc(h)] = merged[h]
            updates += 1

    if inserts:
        existing = pd.concat([existing, pd.DataFrame(inserts, columns=PLAYERS_HEADERS)], ignore_index=True)

    write_df_to_sheet(ss, "players", existing.reindex(columns=PLAYERS_HEADERS))
    return (len(inserts), updates)

def append_raw_matches(ss, df_rows: pd.DataFrame) -> int:
    ws = get_or_create_ws(ss, "raw_matches", headers=RAW_MATCHES_HEADERS)
    existing = read_sheet_as_df(ss, "raw_matches")
    if existing.empty:
        existing = pd.DataFrame(columns=RAW_MATCHES_HEADERS)
    # align
    df_rows = df_rows.reindex(columns=RAW_MATCHES_HEADERS)
    out = pd.concat([existing, df_rows], ignore_index=True)
    write_df_to_sheet(ss, "raw_matches", out)
    return len(df_rows)

# -------------------- FEATURE ENGINEERING --------------------
def safe_div(a,b):
    try:
        if pd.isna(a) or pd.isna(b) or b==0: return np.nan
        return float(a)/float(b)
    except Exception:
        return np.nan

def build_feature_store(ss) -> pd.DataFrame:
    """Aggregate raw_matches to per-player seasonal features."""
    raw = read_sheet_as_df(ss, "raw_matches")
    if raw.empty:
        return pd.DataFrame(columns=["tm_id","player_name","minutes",
                                     "xg","xa","shots","key_passes","prog_passes",
                                     "prog_carries","tackles","interceptions",
                                     "dribbles_won","aerials_won","passes","passes_acc"])
    # coerce
    num_cols = ["minutes","xg","xa","shots","key_passes","progressive_passes",
                "progressive_carries","tackles_won","interceptions","dribbles_won",
                "aerials_won","passes","passes_accurate","duels_won","touches"]
    for c in num_cols:
        if c in raw.columns:
            raw[c] = pd.to_numeric(raw[c], errors="coerce")

    g = raw.groupby(["tm_id","player_name"], dropna=False).agg({
        "minutes":"sum",
        "xg":"sum","xa":"sum","shots":"sum","key_passes":"sum",
        "progressive_passes":"sum","progressive_carries":"sum",
        "tackles_won":"sum","interceptions":"sum","dribbles_won":"sum",
        "aerials_won":"sum","passes":"sum","passes_accurate":"sum"
    }).reset_index()

    # per90s
    mins = g["minutes"].replace({0:np.nan})
    feats = pd.DataFrame({
        "tm_id": g["tm_id"], "player_name": g["player_name"],
        "minutes": g["minutes"],
        "xg_p90": g["xg"]/mins*90, "xa_p90": g["xa"]/mins*90, "shots_p90": g["shots"]/mins*90,
        "kp_p90": g["key_passes"]/mins*90,
        "prog_pass_p90": g["progressive_passes"]/mins*90,
        "prog_carry_p90": g["progressive_carries"]/mins*90,
        "tackles_p90": g["tackles_won"]/mins*90, "inter_p90": g["interceptions"]/mins*90,
        "dribbles_p90": g["dribbles_won"]/mins*90, "aerials_p90": g["aerials_won"]/mins*90,
        "pass_acc": safe_div(g["passes_accurate"], g["passes"])
    })
    return feats

# -------------------- SIMPLE, EXPLAINABLE RATING --------------------
DEFAULT_WEIGHTS = {
    "attack": 0.35,      # xG, xA, shots, key passes
    "progression": 0.25, # progressive passes/carries, dribbles
    "defence": 0.20,     # tackles/interceptions/aerials
    "passing": 0.20      # pass accuracy
}

def _norm01(s):
    if s.empty: return s
    lo, hi = np.nanpercentile(s, 5), np.nanpercentile(s, 95)
    return (s - lo) / (hi - lo + 1e-9)

def compute_ratings_from_features(feats: pd.DataFrame, age_lookup: pd.DataFrame|None=None):
    """Return ratings dataframe with overall_now & overall_5yr."""
    if feats.empty: return pd.DataFrame(columns=RATINGS_HEADERS)

    # Normalize each block
    att = _norm01(0.6*feats["xg_p90"].fillna(0) + 0.4*feats["xa_p90"].fillna(0) + 0.2*feats["shots_p90"].fillna(0) + 0.4*feats["kp_p90"].fillna(0))
    prog = _norm01(0.6*feats["prog_pass_p90"].fillna(0) + 0.4*feats["prog_carry_p90"].fillna(0) + 0.2*feats["dribbles_p90"].fillna(0))
    dfn = _norm01(0.6*feats["tackles_p90"].fillna(0) + 0.6*feats["inter_p90"].fillna(0) + 0.2*feats["aerials_p90"].fillna(0))
    pas = _norm01(feats["pass_acc"].fillna(0))

    overall01 = (
        DEFAULT_WEIGHTS["attack"]*att.fillna(0) +
        DEFAULT_WEIGHTS["progression"]*prog.fillna(0) +
        DEFAULT_WEIGHTS["defence"]*dfn.fillna(0) +
        DEFAULT_WEIGHTS["passing"]*pas.fillna(0)
    )

    # crude age curve (if we have age)
    if age_lookup is not None and not age_lookup.empty and "age" in age_lookup.columns:
        ages = feats.merge(age_lookup[["tm_id","age"]], on="tm_id", how="left")["age"]
        def age_mult(a):
            if pd.isna(a): return 1.0
            a=float(a)
            if a<=21: return 1.10
            if a<=24: return 1.05
            if a<=28: return 1.00
            if a<=31: return 0.97
            if a<=34: return 0.94
            return 0.90
        mult = ages.map(age_mult)
    else:
        mult = 1.0

    overall_now = (overall01.clip(0,1) * mult).clip(0,1) * 100.0
    # 5-year projection: young up, old down; shrink to mean if low mins
    minutes = feats["minutes"].fillna(0)
    min_mult = minutes.apply(lambda m: 0.7 if m < 900 else (0.85 if m<1800 else 1.0))
    growth = ages.map(lambda a: 1.10 if (pd.notna(a) and a<=22) else (1.04 if pd.notna(a) and a<=26 else (0.98 if pd.notna(a) and a>=30 else 1.00))) if ('ages' in locals()) else 1.03
    overall_5 = (overall_now/100.0 * growth * min_mult).clip(0,1) * 100.0

    out = pd.DataFrame({
        "tm_id": feats["tm_id"],
        "player_name": feats["player_name"],
        "position_group": "",  # filled if you store it
        "age": age_lookup.set_index("tm_id").reindex(feats["tm_id"])["age"].values if (age_lookup is not None and not age_lookup.empty and "tm_id" in age_lookup.columns) else np.nan,
        "overall_now": overall_now.round(1),
        "overall_5yr": overall_5.round(1),
        "uncert_low": (overall_now * 0.90).round(1),
        "uncert_high": (overall_now * 1.10).clip(0,100).round(1),
        "minutes_90": feats["minutes"].fillna(0).astype(float).round(0),
        "availability": np.nan,  # can join later from injuries
        "role_fit": np.nan,      # join later from role clustering
        "market_signal": np.nan, # join later from transfer tab
        "updated_at": now_ts()
    })
    return out

# -------------------- OPTIONAL: TM value (best-effort) --------------------
def fetch_tm_
