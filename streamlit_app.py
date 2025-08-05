# DJM — Scouting & Transfer Intelligence (Monolith V2.2 — improved)
# ----------------------------------------------------------------------
# V2.2 Changes:
# - Added notifications for missing dependencies (Plotly, scikit-learn).
# - Improved fuzzy search and filtering for player and club analysis.
# - Enhanced UI styling: consistent dark theme for charts, better labels, card layouts.
# Hotfix (2025-08-05):
# - Robust Google Sheets connector (sheet_url → sheet_id → sheet_name).
# - Fixed load_all_data argument (use ss instead of st.secrets).

import streamlit as st
import pandas as pd
import numpy as np
import pytz, re, requests, json
from datetime import datetime
from dateutil import parser as dtparser
from typing import List, Dict, Any, Optional, Tuple

# -------- Optional Dependencies (with graceful fallbacks) --------
# Fuzzy search
try:
    from rapidfuzz import process as _fuzz
    def fuzzy_pick(options: List[str], query: str, limit: int = 8, score_cutoff: int = 65) -> List[str]:
        if not options or not query:
            return []
        hits = _fuzz.extract(query, options, limit=limit, score_cutoff=score_cutoff)
        return [h[0] for h in hits]
except ImportError:
    import difflib
    def fuzzy_pick(options: List[str], query: str, limit: int = 8, score_cutoff: float = 0.0) -> List[str]:
        if not options or not query:
            return []
        # difflib.get_close_matches returns up to n matches with a similarity cutoff
        return difflib.get_close_matches(query, options, n=limit, cutoff=score_cutoff/100.0)

# Plotly charts
try:
    import plotly.graph_objects as go
    PLOTLY_OK = True
    def plot_radar_chart(stats: Dict[str, float], title: str) -> go.Figure:
        """Plot a radar chart for a player's core stats."""
        categories = list(stats.keys())
        values = list(stats.values())
        fig = go.Figure()
        fig.add_trace(go.Scatterpolar(
            r=values, theta=categories,
            fill='toself',
            line_color='#69E2FF',            # using accent color
            marker=dict(color='#69E2FF'),
            name='Score'
        ))
        fig.update_layout(
            polar=dict(
                radialaxis=dict(visible=True, range=[0, 100], color='#9AA4B2', gridcolor='rgba(255,255,255,0.1)'),
                angularaxis=dict(color='#9AA4B2', linecolor='rgba(255,255,255,0.1)')
            ),
            showlegend=False,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=300,
            margin=dict(l=40, r=40, t=60, b=40),
            title=dict(text=title, font=dict(color='#E9F1FF'))
        )
        return fig
except ImportError:
    PLOTLY_OK = False
    def plot_radar_chart(stats: Dict[str, float], title: str):
        # Return None if plotly is not available
        return None

# Clustering & Similarity
try:
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    from sklearn.decomposition import PCA
    from sklearn.metrics.pairwise import cosine_similarity
    SKLEARN_OK = True
except ImportError:
    SKLEARN_OK = False

# -------- Google Sheets I/O --------
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# -------- App Theme & Styling --------
st.set_page_config(page_title="DJM — Scouting & Transfers", layout="wide", initial_sidebar_state="expanded")
# Define custom CSS for the dark theme
THEME_CSS = """
:root {
    --accent: #69E2FF; --bg: #0A0F1F; --card: #121935;
    --muted: #9AA4B2; --good: #00E88F; --warn: #F2C94C; --bad: #FF6B6B;
}
.stApp {
    background: radial-gradient(1300px 800px at 10% 0%, #0A0F1F 0%, #0B1228 40%, #0A0F1F 100%);
    color: #E9F1FF;
}
.djm-card {
    background: var(--card); border-radius: 16px;
    padding: 18px; border: 1px solid rgba(255,255,255,0.06);
    box-shadow: 0 18px 38px rgba(0,0,0,0.35);
}
.djm-kpi .big { font-size: 38px; font-weight: 900; letter-spacing: 0.2px; }
.djm-kpi .label { color: var(--muted); text-transform: uppercase; font-size: 12px; letter-spacing: 0.3px; }
.stButton>button {
    border-radius: 12px; padding: 8px 14px; font-weight: 600;
    background: linear-gradient(120deg, #5B8CFF, #69E2FF);
    color: #0B1020; border: 0;
}
.st-emotion-cache-1kyxreq { border-radius: 12px; }  /* Progress bar container */
[data-testid="stDataFrame"] {
    border-radius: 12px; overflow: hidden;
    border: 1px solid rgba(255,255,255,0.08);
}
h3 { margin-top: 1.5rem; }
"""
st.markdown(f"<style>{THEME_CSS}</style>", unsafe_allow_html=True)

# -------- Settings & Data Constants --------
# Prefer URL -> ID -> Name: support any of these in secrets.toml
SHEET_URL  = st.secrets.get("sheet_url", "")
SHEET_ID   = st.secrets.get("sheet_id", "")
SHEET_NAME = st.secrets.get("sheet_name", "DJM_Input")  # fallback name
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.readonly"]

# Dataframe column schemas
PLAYERS_HEADERS = [
    "player_id","player_name","player_qid","dob","age","citizenships",
    "height_cm","positions","position_group","current_club","shirt_number",
    "contract_until","tm_url","tm_id"
]
RAW_MATCHES_HEADERS = [
    "tm_id","player_name","date","competition","opponent","minutes",
    "shots","xg","xa","key_passes","progressive_passes","progressive_carries",
    "dribbles_won","tackles_won","interceptions","aerials_won","passes",
    "passes_accurate","touches","duels_won","position"
]
FEATURE_STORE_COLS = [
    "tm_id","player_name","minutes","xg_p90","xa_p90","shots_p90","kp_p90",
    "prog_pass_p90","prog_carry_p90","dribbles_p90","tackles_p90","inter_p90",
    "aerials_p90","pass_acc"
]
RATINGS_HEADERS = [
    "tm_id","player_name","position_group","age",
    "overall_now","potential","uncert_now",
    "minutes_90","league_adj","availability","role_fit","market_signal","updated_at"
]
DEFAULT_LEAGUE_FACTORS = {
    "Premier League": 1.0, "LaLiga": 0.95, "Bundesliga": 0.94,
    "Serie A": 0.93, "Ligue 1": 0.88, "Eredivisie": 0.80, "Default": 0.70
}
DEFAULT_SETTINGS = {
    "w_attack": 0.35, "w_progression": 0.25, "w_defence": 0.20, "w_passing": 0.20,
    "age_peak_start": 25, "age_peak_end": 28,
    "potential_growth_factor": 1.15,
    "minutes_confidence_floor": 450, "minutes_confidence_ceiling": 2200,
    "league_factors": json.dumps(DEFAULT_LEAGUE_FACTORS),
    "tm_value_fetch": True, "last_build": "—"
}

# -------- Utility Functions --------
def now_ts() -> str:
    """Current timestamp in Europe/Rome timezone (for Last updated)."""
    tz = pytz.timezone("Europe/Rome")
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M %Z")

@st.cache_resource(show_spinner="Connecting to Google Sheets...")
def connect_sheet() -> gspread.Spreadsheet:
    """Authenticate and return a gspread Spreadsheet client (url -> id -> name)."""
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
    client = gspread.authorize(creds)
    if SHEET_URL:
        return client.open_by_url(SHEET_URL)
    if SHEET_ID:
        return client.open_by_key(SHEET_ID)
    return client.open(SHEET_NAME)

def get_or_create_ws(ss: gspread.Spreadsheet, name: str, headers: Optional[List[str]] = None) -> gspread.Worksheet:
    """Get a worksheet by name, or create it with given headers if not found."""
    try:
        ws = ss.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=name, rows=2000, cols=max(30, len(headers or [])))
        if headers:
            ws.update('A1', [headers])
    return ws

@st.cache_data(ttl=300)
def read_tab(_ss: gspread.Spreadsheet, name: str) -> pd.DataFrame:
    """Read a worksheet into a DataFrame, or return empty DF if not found/empty."""
    try:
        ws = _ss.worksheet(name)
    except gspread.WorksheetNotFound:
        return pd.DataFrame()
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    return pd.DataFrame() if df is None else df.dropna(how="all")

def write_tab(ss: gspread.Spreadsheet, name: str, df: pd.DataFrame):
    """Write a DataFrame to a worksheet (overwrite existing), then clear cache."""
    ws = get_or_create_ws(ss, name, headers=list(df.columns))
    ws.clear()
    set_with_dataframe(ws, df.fillna(""), row=1, col=1, include_index=False, include_column_header=True)
    st.cache_data.clear()  # Invalidate any cached reads after writing

def load_settings(ss: gspread.Spreadsheet) -> Dict[str, Any]:
    """Load settings from the 'settings' sheet, or use defaults if not set."""
    df = read_tab(ss, "settings")
    if df.empty:
        return DEFAULT_SETTINGS.copy()
    s = DEFAULT_SETTINGS.copy()
    for _, r in df.iterrows():
        key = str(r.get("key", "")).strip()
        val = r.get("value", "")
        if not key:
            continue
        try:
            s[key] = json.loads(val)
        except Exception:
            try:
                s[key] = float(val)
            except Exception:
                s[key] = val
    return s

def save_settings(ss: gspread.Spreadsheet, settings: Dict[str, Any]):
    """Save settings dict back to the 'settings' sheet."""
    rows = []
    for k, v in settings.items():
        if isinstance(v, (dict, list)):
            rows.append({"key": k, "value": json.dumps(v)})
        else:
            rows.append({"key": k, "value": str(v)})
    write_tab(ss, "settings", pd.DataFrame(rows, columns=["key", "value"]))

# Helper functions for DataFrame string operations
def col_str(df: pd.DataFrame, col: str) -> pd.Series:
    """Convert column to str, treating nan/None as empty string."""
    if col not in df.columns or df.empty:
        return pd.Series([], dtype=str)
    return df[col].astype(str).replace({"nan": "", "None": "", "NaT": ""})

def eq_name(df: pd.DataFrame, col: str, target: str) -> pd.Series:
    """Case-insensitive exact match of a name in a DataFrame column."""
    return col_str(df, col).str.lower() == str(target or "").lower()

def eq_id(df: pd.DataFrame, col: str, target: Optional[str]) -> pd.Series:
    """Exact match of an ID in a DataFrame column (treat None as '')."""
    return col_str(df, col) == str(target or "")

def parse_tm_id(url_or_id: str) -> Optional[str]:
    """Parse a Transfermarkt player ID from URL or return numeric string if already given."""
    if not url_or_id:
        return None
    s = str(url_or_id).strip()
    m = re.search(r"/spieler/(\d+)", s)
    if m:
        return m.group(1)
    if re.fullmatch(r"\d{3,}", s):
        return s  # pure numeric ID
    nums = re.findall(r"\d{3,}", s)
    return nums[-1] if nums else None

def position_group_from_text(txt: str) -> str:
    """Infer position group (GK, DF, MF, FW) from position text."""
    t = (txt or "").lower()
    if "gk" in t or "keeper" in t:
        return "GK"
    if any(w in t for w in ["cb", "rb", "lb", "rwb", "lwb", "def", "back"]):
        return "DF"
    if any(w in t for w in ["dm", "cm", "am", "mid"]):
        return "MF"
    if any(w in t for w in ["fw", "st", "wing", "strik", "att"]):
        return "FW"
    return ""

def safe_div(a, b) -> float:
    """Safe division that returns NaN if division is not applicable."""
    try:
        if pd.isna(a) or pd.isna(b) or float(b) == 0:
            return np.nan
        return float(a) / float(b)
    except Exception:
        return np.nan

def norm_by_group(s: pd.Series) -> pd.Series:
    """Normalize a series to [0,1] within its distribution, with winsorization to reduce outliers."""
    if s is None or s.empty:
        return s
    lo, hi = np.nanpercentile(s, 5), np.nanpercentile(s, 95)
    if hi == lo:
        # If no variance, return 0.5 for all (mid-scale)
        return pd.Series(0.5, index=s.index)
    return ((s - lo) / (hi - lo)).clip(0, 1)

def best_effort_tm_value(tm_url: str, enabled: bool = True) -> Optional[str]:
    """Scrape Transfermarkt for market value. Returns formatted value or None if not found."""
    if not enabled or not tm_url:
        return None
    try:
        r = requests.get(tm_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        if r.status_code != 200:
            return None
        match = re.search(r"Market value[^€£]*([€£]\s?[\d\.,]+[mk]?)", r.text, re.I)
        return match.group(1).replace(" ", "") if match else None
    except Exception:
        return None

# -------- Data Update Functions (Players, Matches) --------
def upsert_players(ss: gspread.Spreadsheet, df_in: pd.DataFrame) -> Tuple[int, int]:
    """
    Add new players or update existing ones in the 'players' sheet.
    Matches by tm_id or by (name, dob) if tm_id is not provided.
    Returns (insert_count, update_count).
    """
    get_or_create_ws(ss, "players", headers=PLAYERS_HEADERS)
    existing = read_tab(ss, "players")
    if existing.empty:
        existing = pd.DataFrame(columns=PLAYERS_HEADERS)

    df = df_in.copy()
    # Ensure all required columns exist in df
    for c in PLAYERS_HEADERS:
        if c not in df.columns:
            df[c] = pd.NA

    # Fill tm_id using tm_url if not provided
    df["tm_id"] = df.apply(
        lambda r: r["tm_id"] if pd.notna(r["tm_id"]) and str(r["tm_id"]).strip() != "" else parse_tm_id(str(r.get("tm_url", ""))),
        axis=1
    )
    # Auto-fill position_group from positions text if not provided
    for i, r in df.iterrows():
        if not str(r.get("position_group", "")).strip():
            df.at[i, "position_group"] = position_group_from_text(str(r.get("positions", "")))

    # Strip whitespace and unify types for string columns
    str_cols = ["player_id","player_name","player_qid","dob","tm_url","tm_id","position_group","positions","current_club","contract_until"]
    for c in str_cols:
        if c in df.columns:
            df[c] = col_str(df, c).str.strip()

    # Create lookup for existing players
    existing = existing.reindex(columns=PLAYERS_HEADERS).fillna("")
    ex_by_tm = {str(t): idx for idx, t in enumerate(col_str(existing, "tm_id")) if t}
    ex_by_name_dob = {
        (str(n).lower(), str(d)): idx
        for idx, (n, d) in enumerate(zip(col_str(existing, "player_name"), col_str(existing, "dob")))
        if n
    }

    inserted = updated = 0
    for _, r in df.iterrows():
        tm_id = str(r["tm_id"]) if pd.notna(r["tm_id"]) else ""
        if tm_id:
            idx = ex_by_tm.get(tm_id)
        else:
            idx = ex_by_name_dob.get((str(r["player_name"]).lower(), str(r["dob"])))
        if idx is None:
            # Insert new row
            row = {h: (str(r[h]) if h in r.index and pd.notna(r[h]) else "") for h in PLAYERS_HEADERS}
            existing = pd.concat([existing, pd.DataFrame([row])], ignore_index=True)
            inserted += 1
        else:
            # Update existing row with any non-empty fields from input
            for h in PLAYERS_HEADERS:
                if h in r.index and pd.notna(r[h]) and str(r[h]).strip() != "":
                    existing.iat[idx, existing.columns.get_loc(h)] = str(r[h])
            updated += 1

    write_tab(ss, "players", existing.reindex(columns=PLAYERS_HEADERS))
    return inserted, updated

def guess_map(df: pd.DataFrame) -> Dict[str, str]:
    """Heuristic mapping from arbitrary match stats column names to our standard column names."""
    cols = {c.lower().replace("_", " ").strip(): c for c in df.columns}
    def find(*keys):
        for k in keys:
            for c_norm, c_orig in cols.items():
                if k in c_norm:
                    return c_orig
        return ""
    return {
        "player_name": find("player", "name"),
        "date": find("date", "match date"),
        "competition": find("competition", "league"),
        "opponent": find("opponent", "rival"),
        "minutes": find("minute", "min"),
        "shots": find("shots"),
        "xg": find("xg"),
        "xa": find("xa"),
        "key_passes": find("key passes", "kp"),
        "progressive_passes": find("progressive passes", "prog pass"),
        "progressive_carries": find("progressive carries", "prog carr"),
        "dribbles_won": find("dribbles won", "dribble"),
        "tackles_won": find("tackles won", "tackle"),
        "interceptions": find("interceptions", "int"),
        "aerials_won": find("aerials won", "aerial"),
        "passes": find("passes completed", "passes", "pass att"),
        "passes_accurate": find("accurate", "pass cmp"),
        "touches": find("touches"),
        "duels_won": find("duels won", "duel"),
        "position": find("position", "pos")
    }

def append_raw_matches(ss: gspread.Spreadsheet, df_in: pd.DataFrame, tm_id_for_rows: Optional[str] = None) -> int:
    """
    Append new match records to 'raw_matches' sheet.
    If tm_id_for_rows is provided, all rows will be tagged with that player ID.
    Returns the number of rows appended.
    """
    if df_in is None or df_in.empty:
        return 0
    mapping = guess_map(df_in)
    new_rows = []
    for _, r in df_in.iterrows():
        row = {h: "" for h in RAW_MATCHES_HEADERS}
        # Map each known stat column to our schema
        for std_col, src_col in mapping.items():
            if src_col and src_col in df_in.columns:
                row[std_col] = r[src_col]
        if tm_id_for_rows:
            row["tm_id"] = tm_id_for_rows
        # Normalize date format if possible
        if row.get("date"):
            try:
                row["date"] = dtparser.parse(str(row["date"])).date().isoformat()
            except Exception:
                pass
        new_rows.append(row)
    existing = read_tab(ss, "raw_matches")
    combined = pd.concat([existing, pd.DataFrame(new_rows)], ignore_index=True)
    write_tab(ss, "raw_matches", combined[RAW_MATCHES_HEADERS])
    return len(new_rows)

# -------- Core Logic: Feature Store & Ratings --------
@st.cache_data(show_spinner="Building feature store...")
def build_feature_store(_ss: gspread.Spreadsheet) -> pd.DataFrame:
    """Aggregate raw match data into per-90 features for each player, and save to 'feature_store' sheet."""
    raw = read_tab(_ss, "raw_matches")
    if raw.empty:
        return pd.DataFrame(columns=FEATURE_STORE_COLS)
    # Ensure numeric types for stat columns
    stat_cols = ["minutes","xg","xa","shots","key_passes","progressive_passes","progressive_carries",
                 "dribbles_won","tackles_won","interceptions","aerials_won","passes","passes_accurate","touches","duels_won"]
    for c in stat_cols:
        if c in raw.columns:
            raw[c] = pd.to_numeric(raw[c], errors="coerce")
    # Sum stats by player
    grouped = raw.groupby(["tm_id", "player_name"], dropna=False).agg({c: "sum" for c in stat_cols}).reset_index()
    mins = grouped["minutes"].replace({0: np.nan})
    # Compute per-90 metrics and accuracy
    feats = pd.DataFrame({
        "tm_id": grouped["tm_id"],
        "player_name": grouped["player_name"],
        "minutes": grouped["minutes"],
        "xg_p90": grouped["xg"] / mins * 90,
        "xa_p90": grouped["xa"] / mins * 90,
        "shots_p90": grouped["shots"] / mins * 90,
        "kp_p90": grouped["key_passes"] / mins * 90,
        "prog_pass_p90": grouped["progressive_passes"] / mins * 90,
        "prog_carry_p90": grouped["progressive_carries"] / mins * 90,
        "dribbles_p90": grouped["dribbles_won"] / mins * 90,
        "tackles_p90": grouped["tackles_won"] / mins * 90,
        "inter_p90": grouped["interceptions"] / mins * 90,
        "aerials_p90": grouped["aerials_won"] / mins * 90,
        "pass_acc": grouped.apply(lambda r: safe_div(r["passes_accurate"], r["passes"]), axis=1)
    })
    write_tab(_ss, "feature_store", feats.reindex(columns=FEATURE_STORE_COLS))
    return feats

# Models for adjustments
def age_curve_multiplier(age: float, settings: Dict[str, Any]) -> float:
    """Multiplier (<=1) for performance based on age (youth growth and veteran decline)."""
    if pd.isna(age):
        return 1.0
    peak_start, peak_end = settings["age_peak_start"], settings["age_peak_end"]
    if age < peak_start:
        return 1.0 + (peak_start - age) * 0.01   # slight boost for youth potential
    if age > peak_end:
        return 1.0 - (age - peak_end) * 0.015   # decline after peak years
    return 1.0

def uncertainty_from_minutes(minutes: float, settings: Dict[str, Any]) -> float:
    """Estimate rating uncertainty based on minutes played (less minutes => higher uncertainty)."""
    m = 0 if pd.isna(minutes) else float(minutes)
    floor, ceil = settings["minutes_confidence_floor"], settings["minutes_confidence_ceiling"]
    if m < floor:
        return 20.0  # very uncertain if too few minutes
    if m > ceil:
        return 5.0   # more confidence if a lot of minutes
    # Linearly interpolate uncertainty between 20 and 5
    return 20.0 - (m - floor) / (ceil - floor) * 15.0

@st.cache_data(show_spinner="Rebuilding all player ratings...")
def rebuild_ratings(_ss: gspread.Spreadsheet, settings: Dict[str, Any]) -> pd.DataFrame:
    """
    Compute ratings for all players based on feature store and player info.
    Saves the ratings to 'ratings' sheet and returns the DataFrame.
    """
    feats = read_tab(_ss, "feature_store")
    players_df = read_tab(_ss, "players")
    raw = read_tab(_ss, "raw_matches")
    if feats.empty or players_df.empty:
        return pd.DataFrame(columns=RATINGS_HEADERS)
    # Join features with player age and position group
    df = feats.merge(players_df[["tm_id", "age", "position_group"]], on="tm_id", how="left")
    # Determine league strength factor for each match and average per player
    league_factors = json.loads(settings.get("league_factors", json.dumps(DEFAULT_LEAGUE_FACTORS)))
    if not raw.empty and "competition" in raw.columns:
        raw["league_factor"] = raw["competition"].map(lambda x: league_factors.get(x, league_factors.get("Default", 0.7)))
        league_adj_map = raw.groupby("tm_id")["league_factor"].mean()
        df["league_adj"] = df["tm_id"].map(league_adj_map).fillna(league_factors.get("Default", 0.7))
    else:
        df["league_adj"] = league_factors.get("Default", 0.7)
    # Positional normalization: scale stats relative to others in same position group
    for pg, group in df.groupby("position_group"):
        if group.empty:
            continue
        attack = norm_by_group(0.6*group["xg_p90"] + 0.4*group["xa_p90"] + 0.2*group["shots_p90"] + 0.4*group["kp_p90"])
        progress = norm_by_group(0.6*group["prog_pass_p90"] + 0.4*group["prog_carry_p90"] + 0.2*group["dribbles_p90"])
        defence = norm_by_group(0.6*group["tackles_p90"] + 0.6*group["inter_p90"] + 0.2*group["aerials_p90"])
        passing = norm_by_group(group["pass_acc"])
        group_base = (settings["w_attack"] * attack + settings["w_progression"] * progress +
                      settings["w_defence"] * defence + settings["w_passing"] * passing).clip(0, 1)
        df.loc[group.index, "base_score"] = group_base.fillna(0)
    # Calculate overall ratings
    players_age = pd.to_numeric(df["age"], errors='coerce')
    age_factor = players_age.map(lambda a: age_curve_multiplier(a, settings))
    overall_now = (df["base_score"] * age_factor * df["league_adj"]).clip(0, 1) * 100
    potential = (overall_now * (1 + (settings["age_peak_start"] - players_age).clip(lower=0) / 100 * (1 - overall_now/100))
                ).clip(lower=overall_now, upper=100)
    ratings_df = pd.DataFrame({
        "tm_id": df["tm_id"],
        "player_name": df["player_name"],
        "position_group": df["position_group"].fillna(""),
        "age": df["age"],
        "overall_now": overall_now.round(1),
        "potential": potential.round(1),
        "uncert_now": df["minutes"].map(lambda m: uncertainty_from_minutes(m, settings)).round(1),
        "minutes_90": (df["minutes"].fillna(0) / 90).round(1),
        "league_adj": df["league_adj"].round(2),
        "availability": np.nan,
        "role_fit": np.nan,
        "market_signal": np.nan,
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    })
    write_tab(_ss, "ratings", ratings_df.reindex(columns=RATINGS_HEADERS))
    return ratings_df

# -------- Similarity & Roles Analysis --------
ROLE_FEATURES = ["xg_p90","xa_p90","shots_p90","kp_p90","prog_pass_p90","prog_carry_p90",
                "dribbles_p90","tackles_p90","inter_p90","aerials_p90","pass_acc"]

@st.cache_data(show_spinner="Finding similar players...")
def find_similar_players(target_tm_id: str, feats_df: pd.DataFrame, players_df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    """Find top N most similar players (by stats) to the target player (same position group)."""
    if not SKLEARN_OK or target_tm_id not in feats_df['tm_id'].astype(str).values:
        return pd.DataFrame()
    # Prepare data for similarity comparison
    df = feats_df.copy()
    for col in ROLE_FEATURES:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    df = df.merge(players_df[["tm_id", "position_group"]], on="tm_id", how="left")
    target_row = df[df['tm_id'] == target_tm_id]
    if target_row.empty:
        return pd.DataFrame()
    target_pg = target_row['position_group'].iloc[0]
    # Only compare with players of the same position group
    pool = df[df['position_group'] == target_pg].drop_duplicates(subset='tm_id').reset_index(drop=True)
    if len(pool) < 2:
        return pd.DataFrame()  # not enough players to compare
    # Compute cosine similarity of stats vectors
    X = pool[ROLE_FEATURES].values
    X_scaled = StandardScaler().fit_transform(X)
    target_idx = pool[pool['tm_id'] == target_tm_id].index[0]
    target_vec = X_scaled[target_idx].reshape(1, -1)
    sim_scores = cosine_similarity(target_vec, X_scaled)[0]
    pool["similarity"] = sim_scores
    # Sort by similarity and exclude the target itself
    similar = pool.sort_values("similarity", ascending=False)
    similar = similar[similar["tm_id"] != target_tm_id].head(top_n)
    return similar[["player_name", "tm_id", "similarity"]]

def rebuild_roles(ss: gspread.Spreadsheet, n_clusters: int = 4):
    """
    Cluster players in each position group into role archetypes using KMeans.
    Saves labels to 'roles' sheet (columns: tm_id, player_name, position_group, role_label, pca_x, pca_y).
    """
    feats = read_tab(ss, "feature_store")
    players_df = read_tab(ss, "players")
    if not SKLEARN_OK or feats.empty or players_df.empty:
        # If cannot compute, save empty roles sheet
        write_tab(ss, "roles", pd.DataFrame(columns=["tm_id","player_name","position_group","role_cluster","role_label","pca_x","pca_y"]))
        return
    df = feats.merge(players_df[["tm_id", "position_group"]], on="tm_id", how="left")
    role_results = []
    ROLE_LABELS = {
        "GK": ["Sweeper Keeper", "Shot-Stopper"],
        "DF": ["Ball-Playing Defender", "No-Nonsense CB", "Inverted Full-Back", "Wing-Back", "Overlapping Full-Back"],
        "MF": ["Box-to-Box Midfielder", "Deep-Lying Playmaker", "Ball-Winning Midfielder", "Advanced Playmaker", "Wide Midfielder"],
        "FW": ["Channel Runner", "Target Forward", "Inside Forward", "Classic Winger", "Shadow Striker"]
    }
    for pg in ["GK", "DF", "MF", "FW"]:
        sub = df[df["position_group"] == pg].copy()
        if sub.empty:
            continue
        k = min(n_clusters, max(1, len(sub) // 8))
        if k < 1:
            continue
        X = sub[ROLE_FEATURES].fillna(0.0).values
        X_scaled = StandardScaler().fit_transform(X)
        km = KMeans(n_clusters=k, n_init="auto", random_state=42)
        labels = km.fit_predict(X_scaled)
        pca_coords = PCA(n_components=2, random_state=42).fit_transform(X_scaled)
        label_names = ROLE_LABELS.get(pg, [f"{pg} Role {i}" for i in range(k)])
        human_labels = [label_names[label % len(label_names)] for label in labels]
        result_df = pd.DataFrame({
            "tm_id": sub["tm_id"],
            "player_name": sub["player_name"],
            "position_group": pg,
            "role_cluster": labels,
            "role_label": human_labels,
            "pca_x": pca_coords[:, 0],
            "pca_y": pca_coords[:, 1]
        })
        role_results.append(result_df)
    roles_df = pd.concat(role_results, ignore_index=True) if role_results else pd.DataFrame(columns=["tm_id","player_name","position_group","role_cluster","role_label","pca_x","pca_y"])
    write_tab(ss, "roles", roles_df)

# ---------------- Streamlit App Starts Here ----------------

# ---------------- Sidebar (Data Source Connection) ----------------
with st.sidebar:
    st.header("Data Source")
    try:
        ss = connect_sheet()
        # Show actual connected sheet title
        st.success(f"✅ Connected to **{getattr(ss, 'title', SHEET_NAME)}**")
    except Exception as e:
        st.error(f"❌ GSheets connection failed. Check secrets & sharing. Details: {e}", icon="⚠️")
        st.stop()
    st.caption(f"Last updated: {now_ts()}")
    if st.button("🔄 Refresh Data", help="Force reload data from Google Sheets"):
        st.cache_data.clear()
        st.experimental_rerun()

# Cache all data reads for quick access
@st.cache_data(show_spinner="Loading database sheets...")
def load_all_data(_ss: gspread.Spreadsheet) -> Dict[str, Any]:
    return {
        "players": read_tab(_ss, "players"),
        "raw": read_tab(_ss, "raw_matches"),
        "feats": read_tab(_ss, "feature_store"),
        "ratings": read_tab(_ss, "ratings"),
        "roles": read_tab(_ss, "roles"),
        "settings": load_settings(_ss)
    }

# 🔧 FIX: pass the Spreadsheet object, not st.secrets
data = load_all_data(ss)

players = data["players"]
raw = data["raw"]
feats = data["feats"]
ratings = data["ratings"]
roles = data["roles"]
settings = data["settings"]

# ---------------- App Header -------------------------
st.markdown(
    "<div class='djm-card'>"
    "<div style='font-size:28px; font-weight:800;'>DJM — Scouting & Transfer Intelligence</div>"
    "<div style='color: var(--muted);'>V2.2: Enhanced Stability · Fuzzy Search · UI Improvements</div>"
    "</div>",
    unsafe_allow_html=True
)
st.write("")  # spacer

# ---------------- Main Tabs --------------------------
tab_dash, tab_player, tab_club, tab_roles, tab_admin, tab_settings_tab = st.tabs(
    ["Dashboard", "👤 Player Profile", "🏟️ Club Analysis", "🧩 Roles & Similarity", "⚙️ Admin", "🔧 Settings"]
)

# ===== Dashboard Tab =====
with tab_dash:
    # Top-level KPIs
    kpi_cols = st.columns(4)
    def show_kpi(col, label: str, value):
        col.markdown(
            f"<div class='djm-card djm-kpi'><div class='big'>{value}</div>"
            f"<div class='label'>{label}</div></div>",
            unsafe_allow_html=True
        )
    show_kpi(kpi_cols[0], "Players in DB", len(players) if not players.empty else 0)
    show_kpi(kpi_cols[1], "Match Logs", f"{len(raw):,}" if not raw.empty else 0)
    try:
        rated_unique = len(ratings["tm_id"].astype(str).unique()) if not ratings.empty and "tm_id" in ratings.columns else 0
    except Exception:
        rated_unique = 0
    show_kpi(kpi_cols[2], "Rated Players", rated_unique)
    show_kpi(kpi_cols[3], "Last Model Build", settings.get("last_build", "—"))

    st.subheader("Leaderboard")
    if ratings.empty:
        st.info("No ratings data available. Use the **Admin** tab to build the feature store and ratings model.")
    else:
        # Ensure expected columns exist, create if missing to avoid KeyErrors
        required_cols = ["player_name", "position_group", "age", "overall_now", "potential",
                         "uncert_now", "minutes_90", "league_adj", "tm_id"]
        for c in required_cols:
            if c not in ratings.columns:
                ratings[c] = np.nan

        pos_filter = st.selectbox("Position Filter", ["All", "GK", "DF", "MF", "FW"], index=0)

        # Use a dropdown (no horizontal kw)
        sort_by = st.selectbox("Sort by", ["Overall Now", "Potential"], index=0)

        # If 'potential' is missing or entirely NaN, fall back to 'overall_now'
        potential_missing = ("potential" not in ratings.columns) or ratings["potential"].isna().all()
        sort_col = "potential" if (sort_by == "Potential" and not potential_missing) else "overall_now"
        if sort_by == "Potential" and potential_missing:
            st.caption("⚠️ No 'potential' values found — sorting by Overall instead. Rebuild the ratings in **Admin** to populate Potential.")

        # Apply position filter
        filtered = ratings[ratings["position_group"] == pos_filter].copy() if pos_filter != "All" else ratings.copy()

        # Ensure numeric types for sortable/format columns
        numeric_cols = ["overall_now", "potential", "uncert_now", "minutes_90", "league_adj"]
        for c in numeric_cols:
            if c in filtered.columns:
                filtered[c] = pd.to_numeric(filtered[c], errors="coerce")

        # Sort + select
        existing_cols = [c for c in ["player_name", "position_group", "age", "overall_now",
                                     "potential", "uncert_now", "minutes_90", "league_adj"] if c in filtered.columns]
        if sort_col not in filtered.columns:
            sort_col = "overall_now"
        df_top = filtered.sort_values(by=sort_col, ascending=False, na_position="last").head(25)[existing_cols].copy()

        # Format numeric columns safely
        def fmt1(v):
            return f"{float(v):,.1f}" if pd.notna(v) else ""

        for c in ["overall_now", "potential", "uncert_now", "minutes_90", "league_adj"]:
            if c in df_top.columns:
                df_top[c] = df_top[c].map(fmt1)

        # Rename for display
        rename_map = {
            "player_name": "Name", "position_group": "Pos", "age": "Age",
            "overall_now": "Overall", "potential": "Potential",
            "uncert_now": "Uncert.", "minutes_90": "90s", "league_adj": "Lg Adj"
        }
        df_top = df_top.rename(columns={k: v for k, v in rename_map.items() if k in df_top.columns})

        st.dataframe(df_top, use_container_width=True, hide_index=True)


# ===== Player Profile Tab =====
with tab_player:
    st.subheader("Player Search")
    # Combine known player names from players and ratings tables
    all_names = sorted(set(col_str(players, "player_name")) | set(col_str(ratings, "player_name")))
    col_search, col_load = st.columns([0.7, 0.3])
    query = col_search.text_input("Search by player name...", placeholder="Type a name (e.g., Jude Bellingham)")
    suggestions = fuzzy_pick(all_names, query, limit=10) if query else []
    # Show top suggestions in a selectbox for easy selection
    selected_name = col_search.selectbox("Search Results", options=suggestions, index=0 if suggestions else None, label_visibility="collapsed")
    if col_load.button("Load Profile", type="primary", disabled=(not selected_name), use_container_width=True):
        st.session_state["selected_player_name"] = selected_name
    # If a player is selected (via search or previously)
    if "selected_player_name" in st.session_state:
        target_name = st.session_state["selected_player_name"]
        # Pull relevant data for the selected player
        p_info = players[eq_name(players, "player_name", target_name)]
        p_rating = ratings[eq_name(ratings, "player_name", target_name)]
        p_feat = feats[eq_name(feats, "player_name", target_name)]
        if p_rating.empty:
            st.warning(f"No rating found for **{target_name}**. Please add match stats for this player and rebuild ratings.")
        else:
            # Use the latest record in case of duplicates
            p_info = p_info.iloc[-1] if not p_info.empty else None
            p_rating = p_rating.iloc[-1]
            target_tm_id = str(p_rating.get("tm_id"))
            # Divider and player name header
            st.markdown("<hr style='border-color: rgba(255,255,255,0.1); margin:1.5rem 0;'>", unsafe_allow_html=True)
            st.markdown(f"### {target_name}")
            # --- Player Bio & Key Metrics ---
            bio_col, metrics_col = st.columns([1, 2])
            with bio_col:
                st.markdown("<div class='djm-card'>", unsafe_allow_html=True)
                st.markdown(f"**Position:** {p_rating.get('position_group') or 'N/A'}  \n"
                            f"**Age:** {p_rating.get('age') or 'N/A'}  \n"
                            f"**Club:** {p_info.get('current_club') if p_info is not None else 'N/A'}  \n"
                            f"**Citizenship:** {p_info.get('citizenships') if p_info is not None else 'N/A'}  \n"
                            f"**Height:** {p_info.get('height_cm') if p_info is not None else 'N/A'} cm  \n"
                            f"**Contract:** Until {p_info.get('contract_until') if p_info is not None else 'N/A'}",
                            unsafe_allow_html=True)
                tm_value = best_effort_tm_value(p_info.get("tm_url") if p_info is not None else None,
                                                settings.get("tm_value_fetch", True))
                st.caption(f"Transfermarkt Value: **{tm_value or '—'}**")
                st.markdown("</div>", unsafe_allow_html=True)
            with metrics_col:
                m1, m2, m3 = st.columns(3)
                # Overall Now with uncertainty ±
                m1.metric("Overall Now", f"{p_rating['overall_now']:.1f}", f"±{p_rating['uncert_now']:.1f}")
                # Potential with delta vs current if significant
                current = p_rating["overall_now"]
                potential = p_rating["potential"]
                delta = potential - current
                delta_text = f"{delta:+.1f} vs Current" if delta >= 0.5 else None
                m2.metric("Potential", f"{potential:.1f}", delta_text)
                # Minutes played (last season or total data)
                total_minutes = p_rating["minutes_90"] * 90
                m3.metric("Minutes", f"{total_minutes:,.0f}")
                # Confidence progress bar (based on minutes)
                confidence = min(1.0, float(total_minutes) / settings["minutes_confidence_ceiling"])
                st.progress(confidence, text="Data Confidence")
            # --- Statistical Profile: Radar + Similar Players ---
            st.markdown("### Statistical Profile")
            radar_col, sim_col = st.columns(2)
            with radar_col:
                st.markdown("<div class='djm-card'>", unsafe_allow_html=True)
                if PLOTLY_OK:
                    if not p_feat.empty:
                        f = p_feat.iloc[-1]  # feature row
                        # Calculate scores for radar (normalized within this player's stats set for simplicity)
                        att_score = norm_by_group(pd.Series([f["xg_p90"], f["xa_p90"], f["shots_p90"], f["kp_p90"]])).iloc[0] * 100
                        prog_score = norm_by_group(pd.Series([f["prog_pass_p90"], f["prog_carry_p90"], f["dribbles_p90"]])).iloc[0] * 100
                        dfn_score = norm_by_group(pd.Series([f["tackles_p90"], f["inter_p90"], f["aerials_p90"]])).iloc[0] * 100
                        pas_score = norm_by_group(pd.Series([f["pass_acc"]])).iloc[0] * 100
                        radar_stats = {"Attacking": att_score, "Progression": prog_score, "Defending": dfn_score, "Passing": pas_score}
                        fig_radar = plot_radar_chart(radar_stats, "Core Skill Areas")
                        st.plotly_chart(fig_radar, use_container_width=True)
                    else:
                        st.info("No performance data available to plot.")
                else:
                    st.info("Install Plotly to see radar chart visualization.")
                st.markdown("</div>", unsafe_allow_html=True)
            with sim_col:
                st.markdown("<div class='djm-card' style='text-align:center;'>"
                            "<p style='font-weight:bold; margin-bottom:0.5rem;'>Statistically Similar Players</p>",
                            unsafe_allow_html=True)
                with st.spinner("Searching for similar players..."):
                    similar_df = find_similar_players(target_tm_id, feats, players, top_n=5)
                    if not similar_df.empty:
                        # Merge to get Transfermarkt URLs (for potential linking if desired)
                        similar_df = similar_df.merge(players[["tm_id", "tm_url"]], on="tm_id", how="left")
                        similar_df["Similarity"] = similar_df["similarity"].apply(lambda x: f"{x * 100:.1f}%")
                        sim_display = similar_df[["player_name", "Similarity"]].copy()
                        sim_display.columns = ["Name", "Similarity"]
                        st.dataframe(sim_display, use_container_width=True, hide_index=True)
                    else:
                        if not SKLEARN_OK:
                            st.caption("*Similarity search requires scikit-learn.*")
                        else:
                            st.caption("Not enough data to find similar players.")
                st.markdown("</div>", unsafe_allow_html=True)

# ===== Club Analysis Tab =====
with tab_club:
    st.subheader("Club Squad Analysis")
    if players.empty or ratings.empty:
        st.info("Add players and build ratings to use this section.")
    else:
        all_clubs = sorted([c for c in col_str(players, "current_club").unique() if c])
        if not all_clubs:
            st.info("No club information available in the players database.")
        else:
            default_index = all_clubs.index("Manchester City") if "Manchester City" in all_clubs else 0
            club = st.selectbox("Select a Club", all_clubs, index=default_index)
            # Build roster for selected club
            roster = players[col_str(players, "current_club") == club].merge(ratings, on="tm_id", how="left")
            roster = roster.dropna(subset=["overall_now"])
            if roster.empty:
                st.warning(f"No rated players found for **{club}**.")
            else:
                c1, c2 = st.columns([2, 1])
                with c1:
                    st.markdown(f"#### Squad Overview: {club}")
                    df_roster = roster[["player_name", "position_group", "age", "overall_now", "potential", "minutes_90"]].sort_values("overall_now", ascending=False).copy()
                    df_roster.columns = ["Name", "Pos", "Age", "Overall", "Potential", "90s"]
                    st.dataframe(df_roster, use_container_width=True, hide_index=True)
                with c2:
                    st.markdown("#### Squad Rating Distribution")
                    st.markdown("<div class='djm-card'>", unsafe_allow_html=True)
                    if PLOTLY_OK:
                        if not roster["overall_now"].dropna().empty:
                            fig = go.Figure()
                            fig.add_trace(go.Box(y=roster["overall_now"], name="Current Ability", marker_color='#69E2FF'))
                            fig.add_trace(go.Box(y=roster["potential"], name="Potential", marker_color='#00E88F'))
                            fig.update_layout(
                                height=400,
                                paper_bgcolor="rgba(0,0,0,0)",
                                plot_bgcolor="#121935",
                                yaxis_title="Rating",
                                margin=dict(l=20, r=20, t=40, b=20),
                                legend=dict(orientation="h", yanchor="bottom", y=1.0, xanchor="right", x=1)
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No data to plot.")
                    else:
                        st.info("Install Plotly to see distribution chart.")
                    st.markdown("</div>", unsafe_allow_html=True)
                st.markdown("---")
                st.markdown("#### Compare a Target Player to This Squad")
                target_input = st.text_input("Enter a player name to compare", key="club_compare_name")
                if target_input:
                    # Fuzzy match the input to a player in our ratings
                    choices = fuzzy_pick(list(col_str(ratings, "player_name")), target_input, limit=1, score_cutoff=50)
                    match_name = choices[0] if choices else target_input
                    if choices and match_name.lower() != target_input.lower():
                        st.caption(f"Showing results for **{match_name}** (closest match to \"{target_input}\")")
                    comp_rating = ratings[eq_name(ratings, "player_name", match_name)].tail(1)
                    if comp_rating.empty:
                        st.warning(f"No rating data for **{target_input}**.")
                    else:
                        comp = comp_rating.iloc[0]
                        comp_value = float(comp["overall_now"])
                        comp_pos = comp["position_group"]
                        # Filter club's roster for same position group
                        band = roster[roster["position_group"] == comp_pos]["overall_now"].dropna()
                        st.write(f"**{match_name}** – Overall: **{comp_value:.1f}**, Position: **{comp_pos or 'N/A'}**")
                        if not band.empty:
                            q1, med, q3 = band.quantile(0.25), band.median(), band.quantile(0.75)
                            if comp_value > q3:
                                msg = "a clear **upgrade**"
                            elif comp_value >= med:
                                msg = "a **good fit** for the first team"
                            elif comp_value >= q1:
                                msg = "likely a **rotation option**"
                            else:
                                msg = "**below** the current squad level"
                            st.success(f"Compared to {club}'s {comp_pos}s, {match_name} would be {msg}.")
                            st.caption(f"Squad {comp_pos} rating range at {club}: 25th percentile = {q1:.1f}, median = {med:.1f}, 75th percentile = {q3:.1f}")
                        else:
                            st.info(f"No comparable players in {club}'s squad for the {comp_pos} position.")

# ===== Roles & Similarity Tab =====
with tab_roles:
    st.subheader("Positional Roles & Archetypes")
    if st.button("Rebuild role clusters"):
        with st.spinner("Clustering players..."):
            rebuild_roles(ss)
            st.cache_data.clear()
            st.experimental_rerun()
    # Check library availability first
    if not SKLEARN_OK:
        st.error("Scikit-learn is not installed. Role clustering and similarity analysis are disabled.", icon="⚠️")
    else:
        if roles.empty:
            st.info("No role data available. Click **Rebuild role clusters** to generate role archetypes.")
        else:
            position_group = st.selectbox("Position Group", ["FW", "MF", "DF", "GK"], index=0, key="roles_pg")
            subset = roles[roles["position_group"] == position_group]
            if subset.empty:
                st.info(f"No players found for position group **{position_group}**.")
            else:
                # Plot role clusters (PCA scatter)
                if PLOTLY_OK:
                    fig = go.Figure()
                    for role_name, grp in subset.groupby("role_label"):
                        fig.add_trace(go.Scatter(
                            x=grp["pca_x"], y=grp["pca_y"], mode="markers", name=role_name,
                            text=grp["player_name"], marker=dict(size=8, opacity=0.8)
                        ))
                    fig.update_layout(
                        height=500,
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="#121935",
                        legend_title_text="Identified Roles",
                        margin=dict(l=20, r=20, t=40, b=30)
                    )
                    st.plotly_chart(fig, use_container_width=True)
                # Show role labels for each player
                role_list = subset[["player_name", "role_label"]].sort_values(["role_label", "player_name"]).reset_index(drop=True)
                role_list.columns = ["Player", "Role Archetype"]
                st.dataframe(role_list, use_container_width=True, hide_index=True)

# ===== Admin Tab =====
with tab_admin:
    st.subheader("Admin — Data Management & Model Training")
    col_left, col_right = st.columns(2)
    with col_left:
        with st.expander("➕ Add or Update Player"):
            with st.form("add_player_form"):
                pname = st.text_input("Player Name *")
                tm_input = st.text_input("Transfermarkt URL or ID *")
                pos_text = st.text_input("Positions (e.g., 'LW/ST' or 'CB')")
                club_name = st.text_input("Current Club")
                dob_str = st.text_input("Date of Birth (YYYY-MM-DD)")
                contract_str = st.text_input("Contract Until (YYYY-MM-DD)")
                submitted = st.form_submit_button("Submit")
                if submitted:
                    tm_id_val = parse_tm_id(tm_input)
                    if not pname or not tm_id_val:
                        st.error("Please provide both a player name and a valid Transfermarkt URL/ID.", icon="⚠️")
                    else:
                        new_player = pd.DataFrame([{
                            "player_name": pname,
                            "tm_url": tm_input,
                            "tm_id": tm_id_val,
                            "positions": pos_text,
                            "position_group": position_group_from_text(pos_text),
                            "current_club": club_name,
                            "dob": dob_str,
                            "contract_until": contract_str
                        }])
                        with st.spinner("Updating player database..."):
                            ins, upd = upsert_players(ss, new_player)
                        st.success(f"Player database updated. **Inserted {ins}** new player, **Updated {upd}** existing player.", icon="✅")
        with st.expander("📑 Upload Match Stats"):
            uploaded_file = st.file_uploader("Choose an Excel (.xlsx) or CSV file", type=["xlsx", "csv"])
            tm_override = st.text_input("Associate all rows with Transfermarkt ID (optional)")
            if uploaded_file is not None:
                try:
                    # Read the file into a DataFrame
                    if uploaded_file.name.lower().endswith(".xlsx"):
                        df_stats = pd.read_excel(uploaded_file)
                    else:
                        df_stats = pd.read_csv(uploaded_file)
                    st.dataframe(df_stats.head(10), use_container_width=True, hide_index=True)
                    if st.button("Append to raw_matches"):
                        tm_for_rows = parse_tm_id(tm_override) if tm_override else None
                        with st.spinner("Appending match data..."):
                            added_count = append_raw_matches(ss, df_stats, tm_id_for_rows=tm_for_rows)
                        st.success(f"Appended {added_count} match records to **raw_matches**.", icon="✅")
                except Exception as e:
                    st.error(f"Error reading file: {e}", icon="⚠️")
    with col_right:
        st.markdown("### Build / Refresh Data")
        st.info("Run the steps below in order after adding new data.")
        if st.button("Rebuild Feature Store"):
            with st.spinner("Aggregating match data..."):
                build_feature_store(ss)
            st.success("Feature store rebuilt successfully.", icon="✅")
        if st.button("Rebuild Ratings Model", type="primary"):
            with st.spinner("Calculating player ratings..."):
                new_ratings_df = rebuild_ratings(ss, settings)
            # Update last build timestamp and save
            settings["last_build"] = now_ts()
            save_settings(ss, settings)
            st.success(f"Ratings rebuilt for **{len(new_ratings_df)}** players. ✔️ Check the Dashboard for updated values.", icon="✅")
        st.markdown("---")
        st.markdown("### Danger Zone")
        st.warning("The operations above will overwrite data in your Google Sheet. Use them carefully and keep backups.", icon="⚠️")
        st.caption("*(Note: The old 'Club Rosters' tab is deprecated; club data is now derived from the Players sheet.)*")

# ===== Settings Tab =====
with tab_settings_tab:
    st.subheader("Model Configuration")
    st.info("Adjust parameters below and click **Save Settings**, then rebuild the ratings in the Admin tab for changes to take effect.")
    with st.form("settings_form"):
        s = settings.copy()
        colA, colB = st.columns(2)
        with colA:
            st.markdown("**Weighting of Attributes**")
            s["w_attack"] = st.slider("Attack Weight", 0.0, 1.0, float(s.get("w_attack", 0.35)), 0.01)
            s["w_progression"] = st.slider("Progression Weight", 0.0, 1.0, float(s.get("w_progression", 0.25)), 0.01)
            s["w_defence"] = st.slider("Defence Weight", 0.0, 1.0, float(s.get("w_defence", 0.20)), 0.01)
            s["w_passing"] = st.slider("Passing Weight", 0.0, 1.0, float(s.get("w_passing", 0.20)), 0.01)
        with colB:
            st.markdown("**Player Development Curve**")
            s["age_peak_start"] = st.slider("Peak Performance Starts (Age)", 20, 30, int(s.get("age_peak_start", 25)))
            s["age_peak_end"] = st.slider("Peak Performance Ends (Age)", 25, 36, int(s.get("age_peak_end", 28)))
            st.markdown("**Data Confidence Thresholds**")
            s["minutes_confidence_floor"] = st.slider("Minutes for High Uncertainty", 0, 1000, int(s.get("minutes_confidence_floor", 450)), 50)
            s["minutes_confidence_ceiling"] = st.slider("Minutes for Low Uncertainty", 1000, 4000, int(s.get("minutes_confidence_ceiling", 2200)), 100)
        st.markdown("**League Strength Factors**")
        default_factors = json.dumps(DEFAULT_LEAGUE_FACTORS)
        s["league_factors"] = st.text_area("JSON mapping of competition -> strength factor", value=s.get("league_factors", default_factors), height=120)
        submitted = st.form_submit_button("Save Settings")
        if submitted:
            try:
                # Validate that league_factors is proper JSON
                json.loads(s["league_factors"])
                save_settings(ss, s)
                st.success("Settings saved. Rebuild the ratings model to apply changes.", icon="✅")
                st.cache_data.clear()
                settings = s  # update current settings
            except json.JSONDecodeError:
                st.error("League factors must be valid JSON.", icon="⚠️")
            except Exception as e:
                st.error(f"Failed to save settings: {e}", icon="⚠️")
