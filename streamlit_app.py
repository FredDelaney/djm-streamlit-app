# DJM — Scouting & Transfer Intelligence (Monolith V2.1 — stable)
# ----------------------------------------------------------------------
# V2.1 Changes:
# - Fixed NameError by moving optional-dependency functions (plot_radar_chart)
#   inside the try/except block where the dependency is imported.

import streamlit as st
import pandas as pd
import numpy as np
import pytz, re, requests, json
from datetime import datetime
from dateutil import parser as dtparser
from typing import List, Dict, Any, Optional, Tuple

# -------- Optional deps (graceful fallbacks) --------
# Fuzzy search
try:
    from rapidfuzz import process as _fuzz
    def fuzzy_pick(options: List[str], query: str, limit: int = 8, score_cutoff: int = 65) -> List[str]:
        if not options or not query: return []
        hits = _fuzz.extract(query, options, limit=limit, score_cutoff=score_cutoff)
        return [h[0] for h in hits]
except Exception:
    import difflib
    def fuzzy_pick(options: List[str], query: str, limit: int = 8, score_cutoff: float = 0.0) -> List[str]:
        if not options or not query: return []
        return difflib.get_close_matches(query, options, n=limit, cutoff=0.0)

# Plotly charts
try:
    import plotly.graph_objects as go
    PLOTLY_OK = True
    # V2.1 FIX: Define the function that USES plotly inside the try block.
    def plot_radar_chart(stats: Dict[str, float], title: str) -> "go.Figure":
        categories = list(stats.keys())
        values = list(stats.values())
        fig = go.Figure()
        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=categories,
            fill='toself',
            line_color='var(--accent)',
            marker=dict(color='var(--accent)'),
            name='Score'
        ))
        fig.update_layout(
            polar=dict(
                radialaxis=dict(visible=True, range=[0, 100], color='var(--muted)', gridcolor='rgba(255,255,255,0.1)'),
                angularaxis=dict(color='var(--muted)', linecolor='rgba(255,255,255,0.1)')
            ),
            showlegend=False,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=300,
            margin=dict(l=40, r=40, t=60, b=40),
            title=dict(text=title, font=dict(color='#E9F1FF'))
        )
        return fig

except Exception:
    PLOTLY_OK = False
    # If plotly fails, create a dummy function so the app doesn't crash if it's called.
    def plot_radar_chart(stats: Dict[str, float], title: str):
        return None

# Clustering & Similarity
try:
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    from sklearn.decomposition import PCA
    from sklearn.metrics.pairwise import cosine_similarity
    SKLEARN_OK = True
except Exception:
    SKLEARN_OK = False

# -------- Google Sheets I/O --------
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# -------- App chrome & CSS --------
st.set_page_config(page_title="DJM — Scouting & Transfers", layout="wide", initial_sidebar_state="expanded")
THEME_CSS = """
:root { --accent:#69E2FF; --bg:#0A0F1F; --card:#121935; --muted:#9aa4b2; --good:#00E88F; --warn:#F2C94C; --bad:#FF6B6B; }
.stApp { background: radial-gradient(1300px 800px at 10% 0%, #0A0F1F 0%, #0B1228 40%, #0A0F1F 100%); color:#E9F1FF; }
.djm-card { background:var(--card); border-radius:16px; padding:18px; border:1px solid rgba(255,255,255,.06); box-shadow:0 18px 38px rgba(0,0,0,.35); }
.djm-kpi .big { font-size:38px; font-weight:900; letter-spacing:.2px; }
.djm-kpi .label { color:var(--muted); text-transform:uppercase; font-size:12px; letter-spacing:.3px; }
.stButton>button { border-radius:12px; padding:8px 14px; font-weight:600; background:linear-gradient(120deg, #5B8CFF, #69E2FF); color:#0B1020; border:0; }
.st-emotion-cache-1kyxreq { border-radius:12px; } /* Progress bar container */
[data-testid="stDataFrame"] { border-radius:12px; overflow:hidden; border:1px solid rgba(255,255,255,.08); }
h3 { margin-top: 1.5rem; }
"""
st.markdown(f"<style>{THEME_CSS}</style>", unsafe_allow_html=True)

# -------- Settings & Contracts --------
SHEET_NAME = st.secrets.get("sheet_name", "DJM_Input")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive.readonly"]

# Dataframe column contracts
PLAYERS_HEADERS = [
    "player_id","player_name","player_qid","dob","age","citizenships",
    "height_cm","positions","position_group","current_club","shirt_number",
    "contract_until", "tm_url","tm_id"
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
DEFAULT_LEAGUE_FACTORS = {"Premier League": 1.0, "LaLiga": 0.95, "Bundesliga": 0.94, "Serie A": 0.93, "Ligue 1": 0.88, "Eredivisie": 0.80, "Default": 0.70}
DEFAULT_SETTINGS = {
    "w_attack": 0.35, "w_progression": 0.25, "w_defence": 0.20, "w_passing": 0.20,
    "age_peak_start": 25, "age_peak_end": 28,
    "potential_growth_factor": 1.15,
    "minutes_confidence_floor": 450, "minutes_confidence_ceiling": 2200,
    "league_factors": json.dumps(DEFAULT_LEAGUE_FACTORS),
    "tm_value_fetch": True, "last_build": "—"
}

# -------- Utilities --------
def now_ts() -> str:
    # Set for Lucca, Italy
    tz = pytz.timezone("Europe/Rome")
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M %Z")

@st.cache_resource(show_spinner="Connecting to Google Sheets...")
def connect_sheet() -> gspread.Spreadsheet:
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME)

def get_or_create_ws(ss: gspread.Spreadsheet, name: str, headers: Optional[List[str]] = None) -> gspread.Worksheet:
    try:
        ws = ss.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=name, rows=2000, cols=max(30, len(headers or [])))
        if headers:
            ws.update('A1', [headers])
    return ws

@st.cache_data(ttl=300)
def read_tab(_ss: gspread.Spreadsheet, name: str) -> pd.DataFrame:
    try:
        ws = _ss.worksheet(name)
    except gspread.WorksheetNotFound:
        return pd.DataFrame()
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    return pd.DataFrame() if df is None else df.dropna(how="all")

def write_tab(ss: gspread.Spreadsheet, name: str, df: pd.DataFrame):
    ws = get_or_create_ws(ss, name, headers=list(df.columns))
    ws.clear()
    set_with_dataframe(ws, df.fillna(""), row=1, col=1, include_index=False, include_column_header=True)
    st.cache_data.clear() # Invalidate cache after writing

def load_settings(ss: gspread.Spreadsheet) -> Dict[str, Any]:
    df = read_tab(ss, "settings")
    if df.empty: return DEFAULT_SETTINGS.copy()
    s = DEFAULT_SETTINGS.copy()
    for _, r in df.iterrows():
        k = str(r.get("key","")).strip()
        v = r.get("value","")
        if not k: continue
        try: s[k] = json.loads(v)
        except Exception:
            try: s[k] = float(v)
            except Exception: s[k] = v
    return s

def save_settings(ss: gspread.Spreadsheet, settings: Dict[str, Any]):
    rows = [{"key":k,"value":str(v) if not isinstance(v, (dict, list)) else json.dumps(v)} for k,v in settings.items()]
    write_tab(ss, "settings", pd.DataFrame(rows, columns=["key","value"]))

# .str-safe helpers
def col_str(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns or df.empty: return pd.Series([], dtype=str)
    return df[col].astype(str).replace({"nan":"","None":"", "NaT":""})

def eq_name(df: pd.DataFrame, col: str, target: str) -> pd.Series:
    return col_str(df, col).str.lower() == (str(target or "").lower())

def eq_id(df: pd.DataFrame, col: str, target: Optional[str]) -> pd.Series:
    return col_str(df, col) == (str(target or ""))

def parse_tm_id(url_or_id: str) -> Optional[str]:
    if not url_or_id: return None
    s = str(url_or_id).strip()
    m = re.search(r"/spieler/(\d+)", s)
    if m: return m.group(1)
    m = re.fullmatch(r"\d{3,}", s)
    if m: return m.group(0)
    nums = re.findall(r"\d{3,}", s)
    return nums[-1] if nums else None

def position_group_from_text(txt: str) -> str:
    t = (txt or "").lower()
    if "gk" in t or "keeper" in t: return "GK"
    if any(w in t for w in ["cb","rb","lb","rwb","lwb","def","back"]): return "DF"
    if any(w in t for w in ["dm","cm","am","mid"]): return "MF"
    if any(w in t for w in ["fw","st","wing","w","strik","att"]): return "FW"
    return ""

def safe_div(a,b) -> float:
    try:
        if pd.isna(a) or pd.isna(b) or float(b)==0: return np.nan
        return float(a)/float(b)
    except Exception: return np.nan

def norm_by_group(s: pd.Series) -> pd.Series:
    if s is None or s.empty: return s
    # Winsorize at 5th and 95th percentiles to handle outliers robustly
    lo, hi = np.nanpercentile(s, 5), np.nanpercentile(s, 95)
    if hi == lo: return pd.Series(0.5, index=s.index) # Avoid division by zero for constant series
    return ((s - lo) / (hi - lo)).clip(0, 1)

def best_effort_tm_value(tm_url: str, enabled: bool = True) -> Optional[str]:
    if not enabled or not tm_url: return None
    try:
        r = requests.get(tm_url, headers={"User-Agent":"Mozilla/5.0"}, timeout=8)
        if r.status_code != 200: return None
        m = re.search(r"Market value[^€£]*([€£]\s?[\d\.,]+[mk]?)", r.text, re.I)
        return m.group(1).replace(" ","") if m else None
    except Exception: return None

# -------- Admin upserts & ingestion --------
def upsert_players(ss: gspread.Spreadsheet, df_in: pd.DataFrame) -> Tuple[int, int]:
    # (This function is largely the same as V1, but adapted for new headers)
    _ = get_or_create_ws(ss, "players", headers=PLAYERS_HEADERS)
    existing = read_tab(ss, "players")
    if existing.empty: existing = pd.DataFrame(columns=PLAYERS_HEADERS)

    df = df_in.copy()
    for c in PLAYERS_HEADERS:
        if c not in df.columns: df[c] = pd.NA

    df["tm_id"] = df.apply(lambda r: r["tm_id"] if pd.notna(r["tm_id"]) and str(r["tm_id"]).strip()!=""
                           else parse_tm_id(str(r.get("tm_url",""))), axis=1)
    for i, r in df.iterrows():
        if not str(r.get("position_group","")).strip():
            df.at[i, "position_group"] = position_group_from_text(str(r.get("positions","")))

    str_cols = ["player_id","player_name","player_qid","dob","tm_url","tm_id","position_group","positions","current_club", "contract_until"]
    for c in str_cols:
        if c in df.columns: df[c] = col_str(df, c).str.strip()

    existing = existing.reindex(columns=PLAYERS_HEADERS).fillna("")
    ex_by_tm = {str(t): i for i,t in enumerate(col_str(existing,"tm_id")) if t}
    ex_by_name_dob = {(str(n).lower(), str(d)): i for i,(n,d)
                      in enumerate(zip(col_str(existing,"player_name"), col_str(existing, "dob"))) if n}

    ins, upd = 0, 0
    for _, r in df.iterrows():
        tm_id = str(r["tm_id"]) if pd.notna(r["tm_id"]) else ""
        idx = ex_by_tm.get(tm_id) if tm_id else ex_by_name_dob.get((str(r["player_name"]).lower(), str(r["dob"])))

        if idx is None:
            row = {h: str(r[h]) if h in r.index and pd.notna(r[h]) else "" for h in PLAYERS_HEADERS}
            existing = pd.concat([existing, pd.DataFrame([row])], ignore_index=True)
            ins += 1
        else:
            for h in PLAYERS_HEADERS:
                val = r[h] if h in r.index and pd.notna(r[h]) else ""
                if str(val) != "":
                    existing.iat[idx, existing.columns.get_loc(h)] = str(val)
            upd += 1

    write_tab(ss, "players", existing.reindex(columns=PLAYERS_HEADERS))
    return ins, upd

def guess_map(df: pd.DataFrame) -> Dict[str, str]:
    cols = {c.lower().replace("_", " ").strip(): c for c in df.columns}
    def find(*keys):
        for k in keys:
            for c_norm, c_orig in cols.items():
                if k in c_norm: return c_orig
        return ""
    return {
        "player_name": find("player","name"), "date": find("date","match date"),
        "competition": find("competition","league"), "opponent": find("opponent","rival"),
        "minutes": find("minute","min"), "shots": find("shots"), "xg": find("xg"),
        "xa": find("xa"), "key_passes": find("key passes", "kp"),
        "progressive_passes": find("progressive passes", "prog pass"),
        "progressive_carries": find("progressive carries", "prog carr"),
        "dribbles_won": find("dribbles won", "dribble"), "tackles_won": find("tackles won", "tackle"),
        "interceptions": find("interceptions", "int"), "aerials_won": find("aerials won", "aerial"),
        "passes": find("passes completed", "passes", "pass att"),
        "passes_accurate": find("accurate", "pass cmp"), "touches": find("touches"),
        "duels_won": find("duels won", "duel"), "position": find("position","pos")
    }

def append_raw_matches(ss: gspread.Spreadsheet, df_in: pd.DataFrame, tm_id_for_rows: Optional[str] = None) -> int:
    # (Largely same as V1)
    if df_in is None or df_in.empty: return 0
    m = guess_map(df_in)
    out_rows = []
    for _, r in df_in.iterrows():
        row = {h:"" for h in RAW_MATCHES_HEADERS}
        for k, src in m.items():
            if src and src in df_in.columns: row[k] = r[src]
        if tm_id_for_rows: row["tm_id"] = tm_id_for_rows
        if row.get("date",""):
            try: row["date"] = dtparser.parse(str(row["date"])).date().isoformat()
            except Exception: pass
        out_rows.append(row)
    existing = read_tab(ss, "raw_matches")
    out = pd.concat([existing, pd.DataFrame(out_rows)], ignore_index=True)
    write_tab(ss, "raw_matches", out[RAW_MATCHES_HEADERS])
    return len(out_rows)

# -------- Core Logic: Feature Store & Ratings (V2 Enhancements) --------
@st.cache_data(show_spinner="Building feature store...")
def build_feature_store(_ss: gspread.Spreadsheet) -> pd.DataFrame:
    raw = read_tab(_ss, "raw_matches")
    if raw.empty: return pd.DataFrame(columns=FEATURE_STORE_COLS)
    
    num_cols = ["minutes","xg","xa","shots","key_passes","progressive_passes","progressive_carries",
                "dribbles_won","tackles_won","interceptions","aerials_won","passes","passes_accurate","touches","duels_won"]
    for c in num_cols:
        if c in raw.columns: raw[c] = pd.to_numeric(raw[c], errors="coerce")

    # Aggregate stats per player
    g = raw.groupby(["tm_id","player_name"], dropna=False).agg({c: "sum" for c in num_cols}).reset_index()
    
    # Calculate per-90 metrics
    mins = g["minutes"].replace({0:np.nan})
    feats = pd.DataFrame({
        "tm_id": g["tm_id"], "player_name": g["player_name"], "minutes": g["minutes"],
        "xg_p90": g["xg"]/mins*90, "xa_p90": g["xa"]/mins*90, "shots_p90": g["shots"]/mins*90,
        "kp_p90": g["key_passes"]/mins*90, "prog_pass_p90": g["progressive_passes"]/mins*90,
        "prog_carry_p90": g["progressive_carries"]/mins*90, "dribbles_p90": g["dribbles_won"]/mins*90,
        "tackles_p90": g["tackles_won"]/mins*90, "inter_p90": g["interceptions"]/mins*90,
        "aerials_p90": g["aerials_won"]/mins*90,
        "pass_acc": g.apply(lambda r: safe_div(r["passes_accurate"], r["passes"]), axis=1)
    })
    
    write_tab(_ss, "feature_store", feats.reindex(columns=FEATURE_STORE_COLS))
    return feats

# V2: NEW - Model for age-based performance curve
def age_curve_multiplier(age: float, settings: Dict) -> float:
    if pd.isna(age): return 1.0
    peak_start, peak_end = settings["age_peak_start"], settings["age_peak_end"]
    if age < peak_start: return 1.0 + (peak_start - age) * 0.01 # Gentle incline for youth
    if age > peak_end: return 1.0 - (age - peak_end) * 0.015 # Steadier decline for veterans
    return 1.0 # Peak years

# V2: NEW - Model for uncertainty based on minutes
def uncertainty_from_minutes(minutes: float, settings: Dict) -> float:
    m = 0 if pd.isna(minutes) else float(minutes)
    floor, ceil = settings["minutes_confidence_floor"], settings["minutes_confidence_ceiling"]
    if m < floor: return 20.0 # High uncertainty for low minutes
    if m > ceil: return 5.0  # Low uncertainty for high minutes
    # Linear interpolation of uncertainty between floor and ceiling
    return 20.0 - (m - floor) / (ceil - floor) * 15.0

@st.cache_data(show_spinner="Rebuilding all player ratings...")
def rebuild_ratings(_ss: gspread.Spreadsheet, settings: Dict) -> pd.DataFrame:
    feats = read_tab(_ss, "feature_store")
    players = read_tab(_ss, "players")
    raw = read_tab(_ss, "raw_matches")
    if feats.empty or players.empty: return pd.DataFrame(columns=RATINGS_HEADERS)

    # V2: Join with player and raw data for position and league info
    df = feats.merge(players[["tm_id", "age", "position_group"]], on="tm_id", how="left")
    
    # V2: Apply League Strength adjustment
    league_factors = json.loads(settings.get("league_factors", json.dumps(DEFAULT_LEAGUE_FACTORS)))
    
    if not raw.empty and 'competition' in raw.columns:
        # Calculate avg league factor per player
        raw['league_factor'] = raw['competition'].map(lambda x: league_factors.get(x, league_factors.get("Default", 0.7)))
        league_adj_map = raw.groupby('tm_id')['league_factor'].mean()
        df['league_adj'] = df['tm_id'].map(league_adj_map).fillna(league_factors.get("Default", 0.7))
    else:
        df['league_adj'] = league_factors.get("Default", 0.7)
        
    # V2: Positional Normalization
    # Normalize performance metrics *within* each position group
    block_scores = pd.DataFrame(index=df.index)
    for pg, group in df.groupby("position_group"):
        if group.empty: continue
        att  = norm_by_group(0.6*group["xg_p90"] + 0.4*group["xa_p90"] + 0.2*group["shots_p90"] + 0.4*group["kp_p90"])
        prog = norm_by_group(0.6*group["prog_pass_p90"] + 0.4*group["prog_carry_p90"] + 0.2*group["dribbles_p90"])
        dfn  = norm_by_group(0.6*group["tackles_p90"] + 0.6*group["inter_p90"] + 0.2*group["aerials_p90"])
        pas  = norm_by_group(group["pass_acc"])
        
        # Calculate weighted base score for the group
        base01 = (settings["w_attack"]*att + settings["w_progression"]*prog + settings["w_defence"]*dfn + settings["w_passing"]*pas).clip(0,1)
        block_scores.loc[group.index, 'base01'] = base01
    
    df = df.join(block_scores)
    df['base01'] = df['base01'].fillna(0)
    
    # Calculate final scores
    players_age = pd.to_numeric(df["age"], errors='coerce')
    age_mult = players_age.map(lambda a: age_curve_multiplier(a, settings))
    
    # Current Ability: Base score * Age Curve * League Adjustment
    now = (df["base01"] * age_mult * df["league_adj"]).clip(0,1) * 100.0
    
    # Potential: Based on age and current ability. Younger players with high CA have more room to grow.
    potential = (now * (1 + (settings["age_peak_start"] - players_age).clip(0) / 100 * (1 - now/100))).clip(now, 100)
    
    # Final DataFrame
    out = pd.DataFrame({
        "tm_id": df["tm_id"], "player_name": df["player_name"], "position_group": df["position_group"].fillna(""),
        "age": df["age"], "overall_now": now.round(1), "potential": potential.round(1),
        "uncert_now": df["minutes"].map(lambda m: uncertainty_from_minutes(m, settings)).round(1),
        "minutes_90": (df["minutes"].fillna(0)/90).round(1),
        "league_adj": df["league_adj"].round(2),
        "availability": np.nan, "role_fit": np.nan, "market_signal": np.nan,
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    })
    
    write_tab(_ss, "ratings", out.reindex(columns=RATINGS_HEADERS))
    return out

# -------- V2: Similarity & Roles --------
@st.cache_data(show_spinner="Finding similar players...")
def find_similar_players(_target_tm_id: str, _feats: pd.DataFrame, _players: pd.DataFrame, n: int = 5) -> pd.DataFrame:
    if not SKLEARN_OK or _target_tm_id not in _feats['tm_id'].values: return pd.DataFrame()

    # Ensure consistent dtypes
    for col in ROLE_FEATURES:
        if col in _feats.columns: _feats[col] = pd.to_numeric(_feats[col], errors='coerce')

    # Merge to get position group
    df = _feats.merge(_players[['tm_id', 'position_group']], on='tm_id', how='left')
    target_player = df[df['tm_id'] == _target_tm_id]
    if target_player.empty: return pd.DataFrame()
    target_pg = target_player['position_group'].iloc[0]

    # Filter for players in the same position group
    candidate_pool = df[df['position_group'] == target_pg].copy()
    candidate_pool = candidate_pool.drop_duplicates(subset=['tm_id']) # Ensure unique players
    
    if len(candidate_pool) < 2: return pd.DataFrame()

    # Prepare feature matrix
    X = candidate_pool[ROLE_FEATURES].fillna(0.0).values
    X_scaled = StandardScaler().fit_transform(X)
    
    # Find target player's vector
    target_idx = candidate_pool['tm_id'].to_list().index(_target_tm_id)
    target_vector = X_scaled[target_idx].reshape(1, -1)
    
    # Compute cosine similarity
    sim_scores = cosine_similarity(target_vector, X_scaled)[0]
    candidate_pool['similarity'] = sim_scores
    
    # Return top N similar players (excluding the player themselves)
    similar = candidate_pool.sort_values('similarity', ascending=False).iloc[1:n+1]
    return similar[['player_name', 'tm_id', 'similarity']]

# Role building function (unchanged from V1)
ROLE_FEATURES = ["xg_p90","xa_p90","shots_p90","kp_p90","prog_pass_p90","prog_carry_p90","dribbles_p90","tackles_p90","inter_p90","aerials_p90","pass_acc"]
ROLE_LABELS = {
    "FW": ["Channel Runner","Target Forward","Inside Forward","Classic Winger","Shadow Striker"],
    "MF": ["Box-to-Box Midfielder","Deep-Lying Playmaker","Ball-Winning Midfielder","Advanced Playmaker","Wide Midfielder"],
    "DF": ["Ball-Playing Defender","No-Nonsense Centre-Back","Inverted Full-Back","Overlapping Full-Back","Wing-Back"],
    "GK": ["Sweeper Keeper","Shot-Stopper"]
}
def rebuild_roles(ss, n_clusters=4): # (No major changes needed for V2)
    feats = read_tab(ss, "feature_store")
    players = read_tab(ss, "players")
    if not SKLEARN_OK or feats.empty or players.empty:
        write_tab(ss, "roles", pd.DataFrame(columns=["tm_id","player_name","position_group","role_cluster","role_label","pca_x","pca_y"]))
        return
    df = feats.merge(players[["tm_id","position_group"]], on="tm_id", how="left")
    out = []
    for pg in ["FW","MF","DF","GK"]:
        sub = df[df["position_group"]==pg].copy()
        if len(sub) < n_clusters: continue
        X = sub[ROLE_FEATURES].fillna(0.0).values
        X = StandardScaler().fit_transform(X)
        k = min(n_clusters, max(1, len(sub)//8))
        if k==0: continue
        km = KMeans(n_clusters=k, n_init="auto", random_state=42).fit(X)
        pca = PCA(n_components=2, random_state=42).fit_transform(X)
        labels = km.labels_
        names = ROLE_LABELS.get(pg, [f"{pg} Role {i}" for i in range(k)])
        human = [names[i % len(names)] for i in labels]
        out.append(pd.DataFrame({
            "tm_id": sub["tm_id"], "player_name": sub["player_name"], "position_group": pg,
            "role_cluster": labels, "role_label": human, "pca_x": pca[:,0], "pca_y": pca[:,1]
        }))
    roles = pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=["tm_id","player_name","position_group","role_cluster","role_label","pca_x","pca_y"])
    write_tab(ss, "roles", roles)

# ---------------- App Starts Here ----------------

# ---------------- Sidebar: connect & data load ----------------
with st.sidebar:
    st.header("Data source")
    try:
        ss = connect_sheet()
        st.success(f"Connected to '{SHEET_NAME}'")
    except Exception as e:
        st.error(f"GSheets connection failed. Check secrets & sharing. Details: {e}")
        st.stop()
    st.caption(f"Last updated: {now_ts()}")
    refresh = st.button("🔄 Refresh Data", use_container_width=True)
    if refresh: st.cache_data.clear()

# State-managed data loading
@st.cache_data(show_spinner="Loading database sheets...")
def load_all_data(_ss: gspread.Spreadsheet):
    data = {
        "players": read_tab(_ss, "players"),
        "raw": read_tab(_ss, "raw_matches"),
        "feats": read_tab(_ss, "feature_store"),
        "ratings": read_tab(_ss, "ratings"),
        "roles": read_tab(_ss, "roles"),
        "settings": load_settings(_ss)
    }
    return data

data = load_all_data(ss)
players, raw, feats, ratings, roles, settings = data.values()

# ---------------- Header -------------------------
st.markdown(
    "<div class='djm-card'><div style='font-size:28px;font-weight:800;'>DJM — Scouting & Transfer Intelligence</div>"
    "<div style='color:#9aa4b2'>V2.1: Positional Normalization · League Adjustments · Similarity Search · Dynamic Insights</div></div>",
    unsafe_allow_html=True
)
st.write("")

# ---------------- Tabs ---------------------------
tab_dash, tab_player, tab_club, tab_roles, tab_admin, tab_settings = st.tabs(
    ["Dashboard", "👤 Player Profile", "🏟️ Club Analysis", "🧩 Roles & Similarity", "⚙️ Admin", "🔧 Settings"]
)

# ===== Dashboard =====
with tab_dash:
    c1, c2, c3, c4 = st.columns(4)
    def kpi(c, label, val): c.markdown(f"<div class='djm-card djm-kpi'><div class='big'>{val}</div><div class='label'>{label}</div></div>", unsafe_allow_html=True)
    kpi(c1,"Players in DB", len(players) if not players.empty else 0)
    kpi(c2,"Match Logs", f"{len(raw):,}" if not raw.empty else 0)
    kpi(c3,"Rated Players", len(ratings["tm_id"].unique()) if not ratings.empty and "tm_id" in ratings.columns else 0)
    kpi(c4,"Last Model Build", settings.get("last_build","—"))
    
    st.subheader("Leaderboard")
    if ratings.empty:
        st.info("No ratings found. Go to Admin → Rebuild feature_store → Rebuild ratings.")
    else:
        sort_by = st.selectbox("Sort by", ["Overall Now", "Potential"], horizontal=True)
        sort_col = "overall_now" if sort_by == "Overall Now" else "potential"
        
        display_cols = ["player_name", "position_group", "age", "overall_now", "potential", "uncert_now", "minutes_90", "league_adj"]
        display_ratings = ratings.copy()
        display_ratings['overall_now'] = display_ratings['overall_now'].map('{:,.1f}'.format)
        display_ratings['potential'] = display_ratings['potential'].map('{:,.1f}'.format)

        st.dataframe(display_ratings.sort_values(sort_col, ascending=False).head(25)[display_cols],
                       use_container_width=True, hide_index=True)

# ===== Player Profile =====
with tab_player:
    st.subheader("Player Search")
    all_names = sorted(set(list(col_str(players,"player_name")) + list(col_str(ratings,"player_name"))))
    
    sc1, sc2 = st.columns([3, 1])
    q = sc1.text_input("Search by player name...", placeholder="e.g., Jude Bellingham")
    picks = fuzzy_pick(all_names, q, limit=10) if q else []
    name = sc1.selectbox("Select Player", options=picks, index=0 if picks else None, label_visibility="collapsed")
    
    if sc2.button("Load Profile", type="primary", use_container_width=True) and name:
        st.session_state["selected_player_name"] = name

    if "selected_player_name" in st.session_state:
        target_name = st.session_state["selected_player_name"]
        
        # Get player data from all tables
        p_info = players[eq_name(players, "player_name", target_name)].iloc[-1:]
        p_rating = ratings[eq_name(ratings, "player_name", target_name)].iloc[-1:]
        p_feat = feats[eq_name(feats, "player_name", target_name)].iloc[-1:]
        
        if p_rating.empty:
            st.warning(f"No rating found for '{target_name}'. Please process their stats in the Admin panel.")
        else:
            p_info = p_info.iloc[0] if not p_info.empty else {}
            p_rating = p_rating.iloc[0]
            target_tm_id = p_rating.get('tm_id')

            st.markdown(f"<hr style='margin:1.5rem 0; border-color: rgba(255,255,255,0.1);'>", unsafe_allow_html=True)
            st.markdown(f"### {target_name}")

            # --- Bio & Core Metrics ---
            col1, col2 = st.columns([1, 2])
            with col1:
                st.markdown("<div class='djm-card'>", unsafe_allow_html=True)
                st.markdown(f"""
                **Position:** {p_rating.get('position_group', 'N/A')} <br>
                **Age:** {p_rating.get('age', 'N/A')} <br>
                **Club:** {p_info.get('current_club', 'N/A')} <br>
                **Citizenship:** {p_info.get('citizenships', 'N/A')} <br>
                **Height:** {p_info.get('height_cm', 'N/A')} cm <br>
                **Contract:** Until {p_info.get('contract_until', 'N/A')}
                """, unsafe_allow_html=True)
                mv = best_effort_tm_value(p_info.get("tm_url"), settings.get("tm_value_fetch", True))
                st.caption(f"TM Value: **{mv or '—'}**")
                st.markdown("</div>", unsafe_allow_html=True)

            with col2:
                m1, m2, m3 = st.columns(3)
                m1.metric("Overall Now", f"{p_rating['overall_now']:.1f}", f"± {p_rating['uncert_now']:.1f} uncertainty")
                m2.metric("Potential", f"{p_rating['potential']:.1f}")
                m3.metric("Minutes (last season)", f"{p_rating['minutes_90']*90:,.0f}")
                st.progress(min(1.0, float(p_rating["minutes_90"]*90 or 0)/settings["minutes_confidence_ceiling"]), text=f"Data Confidence (based on mins)")

            # --- Radar Chart & Similar Players ---
            st.markdown("### Statistical Profile")
            col_radar, col_sim = st.columns(2)
            with col_radar:
                if not p_feat.empty and PLOTLY_OK:
                    f = p_feat.iloc[0]
                    # Create a temporary series for normalization to work correctly
                    temp_series = pd.Series([f["xg_p90"], f["xa_p90"], f["shots_p90"], f["kp_p90"]])
                    att_score  = norm_by_group(temp_series).iloc[0] * 100

                    temp_series = pd.Series([f["prog_pass_p90"], f["prog_carry_p90"], f["dribbles_p90"]])
                    prog_score = norm_by_group(temp_series).iloc[0] * 100

                    temp_series = pd.Series([f["tackles_p90"], f["inter_p90"], f["aerials_p90"]])
                    dfn_score  = norm_by_group(temp_series).iloc[0] * 100
                    
                    pas_score  = norm_by_group(pd.Series([f["pass_acc"]]))[0] * 100
                    
                    radar_stats = {
                        "Attacking": att_score, "Progression": prog_score,
                        "Defending": dfn_score, "Passing": pas_score
                    }
                    st.plotly_chart(plot_radar_chart(radar_stats, "Core Skill Areas"), use_container_width=True, theme=None)
                else:
                    st.info("Plotly not available or no features for radar chart.")

            with col_sim:
                st.markdown("<p style='text-align:center; font-weight:bold;'>Statistically Similar Players</p>", unsafe_allow_html=True)
                with st.spinner("Searching for matches..."):
                    similar_df = find_similar_players(target_tm_id, feats, players, n=5)
                    if not similar_df.empty:
                        similar_df['similarity'] = (similar_df['similarity'] * 100).map('{:.1f}%'.format)
                        st.dataframe(similar_df[['player_name', 'similarity']], use_container_width=True, hide_index=True)
                    else:
                        st.caption("Not enough data to find similar players in the same position group.")

# ===== Club Analysis =====
with tab_club:
    st.subheader("Club Squad Analysis")
    if players.empty or ratings.empty:
        st.info("Requires `players` and `ratings` data. Please add players and build ratings in the Admin panel.")
    else:
        # V2: Dynamic roster generation from players table
        all_clubs = sorted([c for c in col_str(players,"current_club").unique() if c])
        club_default_index = all_clubs.index('Manchester City') if 'Manchester City' in all_clubs else 0
        club = st.selectbox("Select a Club", all_clubs, index=club_default_index)
        
        # Merge players at the selected club with their ratings
        roster = players[col_str(players, "current_club") == club].merge(
            ratings, on="tm_id", how="left", suffixes=('', '_rating')
        )
        roster = roster.dropna(subset=['overall_now']) # Only show rated players

        if roster.empty:
            st.warning(f"No rated players found for {club}.")
        else:
            c1, c2 = st.columns([2,1])
            with c1:
                st.markdown(f"#### Squad Overview: {club}")
                st.dataframe(roster[["player_name", "position_group", "age", "overall_now", "potential", "minutes_90"]].sort_values("overall_now", ascending=False),
                             use_container_width=True, hide_index=True)
            with c2:
                st.markdown("#### Squad Rating Distribution")
                if PLOTLY_OK and not roster["overall_now"].dropna().empty:
                    fig = go.Figure()
                    fig.add_trace(go.Box(y=roster["overall_now"], name="Current Ability", marker_color='var(--accent)'))
                    fig.add_trace(go.Box(y=roster["potential"], name="Potential", marker_color='var(--good)'))
                    fig.update_layout(height=400, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="var(--card)", yaxis_title="Rating", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
                    st.plotly_chart(fig, use_container_width=True)
            
            st.divider()
            st.markdown("#### Compare a Target Player")
            target_name_club = st.text_input("Player name to compare", key="club_compare_name")
            if target_name_club:
                r_target = ratings[eq_name(ratings, "player_name", target_name_club)].tail(1)
                if r_target.empty:
                    st.warning("No rating for that player.")
                else:
                    t_now = float(r_target.iloc[0]["overall_now"])
                    t_pg = r_target.iloc[0]["position_group"]
                    band = roster[roster['position_group']==t_pg]["overall_now"].dropna()
                    
                    st.write(f"**{target_name_club} (Overall: {t_now:.1f}, Position: {t_pg})**")
                    if not band.empty:
                        q1, med, q3 = band.quantile(0.25), band.median(), band.quantile(0.75)
                        msg = "a clear upgrade" if t_now > q3 else "a good fit" if t_now >= med else "a potential rotation option" if t_now >= q1 else "below the current standard"
                        st.success(f"Compared to {club}'s {t_pg}s, this player is **{msg}**.")
                        st.caption(f"Positional band at {club}: Lower Quartile {q1:.1f}, Median {med:.1f}, Upper Quartile {q3:.1f}")
                    else:
                        st.info(f"The club has no rated players in the '{t_pg}' position group to compare against.")

# ===== Roles =====
with tab_roles:
    st.subheader("Positional Roles & Archetypes")
    if st.button("Rebuild role clusters", key="roles_rebuild"):
        with st.spinner("Clustering players... this may take a moment."):
            rebuild_roles(ss)
            st.cache_data.clear()
            st.experimental_rerun()
    
    if roles.empty:
        st.info("No role data found. Click 'Rebuild' to generate archetypes from the feature store.")
    else:
        pg = st.selectbox("Position group", ["FW","MF","DF","GK"], key="roles_pg")
        rview = roles[col_str(roles,"position_group")==pg]
        
        if rview.empty:
            st.info(f"No players found for position group '{pg}' to build roles.")
        else:
            if PLOTLY_OK:
                fig = go.Figure()
                for lab, grp in rview.groupby("role_label"):
                    fig.add_trace(go.Scatter(x=grp["pca_x"], y=grp["pca_y"], mode="markers", name=lab, text=grp["player_name"],
                                             marker=dict(size=8, opacity=0.8)))
                fig.update_layout(height=500, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="var(--card)", legend_title_text='Identified Roles')
                st.plotly_chart(fig, use_container_width=True)
            st.dataframe(rview[["player_name","role_label"]].sort_values(["role_label","player_name"]),
                         use_container_width=True, hide_index=True)


# ===== Admin =====
with tab_admin:
    st.subheader("Admin — Data Management & Model Building")
    col1, col2 = st.columns(2)

    with col1:
        with st.expander("➕ Add / Update a Player", expanded=False):
            with st.form("add_player_form"):
                pname = st.text_input("Player name *")
                tm_in = st.text_input("Transfermarkt URL/ID *")
                pos_txt = st.text_input("Positions (e.g., LW/ST or CB)")
                club = st.text_input("Current club")
                dob = st.text_input("DOB (YYYY-MM-DD)")
                contract = st.text_input("Contract Until (YYYY-MM-DD)")
                submitted = st.form_submit_button("Upsert Player")
                if submitted:
                    tm = parse_tm_id(tm_in)
                    if not pname or not tm:
                        st.error("Player name and a valid Transfermarkt URL/ID are required.")
                    else:
                        rec = pd.DataFrame([{"player_name":pname, "tm_url":tm_in, "tm_id":tm, "positions":pos_txt,
                                             "position_group": position_group_from_text(pos_txt), "current_club": club,
                                             "dob": dob, "contract_until": contract}])
                        with st.spinner("Updating players database..."):
                            ins, upd = upsert_players(ss, rec)
                        st.success(f"Player DB updated: Inserted {ins}, Updated {upd}.")

        with st.expander("📈 Upload Match Stats (Excel/CSV)", expanded=True):
            upload = st.file_uploader("Upload .xlsx or .csv", type=["xlsx","csv"], key="adm_file")
            tm_rows = st.text_input("Transfermarkt ID for all rows in this file (optional)", key="adm_tm_rows")
            if upload is not None:
                try:
                    df_in = pd.read_excel(upload) if upload.name.lower().endswith(".xlsx") else pd.read_csv(upload)
                    st.dataframe(df_in.head(10), use_container_width=True, hide_index=True)
                    if st.button("Append to raw_matches", key="append_raw"):
                        tm_rows_id = parse_tm_id(tm_rows) if tm_rows else None
                        with st.spinner("Appending data..."):
                            added = append_raw_matches(ss, df_in, tm_id_for_rows=tm_rows_id)
                        st.success(f"Appended {added} rows to `raw_matches` sheet.")
                except Exception as e:
                    st.error(f"Could not read file. Error: {e}")

    with col2:
        st.markdown("### Build Pipeline")
        st.info("Build the feature store first, then build the ratings.")
        if st.button("Rebuild Feature Store"):
            build_feature_store(ss)
            st.success("Feature store rebuilt successfully.")
        
        if st.button("Rebuild Ratings (Primary Model)", type="primary"):
            new_ratings = rebuild_ratings(ss, settings)
            settings["last_build"] = now_ts()
            save_settings(ss, settings)
            st.success(f"Ratings rebuilt for {len(new_ratings)} players. See Dashboard.")
        
        st.divider()
        st.markdown("### Danger Zone")
        st.warning("These operations will overwrite existing data.")
        st.caption("The 'Club Rosters' tab has been deprecated. Club data is now generated automatically from the 'players' sheet.")


# ===== Settings =====
with tab_settings:
    st.subheader("Model Configuration")
    st.info("After changing settings, you must go to the Admin tab and click 'Rebuild Ratings' for them to take effect.")
    
    with st.form("settings_form"):
        s = settings.copy() # Work on a copy
        
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Core Weights**")
            s["w_attack"] = st.slider("Attack Weight", 0.0, 1.0, float(s.get("w_attack",0.35)), 0.01)
            s["w_progression"] = st.slider("Progression Weight", 0.0, 1.0, float(s.get("w_progression",0.25)), 0.01)
            s["w_defence"] = st.slider("Defence Weight", 0.0, 1.0, float(s.get("w_defence",0.20)), 0.01)
            s["w_passing"] = st.slider("Passing Weight", 0.0, 1.0, float(s.get("w_passing",0.20)), 0.01)
        
        with c2:
            st.markdown("**Age & Potential**")
            s["age_peak_start"] = st.slider("Peak Age Start", 22, 30, int(s.get("age_peak_start", 25)))
            s["age_peak_end"] = st.slider("Peak Age End", 25, 33, int(s.get("age_peak_end", 28)))
            
            st.markdown("**Data Confidence**")
            s["minutes_confidence_floor"] = st.slider("Uncertainty Floor (min minutes)", 100, 1000, int(s.get("minutes_confidence_floor", 450)), 50)
            s["minutes_confidence_ceiling"] = st.slider("Uncertainty Ceiling (max minutes)", 1500, 3500, int(s.get("minutes_confidence_ceiling", 2200)), 100)

        st.markdown("**League Strength Factors**")
        lf_json = s.get("league_factors", json.dumps(DEFAULT_LEAGUE_FACTORS))
        s["league_factors"] = st.text_area("Competition factors (JSON format)", value=lf_json, height=150)
        
        submitted = st.form_submit_button("Save Settings")
        if submitted:
            try:
                # Validate JSON
                json.loads(s["league_factors"])
                save_settings(ss, s)
                st.success("Settings saved successfully!")
                st.cache_data.clear() # Clear cache to reload settings on next run
            except json.JSONDecodeError:
                st.error("Invalid JSON format in League Strength Factors.")
            except Exception as e:
                st.error(f"Failed to save settings: {e}")