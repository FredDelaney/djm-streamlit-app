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
def fetch_tm_value_eur(tm_url: str) -> str|None:
    """Best-effort: parse TM market value text from the page. If blocked, return None."""
    try:
        if not tm_url: return None
        hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        r = requests.get(tm_url, headers=hdrs, timeout=10)
        if r.status_code != 200: return None
        html = r.text
        # Very loose regex for "Market value" blocks like "€60.00m"
        m = re.search(r"Market value[^€]*([€£]\s?[\d\.,]+[mk]?)", html, flags=re.IGNORECASE)
        if m: return m.group(1).replace(" ", "")
        return None
    except Exception:
        return None

# -------------------- SIDEBAR --------------------
with st.sidebar:
    st.header("Data source")
    try:
        ss = connect_gsheet()
        st.success("Connected ✅")
    except Exception:
        st.error("Could not connect to Google Sheet. Check secrets & sharing.")
        st.stop()
    st.write("Sheet:", f"**{SHEET_NAME}**")
    st.write("As of:", now_ts())
    refresh = st.button("🔄 Refresh", key="refresh_btn")

# -------------------- LOAD DATA --------------------
scores_transfers = read_tab(ss, "scores_transfers")
scores_youth     = read_tab(ss, "scores_youth")
players_df       = read_sheet_as_df(ss, "players")
raw_matches      = read_sheet_as_df(ss, "raw_matches")

if refresh:
    scores_transfers = read_tab(ss, "scores_transfers")
    scores_youth     = read_tab(ss, "scores_youth")
    players_df       = read_sheet_as_df(ss, "players")
    raw_matches      = read_sheet_as_df(ss, "raw_matches")

# -------------------- HEADER --------------------
st.markdown(f"<div class='djm-card'><div class='djm-kpi'><div class='big'>DJM Scouting Platform</div><div class='label'>Live</div></div><div style='color:#9aa4b2'>Search, score, compare. Upload Excel/CSV to grow the database.</div></div>", unsafe_allow_html=True)
st.write("")

# -------------------- TABS --------------------
tab1, tab2, tab3, tab4 = st.tabs(["Likely Movers", "Youth", "Player Profile", "Admin / Data"])

# ---------- TAB 1: TRANSFERS ----------
with tab1:
    st.subheader("Likely Movers — ranked probabilities")
    if scores_transfers is None:
        st.info("`scores_transfers` tab missing. Generate in Colab (Cell 13).")
    else:
        df = scores_transfers.copy()
        c1, c2, c3, c4 = st.columns(4)
        pos_list = sorted(df["position_group"].dropna().unique()) if "position_group" in df.columns else []
        pos_sel  = c1.multiselect("Position(s)", pos_list, default=pos_list, key="t_pos")
        pmin     = c2.slider("Min probability", 0.0, 1.0, 0.50, 0.01, key="t_pmin")
        search   = c3.text_input("Search name/club", "", key="t_search")
        sort_desc= c4.checkbox("Sort by probability (desc)", True, key="t_sort")

        filt = pd.Series(True, index=df.index)
        if pos_list and "position_group" in df.columns:
            filt &= df["position_group"].isin(pos_sel)
        if "p_move" in df.columns:
            filt &= df["p_move"].fillna(0) >= pmin
        if search:
            s = search.lower()
            name_hit = df.get("player_name", pd.Series("", index=df.index)).astype(str).str.lower().str.contains(s, na=False)
            club_hit = df.get("current_club", pd.Series("", index=df.index)).astype(str).str.lower().str.contains(s, na=False)
            filt &= (name_hit | club_hit)

        dfv = df[filt].copy()
        if sort_desc and "p_move" in dfv.columns:
            dfv = dfv.sort_values("p_move", ascending=False)
        elif set(["position_group","p_move"]).issubset(dfv.columns):
            dfv = dfv.sort_values(["position_group","p_move"], ascending=[True, False])

        show = [c for c in ["player_name","position_group","current_club","p_move",
                            "contract_months_left","buyer_need_index","role_fit",
                            "media_rumor_score","scarcity_index","injury_days_pct"] if c in dfv.columns]
        disp = dfv.copy()
        if "p_move" in disp.columns: disp["p_move"] = disp["p_move"].map(percent)
        if "injury_days_pct" in disp.columns: disp["injury_days_pct"] = disp["injury_days_pct"].map(percent)
        for col in ["buyer_need_index","role_fit","media_rumor_score","scarcity_index"]:
            if col in disp.columns:
                disp[col] = disp[col].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
        st.dataframe(disp[show], use_container_width=True, hide_index=True)

# ---------- TAB 2: YOUTH ----------
with tab2:
    st.subheader("Youth — make-it probabilities")
    if scores_youth is None:
        st.info("`scores_youth` tab missing. Generate in Colab (Cell 15).")
    else:
        df = scores_youth.copy()
        c1, c2, c3, c4 = st.columns(4)
        pos_list = sorted(df["position_group"].dropna().unique()) if "position_group" in df.columns else []
        pos_sel  = c1.multiselect("Position(s)", pos_list, default=pos_list, key="y_pos")
        pmin     = c2.slider("Min probability", 0.0, 1.0, 0.50, 0.01, key="y_pmin")
        search   = c3.text_input("Search name", "", key="y_search")
        sort_desc= c4.checkbox("Sort by probability (desc)", True, key="y_sort")

        filt = pd.Series(True, index=df.index)
        if pos_list and "position_group" in df.columns: filt &= df["position_group"].isin(pos_sel)
        if "p_make_it" in df.columns: filt &= df["p_make_it"].fillna(0) >= pmin
        if search: filt &= df.get("player_name", pd.Series("", index=df.index)).astype(str).str.lower().str.contains(search.lower(), na=False)
        dfv = df[filt].copy()

        if sort_desc and "p_make_it" in dfv.columns:
            dfv = dfv.sort_values("p_make_it", ascending=False)
        elif set(["position_group","p_make_it"]).issubset(dfv.columns):
            dfv = dfv.sort_values(["position_group","p_make_it"], ascending=[True, False])

        show = [c for c in ["player_name","position_group","age","p_make_it",
                            "adj_minutes","availability_pct","role_percentile"] if c in dfv.columns]
        disp = dfv.copy()
        if "p_make_it" in disp.columns: disp["p_make_it"] = disp["p_make_it"].map(percent)
        if "availability_pct" in disp.columns: disp["availability_pct"] = disp["availability_pct"].map(percent)
        if "role_percentile" in disp.columns: disp["role_percentile"] = disp["role_percentile"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
        st.dataframe(disp[show], use_container_width=True, hide_index=True)

# ---------- TAB 3: PLAYER PROFILE ----------
with tab3:
    st.subheader("Player Profile — search & score")
    colA, colB = st.columns([2,1])

    # Build a simple search index from players + raw matches
    candidates = []
    if not players_df.empty and "player_name" in players_df.columns:
        candidates = sorted(players_df["player_name"].dropna().astype(str).unique().tolist())
    if not raw_matches.empty and "player_name" in raw_matches.columns:
        candidates = sorted(set(list(candidates) + raw_matches["player_name"].dropna().astype(str).unique().tolist()))
    q = colA.text_input("Search player by name", "", key="pp_q")
    pick = None
    if q:
        picks = fuzzy_pick(candidates, q, limit=5)
        pick = colA.selectbox("Matches", picks, index=0 if picks else None, key="pp_pick")

    # Optional: choose by tm_id
    tm_pick = colB.text_input("…or paste Transfermarkt URL/ID", "", key="pp_tm")
    tm_id = parse_tm_id(tm_pick)

    if st.button("Load profile", key="pp_load"):
        st.session_state["_pp_go"] = True

    if st.session_state.get("_pp_go"):
        # Resolve tm_id and name
        name = pick or q
        if tm_id and not players_df.empty and "tm_id" in players_df.columns:
            row = players_df.loc[players_df["tm_id"].astype(str)==str(tm_id)].head(1)
            if not row.empty:
                name = row.iloc[0].get("player_name", name)
        st.markdown(f"### {name}")

        # Join latest features → ratings
        feats = build_feature_store(ss)
        ages = players_df[["tm_id","age"]] if ("tm_id" in players_df.columns and "age" in players_df.columns) else pd.DataFrame()
        ratings = compute_ratings_from_features(feats, ages)

        # Filter to this player
        mask = (ratings["player_name"].str.lower()==str(name).lower())
        if tm_id:
            mask |= (ratings["tm_id"].astype(str)==str(tm_id))
        r = ratings.loc[mask].tail(1)

        if r.empty:
            st.info("No stats ingested for this player yet. Upload in **Admin / Data → Upload stats**.")
        else:
            row = r.iloc[0]
            c1, c2, c3, c4 = st.columns(4)
            c1.markdown(f"<div class='djm-card djm-kpi'><div class='big'>{row['overall_now']:.1f}</div><div class='label'>Overall now</div></div>", unsafe_allow_html=True)
            c2.markdown(f"<div class='djm-card djm-kpi'><div class='big'>{row['overall_5yr']:.1f}</div><div class='label'>Projected 5-yr</div></div>", unsafe_allow_html=True)
            c3.markdown(f"<div class='djm-card djm-kpi'><div class='big'>{int(row['minutes_90'])}</div><div class='label'>Minutes</div></div>", unsafe_allow_html=True)
            tm_url = None
            if not players_df.empty and "tm_url" in players_df.columns:
                cand = players_df.loc[players_df["player_name"].str.lower()==str(name).lower()]
                tm_url = cand.iloc[0]["tm_url"] if not cand.empty else None
            mv = fetch_tm_value_eur(tm_url) if tm_url else None
            c4.markdown(f"<div class='djm-card djm-kpi'><div class='big'>{mv if mv else '—'}</div><div class='label'>TM value (best-effort)</div></div>", unsafe_allow_html=True)

            st.markdown("#### Breakdown")
            st.write("This rating is a transparent blend of attack, progression, defence and passing, normalized within your imported dataset.")

# ---------- TAB 4: ADMIN / DATA ----------
with tab4:
    st.subheader("Admin — Players & Stats Ingestion")

    with st.expander("➕ Add / update a player (name + Transfermarkt URL/ID)", expanded=False):
        pname = st.text_input("Player name *", key="adm_name")
        tm_input = st.text_input("Transfermarkt URL or ID *", key="adm_tm")
        pid = st.text_input("Your player_id (optional)", key="adm_pid")
        p_qid = st.text_input("Wikidata Q-ID (optional, e.g., Q11886275)", key="adm_qid")
        dob = st.text_input("DOB (optional, YYYY-MM-DD)", key="adm_dob")
        if st.button("Add / Update player", key="adm_add"):
            tm = parse_tm_id(tm_input)
            if not pname or not tm:
                st.error("Need player name and a valid Transfermarkt URL/ID.")
            else:
                rec = pd.DataFrame([{
                    "player_id": pid, "player_name": pname, "player_qid": p_qid,
                    "dob": dob, "tm_url": tm_input, "tm_id": tm
                }])
                ins, upd = upsert_players(ss, rec)
                st.success(f"Done. Inserted {ins}, Updated {upd}. See `players` tab in Sheet.")

    st.divider()
    st.markdown("### 📥 Upload stats (Excel/CSV) → append to `raw_matches`")
    st.caption("Drop per-match stats like your ‘PlayerStats’ sheet. We auto-map common columns.")

    file = st.file_uploader("Upload .xlsx or .csv", type=["xlsx","csv"], key="adm_upload")
    if file is not None:
        if file.name.lower().endswith(".xlsx"):
            try:
                df_in = pd.read_excel(file, sheet_name=0)
            except Exception:
                st.error("Could not read Excel.")
                df_in = None
        else:
            try:
                df_in = pd.read_csv(file)
            except Exception:
                file.seek(0)
                df_in = pd.read_csv(file, encoding="utf-8", engine="python")

        if df_in is not None and not df_in.empty:
            st.write("Preview of uploaded data:")
            st.dataframe(df_in.head(15), use_container_width=True, hide_index=True)

            # ---- Heuristic mapping for your sample columns ----
            cols = {c.lower(): c for c in df_in.columns}
            def find(*keys):
                for k in keys:
                    for c in cols:
                        if k in c:
                            return cols[c]
                return None

            map_guess = {
                "player_name": find("player", "name"),
                "date": find("date"),
                "competition": find("competition","league"),
                "opponent": find("opponent"),
                "minutes": find("minute"),
                "shots": find("shots"),
                "xg": find("xg"),
                "xa": find("xa"),
                "key_passes": find("key passes"),
                "dribbles_won": find("dribbles won"),
                "progressive_passes": find("progressive passes"),
                "progressive_carries": find("progressive carries"),
                "tackles_won": find("tackles won","tackles"),
                "interceptions": find("interceptions"),
                "aerials_won": find("aerials won"),
                "passes": find("passes /","passes"),
                "passes_accurate": find("unnamed:","accurate"),  # many exports have adjacent 'Unnamed' accurate cols
                "duels_won": find("duels won"),
                "touches": find("touches"),
                "position": find("position"),
            }

            st.write("**Auto-mapped fields** (you can edit below):")
            cols1, cols2, cols3 = st.columns(3)
            keys = list(map_guess.keys())
            for i,k in enumerate(keys):
                (cols1 if i%3==0 else cols2 if i%3==1 else cols3).text_input(k, value=map_guess[k] or "", key=f"map_{k}")

            tm_url_or_id = st.text_input("Transfermarkt URL/ID for these rows (optional — we’ll store tm_id on all rows)", key="adm_tm_rows")
            tm_id_for_rows = parse_tm_id(tm_url_or_id) if tm_url_or_id else None

            if st.button("Append to raw_matches", key="adm_append"):
                # Build output rows
                out_rows = []
                for idx, r in df_in.iterrows():
                    row = {h:"" for h in RAW_MATCHES_HEADERS}
                    # mapping
                    for k in keys:
                        src = st.session_state.get(f"map_{k}", "")
                        if src and src in df_in.columns:
                            row[k] = r[src]
                    row["tm_id"] = tm_id_for_rows or ""
                    out_rows.append(row)
                out_df = pd.DataFrame(out_rows)
                # coerce date if needed
                if "date" in out_df.columns:
                    out_df["date"] = pd.to_datetime(out_df["date"], errors="coerce").dt.date.astype(str)
                added = append_raw_matches(ss, out_df)
                st.success(f"Appended {added} match rows to `raw_matches` in your Google Sheet.")

                # Rebuild ratings preview immediately
                feats = build_feature_store(ss)
                ages = players_df[["tm_id","age"]] if ("tm_id" in players_df.columns and "age" in players_df.columns) else pd.DataFrame()
                ratings = compute_ratings_from_features(feats, ages)
                if not ratings.empty:
                    write_df_to_sheet(ss, "ratings", ratings.reindex(columns=RATINGS_HEADERS))
                    st.info("Rebuilt `ratings` tab.")
                    st.dataframe(ratings.tail(10), use_container_width=True, hide_index=True)

st.caption("All numbers are explainable. Upload more stats to improve ratings. Use Player Profile to sanity-check individual cases.")
