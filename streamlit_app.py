# DJM — SCOUTING & TRANSFER PLATFORM (robust V2)
# - Runs even if rapidfuzz/plotly/sklearn are missing (degrades gracefully).
# - Modern UI, Ratings & Projection, Club Compare, Roles (if sklearn present),
#   Admin ingestion (Excel/CSV -> raw_matches), Players upsert (TM URL/ID).

import streamlit as st
import pandas as pd
import numpy as np
import pytz, re, requests, json
from datetime import datetime
from dateutil import parser as dtparser

# ---------- Optional deps (graceful fallbacks) ----------
# Fuzzy search
try:
    from rapidfuzz import process as _fuzz
    def fuzzy_pick(options, query, limit=8, score_cutoff=65):
        if not options or not query: return []
        hits = _fuzz.extract(query, options, limit=limit, score_cutoff=score_cutoff)
        return [h[0] for h in hits]
except Exception:
    import difflib
    def fuzzy_pick(options, query, limit=8, score_cutoff=0):
        if not options or not query: return []
        return difflib.get_close_matches(query, options, n=limit, cutoff=0.0)

# Plotly charts
try:
    import plotly.graph_objects as go
    PLOTLY_OK = True
except Exception:
    PLOTLY_OK = False

# Clustering (roles)
try:
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    from sklearn.decomposition import PCA
    SKLEARN_OK = True
except Exception:
    SKLEARN_OK = False

# ---------- Google Sheets ----------
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# ---------- App config & theme ----------
st.set_page_config(page_title="DJM — Scouting & Transfers", layout="wide", initial_sidebar_state="expanded")
st.markdown("""
<style>
:root { --accent:#5B8CFF; --bg:#0B1020; --card:#121933; --muted:#9aa4b2; --good:#00E88F; --warn:#F2C94C; --bad:#FF6B6B; }
.stApp { background: radial-gradient(1200px 800px at 10% 0%, #0B1020 0%, #0A1228 40%, #0B1020 100%); color:#E9F1FF; }
.djm-card { background:var(--card); border-radius:16px; padding:18px; border:1px solid rgba(255,255,255,.06); box-shadow:0 20px 40px rgba(0,0,0,.35); }
.djm-kpi .big { font-size:40px; font-weight:900; letter-spacing:.2px; }
.djm-kpi .label { color:var(--muted); text-transform:uppercase; font-size:12px; letter-spacing:.3px; }
.stButton>button { border-radius:12px; padding:8px 14px; font-weight:600; background:linear-gradient(120deg, #5B8CFF, #6BD6FF); color:#0B1020; border:0; }
[data-testid="stDataFrame"] { border-radius:12px; overflow:hidden; border:1px solid rgba(255,255,255,.08); }
</style>
""", unsafe_allow_html=True)

# ---------- Settings ----------
SHEET_NAME = st.secrets.get("sheet_name", "DJM_Input")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

PLAYERS_HEADERS = [
    "player_id","player_name","player_qid","dob","age","citizenships",
    "height_cm","positions","position_group","current_club","shirt_number",
    "tm_url","tm_id"
]

RAW_MATCHES_HEADERS = [
    "tm_id","player_name","date","competition","opponent","minutes",
    "shots","xg","xa","key_passes",
    "progressive_passes","progressive_carries",
    "dribbles_won","tackles_won","interceptions","aerials_won",
    "passes","passes_accurate","touches","duels_won","position"
]

FEATURE_STORE_COLS = [
    "tm_id","player_name","minutes",
    "xg_p90","xa_p90","shots_p90","kp_p90",
    "prog_pass_p90","prog_carry_p90","dribbles_p90",
    "tackles_p90","inter_p90","aerials_p90","pass_acc"
]

RATINGS_HEADERS = [
    "tm_id","player_name","position_group","age",
    "overall_now","overall_5yr","uncert_low","uncert_high",
    "minutes_90","availability","role_fit","market_signal","updated_at"
]

DEFAULT_SETTINGS = {
    "w_attack": 0.35, "w_progression": 0.25, "w_defence": 0.20, "w_passing": 0.20,
    "age_curve_u21": 1.10, "age_curve_22_24": 1.05, "age_curve_25_28": 1.00,
    "age_curve_29_31": 0.97, "age_curve_32_34": 0.94, "age_curve_35p": 0.90,
    "projection_u22": 1.10, "projection_23_26": 1.04, "projection_30p": 0.98,
    "minutes_shrink_lt900": 0.70, "minutes_shrink_900_1799": 0.85,
    "tm_value_fetch": True
}

# ---------- GSheet I/O ----------
@st.cache_resource(show_spinner=False)
def connect_gsheet():
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME)

def get_or_create_ws(ss, name, headers=None):
    try:
        ws = ss.worksheet(name)
    except Exception:
        ws = ss.add_worksheet(title=name, rows=2000, cols=max(30, len(headers or [])))
        if headers:
            ws.update('A1', [headers])
    return ws

def read_sheet(ss, name):
    try:
        ws = ss.worksheet(name)
    except Exception:
        return pd.DataFrame()
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    if df is None:
        return pd.DataFrame()
    return df.dropna(how="all")

def write_sheet(ss, name, df):
    ws = get_or_create_ws(ss, name, headers=list(df.columns))
    ws.clear()
    set_with_dataframe(ws, df, row=1, col=1, include_index=False, include_column_header=True)

def now_ts():
    tz = pytz.timezone("Europe/Rome")
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M %Z")

# ---------- Settings persist ----------
def load_settings(ss):
    df = read_sheet(ss, "settings")
    if df.empty: return DEFAULT_SETTINGS.copy()
    s = DEFAULT_SETTINGS.copy()
    for _, r in df.iterrows():
        k = str(r.get("key","")).strip(); v = r.get("value","")
        if not k: continue
        try: s[k] = json.loads(v)
        except Exception:
            try: s[k] = float(v)
            except Exception: s[k] = v
    return s

def save_settings(ss, settings):
    rows = [{"key":k,"value":json.dumps(v)} for k,v in settings.items()]
    write_sheet(ss, "settings", pd.DataFrame(rows, columns=["key","value"]))

# ---------- Helpers ----------
def parse_tm_id(url_or_id: str):
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

def percent(x):
    try: return f"{100*float(x):.1f}%"
    except Exception: return ""

def best_effort_tm_value(tm_url: str, enabled=True):
    if not enabled or not tm_url: return None
    try:
        r = requests.get(tm_url, headers={"User-Agent":"Mozilla/5.0"}, timeout=8)
        if r.status_code != 200: return None
        m = re.search(r"Market value[^€£]*([€£]\s?[\d\.,]+[mk]?)", r.text, re.I)
        return m.group(1).replace(" ","") if m else None
    except Exception:
        return None

def norm01(s: pd.Series):
    if s is None or s.empty: return s
    lo, hi = np.nanpercentile(s, 5), np.nanpercentile(s, 95)
    return (s - lo) / (hi - lo + 1e-9)

def safe_div(a,b):
    try:
        if pd.isna(a) or pd.isna(b) or float(b)==0: return np.nan
        return float(a)/float(b)
    except Exception:
        return np.nan

# ---------- Upserts / ingestion ----------
def upsert_players(ss, df_in: pd.DataFrame):
    _ = get_or_create_ws(ss, "players", headers=PLAYERS_HEADERS)
    existing = read_sheet(ss, "players")
    if existing.empty: existing = pd.DataFrame(columns=PLAYERS_HEADERS)

    df = df_in.copy()
    for c in PLAYERS_HEADERS:
        if c not in df.columns: df[c] = pd.NA

    df["tm_id"] = df.apply(lambda r: r["tm_id"] if pd.notna(r["tm_id"]) and str(r["tm_id"]).strip()!=""
                           else parse_tm_id(str(r.get("tm_url",""))), axis=1)
    # auto position_group if missing
    for i, r in df.iterrows():
        if not str(r.get("position_group","")).strip():
            df.at[i, "position_group"] = position_group_from_text(str(r.get("positions","")))
    for c in ["player_id","player_name","player_qid","dob","tm_url","tm_id","position_group","positions","current_club"]:
        df[c] = df[c].astype(str).str.strip().replace({"None":"","nan":""})

    existing = existing.reindex(columns=PLAYERS_HEADERS).fillna("")
    ex_by_tm = {str(t): i for i,t in enumerate(existing["tm_id"].astype(str)) if t}
    ex_by_name_dob = {(str(n).lower(), str(d)): i for i,(n,d)
                      in enumerate(zip(existing["player_name"].astype(str), existing["dob"].astype(str))) if n}

    ins, upd = 0, 0
    for _, r in df.iterrows():
        tm_id = str(r["tm_id"]) if pd.notna(r["tm_id"]) else ""
        idx = ex_by_tm.get(tm_id) if tm_id else None
        if idx is None:
            idx = ex_by_name_dob.get((str(r["player_name"]).lower(), str(r["dob"])))
        if idx is None:
            row = {h: str(r[h]) if h in r.index and pd.notna(r[h]) else "" for h in PLAYERS_HEADERS}
            existing = pd.concat([existing, pd.DataFrame([row])], ignore_index=True)
            ins += 1
        else:
            for h in PLAYERS_HEADERS:
                val = r[h] if h in r.index else ""
                if pd.notna(val) and str(val)!="":
                    existing.iat[idx, existing.columns.get_loc(h)] = str(val)
            upd += 1

    write_sheet(ss, "players", existing.reindex(columns=PLAYERS_HEADERS))
    return ins, upd

def append_raw_matches(ss, df_rows: pd.DataFrame):
    ws = get_or_create_ws(ss, "raw_matches", headers=RAW_MATCHES_HEADERS)
    existing = read_sheet(ss, "raw_matches")
    if existing.empty: existing = pd.DataFrame(columns=RAW_MATCHES_HEADERS)
    out = pd.concat([existing, df_rows.reindex(columns=RAW_MATCHES_HEADERS)], ignore_index=True)
    write_sheet(ss, "raw_matches", out)
    return len(df_rows)

# ---------- Feature store / ratings ----------
def rebuild_feature_store(ss):
    raw = read_sheet(ss, "raw_matches")
    if raw.empty:
        write_sheet(ss, "feature_store", pd.DataFrame(columns=FEATURE_STORE_COLS))
        return
    num_cols = ["minutes","xg","xa","shots","key_passes","progressive_passes","progressive_carries",
                "dribbles_won","tackles_won","interceptions","aerials_won","passes","passes_accurate","touches","duels_won"]
    for c in num_cols:
        if c in raw.columns: raw[c] = pd.to_numeric(raw[c], errors="coerce")
    g = raw.groupby(["tm_id","player_name"], dropna=False).agg({
        "minutes":"sum","xg":"sum","xa":"sum","shots":"sum","key_passes":"sum",
        "progressive_passes":"sum","progressive_carries":"sum","dribbles_won":"sum",
        "tackles_won":"sum","interceptions":"sum","aerials_won":"sum",
        "passes":"sum","passes_accurate":"sum"
    }).reset_index()
    mins = g["minutes"].replace({0:np.nan})
    feats = pd.DataFrame({
        "tm_id": g["tm_id"], "player_name": g["player_name"], "minutes": g["minutes"],
        "xg_p90": g["xg"]/mins*90, "xa_p90": g["xa"]/mins*90, "shots_p90": g["shots"]/mins*90, "kp_p90": g["key_passes"]/mins*90,
        "prog_pass_p90": g["progressive_passes"]/mins*90, "prog_carry_p90": g["progressive_carries"]/mins*90,
        "dribbles_p90": g["dribbles_won"]/mins*90, "tackles_p90": g["tackles_won"]/mins*90,
        "inter_p90": g["interceptions"]/mins*90, "aerials_p90": g["aerials_won"]/mins*90,
        "pass_acc": g.apply(lambda r: safe_div(r["passes_accurate"], r["passes"]), axis=1)
    })
    write_sheet(ss, "feature_store", feats.reindex(columns=FEATURE_STORE_COLS))

def age_mult(age, s):  # settings map
    if pd.isna(age): return 1.0
    a = float(age)
    if a <= 21: return s["age_curve_u21"]
    if a <= 24: return s["age_curve_22_24"]
    if a <= 28: return s["age_curve_25_28"]
    if a <= 31: return s["age_curve_29_31"]
    if a <= 34: return s["age_curve_32_34"]
    return s["age_curve_35p"]

def proj_growth(age, s):
    if pd.isna(age): return 1.03
    a = float(age)
    if a <= 22: return s["projection_u22"]
    if a <= 26: return s["projection_23_26"]
    if a >= 30: return s["projection_30p"]
    return 1.00

def mins_shrink(m, s):
    m = 0 if pd.isna(m) else float(m)
    if m < 900: return s["minutes_shrink_lt900"]
    if m < 1800: return s["minutes_shrink_900_1799"]
    return 1.00

def rebuild_ratings(ss, settings):
    feats = read_sheet(ss, "feature_store")
    players = read_sheet(ss, "players")
    if feats.empty:
        write_sheet(ss, "ratings", pd.DataFrame(columns=RATINGS_HEADERS)); return
    att = norm01(0.6*feats["xg_p90"].fillna(0) + 0.4*feats["xa_p90"].fillna(0) + 0.2*feats["shots_p90"].fillna(0) + 0.4*feats["kp_p90"].fillna(0))
    prog = norm01(0.6*feats["prog_pass_p90"].fillna(0) + 0.4*feats["prog_carry_p90"].fillna(0) + 0.2*feats["dribbles_p90"].fillna(0))
    dfn = norm01(0.6*feats["tackles_p90"].fillna(0) + 0.6*feats["inter_p90"].fillna(0) + 0.2*feats["aerials_p90"].fillna(0))
    pas = norm01(feats["pass_acc"].fillna(0))
    base01 = (settings["w_attack"]*att + settings["w_progression"]*prog + settings["w_defence"]*dfn + settings["w_passing"]*pas).clip(0,1)
    lookup = players[["tm_id","age","position_group"]] if not players.empty else pd.DataFrame(columns=["tm_id","age","position_group"])
    df = feats[["tm_id","player_name","minutes"]].copy().join(base01.rename("base01"))
    df = df.merge(lookup, on="tm_id", how="left")
    now = (df["base01"] * df["age"].map(lambda a: age_mult(a, settings)).fillna(1.0)).clip(0,1)*100.0
    proj5 = (now/100.0 * df["age"].map(lambda a: proj_growth(a, settings)).fillna(1.03) * df["minutes"].map(lambda m: mins_shrink(m, settings))).clip(0,1)*100.0
    out = pd.DataFrame({
        "tm_id": df["tm_id"], "player_name": df["player_name"],
        "position_group": df["position_group"].fillna(""), "age": df["age"],
        "overall_now": now.round(1), "overall_5yr": proj5.round(1),
        "uncert_low": (now*0.90).round(1), "uncert_high": (now*1.10).clip(0,100).round(1),
        "minutes_90": df["minutes"].fillna(0).round(0),
        "availability": np.nan, "role_fit": np.nan, "market_signal": np.nan,
        "updated_at": now_ts()
    })
    write_sheet(ss, "ratings", out.reindex(columns=RATINGS_HEADERS))

# ---------- Roles (only if sklearn exists) ----------
ROLE_FEATURES = ["xg_p90","xa_p90","shots_p90","kp_p90","prog_pass_p90","prog_carry_p90","dribbles_p90","tackles_p90","inter_p90","aerials_p90","pass_acc"]
ROLE_LABELS = {
    "FW": ["Channel 9","Target 9","Wide Inside Fwd","Winger","Second Striker"],
    "MF": ["Box-to-Box 8","Deep Playmaker 6","Ball-Winning 6/8","Advanced 8/10","Progressor 8"],
    "DF": ["Ball-Playing CB","Stopper CB","Inverted FB","Overlapping FB","Wing-Back"],
    "GK": ["Sweeper GK","Shot-Stopper GK"]
}

def rebuild_roles(ss, n_clusters_per_group=4):
    if not SKLEARN_OK:
        write_sheet(ss, "roles", pd.DataFrame(columns=["tm_id","player_name","position_group","role_cluster","role_label","pca_x","pca_y"]))
        return
    feats = read_sheet(ss, "feature_store")
    players = read_sheet(ss, "players")
    if feats.empty or players.empty:
        write_sheet(ss, "roles", pd.DataFrame(columns=["tm_id","player_name","position_group","role_cluster","role_label","pca_x","pca_y"]))
        return
    df = feats.merge(players[["tm_id","position_group"]], on="tm_id", how="left")
    out = []
    for pg in ["FW","MF","DF","GK"]:
        sub = df[df["position_group"]==pg].copy()
        if sub.empty: continue
        X = sub[ROLE_FEATURES].fillna(0.0).values
        X = StandardScaler().fit_transform(X)
        k = min(n_clusters_per_group, max(1, len(sub)//8))
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
    write_sheet(ss, "roles", roles)

# ---------- Sidebar / connect ----------
with st.sidebar:
    st.header("Data source")
    try:
        ss = connect_gsheet()
        st.success("Connected ✅")
    except Exception:
        st.error("Cannot open Google Sheet. Check `.streamlit/secrets.toml` & sharing.")
        st.stop()
    st.caption(f"Sheet: **{SHEET_NAME}** · {now_ts()}")
    refresh = st.button("🔄 Refresh", use_container_width=True)

players      = read_sheet(ss, "players")
raw_matches  = read_sheet(ss, "raw_matches")
feats        = read_sheet(ss, "feature_store")
ratings      = read_sheet(ss, "ratings")
roles        = read_sheet(ss, "roles")
settings     = load_settings(ss)

if refresh:
    players = read_sheet(ss, "players")
    raw_matches = read_sheet(ss, "raw_matches")
    feats = read_sheet(ss, "feature_store")
    ratings = read_sheet(ss, "ratings")
    roles = read_sheet(ss, "roles")
    settings = load_settings(ss)

# ---------- Header ----------
st.markdown(
    "<div class='djm-card'><div style='font-size:28px;font-weight:800;'>DJM Scouting & Transfer Intelligence</div>"
    "<div style='color:#9aa4b2'>Search, score, roles, club fit. Upload Excel/CSV to grow your dataset.</div></div>",
    unsafe_allow_html=True
)
st.write("")

# ---------- Tabs ----------
tab_dash, tab_profile, tab_club, tab_roles, tab_admin, tab_settings = st.tabs(
    ["Dashboard", "Search / Player Profile", "Club Profile & Compare", "Roles", "Admin / Data", "Settings"]
)

# ===== DASHBOARD =====
with tab_dash:
    def kpi(label, val):
        st.markdown(f"<div class='djm-card djm-kpi'><div class='big'>{val}</div><div class='label'>{label}</div></div>", unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    kpi("Players", len(players) if not players.empty else 0)
    kpi("Match rows", len(raw_matches) if not raw_matches.empty else 0)
    kpi("Rated players", len(ratings["tm_id"].unique()) if not ratings.empty else 0)
    kpi("Last build", settings.get("last_build","—"))
    st.subheader("Top ratings (snapshot)")
    if ratings.empty:
        st.info("No ratings yet. Go to **Admin / Data** to rebuild after uploading stats.")
    else:
        st.dataframe(
            ratings.sort_values("overall_now", ascending=False).head(30)[
                ["player_name","position_group","age","overall_now","overall_5yr","minutes_90"]
            ],
            use_container_width=True, hide_index=True
        )

# ===== PLAYER PROFILE =====
with tab_profile:
    st.subheader("Search & Player Profile")
    all_names = sorted(set(
        list(players.get("player_name", pd.Series([],dtype=str)).dropna().astype(str)) +
        list(ratings.get("player_name", pd.Series([],dtype=str)).dropna().astype(str))
    ))
    q = st.text_input("Type a player name", "")
    picks = fuzzy_pick(all_names, q, limit=8) if q else []
    name = st.selectbox("Pick", options=picks, index=0 if picks else None, placeholder="Select…")
    tm_input = st.text_input("…or paste Transfermarkt URL/ID", "")
    tm_id = parse_tm_id(tm_input)
    if st.button("Load profile", type="primary"):
        st.session_state["_pp"] = {"name": name or q, "tm_id": tm_id}

    if st.session_state.get("_pp"):
        target_name = st.session_state["_pp"]["name"]
        target_tm   = st.session_state["_pp"]["tm_id"]
        st.markdown(f"### {target_name}")
        rr = pd.DataFrame()
        if not ratings.empty:
            rr = ratings[(ratings["player_name"].str.lower()==str(target_name).lower()) |
                         (ratings["tm_id"].astype(str)==str(target_tm))].tail(1)
        if rr.empty:
            st.info("No ratings yet for this player. Upload stats in **Admin / Data** and rebuild.")
        else:
            row = rr.iloc[0]
            if PLOTLY_OK:
                # Gauge charts
                def gauge(value, title):
                    fig = go.Figure(go.Indicator(mode="gauge+number", value=float(value),
                                                 number={'font': {'size': 30}},
                                                 gauge={'axis': {'range': [0, 100]},
                                                        'bar': {'color': '#5B8CFF'},
                                                        'bgcolor': "#0B1020",
                                                        'borderwidth': 1,'bordercolor': "rgba(255,255,255,.15)"}))
                    fig.update_layout(height=220, margin=dict(l=10,r=10,t=30,b=10), paper_bgcolor="rgba(0,0,0,0)", title=title)
                    st.plotly_chart(fig, use_container_width=True, theme=None)
                c1, c2, c3 = st.columns(3)
                gauge(row["overall_now"], "Overall now")
                gauge(row["overall_5yr"], "Projected 5-yr")
                # minutes bar
                st.markdown("<div class='djm-card'>", unsafe_allow_html=True)
                st.progress(min(1.0, float(row["minutes_90"] or 0)/3000.0), text=f"Minutes {int(row['minutes_90'])}")
                st.markdown("</div>", unsafe_allow_html=True)
            else:
                c1, c2, c3 = st.columns(3)
                c1.metric("Overall now", f"{row['overall_now']:.1f}")
                c2.metric("Projected 5-yr", f"{row['overall_5yr']:.1f}")
                c3.metric("Minutes", f"{int(row['minutes_90'])}")

            tm_url = None
            if not players.empty and "tm_url" in players.columns:
                cand = players[players["player_name"].str.lower()==str(target_name).lower()]
                tm_url = cand.iloc[0]["tm_url"] if not cand.empty else None
            if settings.get("tm_value_fetch", True):
                mv = best_effort_tm_value(tm_url, True)
                st.caption(f"TM value (best-effort): {mv or '—'}")

            # Radar blocks if features exist
            f = feats[(feats["player_name"].str.lower()==str(target_name).lower()) |
                      (feats["tm_id"].astype(str)==str(target_tm))].tail(1)
            if not f.empty and PLOTLY_OK:
                blocks = {
                    "Attack": float(100*norm01(0.6*f["xg_p90"]+0.4*f["xa_p90"]+0.2*f["shots_p90"]+0.4*f["kp_p90"]).iloc[0]),
                    "Progress": float(100*norm01(0.6*f["prog_pass_p90"]+0.4*f["prog_carry_p90"]+0.2*f["dribbles_p90"]).iloc[0]),
                    "Defence": float(100*norm01(0.6*f["tackles_p90"]+0.6*f["inter_p90"]+0.2*f["aerials_p90"]).iloc[0]),
                    "Passing": float(100*norm01(f["pass_acc"]).iloc[0]),
                }
                labels = list(blocks.keys()) + [list(blocks.keys())[0]]
                vals = list(blocks.values()) + [list(blocks.values())[0]]
                fig = go.Figure()
                fig.add_trace(go.Scatterpolar(r=vals, theta=labels, fill='toself', name='Score', line=dict(width=2)))
                fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 100])), showlegend=False,
                                  height=360, margin=dict(l=10,r=10,t=30,b=10), paper_bgcolor="rgba(0,0,0,0)", title="Skill blend")
                st.plotly_chart(fig, use_container_width=True, theme=None)

# ===== CLUB PROFILE & COMPARE =====
with tab_club:
    st.subheader("Club Profile & Compare")
    rosters = read_sheet(ss, "club_rosters")
    if rosters.empty:
        st.info("Upload `club_rosters` in Admin (columns: tm_id, player_name, club_name, position_group, minutes).")
    else:
        clubs = sorted(rosters["club_name"].dropna().astype(str).unique())
        club = st.selectbox("Club", clubs)
        view = rosters[rosters["club_name"]==club].merge(ratings[["tm_id","overall_now","overall_5yr"]], on="tm_id", how="left")
        c1, c2 = st.columns(2)
        c1.dataframe(view[["player_name","position_group","minutes","overall_now","overall_5yr"]]
                     .sort_values("overall_now", ascending=False),
                     use_container_width=True, hide_index=True)
        if PLOTLY_OK and not view["overall_now"].dropna().empty:
            fig = go.Figure()
            fig.add_trace(go.Box(y=view["overall_now"].dropna(), name="XI band", boxpoints='outliers'))
            fig.update_layout(height=320, paper_bgcolor="rgba(0,0,0,0)")
            c2.plotly_chart(fig, use_container_width=True)
        st.markdown("#### Compare a target")
        target = st.text_input("Player name", "")
        if target:
            r_target = ratings[ratings["player_name"].str.lower()==target.lower()].tail(1)
            if r_target.empty:
                st.warning("No rating for that player.")
            else:
                t_now = float(r_target.iloc[0]["overall_now"])
                st.write(f"**{target} now:** {t_now:.1f}")
                band = view["overall_now"].dropna()
                if not band.empty:
                    q1, q3 = band.quantile(0.25), band.quantile(0.75)
                    msg = "inside XI band" if q1 <= t_now <= q3 else ("above XI band" if t_now > q3 else "below XI band")
                    st.success(f"Fit vs {club}: **{msg}** (IQR {q1:.1f}–{q3:.1f})")

# ===== ROLES =====
with tab_roles:
    st.subheader("Roles & Archetypes")
    if not SKLEARN_OK:
        st.info("`scikit-learn` not available. Install from requirements to enable clustering.")
    if st.button("Rebuild role clusters"):
        rebuild_roles(ss)
        roles = read_sheet(ss, "roles")
        st.success("Roles rebuilt.")
    if roles.empty:
        st.info("No roles yet.")
    else:
        pg = st.selectbox("Position group", ["FW","MF","DF","GK"])
        rview = roles[roles["position_group"]==pg]
        if rview.empty:
            st.info("No players for this group.")
        else:
            if PLOTLY_OK:
                fig = go.Figure()
                for lab, grp in rview.groupby("role_label"):
                    fig.add_trace(go.Scatter(x=grp["pca_x"], y=grp["pca_y"], mode="markers", name=lab, text=grp["player_name"]))
                fig.update_layout(height=480, paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig, use_container_width=True)
            st.dataframe(rview[["player_name","role_label","role_cluster"]]
                         .sort_values(["role_label","player_name"]), use_container_width=True, hide_index=True)

# ===== ADMIN / DATA =====
with tab_admin:
    st.subheader("Admin — Players & Stats")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Add / update a player")
        pname = st.text_input("Player name *", key="adm_name")
        tm_in = st.text_input("Transfermarkt URL/ID *", key="adm_tm")
        pos_txt = st.text_input("Positions (e.g., LW/ST or CB)", key="adm_pos")
        club = st.text_input("Current club (optional)", key="adm_club")
        dob = st.text_input("DOB (YYYY-MM-DD) (optional)", key="adm_dob")
        if st.button("Upsert player", key="adm_up"):
            tm = parse_tm_id(tm_in)
            if not pname or not tm:
                st.error("Need player name + valid Transfermarkt URL/ID.")
            else:
                rec = pd.DataFrame([{
                    "player_name":pname, "tm_url":tm_in, "tm_id":tm,
                    "positions":pos_txt, "position_group": position_group_from_text(pos_txt),
                    "current_club": club, "dob": dob
                }])
                ins, upd = upsert_players(ss, rec)
                st.success(f"Inserted {ins}, Updated {upd}. See `players` sheet.")

        st.divider()
        st.markdown("### Upload player match stats (Excel/CSV) → `raw_matches`")
        upload = st.file_uploader("Upload .xlsx or .csv", type=["xlsx","csv"], key="adm_file")
        tm_rows = st.text_input("Transfermarkt URL/ID for these rows (optional)", key="adm_tm_rows")
        tm_rows_id = parse_tm_id(tm_rows) if tm_rows else None
        if upload is not None:
            df_in = None
            try:
                df_in = pd.read_excel(upload, sheet_name=0) if upload.name.lower().endswith(".xlsx") else pd.read_csv(upload)
            except Exception:
                upload.seek(0)
                try: df_in = pd.read_csv(upload, encoding="utf-8", engine="python")
                except Exception: st.error("Could not read file.")
            if df_in is not None and not df_in.empty:
                st.dataframe(df_in.head(15), use_container_width=True, hide_index=True)
                cols = {c.lower(): c for c in df_in.columns}
                def find(*keys):
                    for k in keys:
                        for c in cols:
                            if k in c:
                                return cols[c]
                    return ""
                st.markdown("**Field mapping (edit if needed):**")
                grid = [
                    ("player_name",("player","name")), ("date",("date","match date")),
                    ("competition",("competition","league")), ("opponent",("opponent","rival")),
                    ("minutes",("minute","min")), ("shots",("shots",)),
                    ("xg",("xg",)), ("xa",("xa",)), ("key_passes",("key passes","key_pass")),
                    ("progressive_passes",("progressive passes","prog pass")),
                    ("progressive_carries",("progressive carries","prog carr")),
                    ("dribbles_won",("dribbles won","dribble")), ("tackles_won",("tackles won","tackle")),
                    ("interceptions",("interceptions",)), ("aerials_won",("aerials won","aerial")),
                    ("passes",("passes /","passes")), ("passes_accurate",("accurate","unnamed")),
                    ("touches",("touches",)), ("duels_won",("duels won","duel")), ("position",("position","pos"))
                ]
                m = {}
                cA, cB, cC = st.columns(3)
                for i,(k,keys) in enumerate(grid):
                    default = find(*keys)
                    m[k] = (cA if i%3==0 else cB if i%3==1 else cC).text_input(k, value=default, key=f"map_{k}")
                if st.button("Append to raw_matches", key="append_raw"):
                    rows = []
                    for _, r in df_in.iterrows():
                        row = {h:"" for h in RAW_MATCHES_HEADERS}
                        for k in m:
                            src = st.session_state.get(f"map_{k}", "")
                            if src and src in df_in.columns:
                                row[k] = r[src]
                        if tm_rows_id: row["tm_id"] = tm_rows_id
                        if row.get("date",""):
                            try: row["date"] = dtparser.parse(str(row["date"])).date().isoformat()
                            except Exception: pass
                        rows.append(row)
                    added = append_raw_matches(ss, pd.DataFrame(rows))
                    st.success(f"Appended {added} rows to `raw_matches`.")

    with col2:
        st.markdown("### Build Feature Store & Ratings")
        if st.button("Rebuild feature_store"):
            rebuild_feature_store(ss)
            st.success("Feature store rebuilt.")
        if st.button("Rebuild ratings", type="primary"):
            rebuild_ratings(ss, settings)
            settings["last_build"] = now_ts()
            save_settings(ss, settings)
            st.success("Ratings rebuilt. See `ratings` tab.")
        st.divider()
        st.markdown("### Upload club roster CSV → `club_rosters`")
        st.caption("Required columns: tm_id, player_name, club_name, position_group, minutes")
        roster = st.file_uploader("Upload roster CSV", type=["csv"], key="adm_roster")
        if roster is not None:
            try:
                df_r = pd.read_csv(roster)
                need = {"tm_id","player_name","club_name","position_group","minutes"}
                if not need.issubset(set(df_r.columns)):
                    st.error(f"Missing columns: {need - set(df_r.columns)}")
                else:
                    write_sheet(ss, "club_rosters", df_r[["tm_id","player_name","club_name","position_group","minutes"]])
                    st.success("club_rosters updated.")
            except Exception as e:
                st.error(f"Failed to read roster: {e}")

# ===== SETTINGS =====
with tab_settings:
    st.subheader("Weights & Toggles")
    w1, w2 = st.columns(2)
    with w1:
        st.markdown("**Block weights**")
        settings["w_attack"] = st.slider("Attack", 0.0, 1.0, float(settings["w_attack"]), 0.01)
        settings["w_progression"] = st.slider("Progression", 0.0, 1.0, float(settings["w_progression"]), 0.01)
        settings["w_defence"] = st.slider("Defence", 0.0, 1.0, float(settings["w_defence"]), 0.01)
        settings["w_passing"] = st.slider("Passing", 0.0, 1.0, float(settings["w_passing"]), 0.01)
    with w2:
        st.markdown("**Age curve & projection**")
        settings["age_curve_u21"] = st.slider("Age ≤21", 0.8, 1.3, float(settings["age_curve_u21"]), 0.01)
        settings["age_curve_22_24"] = st.slider("22–24", 0.8, 1.3, float(settings["age_curve_22_24"]), 0.01)
        settings["age_curve_25_28"] = st.slider("25–28", 0.8, 1.3, float(settings["age_curve_25_28"]), 0.01)
        settings["age_curve_29_31"] = st.slider("29–31", 0.7, 1.2, float(settings["age_curve_29_31"]), 0.01)
        settings["age_curve_32_34"] = st.slider("32–34", 0.7, 1.2, float(settings["age_curve_32_34"]), 0.01)
        settings["age_curve_35p"] = st.slider("35+", 0.6, 1.1, float(settings["age_curve_35p"]), 0.01)
        settings["projection_u22"] = st.slider("Projection ≤22", 0.9, 1.3, float(settings["projection_u22"]), 0.01)
        settings["projection_23_26"] = st.slider("Projection 23–26", 0.9, 1.2, float(settings["projection_23_26"]), 0.01)
        settings["projection_30p"] = st.slider("Projection ≥30", 0.8, 1.1, float(settings["projection_30p"]), 0.01)
        settings["minutes_shrink_lt900"] = st.slider("Shrink <900 mins", 0.5, 1.0, float(settings["minutes_shrink_lt900"]), 0.01)
        settings["minutes_shrink_900_1799"] = st.slider("Shrink 900–1799", 0.6, 1.0, float(settings["minutes_shrink_900_1799"]), 0.01)
        settings["tm_value_fetch"] = st.toggle("Best-effort TM value fetch", value=bool(settings.get("tm_value_fetch", True)))
    if st.button("Save settings", type="primary"):
        save_settings(ss, settings)
        st.success("Saved. Rebuild ratings to apply changes.")

st.caption("DJM © — Automated, explainable scouting. Upload stats → roles → ratings → deals.")
