import streamlit as st
import pandas as pd
import numpy as np
import pytz
from datetime import datetime
import re

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

st.set_page_config(page_title="DJM Transfers & Youth — Free MVP",
                   layout="wide")

# ---------- SETTINGS ----------
SHEET_NAME = st.secrets.get("sheet_name", "DJM_Input")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",   # read/write Sheets
    "https://www.googleapis.com/auth/drive.readonly", # optional
]

# ---------- GOOGLE SHEET ----------
@st.cache_resource(show_spinner=False)
def connect_gsheet():
    creds_info = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME)

def read_tab(ss, tab):
    """Read a tab into a DataFrame; coerce key numeric columns if present."""
    try:
        ws = ss.worksheet(tab)
    except Exception:
        return None
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    if df is None or df.empty:
        return None
    df = df.dropna(how="all").reset_index(drop=True)
    for c in df.columns:
        if c in {
            "p_move","p_make_it","contract_months_left","buyer_need_index",
            "role_fit","media_rumor_score","scarcity_index","injury_days_pct",
            "availability_pct","adj_minutes","role_percentile"
        }:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def percent(x):
    try:
        return f"{100*float(x):.1f}%"
    except Exception:
        return ""

# ---------- ADMIN HELPERS (players sheet) ----------
PLAYERS_HEADERS = [
    "player_id","player_name","player_qid","dob","age","citizenships",
    "height_cm","positions","current_club","shirt_number",
    "tm_url","tm_id"
]

def get_or_create_ws(ss, name, headers=None):
    """Return a worksheet; create with headers if missing."""
    try:
        ws = ss.worksheet(name)
    except Exception:
        ws = ss.add_worksheet(title=name, rows=1000, cols=max(20, len(headers or [])))
        if headers:
            ws.update('A1', [headers])
    return ws

def read_sheet_as_df(ss, name):
    """Read a sheet to DataFrame; return empty DF if missing."""
    try:
        ws = ss.worksheet(name)
    except Exception:
        return pd.DataFrame()
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    if df is None:
        return pd.DataFrame()
    return df.dropna(how="all")

def write_df_to_sheet(ss, name, df):
    """Overwrite entire sheet with DataFrame (includes headers)."""
    ws = get_or_create_ws(ss, name, headers=list(df.columns))
    ws.clear()
    set_with_dataframe(ws, df, row=1, col=1, include_index=False, include_column_header=True)

def parse_transfermarkt_id(url_or_id: str):
    """
    Accept a Transfermarkt player URL or numeric ID; return tm_id or None.
    Examples:
      https://www.transfermarkt.com/.../spieler/418560
      418560
    """
    if not url_or_id:
        return None
    s = str(url_or_id).strip()
    m = re.search(r"/spieler/(\d+)", s)
    if m:
        return m.group(1)
    m = re.fullmatch(r"\d{3,}", s)
    if m:
        return m.group(0)
    nums = re.findall(r"\d{3,}", s)
    return nums[-1] if nums else None

def upsert_players(ss, records: pd.DataFrame) -> tuple[int, int]:
    """
    Upsert rows into 'players' by tm_id (primary) else (player_name, dob).
    Accepts any subset of PLAYERS_HEADERS; missing cols are created as blanks.
    Returns: (inserted_count, updated_count)
    """
    _ = get_or_create_ws(ss, "players", headers=PLAYERS_HEADERS)

    existing = read_sheet_as_df(ss, "players")
    if existing.empty:
        existing = pd.DataFrame(columns=PLAYERS_HEADERS)

    df = records.copy()
    # Ensure all expected columns exist
    for col in PLAYERS_HEADERS:
        if col not in df.columns:
            df[col] = pd.NA

    # Parse tm_id if missing but tm_url provided
    df["tm_id"] = df.apply(
        lambda r: r["tm_id"] if pd.notna(r["tm_id"]) and str(r["tm_id"]).strip() != ""
        else parse_transfermarkt_id(str(r.get("tm_url", ""))),
        axis=1
    )

    # Clean strings
    for c in ["player_id","player_name","player_qid","dob","tm_url","tm_id"]:
        df[c] = df[c].astype(str).str.strip().replace({"None":"","nan":""})

    # Build indices on existing
    existing = existing.reindex(columns=PLAYERS_HEADERS).fillna("")
    ex_by_tm = {str(t): i for i, t in enumerate(existing["tm_id"].astype(str)) if t}
    ex_by_name_dob = {(str(n).lower(), str(d)): i
                      for i,(n,d) in enumerate(zip(existing["player_name"].astype(str),
                                                   existing["dob"].astype(str))) if n}

    inserts, updates = [], 0

    for _, r in df.iterrows():
        tm_id = str(r["tm_id"]) if pd.notna(r["tm_id"]) else ""
        idx = ex_by_tm.get(tm_id) if tm_id else None
        if idx is None:
            key = (str(r["player_name"]).strip().lower(), str(r["dob"]).strip())
            idx = ex_by_name_dob.get(key)

        merged = {h: "" for h in PLAYERS_HEADERS}
        for h in PLAYERS_HEADERS:
            incoming = r[h] if h in r.index else ""
            if idx is None:
                merged[h] = "" if pd.isna(incoming) else str(incoming)
            else:
                prev = existing.iat[idx, existing.columns.get_loc(h)] if h in existing.columns else ""
                merged[h] = str(incoming) if (pd.notna(incoming) and str(incoming) != "") else str(prev)

        if idx is None:
            inserts.append(merged)
        else:
            for h in PLAYERS_HEADERS:
                existing.iat[idx, existing.columns.get_loc(h)] = merged[h]
            updates += 1

    if inserts:
        existing = pd.concat([existing, pd.DataFrame(inserts, columns=PLAYERS_HEADERS)], ignore_index=True)

    existing = existing.reindex(columns=PLAYERS_HEADERS)
    write_df_to_sheet(ss, "players", existing)
    return (len(inserts), updates)

# ---------- SIDEBAR ----------
with st.sidebar:
    st.header("Data source")
    st.write("Google Sheet:", f"**{SHEET_NAME}**")

    try:
        ss = connect_gsheet()
        st.success("Connected ✅")
    except Exception:
        st.error("Could not connect to Google Sheet. Check secrets & sharing.")
        st.stop()

    tz = pytz.timezone("Europe/Rome")
    st.write("As of:", datetime.now(tz).strftime("%Y-%m-%d %H:%M %Z"))

    st.divider()
    st.markdown("**Tips**")
    st.markdown("- Update the sheet → click **Refresh** below.")
    refresh = st.button("🔄 Refresh data", key="refresh_btn")

# ---------- LOAD TABS ----------
scores_transfers = read_tab(ss, "scores_transfers")
scores_youth     = read_tab(ss, "scores_youth")
players_df       = read_sheet_as_df(ss, "players")  # may be empty first time

if refresh:
    scores_transfers = read_tab(ss, "scores_transfers")
    scores_youth     = read_tab(ss, "scores_youth")
    players_df       = read_sheet_as_df(ss, "players")

# Note: do NOT stop the whole app if scoring tabs are missing; Admin can still run.
if scores_transfers is None and scores_youth is None:
    st.warning("`scores_transfers` and `scores_youth` tabs not found. "
               "Run Colab Step 3 to create them. Admin tab still works below.")

# ---------- MAIN ----------
st.title("DJM Transfers & Youth — Free MVP")
st.caption("Live from your Google Sheet. Filter, sort, import players, and export. (Streamlit Cloud)")

tab1, tab2, tab3 = st.tabs(["Likely Movers", "Youth: Make-It Odds", "Admin — Players (TM URL / CSV)"])

# ---------- TAB 1 — TRANSFERS ----------
with tab1:
    st.subheader("Likely Movers — ranked probabilities")

    if scores_transfers is None:
        st.info("`scores_transfers` tab missing. Generate in Colab (Cell 13).")
    else:
        df_t = scores_transfers.copy()

        # Filters (widget keys start with t_)
        c1, c2, c3, c4 = st.columns(4)
        pos_list_t = sorted(df_t["position_group"].dropna().unique()) if "position_group" in df_t.columns else []
        pos_sel_t  = c1.multiselect("Position(s)", pos_list_t, default=pos_list_t, key="t_pos")
        pmin_t     = c2.slider("Min probability", 0.0, 1.0, 0.50, 0.01, key="t_pmin")
        search_t   = c3.text_input("Search name/club", "", key="t_search")
        sort_desc  = c4.checkbox("Sort by probability (desc)", True, key="t_sort")

        # Apply filters defensively
        filt = pd.Series(True, index=df_t.index)
        if pos_list_t and "position_group" in df_t.columns:
            filt &= df_t["position_group"].isin(pos_sel_t)
        if "p_move" in df_t.columns:
            filt &= df_t["p_move"].fillna(0) >= pmin_t
        if search_t:
            s = search_t.lower()
            name_hit = df_t.get("player_name", pd.Series("", index=df_t.index)).astype(str).str.lower().str.contains(s, na=False)
            club_hit = df_t.get("current_club", pd.Series("", index=df_t.index)).astype(str).str.lower().str.contains(s, na=False)
            filt &= (name_hit | club_hit)

        dfv = df_t[filt].copy()

        # Sort
        if sort_desc and "p_move" in dfv.columns:
            dfv = dfv.sort_values("p_move", ascending=False)
        elif set(["position_group","p_move"]).issubset(dfv.columns):
            dfv = dfv.sort_values(["position_group","p_move"], ascending=[True, False])

        # Display
        show_cols = [c for c in [
            "player_name","position_group","current_club","p_move",
            "contract_months_left","buyer_need_index","role_fit",
            "media_rumor_score","scarcity_index","injury_days_pct"
        ] if c in dfv.columns]
        disp = dfv.copy()
        if "p_move" in disp.columns:
            disp["p_move"] = disp["p_move"].map(percent)
        if "injury_days_pct" in disp.columns:
            disp["injury_days_pct"] = disp["injury_days_pct"].map(percent)
        for col in ["buyer_need_index","role_fit","media_rumor_score","scarcity_index"]:
            if col in disp.columns:
                disp[col] = disp[col].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
        st.dataframe(disp[show_cols], use_container_width=True, hide_index=True)

        # Download
        csv_bytes = df_t.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download full movers CSV", csv_bytes,
                           file_name="scores_transfers.csv", mime="text/csv",
                           key="t_dl")

# ---------- TAB 2 — YOUTH ----------
with tab2:
    st.subheader("Youth — make-it probabilities")

    if scores_youth is None:
        st.info("`scores_youth` tab missing. Generate in Colab (Cell 15).")
    else:
        df_y = scores_youth.copy()

        c1, c2, c3, c4 = st.columns(4)
        pos_list = sorted(df_y["position_group"].dropna().unique()) if "position_group" in df_y.columns else []
        pos_sel  = c1.multiselect("Position(s)", pos_list, default=pos_list, key="y_pos")
        pmin     = c2.slider("Min probability", 0.0, 1.0, 0.50, 0.01, key="y_pmin")
        search   = c3.text_input("Search name", "", key="y_search")
        sort_desc = c4.checkbox("Sort by probability (desc)", True, key="y_sort")

        filt = pd.Series(True, index=df_y.index)
        if pos_list and "position_group" in df_y.columns:
            filt &= df_y["position_group"].isin(pos_sel)
        if "p_make_it" in df_y.columns:
            filt &= df_y["p_make_it"].fillna(0) >= pmin
        if search:
            s = search.lower()
            filt &= df_y.get("player_name", pd.Series("", index=df_y.index)).astype(str).str.lower().str.contains(s, na=False)

        dfv = df_y[filt].copy()

        if sort_desc and "p_make_it" in dfv.columns:
            dfv = dfv.sort_values("p_make_it", ascending=False)
        elif set(["position_group","p_make_it"]).issubset(dfv.columns):
            dfv = dfv.sort_values(["position_group","p_make_it"], ascending=[True, False])

        show_cols = [c for c in [
            "player_name","position_group","age","p_make_it",
            "adj_minutes","availability_pct","role_percentile"
        ] if c in dfv.columns]
        disp = dfv.copy()
        if "p_make_it" in disp.columns:
            disp["p_make_it"] = disp["p_make_it"].map(percent)
        if "availability_pct" in disp.columns:
            disp["availability_pct"] = disp["availability_pct"].map(percent)
        if "role_percentile" in disp.columns:
            disp["role_percentile"] = disp["role_percentile"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
        st.dataframe(disp[show_cols], use_container_width=True, hide_index=True)

        csv_bytes = df_y.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download full youth CSV", csv_bytes,
                           file_name="scores_youth.csv", mime="text/csv",
                           key="y_dl")

# ---------- TAB 3 — ADMIN: PLAYERS (TM URL / CSV) ----------
with tab3:
    st.subheader("Admin — Players (Transfermarkt URL / CSV import)")
    st.caption("Upsert rules: primary key = tm_id, fallback = (player_name, dob). We store tm_url & tm_id only (no scraping).")

    # Live preview of players (if exists)
    if not players_df.empty:
        st.markdown("**Current `players` table (head):**")
        st.dataframe(players_df.head(25), use_container_width=True, hide_index=True)
        dl_players = players_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download players CSV", dl_players, file_name="players_export.csv", mime="text/csv", key="p_dl")

    st.divider()
    c1, c2 = st.columns(2)

    with c1:
        st.markdown("### ➕ Add/Update a single player")
        pname = st.text_input("Player name *", key="adm_name")
        tm_input = st.text_input(
            "Transfermarkt URL or ID *",
            help="Paste a player URL like https://www.transfermarkt.com/.../spieler/418560 or just 418560",
            key="adm_tm"
        )
        pid = st.text_input("Your player_id (optional)", key="adm_pid")
        p_qid = st.text_input("Wikidata Q-ID (optional, e.g., Q11886275)", key="adm_qid")
        dob = st.text_input("DOB (optional, YYYY-MM-DD)", key="adm_dob")

        if st.button("Add / Update player", key="adm_add_btn"):
            if not pname or not tm_input:
                st.error("Please provide both a Player name and a Transfermarkt URL/ID.")
            else:
                tm_id = parse_transfermarkt_id(tm_input)
                if not tm_id:
                    st.error("Could not parse a numeric Transfermarkt ID from your input.")
                else:
                    rec = pd.DataFrame([{
                        "player_id": pid,
                        "player_name": pname,
                        "player_qid": p_qid,
                        "dob": dob,
                        "tm_url": tm_input,
                        "tm_id": tm_id
                    }])
                    try:
                        ins, upd = upsert_players(ss, rec)
                        st.success(f"Done. Inserted: {ins}, Updated: {upd}. (Sheet: players)")
                        # Refresh preview
                        players_df = read_sheet_as_df(ss, "players")
                        if not players_df.empty:
                            st.dataframe(players_df.tail(10), use_container_width=True, hide_index=True)
                    except Exception as e:
                        st.error(f"Upsert failed: {e}")

    with c2:
        st.markdown("### 📥 Bulk import from CSV")
        st.write("CSV may include any subset of these columns (extra columns are ignored):")
        st.code(", ".join(PLAYERS_HEADERS), language="text")
        f = st.file_uploader("Upload CSV", type=["csv"], key="adm_csv")
        if f is not None:
            try:
                df_in = pd.read_csv(f)
            except Exception:
                f.seek(0)
                df_in = pd.read_csv(f, encoding="utf-8", engine="python")

            st.write("Preview (first 20 rows):")
            st.dataframe(df_in.head(20), use_container_width=True, hide_index=True)

            if st.button("Validate & Import", key="adm_import_btn"):
                try:
                    ins, upd = upsert_players(ss, df_in)
                    st.success(f"Import complete. Inserted: {ins}, Updated: {upd}. (Sheet: players)")
                    # Refresh preview
                    players_df = read_sheet_as_df(ss, "players")
                    if not players_df.empty:
                        st.dataframe(players_df.tail(10), use_container_width=True, hide_index=True)
                except Exception as e:
                    st.error(f"Import failed: {e}")

st.caption("Update the sheet, then click *Refresh* in the sidebar to pull fresh data.")
