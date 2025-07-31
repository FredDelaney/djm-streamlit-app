import streamlit as st
import pandas as pd
import numpy as np
import pytz
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe

st.set_page_config(page_title="DJM Transfers & Youth — Free MVP", layout="wide")

# ---------- SETTINGS (from Streamlit Secrets) ----------
# You will add these in Streamlit Cloud > Settings > Secrets (next step)
SHEET_NAME = st.secrets.get("sheet_name", "DJM_Input")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ---------- CONNECT TO GOOGLE SHEET ----------
@st.cache_resource(show_spinner=False)
def connect_gsheet():
    # Expect the full service-account JSON under key: gcp_service_account
    creds_info = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    client = gspread.authorize(creds)
    ss = client.open(SHEET_NAME)
    return ss

def read_tab(ss, tab_name):
    try:
        ws = ss.worksheet(tab_name)
    except Exception:
        return None
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    if df is None or df.empty or df.shape[0] == 0:
        return None
    # Drop fully empty rows
    df = df.dropna(how="all").reset_index(drop=True)
    # Coerce common numeric cols
    for c in df.columns:
        if c in {"p_move","p_make_it","contract_months_left","buyer_need_index","role_fit","media_rumor_score","scarcity_index","injury_days_pct","availability_pct","adj_minutes","role_percentile"}:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def percent(x):
    try:
        return f"{100*float(x):.1f}%"
    except Exception:
        return ""

# ---------- UI ----------
st.title("DJM Transfers & Youth — Free MVP")
st.caption("Live from your Google Sheet. Filter, sort, and export. (Runs on Streamlit Community Cloud)")

# Connection + status
with st.sidebar:
    st.header("Data source")
    st.write("Google Sheet:", f"**{SHEET_NAME}**")
    try:
        ss = connect_gsheet()
        st.success("Connected ✅")
    except Exception as e:
        st.error("Could not connect to the Google Sheet. Check secrets & sharing.")
        st.stop()

    tz = pytz.timezone("Europe/Rome")
    st.write("As of:", datetime.now(tz).strftime("%Y-%m-%d %H:%M %Z"))

    st.divider()
    st.markdown("**Tips**")
    st.markdown("- Update the sheet → click **Refresh** below.")
    refresh = st.button("🔄 Refresh data")

# Load tabs
scores_transfers = read_tab(ss, "scores_transfers")
scores_youth = read_tab(ss, "scores_youth")

if refresh:
    # Re-read
    scores_transfers = read_tab(ss, "scores_transfers")
    scores_youth = read_tab(ss, "scores_youth")

# Guardrails
if scores_transfers is None and scores_youth is None:
    st.warning("I can't find `scores_transfers` or `scores_youth` tabs. Run your Colab Step 3 to create them.")
    st.stop()

tab1, tab2 = st.tabs(["Likely Movers", "Youth: Make-It Odds"])

# ---------- TAB 1: TRANSFERS ----------
with tab1:
    st.subheader("Likely Movers — ranked probabilities")
    if scores_transfers is None:
        st.info("`scores_transfers` tab not found. Generate it from Colab (Cell 13).")
    else:
        df = scores_transfers.copy()

        # Filters
        cols = st.columns(4)
        pos_list = sorted(df["position_group"].dropna().unique().tolist())
        pos_sel = cols[0].multiselect("Position(s)", pos_list, default=pos_list)
        pmin = cols[1].slider("Min probability", 0.0, 1.0, 0.50, 0.01)
        search = cols[2].text_input("Search name/club", "")
        sort_desc = cols[3].checkbox("Sort by probability (desc)", value=True)

        # Apply filters
        m = df["position_group"].isin(pos_sel)
        m &= df["p_move"].fillna(0) >= pmin
        if search:
            s = search.lower()
            m &= df["player_name"].str.lower().str.contains(s, na=False) | df["current_club"].str.lower().str.contains(s, na=False)
        dfv = df[m].copy()

        # Rank and small presentation tweaks
        if sort_desc:
            dfv = dfv.sort_values("p_move", ascending=False)
        else:
            dfv = dfv.sort_values(["position_group","p_move"], ascending=[True, False])

        show_cols = [
            "player_name","position_group","current_club","p_move",
            "contract_months_left","buyer_need_index","role_fit","media_rumor_score","scarcity_index","injury_days_pct"
        ]
        dfv["p_move"] = dfv["p_move"].map(percent)
        dfv["injury_days_pct"] = dfv["injury_days_pct"].map(percent)
        dfv["buyer_need_index"] = dfv["buyer_need_index"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
        dfv["role_fit"] = dfv["role_fit"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
        dfv["media_rumor_score"] = dfv["media_rumor_score"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
        dfv["scarcity_index"] = dfv["scarcity_index"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")

        st.dataframe(dfv[show_cols], use_container_width=True, hide_index=True)

        # Download
        csv = scores_transfers.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download full movers CSV", data=csv, file_name="scores_transfers.csv", mime="text/csv")

# ---------- TAB 2: YOUTH ----------
with tab2:
    st.subheader("Youth — make-it probabilities")
    if scores_youth is None:
        st.info("`scores_youth` tab not found. Generate it from Colab (Cell 15).")
    else:
        df = scores_youth.copy()

        # Filters
        cols = st.columns(4)
        pos_list = sorted(df["position_group"].dropna().unique().tolist())
        pos_sel = cols[0].multiselect("Position(s)", pos_list, default=pos_list)
        pmin = cols[1].slider("Min probability", 0.0, 1.0, 0.50, 0.01)
        search = cols[2].text_input("Search name", "")
        sort_desc = cols[3].checkbox("Sort by probability (desc)", value=True)

        # Apply filters
        m = df["position_group"].isin(pos_sel)
        m &= df["p_make_it"].fillna(0) >= pmin
        if search:
            s = search.lower()
            m &= df["player_name"].str.lower().str.contains(s, na=False)
        dfv = df[m].copy()

        # Rank and presentation
        if sort_desc:
            dfv = dfv.sort_values("p_make_it", ascending=False)
        else:
            dfv = dfv.sort_values(["position_group","p_make_it"], ascending=[True, False])

        show_cols = [
            "player_name","position_group","age","p_make_it",
            "adj_minutes","availability_pct","role_percentile"
        ]
        dfv["p_make_it"] = dfv["p_make_it"].map(percent)
        dfv["availability_pct"] = dfv["availability_pct"].map(percent)
        dfv["role_percentile"] = dfv["role_percentile"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")

        st.dataframe(dfv[show_cols], use_container_width=True, hide_index=True)

        # Download
        csv = scores_youth.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download full youth CSV", data=csv, file_name="scores_youth.csv", mime="text/csv")

st.caption("If you update the Google Sheet, hit Refresh in the sidebar.")

