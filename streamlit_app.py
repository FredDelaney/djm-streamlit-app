import streamlit as st
import pandas as pd
import numpy as np
import pytz
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe

st.set_page_config(page_title="DJM Transfers & Youth — Free MVP",
                   layout="wide")

# ---------- SETTINGS ----------
SHEET_NAME = st.secrets.get("sheet_name", "DJM_Input")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ---------- GOOGLE SHEET ----------
@st.cache_resource(show_spinner=False)
def connect_gsheet():
    creds_info = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME)

def read_tab(ss, tab):
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
if refresh:
    scores_transfers = read_tab(ss, "scores_transfers")
    scores_youth     = read_tab(ss, "scores_youth")

if scores_transfers is None and scores_youth is None:
    st.warning("`scores_transfers` and `scores_youth` tabs not found. "
               "Run Colab Step 3 to create them.")
    st.stop()

# ---------- MAIN ----------
st.title("DJM Transfers & Youth — Free MVP")
st.caption("Live from your Google Sheet. Filter, sort, export. (Streamlit Cloud)")

tab1, tab2 = st.tabs(["Likely Movers", "Youth: Make-It Odds"])

# ---------- TAB 1 — TRANSFERS ----------
with tab1:
    st.subheader("Likely Movers — ranked probabilities")

    if scores_transfers is None:
        st.info("`scores_transfers` tab missing. Generate in Colab (Cell 13).")
    else:
        df_t = scores_transfers.copy()

        # Filters (widget keys start with t_)
        c1, c2, c3, c4 = st.columns(4)
        pos_list_t   = sorted(df_t["position_group"].dropna().unique())
        pos_sel_t    = c1.multiselect("Position(s)", pos_list_t, default=pos_list_t,
                                      key="t_pos")
        pmin_t       = c2.slider("Min probability", 0.0, 1.0, 0.50, 0.01,
                                 key="t_pmin")
        search_t     = c3.text_input("Search name/club", "", key="t_search")
        sort_desc_t  = c4.checkbox("Sort by probability (desc)", True, key="t_sort")

        # Apply filters
        filt_t = df_t["position_group"].isin(pos_sel_t)
        filt_t &= df_t["p_move"].fillna(0) >= pmin_t
        if search_t:
            s = search_t.lower()
            filt_t &= (df_t["player_name"].str.lower().str.contains(s, na=False) |
                       df_t["current_club"].str.lower().str.contains(s, na=False))
        dfv_t = df_t[filt_t].copy()

        # Sort
        dfv_t = dfv_t.sort_values(
            "p_move" if sort_desc_t else ["position_group", "p_move"],
            ascending=[False] if sort_desc_t else [True, False]
        )

        # Display
        show_t = [
            "player_name","position_group","current_club","p_move",
            "contract_months_left","buyer_need_index","role_fit",
            "media_rumor_score","scarcity_index","injury_days_pct"
        ]
        disp_t = dfv_t.copy()
        disp_t["p_move"]          = disp_t["p_move"].map(percent)
        disp_t["injury_days_pct"] = disp_t["injury_days_pct"].map(percent)
        for col in ["buyer_need_index","role_fit","media_rumor_score","scarcity_index"]:
            disp_t[col] = disp_t[col].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
        st.dataframe(disp_t[show_t], use_container_width=True, hide_index=True)

        # Download
        csv_t = df_t.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download full movers CSV", csv_t,
                           file_name="scores_transfers.csv", mime="text/csv",
                           key="t_dl")

# ---------- TAB 2 — YOUTH ----------
with tab2:
    st.subheader("Youth — make-it probabilities")

    if scores_youth is None:
        st.info("`scores_youth` tab missing. Generate in Colab (Cell 15).")
    else:
        df_y = scores_youth.copy()

        # Filters (widget keys start with y_)
        c1, c2, c3, c4 = st.columns(4)
        pos_list_y   = sorted(df_y["position_group"].dropna().unique())
        pos_sel_y    = c1.multiselect("Position(s)", pos_list_y, default=pos_list_y,
                                      key="y_pos")
        pmin_y       = c2.slider("Min probability", 0.0, 1.0, 0.50, 0.01,
                                 key="y_pmin")
        search_y     = c3.text_input("Search name", "", key="y_search")
        sort_desc_y  = c4.checkbox("Sort by probability (desc)", True, key="y_sort")

        # Apply filters
        filt_y = df_y["position_group"].isin(pos_sel_y)
        filt_y &= df_y["p_make_it"].fillna(0) >= pmin_y
        if search_y:
            s = search_y.lower()
            filt_y &= df_y["player_name"].str.lower().str.contains(s, na=False)
        dfv_y = df_y[filt_y].copy()

        # Sort
        dfv_y = dfv_y.sort_values(
            "p_make_it" if sort_desc_y else ["position_group", "p_make_it"],
            ascending=[False] if sort_desc_y else [True, False]
        )

        # Display
        show_y = [
            "player_name","position_group","age","p_make_it",
            "adj_minutes","availability_pct","role_percentile"
        ]
        disp_y = dfv_y.copy()
        disp_y["p_make_it"]      = disp_y["p_make_it"].map(percent)
        disp_y["availability_pct"] = disp_y["availability_pct"].map(percent)
        disp_y["role_percentile"]  = disp_y["role_percentile"].map(
            lambda x: f"{x:.2f}" if pd.notna(x) else "")
        st.dataframe(disp_y[show_y], use_container_width=True, hide_index=True)

        # Download
        csv_y = df_y.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download full youth CSV", csv_y,
                           file_name="scores_youth.csv", mime="text/csv",
                           key="y_dl")

st.caption("Update the sheet, then click *Refresh* in the sidebar to pull fresh data.")
