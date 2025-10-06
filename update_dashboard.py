# app.py
import streamlit as st
import pandas as pd
import altair as alt
import config

st.set_page_config(page_title="Ad Intelligence – Analysis Period", layout="wide")

# ---- Groups with updated names ----
AKROPOLIS_LOCATIONS = [
    "AKROPOLIS | Vilnius",
    "AKROPOLIS | Klaipėda",
    "AKROPOLIS | Šiauliai",
]

BIG_PLAYERS = ["PANORAMA", "OZAS", "Kauno Akropolis"]
SMALLER_PLAYERS = [
    "Vilnius Outlet",
    "BIG Vilnius",
    "Outlet Park",
    "CUP prekybos centras",
    "PC Europa",
    "G9",
]
OTHER_CITIES = [
    "SAULĖS MIESTAS",
    "PLC Mega",     # covers Kaunas Mega
]
RETAIL = ["Maxima LT", "Lidl Lietuva", "Rimi Lietuva", "IKI"]

SUBSETS_CORE = {
    "Big players": BIG_PLAYERS,
    "Smaller players": SMALLER_PLAYERS,
    "Other cities": OTHER_CITIES,
}
SUBSETS_WITH_RETAIL = {
    **SUBSETS_CORE,
    "Retail": RETAIL,
}

# ---- Load the provided Excel ----
# Use the configured master file path
EXCEL_PATH = config.MASTER_XLSX

@st.cache_data(show_spinner=False)
def load_data():
    df = pd.read_excel(EXCEL_PATH)

    df = df.rename(
        columns={
            "ad_details/aaa_info/eu_total_reach": "reach",
            "startDateFormatted": "start_date",
            "pageInfo/page/name": "brand",
            "adArchiveID": "ad_id",
            "content": "caption",
        }
    )

    # Parse to datetime and drop timezone (convert to naive UTC)
    df["date"] = (
        pd.to_datetime(df["start_date"], errors="coerce")
          .dt.tz_convert(None)          # <-- key line
    )

    # Filter by configured analysis period
    start = pd.Timestamp(config.ANALYSIS_START_DATE)
    end   = pd.Timestamp(config.ANALYSIS_END_DATE)
    df = df[(df["date"] >= start) & (df["date"] <= end)]

    df["reach"] = pd.to_numeric(df["reach"], errors="coerce").fillna(0)
    return df[["date", "brand", "ad_id", "reach", "caption"]]


df = load_data()

# ---- UI controls ----
st.title("Brand Intelligence – Analysis Period (Ads)")

st.markdown("**Select Akropolis locations (always included):**")
ak_cols = st.columns(4)
ak_selected = []
for i, loc in enumerate(AKROPOLIS_LOCATIONS):
    with ak_cols[i]:
        if st.checkbox(loc, value=True, key=f"ak_{i}"):
            ak_selected.append(loc)

st.markdown("---")

left, right = st.columns([1.2, 3])
with left:
    subset_name = st.selectbox(
        "Subset of companies",
        options=list(SUBSETS_WITH_RETAIL.keys()),
        index=0,
        help="Charts include the selected Akropolis locations **plus** this subset.",
    )

with right:
    # Filter to chosen brands
    brands_universe = set(ak_selected) | set(SUBSETS_WITH_RETAIL.get(subset_name, []))
    df_f = df[df["brand"].isin(brands_universe)].copy()

    st.subheader("Ad Intelligence (Analysis Period)")
    st.caption(
        f"{df_f['brand'].nunique()} brands · {df_f['ad_id'].nunique()} ads · {int(df_f['reach'].sum()):,} total reach"
    )

    # ---- 1) Daily line chart by brand ----
    st.markdown("#### Ads Posted per Day")
    daily = (
        df_f.groupby(["date", "brand"], as_index=False)
        .agg(ads_count=("ad_id", "nunique"))
        .sort_values("date")
    )

    if daily.empty:
        st.info(f"No ads found for these brands in the analysis period ({config.ANALYSIS_START_DATE} to {config.ANALYSIS_END_DATE}).")
    else:
        chart = (
            alt.Chart(daily)
            .mark_line(point=True)
            .encode(
                x=alt.X("date:T", title="Day"),
                y=alt.Y("ads_count:Q", title="Ads posted"),
                color=alt.Color("brand:N", title="Brand"),
                tooltip=["date", "brand", "ads_count"],
            )
            .properties(height=360)
        )
        st.altair_chart(chart, use_container_width=True)

    # ---- 2) Top 3 ads by reach with tabs ----
    st.markdown("#### Top 3 Ads by Reach")
    ad_rollup = (
        df_f.groupby(["ad_id", "brand"], as_index=False)
        .agg(reach=("reach", "max"), caption=("caption", "first"))
    )

    if ad_rollup.empty:
        st.info("No ads to show.")
    else:
        brands_in_view = sorted(ad_rollup["brand"].unique())
        tabs = st.tabs(["Overall"] + brands_in_view)

        def top3(d):
            return d.sort_values("reach", ascending=False).head(3).reset_index(drop=True)

        with tabs[0]:
            st.dataframe(top3(ad_rollup), use_container_width=True)

        for i, b in enumerate(brands_in_view, start=1):
            with tabs[i]:
                st.dataframe(top3(ad_rollup[ad_rollup["brand"] == b]), use_container_width=True)

    # ---- Optional totals by brand ----
    with st.expander("Totals by brand"):
        totals = (
            df_f.groupby("brand", as_index=False)
            .agg(ads=("ad_id", "nunique"), total_reach=("reach", "sum"))
            .sort_values("total_reach", ascending=False)
        )
        st.dataframe(totals, use_container_width=True)
