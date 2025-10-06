# dashboard.py
import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timedelta
import numpy as np

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

# ---- Load data from master file ----
import config
MASTER_FILE_PATH = config.MASTER_XLSX

@st.cache_data(show_spinner=False)
def load_available_periods():
    """Load available 14-day periods from summaries file"""
    try:
        summaries_df = pd.read_excel(config.SUMMARIES_XLSX)
        if not summaries_df.empty:
            periods = []
            for idx, row in summaries_df.iterrows():
                start_date = pd.to_datetime(row['start_date']).date()
                end_date = pd.to_datetime(row['end_date']).date()
                generated_at = row.get('generated_at', 'Unknown')
                periods.append({
                    'index': idx,
                    'start_date': start_date,
                    'end_date': end_date,
                    'generated_at': generated_at,
                    'label': f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"
                })
            return periods
        return []
    except Exception as e:
        st.error(f"Error loading periods: {e}")
        return []

@st.cache_data(show_spinner=False)
def load_data(start_date=None, end_date=None):
    """Load and process data from the master file for specified date range"""
    df = pd.read_excel(MASTER_FILE_PATH)
    
    # Rename columns to match expected format
    df = df.rename(columns={
        "ad_details/aaa_info/eu_total_reach": "reach",
        "startDateFormatted": "start_date",
        "pageInfo/page/name": "brand",
        "adArchiveID": "ad_id",
        "snapshot/body/text": "caption",
    })
    
    # Parse dates (already timezone-naive strings)
    df["date"] = pd.to_datetime(df["start_date"], errors="coerce")
    
    # Convert reach to numeric
    df["reach"] = pd.to_numeric(df["reach"], errors="coerce").fillna(0)
    
    # Use provided dates or fall back to configured dates
    if start_date is None or end_date is None:
        # Use configured analysis dates as fallback
        last_14_days_start = config.ANALYSIS_START_DATE
        last_14_days_end = config.ANALYSIS_END_DATE
    else:
        # Use provided dates
        last_14_days_start = start_date
        last_14_days_end = end_date
    
    # Calculate 7-day periods within the analysis period
    total_days = (last_14_days_end - last_14_days_start).days + 1
    if total_days >= 14:
        # Split into two 7-day periods
        current_7_days_start = last_14_days_end - timedelta(days=6)
        current_7_days_end = last_14_days_end
        prev_7_days_start = last_14_days_start
        prev_7_days_end = last_14_days_start + timedelta(days=6)
    else:
        # If less than 14 days, use the full period for both
        current_7_days_start = last_14_days_start
        current_7_days_end = last_14_days_end
        prev_7_days_start = last_14_days_start
        prev_7_days_end = last_14_days_end
    
    # Filter for analysis period (for charts)
    df_14_days = df[
        (df["date"].dt.date >= last_14_days_start) & 
        (df["date"].dt.date <= last_14_days_end)
    ].copy()
    
    # Filter for current 7 days (for comparison stats)
    df_current = df[
        (df["date"].dt.date >= current_7_days_start) & 
        (df["date"].dt.date <= current_7_days_end)
    ].copy()
    
    # Filter for previous 7 days (for comparison stats)
    df_previous = df[
        (df["date"].dt.date >= prev_7_days_start) & 
        (df["date"].dt.date <= prev_7_days_end)
    ].copy()
    
    return df_14_days, df_current, df_previous, last_14_days_start, last_14_days_end

def calculate_comparison_stats(current_df, previous_df, akropolis_brands):
    """Calculate comparison statistics between current and previous week"""
    # Current week stats for Akropolis brands
    current_akropolis = current_df[current_df["brand"].isin(akropolis_brands)]
    current_ads = current_akropolis["ad_id"].nunique()
    current_reach = current_akropolis["reach"].sum()
    
    # Previous week stats for Akropolis brands
    previous_akropolis = previous_df[previous_df["brand"].isin(akropolis_brands)]
    previous_ads = previous_akropolis["ad_id"].nunique()
    previous_reach = previous_akropolis["reach"].sum()
    
    # Calculate percentage changes
    ads_change = ((current_ads - previous_ads) / previous_ads * 100) if previous_ads > 0 else 0
    reach_change = ((current_reach - previous_reach) / previous_reach * 100) if previous_reach > 0 else 0
    
    return {
        "current_ads": current_ads,
        "current_reach": current_reach,
        "previous_ads": previous_ads,
        "previous_reach": previous_reach,
        "ads_change": ads_change,
        "reach_change": reach_change
    }

def get_color_for_change(change):
    """Get color based on percentage change"""
    if change > 0:
        return "green"
    elif change < 0:
        return "red"
    else:
        return "black"

def create_ad_card(brand, reach, caption, ad_id):
    """Create a card-style display for an ad"""
    truncated_caption = caption[:100] + "..." if len(caption) > 100 else caption
    ad_url = f"https://www.facebook.com/ads/library/?id={ad_id}"
    return f"""
    <div style="border: 1px solid #ddd; border-radius: 8px; padding: 15px; margin: 10px 0; background-color: #f9f9f9;">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <div>
                <h4 style="margin: 0; color: #333;">{brand}</h4>
                <p style="margin: 5px 0; color: #666; font-size: 14px;">
                    <a href="{ad_url}" target="_blank" style="color: #1E90FF; text-decoration: none;">{truncated_caption}</a>
                </p>
            </div>
            <div style="text-align: right;">
                <h3 style="margin: 0; color: #2E8B57;">{int(reach):,}</h3>
                <p style="margin: 0; color: #666; font-size: 12px;">reach</p>
            </div>
        </div>
    </div>
    """

def create_cluster_card(cluster_name, ads_count, total_reach, rank):
    """Create a card-style display for a cluster"""
    return f"""
    <div style="border: 1px solid #ddd; border-radius: 8px; padding: 15px; margin: 10px 0; background-color: #f9f9f9;">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <div>
                <h4 style="margin: 0; color: #333;">{cluster_name}</h4>
                <p style="margin: 5px 0; color: #666; font-size: 14px;">{ads_count} ads</p>
            </div>
            <div style="text-align: right;">
                <h3 style="margin: 0; color: #2E8B57;">{int(total_reach):,}</h3>
                <p style="margin: 0; color: #666; font-size: 12px;">reach</p>
            </div>
        </div>
    </div>
    """

def create_cluster_card_with_examples(cluster_name, ads_count, total_reach, examples):
    """Create a card-style display for a cluster with examples"""
    examples_html = ""
    if examples:
        examples_html = "<div style='margin-top: 10px; padding-top: 10px; border-top: 1px solid #eee;'>"
        examples_html += "<p style='margin: 0 0 5px 0; color: #888; font-size: 12px; font-weight: bold;'>Examples:</p>"
        for example in examples:
            examples_html += f"<p style='margin: 2px 0; color: #666; font-size: 12px; font-style: italic;'>• {example}</p>"
        examples_html += "</div>"
    
    return f"""
    <div style="border: 1px solid #ddd; border-radius: 8px; padding: 15px; margin: 10px 0; background-color: #f9f9f9;">
        <div style="display: flex; justify-content: space-between; align-items: flex-start;">
            <div style="flex: 1;">
                <h4 style="margin: 0; color: #333;">{cluster_name}</h4>
                <p style="margin: 5px 0; color: #666; font-size: 14px;">{ads_count} ads</p>
                {examples_html}
            </div>
            <div style="text-align: right; margin-left: 15px;">
                <h3 style="margin: 0; color: #2E8B57;">{int(total_reach):,}</h3>
                <p style="margin: 0; color: #666; font-size: 12px;">reach</p>
            </div>
        </div>
    </div>
    """

# Load available periods
available_periods = load_available_periods()

# ---- Sidebar Period Selector ----
st.sidebar.title("📅 Analysis Period")
if available_periods:
    period_options = [period['label'] for period in available_periods]
    selected_period_idx = st.sidebar.selectbox(
        "Choose 14-day period:",
        options=range(len(period_options)),
        format_func=lambda x: period_options[x],
        index=len(available_periods) - 1,  # Default to most recent
        help="Select a 14-day period for which summaries are available"
    )
    
    selected_period = available_periods[selected_period_idx]
    selected_start_date = selected_period['start_date']
    selected_end_date = selected_period['end_date']
    
    st.sidebar.caption(f"Generated: {selected_period['generated_at']}")
else:
    st.sidebar.error("No summary periods available")
    selected_start_date = None
    selected_end_date = None

# Load data for selected period
df_14_days, df_current, df_previous, start_date, end_date = load_data(selected_start_date, selected_end_date)

# Load summaries for selected period
@st.cache_data(show_spinner=False)
def load_summaries_for_period(period_idx):
    """Load summaries for a specific period"""
    try:
        summaries_df = pd.read_excel(config.SUMMARIES_XLSX)
        if not summaries_df.empty and period_idx < len(summaries_df):
            return summaries_df.iloc[period_idx].to_dict()
        return None
    except Exception as e:
        st.error(f"Error loading summaries: {e}")
        return None

summaries = load_summaries_for_period(selected_period_idx) if available_periods else None

# ---- UI controls ----
st.title("Brand Intelligence – Analysis Period")

# Date range display
st.caption(f"Analysis period: {start_date.strftime('%B %d')} - {end_date.strftime('%B %d, %Y')}")

st.markdown("**Select Akropolis locations (always included):**")
ak_cols = st.columns(4)
ak_selected = []
for i, loc in enumerate(AKROPOLIS_LOCATIONS):
    with ak_cols[i]:
        if st.checkbox(loc, value=True, key=f"ak_{i}"):
            ak_selected.append(loc)

# Company cluster selector - full width at the top
st.markdown("**Select company cluster to analyze:**")
subset_name = st.selectbox(
    "Subset of companies",
    options=list(SUBSETS_WITH_RETAIL.keys()),
    index=0,
    help="Charts include the selected Akropolis locations **plus** this subset.",
)

st.markdown("---")

# ---- Comparison Statistics with Tabs ----
st.subheader("📊 Performance vs Previous Week")

# Get all brands in the selected cluster
brands_universe = set(ak_selected) | set(SUBSETS_WITH_RETAIL.get(subset_name, []))
all_brands = sorted(list(brands_universe))

# Create tabs for each brand
if all_brands:
    performance_tabs = st.tabs(all_brands)
    
    for i, brand in enumerate(all_brands):
        with performance_tabs[i]:
            # Calculate stats for this specific brand
            brand_current = df_current[df_current["brand"] == brand]
            brand_previous = df_previous[df_previous["brand"] == brand]
            
            current_ads = brand_current["ad_id"].nunique()
            current_reach = brand_current["reach"].sum()
            previous_ads = brand_previous["ad_id"].nunique()
            previous_reach = brand_previous["reach"].sum()
            
            # Calculate percentage changes - handle 0 previous week case
            if previous_ads > 0:
                ads_change = ((current_ads - previous_ads) / previous_ads * 100)
            elif current_ads > 0:
                ads_change = 100  # 100% increase from 0
            else:
                ads_change = 0  # Both are 0
            
            if previous_reach > 0:
                reach_change = ((current_reach - previous_reach) / previous_reach * 100)
            elif current_reach > 0:
                reach_change = 100  # 100% increase from 0
            else:
                reach_change = 0  # Both are 0
            
            # Create comparison cards for this brand
            col1, col2 = st.columns(2)
            
            with col1:
                ads_color = get_color_for_change(ads_change)
                st.markdown(f"""
                <div style="border: 2px solid black; padding: 20px; border-radius: 10px; margin: 10px 0;">
                    <h3 style="color: {ads_color}; margin: 0;">📈 Total Ads: {current_ads}</h3>
                    <p style="color: {ads_color}; font-size: 18px; margin: 5px 0;">
                        {ads_change:+.1f}% vs previous week
                    </p>
                    <p style="color: gray; font-size: 14px; margin: 0;">Previous week: {previous_ads} ads</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                reach_color = get_color_for_change(reach_change)
                st.markdown(f"""
                <div style="border: 2px solid black; padding: 20px; border-radius: 10px; margin: 10px 0;">
                    <h3 style="color: {reach_color}; margin: 0;">👥 Total Reach: {int(current_reach):,}</h3>
                    <p style="color: {reach_color}; font-size: 18px; margin: 5px 0;">
                        {reach_change:+.1f}% vs previous week
                    </p>
                    <p style="color: gray; font-size: 14px; margin: 0;">Previous week: {int(previous_reach):,} reach</p>
                </div>
                """, unsafe_allow_html=True)
            
            # Show info if no data available
            if current_ads == 0 and previous_ads == 0:
                st.info(f"No data available for {brand} in the selected time periods.")

st.markdown("---")

# ---- Weekly Summaries ----
if summaries:
    st.subheader("📝 Weekly AI Summaries")
    
    # Display the date range for the summaries
    if "start_date" in summaries and "end_date" in summaries:
        summary_start = summaries["start_date"]
        summary_end = summaries["end_date"]
        if hasattr(summary_start, 'strftime'):
            start_str = summary_start.strftime('%B %d')
        else:
            start_str = str(summary_start)
        if hasattr(summary_end, 'strftime'):
            end_str = summary_end.strftime('%B %d, %Y')
        else:
            end_str = str(summary_end)
        
        caption_text = f"Summary period: {start_str} - {end_str}"
        if "generated_at" in summaries:
            caption_text += f" | Generated: {summaries['generated_at']}"
        st.caption(caption_text)
    
    # Get all available summaries (including Akropolis)
    all_summaries = {k: v for k, v in summaries.items() if k not in ["start_date", "end_date", "generated_at"] and v}
    
    if all_summaries:
        # Filter summaries based on selected cluster, but always include Akropolis
        relevant_summaries = {}
        
        # Always include Akropolis if available
        if "Akropolis" in all_summaries:
            relevant_summaries["Akropolis"] = all_summaries["Akropolis"]
        
        # Add competitors from selected cluster
        for brand, summary in all_summaries.items():
            if brand != "Akropolis" and brand in brands_universe:
                relevant_summaries[brand] = summary
        
        if relevant_summaries:
            # Create tabs with Akropolis at the end
            tab_names = [brand for brand in relevant_summaries.keys() if brand != "Akropolis"]
            if "Akropolis" in relevant_summaries:
                tab_names.append("Akropolis")
            
            summary_tabs = st.tabs(tab_names)
            
            for i, brand in enumerate(tab_names):
                with summary_tabs[i]:
                    st.markdown(f"""
                    <div style="border: 2px solid black; padding: 20px; border-radius: 10px; margin: 10px 0; background-color: #f8f9fa;">
                        <p style="margin: 0; line-height: 1.6; color: #333;">{relevant_summaries[brand]}</p>
                    </div>
                    """, unsafe_allow_html=True)
        else:
            st.info("No summaries available for the selected cluster.")
    else:
        st.info("No summaries available.")
else:
    st.info("Weekly summaries not available. Run the pipeline to generate summaries.")

st.markdown("---")

# ---- Main Analysis Section ----
# Filter to chosen brands
brands_universe = set(ak_selected) | set(SUBSETS_WITH_RETAIL.get(subset_name, []))
df_f = df_14_days[df_14_days["brand"].isin(brands_universe)].copy()
df_f_current = df_current[df_current["brand"].isin(brands_universe)].copy()

st.subheader("Ad Intelligence (Analysis Period)")
st.caption(
    f"{df_f['brand'].nunique()} brands · {df_f['ad_id'].nunique()} ads · {int(df_f['reach'].sum()):,} total reach"
)

# ---- 1) Daily chart with tabs ----
st.markdown("#### Daily Performance")

# Chart type selector
chart_type = st.radio(
    "Chart type:",
    ["Bar Chart", "Line Chart"],
    horizontal=True,
    key="chart_type_selector"
)

# Create tabs for ads count vs reach
tab1, tab2 = st.tabs(["📊 Ads Posted per Day", "👥 Reach per Day"])

with tab1:
    # Group by date (not datetime) to get daily totals
    df_f_copy = df_f.copy()
    df_f_copy["date_only"] = df_f_copy["date"].dt.date
    daily_ads = (
        df_f_copy.groupby(["date_only", "brand"], as_index=False)
        .agg(ads_count=("ad_id", "nunique"))
        .sort_values("date_only")
    )
    daily_ads["date"] = pd.to_datetime(daily_ads["date_only"])
    
    if daily_ads.empty:
        st.info("No ads found for these brands in the last 14 days.")
    else:
        # Choose chart type based on selection
        if chart_type == "Bar Chart":
            chart = (
                alt.Chart(daily_ads)
                .mark_bar()
                .encode(
                    x=alt.X("date:T", title="Day", axis=alt.Axis(format="%m/%d")),
                    y=alt.Y("ads_count:Q", title="Ads posted"),
                    color=alt.Color("brand:N", title="Brand"),
                    tooltip=["date", "brand", "ads_count"],
                )
                .properties(height=360)
            )
        else:  # Line Chart
            chart = (
                alt.Chart(daily_ads)
                .mark_line(point=True)
                .encode(
                    x=alt.X("date:T", title="Day", axis=alt.Axis(format="%m/%d")),
                    y=alt.Y("ads_count:Q", title="Ads posted"),
                    color=alt.Color("brand:N", title="Brand"),
                    tooltip=["date", "brand", "ads_count"],
                )
                .properties(height=360)
            )
        st.altair_chart(chart, use_container_width=True)

with tab2:
    # Group by date (not datetime) to get daily totals
    df_f_copy2 = df_f.copy()
    df_f_copy2["date_only"] = df_f_copy2["date"].dt.date
    daily_reach = (
        df_f_copy2.groupby(["date_only", "brand"], as_index=False)
        .agg(total_reach=("reach", "sum"))
        .sort_values("date_only")
    )
    daily_reach["date"] = pd.to_datetime(daily_reach["date_only"])
    
    if daily_reach.empty:
        st.info("No reach data found for these brands in the last 14 days.")
    else:
        # Choose chart type based on selection
        if chart_type == "Bar Chart":
            chart = (
                alt.Chart(daily_reach)
                .mark_bar()
                .encode(
                    x=alt.X("date:T", title="Day", axis=alt.Axis(format="%m/%d")),
                    y=alt.Y("total_reach:Q", title="Total reach"),
                    color=alt.Color("brand:N", title="Brand"),
                    tooltip=["date", "brand", "total_reach"],
                )
                .properties(height=360)
            )
        else:  # Line Chart
            chart = (
                alt.Chart(daily_reach)
                .mark_line(point=True)
                .encode(
                    x=alt.X("date:T", title="Day", axis=alt.Axis(format="%m/%d")),
                    y=alt.Y("total_reach:Q", title="Total reach"),
                    color=alt.Color("brand:N", title="Brand"),
                    tooltip=["date", "brand", "total_reach"],
                )
                .properties(height=360)
            )
        st.altair_chart(chart, use_container_width=True)

# ---- 2) Top 3 ads by reach ----
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
        top3_overall = top3(ad_rollup)
        for idx, row in top3_overall.iterrows():
            st.markdown(create_ad_card(row["brand"], row["reach"], row["caption"], row["ad_id"]), unsafe_allow_html=True)
    
    for i, b in enumerate(brands_in_view, start=1):
        with tabs[i]:
            top3_brand = top3(ad_rollup[ad_rollup["brand"] == b])
            for idx, row in top3_brand.iterrows():
                st.markdown(create_ad_card(row["brand"], row["reach"], row["caption"], row["ad_id"]), unsafe_allow_html=True)

# ---- 3) Top 3 clusters by reach ----
st.markdown("#### Top 3 Clusters by Reach")

# Filter for ads with cluster_1 data
df_with_clusters = df_f[df_f["cluster_1"].notna() & (df_f["cluster_1"] != "")]

if df_with_clusters.empty:
    st.info("No cluster data available.")
else:
    cluster_rollup = (
        df_with_clusters.groupby(["cluster_1", "brand"], as_index=False)
        .agg(
            ads_count=("ad_id", "nunique"),
            total_reach=("reach", "sum")
        )
    )
    
    brands_with_clusters = sorted(cluster_rollup["brand"].unique())
    cluster_tabs = st.tabs(["Overall"] + brands_with_clusters)
    
    def top3_clusters(d):
        return d.sort_values("total_reach", ascending=False).head(3).reset_index(drop=True)
    
    with cluster_tabs[0]:
        top3_clusters_overall = top3_clusters(cluster_rollup)
        for idx, row in top3_clusters_overall.iterrows():
            st.markdown(create_cluster_card(row["cluster_1"], row["ads_count"], row["total_reach"], 0), unsafe_allow_html=True)
    
    for i, b in enumerate(brands_with_clusters, start=1):
        with cluster_tabs[i]:
            top3_clusters_brand = top3_clusters(cluster_rollup[cluster_rollup["brand"] == b])
            for idx, row in top3_clusters_brand.iterrows():
                st.markdown(create_cluster_card(row["cluster_1"], row["ads_count"], row["total_reach"], 0), unsafe_allow_html=True)

# ---- 4) Top 3 ad clusters comparison ----
st.markdown("#### Top 3 Ad Clusters: This Week vs Previous Week")

# Current week clusters
df_current_clusters = df_current[df_current["brand"].isin(brands_universe)]
df_current_clusters = df_current_clusters[df_current_clusters["cluster_1"].notna() & (df_current_clusters["cluster_1"] != "")]

# Previous week clusters
df_prev_clusters = df_previous[df_previous["brand"].isin(brands_universe)]
df_prev_clusters = df_prev_clusters[df_prev_clusters["cluster_1"].notna() & (df_prev_clusters["cluster_1"] != "")]

if df_current_clusters.empty and df_prev_clusters.empty:
    st.info("No cluster data available for comparison.")
else:
    # Create comparison tabs
    comparison_brands = sorted(set(df_current_clusters["brand"].unique()) | set(df_prev_clusters["brand"].unique()))
    comparison_tabs = st.tabs(["Overall"] + comparison_brands)
    
    def get_top_clusters_with_examples(df, brand_filter=None):
        if brand_filter:
            df_filtered = df[df["brand"] == brand_filter]
        else:
            df_filtered = df
        
        cluster_stats = (
            df_filtered.groupby("cluster_1", as_index=False)
            .agg(
                ads_count=("ad_id", "nunique"),
                total_reach=("reach", "sum")
            )
            .sort_values("ads_count", ascending=False)
            .head(3)
        )
        
        # Add examples for each cluster
        cluster_examples = []
        for _, row in cluster_stats.iterrows():
            cluster_name = row["cluster_1"]
            # Get up to 2 examples from ad_summary for this cluster
            examples = (
                df_filtered[df_filtered["cluster_1"] == cluster_name]
                ["ad_summary"]
                .dropna()
                .head(2)
                .tolist()
            )
            cluster_examples.append(examples)
        
        cluster_stats["examples"] = cluster_examples
        
        if not cluster_stats.empty:
            total_ads = df_filtered["ad_id"].nunique()
            cluster_stats["percentage"] = (cluster_stats["ads_count"] / total_ads * 100).round(1)
        
        return cluster_stats
    
    with comparison_tabs[0]:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**This Week**")
            current_overall = get_top_clusters_with_examples(df_current_clusters)
            if not current_overall.empty:
                for idx, row in current_overall.iterrows():
                    st.markdown(create_cluster_card_with_examples(row["cluster_1"], row["ads_count"], row["total_reach"], row["examples"]), unsafe_allow_html=True)
            else:
                st.info("No data for this week")
        
        with col2:
            st.markdown("**Previous Week**")
            prev_overall = get_top_clusters_with_examples(df_prev_clusters)
            if not prev_overall.empty:
                for idx, row in prev_overall.iterrows():
                    st.markdown(create_cluster_card_with_examples(row["cluster_1"], row["ads_count"], row["total_reach"], row["examples"]), unsafe_allow_html=True)
            else:
                st.info("No data for previous week")
    
    for i, b in enumerate(comparison_brands, start=1):
        with comparison_tabs[i]:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"**{b} - This Week**")
                current_brand = get_top_clusters_with_examples(df_current_clusters, b)
                if not current_brand.empty:
                    for idx, row in current_brand.iterrows():
                        st.markdown(create_cluster_card_with_examples(row["cluster_1"], row["ads_count"], row["total_reach"], row["examples"]), unsafe_allow_html=True)
                else:
                    st.info("No data for this week")
            
            with col2:
                st.markdown(f"**{b} - Previous Week**")
                prev_brand = get_top_clusters_with_examples(df_prev_clusters, b)
                if not prev_brand.empty:
                    for idx, row in prev_brand.iterrows():
                        st.markdown(create_cluster_card_with_examples(row["cluster_1"], row["ads_count"], row["total_reach"], row["examples"]), unsafe_allow_html=True)
                else:
                    st.info("No data for previous week")

# ---- Optional totals by brand ----
with st.expander("Totals by brand"):
    totals = (
        df_f.groupby("brand", as_index=False)
        .agg(ads=("ad_id", "nunique"), total_reach=("reach", "sum"))
        .sort_values("total_reach", ascending=False)
    )
    st.dataframe(totals, use_container_width=True)
