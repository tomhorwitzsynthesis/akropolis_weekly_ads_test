#!/usr/bin/env python3
"""
Weekly Summary Generator for Ad Intelligence Dashboard
Generates LLM-powered summaries for Akropolis and competitor performance
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed
import config

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY", config.OPENAI_API_KEY))

# Brand groupings
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
    "PLC Mega",
]
RETAIL = ["Maxima LT", "Lidl Lietuva", "Rimi Lietuva", "IKI"]

ALL_COMPETITORS = BIG_PLAYERS + SMALLER_PLAYERS + OTHER_CITIES + RETAIL

def load_and_filter_data():
    """Load data from master file and filter for the configured analysis period"""
    df = pd.read_excel(config.MASTER_XLSX)
    
    # Rename columns
    df = df.rename(columns={
        "ad_details/aaa_info/eu_total_reach": "reach",
        "startDateFormatted": "start_date",
        "pageInfo/page/name": "brand",
        "adArchiveID": "ad_id",
        "snapshot/body/text": "caption",
    })
    
    # Parse dates
    df["date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["reach"] = pd.to_numeric(df["reach"], errors="coerce").fillna(0)
    
    # Use configured date ranges
    analysis_start = config.ANALYSIS_START_DATE
    analysis_end = config.ANALYSIS_END_DATE
    
    # Calculate 7-day periods within the analysis period
    total_days = (analysis_end - analysis_start).days + 1
    if total_days >= 14:
        # Split into two 7-day periods
        current_7_days_start = analysis_end - timedelta(days=6)
        current_7_days_end = analysis_end
        prev_7_days_start = analysis_start
        prev_7_days_end = analysis_start + timedelta(days=6)
    else:
        # If less than 14 days, use the full period for both
        current_7_days_start = analysis_start
        current_7_days_end = analysis_end
        prev_7_days_start = analysis_start
        prev_7_days_end = analysis_end
    
    # Filter data for the full analysis period
    df_14_days = df[
        (df["date"].dt.date >= analysis_start) & 
        (df["date"].dt.date <= analysis_end)
    ].copy()
    
    # Filter data for current 7-day period
    df_current = df[
        (df["date"].dt.date >= current_7_days_start) & 
        (df["date"].dt.date <= current_7_days_end)
    ].copy()
    
    # Filter data for previous 7-day period
    df_previous = df[
        (df["date"].dt.date >= prev_7_days_start) & 
        (df["date"].dt.date <= prev_7_days_end)
    ].copy()
    
    return df_14_days, df_current, df_previous, analysis_start, analysis_end

def get_brand_stats(df_current, df_previous, brand_name):
    """Get statistics for a specific brand"""
    current_data = df_current[df_current["brand"] == brand_name]
    previous_data = df_previous[df_previous["brand"] == brand_name]
    
    current_ads = current_data["ad_id"].nunique()
    current_reach = current_data["reach"].sum()
    previous_ads = previous_data["ad_id"].nunique()
    previous_reach = previous_data["reach"].sum()
    
    # Calculate percentage changes
    ads_change = ((current_ads - previous_ads) / previous_ads * 100) if previous_ads > 0 else (100 if current_ads > 0 else 0)
    reach_change = ((current_reach - previous_reach) / previous_reach * 100) if previous_reach > 0 else (100 if current_reach > 0 else 0)
    
    return {
        "current_ads": current_ads,
        "current_reach": current_reach,
        "previous_ads": previous_ads,
        "previous_reach": previous_reach,
        "ads_change": ads_change,
        "reach_change": reach_change,
        "current_captions": current_data["caption"].dropna().tolist(),
        "previous_captions": previous_data["caption"].dropna().tolist(),
        "current_clusters": current_data["cluster_1"].dropna().tolist(),
        "previous_clusters": previous_data["cluster_1"].dropna().tolist(),
    }

def generate_akropolis_summary(df_current, df_previous):
    """Generate summary for all Akropolis brands combined"""
    # Get combined stats for all Akropolis brands
    akropolis_current = df_current[df_current["brand"].isin(AKROPOLIS_LOCATIONS)]
    akropolis_previous = df_previous[df_previous["brand"].isin(AKROPOLIS_LOCATIONS)]
    
    current_ads = akropolis_current["ad_id"].nunique()
    current_reach = akropolis_current["reach"].sum()
    previous_ads = akropolis_previous["ad_id"].nunique()
    previous_reach = akropolis_previous["reach"].sum()
    
    ads_change = ((current_ads - previous_ads) / previous_ads * 100) if previous_ads > 0 else (100 if current_ads > 0 else 0)
    reach_change = ((current_reach - previous_reach) / previous_reach * 100) if previous_reach > 0 else (100 if current_reach > 0 else 0)
    
    # Get ad content and clusters
    current_captions = akropolis_current["caption"].dropna().tolist()
    previous_captions = akropolis_previous["caption"].dropna().tolist()
    current_clusters = akropolis_current["cluster_1"].dropna().tolist()
    previous_clusters = akropolis_previous["cluster_1"].dropna().tolist()
    
    prompt = f"""
You are analyzing advertising performance for Akropolis shopping centers in Lithuania. Please provide a factual summary of their advertising performance this week compared to last week.

PERFORMANCE METRICS:
- Current week: {current_ads} ads, {current_reach:,.0f} reach
- Previous week: {previous_ads} ads, {previous_reach:,.0f} reach
- Ads change: {ads_change:+.1f}%
- Reach change: {reach_change:+.1f}%

CURRENT WEEK AD CONTENT (first 20 ads):
{chr(10).join(current_captions[:20])}

PREVIOUS WEEK AD CONTENT (first 20 ads):
{chr(10).join(previous_captions[:20])}

CURRENT WEEK CLUSTERS:
{', '.join(current_clusters)}

PREVIOUS WEEK CLUSTERS:
{', '.join(previous_clusters)}

Please provide a concise 2-3 paragraph summary covering:
1. Performance metrics (ads and reach changes)
2. Cluster focus areas and changes
3. Specific examples of ads posted this week vs last week

Focus only on facts and actual data. Do not make assumptions about strategy, intentions, or potential outcomes. Include specific examples of actual ads posted.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=500
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Error generating summary: {str(e)}"

def generate_competitor_summary(brand_name, stats):
    """Generate summary for a specific competitor brand"""
    if stats["current_ads"] == 0 and stats["previous_ads"] == 0:
        return f"{brand_name} had no ads in both the current and previous week."
    
    prompt = f"""
You are analyzing advertising performance for {brand_name} in Lithuania. Please provide a factual summary of their advertising performance this week compared to last week.

PERFORMANCE METRICS:
- Current week: {stats['current_ads']} ads, {stats['current_reach']:,.0f} reach
- Previous week: {stats['previous_ads']} ads, {stats['previous_reach']:,.0f} reach
- Ads change: {stats['ads_change']:+.1f}%
- Reach change: {stats['reach_change']:+.1f}%

CURRENT WEEK AD CONTENT (first 15 ads):
{chr(10).join(stats['current_captions'][:15])}

PREVIOUS WEEK AD CONTENT (first 15 ads):
{chr(10).join(stats['previous_captions'][:15])}

CURRENT WEEK CLUSTERS:
{', '.join(stats['current_clusters'])}

PREVIOUS WEEK CLUSTERS:
{', '.join(stats['previous_clusters'])}

Please provide a concise 2-3 paragraph summary covering:
1. Performance metrics (ads and reach changes)
2. Cluster focus areas and changes
3. Specific examples of ads posted this week vs last week

Focus only on facts and actual data. Do not make assumptions about strategy, intentions, or potential outcomes. Include specific examples of actual ads posted.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=500
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Error generating summary: {str(e)}"

def generate_single_summary(brand_name, df_current, df_previous):
    """Generate summary for a single brand (for parallel processing)"""
    if brand_name == "Akropolis":
        return generate_akropolis_summary(df_current, df_previous)
    else:
        stats = get_brand_stats(df_current, df_previous, brand_name)
        return generate_competitor_summary(brand_name, stats)

def generate_all_summaries():
    """Generate summaries for all brands and append to Excel using parallel processing"""
    print("Loading data...")
    df_14_days, df_current, df_previous, start_date, end_date = load_and_filter_data()
    
    # Prepare all brands for parallel processing
    all_brands = ["Akropolis"] + ALL_COMPETITORS
    
    print(f"Generating summaries for {len(all_brands)} brands using parallel processing...")
    
    summaries = {
        "start_date": start_date,
        "end_date": end_date,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # Use ThreadPoolExecutor for parallel processing
    max_workers = min(config.GPT_MAX_WORKERS, len(all_brands))  # Use config setting or number of brands
    print(f"Using {max_workers} parallel workers...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_brand = {
            executor.submit(generate_single_summary, brand, df_current, df_previous): brand 
            for brand in all_brands
        }
        
        # Collect results as they complete
        completed = 0
        for future in as_completed(future_to_brand):
            brand = future_to_brand[future]
            try:
                summary = future.result()
                summaries[brand] = summary
                completed += 1
                print(f"Completed {completed}/{len(all_brands)}: {brand}")
            except Exception as e:
                print(f"Error generating summary for {brand}: {e}")
                summaries[brand] = f"Error generating summary: {str(e)}"
                completed += 1
    
    # Load existing summaries if they exist
    output_path = config.SUMMARIES_XLSX
    existing_summaries = pd.DataFrame()
    
    try:
        if Path(output_path).exists():
            existing_summaries = pd.read_excel(output_path)
            print(f"Loaded {len(existing_summaries)} existing summary entries")
    except Exception as e:
        print(f"Could not load existing summaries: {e}")
    
    # Create new summary entry
    new_summary = pd.DataFrame([summaries])
    
    # Append new summary to existing ones
    if not existing_summaries.empty:
        combined_summaries = pd.concat([existing_summaries, new_summary], ignore_index=True)
    else:
        combined_summaries = new_summary
    
    # Ensure data directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Save combined summaries to Excel
    combined_summaries.to_excel(output_path, index=False)
    
    print(f"All summaries completed and saved to {output_path} (total entries: {len(combined_summaries)})")
    return output_path

if __name__ == "__main__":
    if config.ENABLE_WEEKLY_SUMMARIES:
        generate_all_summaries()
    else:
        print("Weekly summaries are disabled in config.")
