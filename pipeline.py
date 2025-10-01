#!/usr/bin/env python3
import os
from pathlib import Path
import pandas as pd

import config
from scraper import scrape_all
from transform import flatten, ensure_columns, filter_recent, process_card_extraction
from storage import load_excel, save_excel, deduplicate
from gpt_labeler import label_ads_with_gpt, print_cluster_stats
from summary_generator import generate_all_summaries

def main():
    token = os.getenv("APIFY_API_TOKEN", config.APIFY_TOKEN)
    if not token:
        raise SystemExit("Set APIFY_API_TOKEN env var or fill it in config.py")

    all_items = scrape_all(config.URLS, token, config.MAX_ADS, config.MAX_WORKERS)
    if not all_items:
        print("[DONE] No ads fetched.")
        return
    else:
        print("This many ads were fetched: ", len(all_items))

    df = flatten(all_items)
    print("Flattened ads: ", len(df))
    df = ensure_columns(df, url="", total_count=len(all_items))
    print("Ensured columns: ", len(df))
    df = process_card_extraction(df)
    print("Processed card extraction: ", len(df))
    df = filter_recent(df, config.TIMEZONE, config.DAYS_BACK)
    print("Filtered ads: ", len(df))
    
    # GPT Labeling (if enabled)
    if config.ENABLE_GPT_LABELING and not df.empty:
        print("Starting GPT labeling...")
        df = label_ads_with_gpt(df, config.GPT_MAX_WORKERS)
        print_cluster_stats(df)
    else:
        print("GPT labeling skipped (disabled or no data)")

    if df.empty:
        print("[DONE] No ads in the selected time window.")
        return

    master_path = Path(config.MASTER_XLSX)
    master = load_excel(master_path)
    combined = pd.concat([master, df], ignore_index=True, sort=False)
    combined = deduplicate(combined, config.DEDUP_KEYS)

    save_excel(combined, master_path)
    print(f"[DONE] Master Excel file updated: {master_path} ({len(combined)} rows)")
    
    # Generate weekly summaries if enabled
    if config.ENABLE_WEEKLY_SUMMARIES:
        print("Generating weekly summaries...")
        try:
            summary_path = generate_all_summaries()
            print(f"[DONE] Weekly summaries generated: {summary_path}")
        except Exception as e:
            print(f"[ERROR] Failed to generate summaries: {e}")
    else:
        print("Weekly summaries disabled in config.")

if __name__ == "__main__":
    main()
