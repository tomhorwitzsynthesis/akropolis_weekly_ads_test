from apify_client import ApifyClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from typing import List, Dict

APIFY_ACTOR = "apify/facebook-ads-scraper"

def scrape_url(client: ApifyClient, url: str, max_ads: int) -> list[dict]:
    """Scrape ads from a single URL using Apify client."""
    run_input = {
        "isDetailsPerAd": True,
        "onlyTotal": False,
        "startUrls": [{"url": url}],
        "resultsLimit": max_ads,
        "activeStatus": "",
    }
    run = client.actor(APIFY_ACTOR).call(run_input=run_input)
    dataset_id = run["defaultDatasetId"]

    out = []
    for item in client.dataset(dataset_id).iterate_items():
        item["source_url"] = url
        out.append(item)
    print(f"[OK] {url} -> {len(out)} ads (dataset {dataset_id})")
    return out

def scrape_url_with_client(url: str, token: str, max_ads: int) -> tuple[str, list[dict]]:
    """Scrape a single URL with its own client instance for thread safety."""
    client = ApifyClient(token)
    try:
        results = scrape_url(client, url, max_ads)
        return url, results
    except Exception as e:
        print(f"[ERROR] {url}: {e}")
        return url, []

def scrape_all(urls: list[str], token: str, max_ads: int, max_workers: int = 3) -> list[dict]:
    """
    Scrape all URLs in parallel using ThreadPoolExecutor.
    
    Args:
        urls: List of Facebook page URLs to scrape
        token: Apify API token
        max_ads: Maximum number of ads to fetch per URL
        max_workers: Maximum number of concurrent threads (default: 3)
    
    Returns:
        List of all scraped ad items
    """
    all_items = []
    completed_count = 0
    total_urls = len(urls)
    
    print(f"[INFO] Starting parallel scraping of {total_urls} URLs with {max_workers} workers...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all scraping tasks
        future_to_url = {
            executor.submit(scrape_url_with_client, url, token, max_ads): url 
            for url in urls
        }
        
        # Process completed tasks as they finish
        for future in as_completed(future_to_url):
            url, results = future.result()
            all_items.extend(results)
            completed_count += 1
            print(f"[PROGRESS] {completed_count}/{total_urls} URLs completed")
    
    print(f"[DONE] Parallel scraping completed. Total ads collected: {len(all_items)}")
    return all_items
