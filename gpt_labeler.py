import os
import re
import json
import hashlib
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple
import pandas as pd
from tqdm import tqdm
from openai import OpenAI

# ----------------------------
# CONFIG
# ----------------------------
MODEL = "gpt-4o-mini"
TEMPERATURE = 0
MAX_WORKERS = 10  # Conservative for rate limits
MAX_CHARS_PER_AD = 1400

# Column names in your dataset
COL_TEXT = "snapshot/body/text"
COL_BRAND = "ad_details/advertiser/ad_library_page_info/page_info/page_name"
COL_SUMMARY = "ad_summary"
COL_CLUSTER_1 = "cluster_1"  # Most appropriate
COL_CLUSTER_2 = "cluster_2"  # Second most appropriate
COL_CLUSTER_3 = "cluster_3"  # Third most appropriate

# ----------------------------
# Helpers
# ----------------------------
def normalize_text(s):
    """Normalize text by cleaning whitespace and line breaks."""
    if not isinstance(s, str):
        return ""
    s = s.replace("\r", "\n")
    s = re.sub(r"\n+", "\n", s)
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

def compact_text(s, limit=MAX_CHARS_PER_AD):
    """Truncate text to limit while preserving readability."""
    s = normalize_text(s)
    return s if len(s) <= limit else (s[:limit] + "…")

def hash_text(s: str) -> str:
    """Create hash for text deduplication."""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def parse_label_line(text):
    """Parse the label line format: 'Labels: <Theme A>; <Theme B>; <Theme C>'"""
    if not isinstance(text, str):
        return [None, None, None]
    m = re.search(r"Labels\s*:\s*(.+)$", text.strip(), flags=re.I)
    if not m:
        return [None, None, None]
    parts = [p.strip() for p in m.group(1).split(";") if p.strip()]
    parts = parts[:3]
    while len(parts) < 3:
        parts.append(None)
    return parts

# ----------------------------
# GPT Prompts
# ----------------------------
SUMMARY_SYSTEM_PROMPT = (
    "You are a precise annotator of ad copy.\n"
    "Given an ad's text, return a ONE-SENTENCE description of the clear product/service/promotion being advertised.\n"
    "Rules:\n"
    "- If a clear single product/service/promotion/venue/event is identifiable, describe it succinctly in one sentence.\n"
    "- If the ad is only brand building, employer branding, atmosphere, or ambiguous with no concrete offer, still summarize the ad in one sentence.\n"
    "- Keep it factual (no hype), <= 140 characters where feasible, no emojis, no hashtags, no URLs.\n"
    "- Treat promotions/discount weekends/contests as valid 'products' (e.g., '50% off non-food at Maxima 05-16–05-18').\n"
    "- ALWAYS return everything in English, even if the ad is in another language!\n"
    'Return STRICT JSON ONLY as: {"summary":"<ONE_SENTENCE_OR_NONE>"}'
)

CLUSTER_SYSTEM_PROMPT = (
    "You are labeling a product/promotion one-liner against a FIXED taxonomy.\n"
    "Rules:\n"
    "- Choose 1 to 3 labels from ALLOWED THEMES (listed below with examples).\n"
    "- The FIRST label must be the single MOST APPROPRIATE cluster.\n"
    "- If no cluster fits, output OTHER.\n"
    "- VERY IMPORTANT: do NOT force-fit; keep OTHER if uncertain.\n"
    "- Output ENGLISH only in EXACTLY this format:\n"
    "Labels: <Theme A>; <Theme B>; <Theme C>\n"
    "(Use 1–3 labels; separate with semicolons; do not number them.)\n"
    "- Prefer the most specific matching themes.\n\n"
    "Output requirement:\n"
    "- Each cluster name is followed by a dash and examples. RETURN ONLY the text before the dash (the cluster name itself), not the examples.\n"
    "  Example: If you pick 'Seasonal Promotions and Discounts — Christmas sale, Black Friday offers', output just 'Seasonal Promotions and Discounts'.\n\n"
    "Key distinction:\n"
    "- Seasonal Promotions and Discounts = time-bound events linked to a specific season, holiday, or calendar moment (e.g. Christmas sale, Black Friday, back-to-school discounts).\n"
    "- General Discounts and Promotions = price cuts or deals not tied to a season or holiday (e.g. permanent weekly sale, everyday low prices).\n\n"
    "Clarification:\n"
    "- Shopping Experiences = initiatives improving the overall mall/supermarket visit, unrelated to individual store products. Examples: free parking, free changing rooms, mall-wide gift card, stroller rental, lounge areas.\n\n"
    "Available clusters (with illustrative examples—DO NOT RETURN the examples, just the theme name before the dash!):\n"
    "1. Seasonal Promotions and Discounts — Christmas sale, Black Friday offers, Easter weekend deals.\n"
    "2. Community Engagement and Events — charity drive, blood donation day, local farmer market.\n"
    "3. Health and Wellness Initiatives — free health check, flu shot clinic, yoga session.\n"
    "4. Family-Friendly Activities — kids’ play zone, family movie day, puppet show.\n"
    "5. Fashion and Style Trends — new clothing line launch, styling workshop.\n"
    "6. Food and Culinary Experiences — cooking class, wine tasting, gourmet pop-up.\n"
    "7. Contests and Giveaways — raffle for prizes, social media giveaway.\n"
    "8. Shopping Experiences — free parking, free changing rooms, mall gift card, stroller rental.\n"
    "9. Beauty and Personal Care — skincare demo, hair salon discounts.\n"
    "10. Sustainable Practices and Eco-Friendly Initiatives — recycling program, zero-waste fair.\n"
    "11. Technology and Innovation — tech gadget demo, AR shopping guide.\n"
    "12. Entertainment and Leisure Activities — live concert, art performance.\n"
    "13. Pet Care and Events — pet adoption day, pet grooming promo.\n"
    "14. Cultural and Artistic Experiences — art exhibition, craft workshop.\n"
    "15. Travel and Vacation Essentials — luggage sale, travel insurance promo.\n"
    "16. Home and Lifestyle Products — furniture discounts, home décor ideas.\n"
    "17. Education and Learning Activities — coding camp, book reading.\n"
    "18. Sports and Fitness — sports gear sale, fitness challenge.\n"
    "19. Job Opportunities and Career Development — job fair, career coaching.\n"
    "20. Customer Engagement and Loyalty Programs — new loyalty card, double points week.\n"
    "21. Warnings and Announcements — changed opening hours, construction notice.\n"
    "22. General Discounts and Promotions — everyday low prices, ongoing 2-for-1 deal.\n"
)


def build_summary_prompt(ad_text: str) -> str:
    """Build user prompt for summary generation."""
    return f"Ad text:\n{ad_text}"

def build_cluster_prompt(ad_text: str) -> str:
    """Build user prompt for cluster categorization."""
    return f"Item:\n{ad_text}\n\nChoose 1–3 from ALLOWED THEMES."

# ----------------------------
# Model calls
# ----------------------------
def get_openai_client() -> OpenAI:
    """Get OpenAI client with API key from environment or config."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        # Try to import from config if available
        try:
            import config
            api_key = getattr(config, 'OPENAI_API_KEY', None)
        except ImportError:
            pass
    
    if not api_key:
        raise ValueError("Set OPENAI_API_KEY environment variable or add it to config.py")
    
    return OpenAI(api_key=api_key)

def generate_summary(ad_text: str) -> str:
    """Generate a one-sentence summary for an ad."""
    try:
        client = get_openai_client()
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": SUMMARY_SYSTEM_PROMPT},
                {"role": "user", "content": build_summary_prompt(ad_text)},
            ],
            temperature=TEMPERATURE,
            response_format={"type": "json_object"},
            max_tokens=200,
        )
        raw = (resp.choices[0].message.content or "").strip()
        # Parse JSON strictly or by bracket slice
        data = json.loads(raw) if raw.startswith("{") else json.loads(raw[raw.find("{"):raw.rfind("}")+1])
        summary = (data.get("summary") or "").strip()
        if not summary or summary.upper() == "NULL":
            return "NONE"
        summary = re.sub(r'https?://\S+', '', summary)
        summary = normalize_text(summary)
        if len(summary) > 160:
            summary = summary[:160].rstrip(" ,.;:") + "."
        return summary
    except Exception as e:
        print(f"[ERROR] Summary generation failed: {e}")
        return "NONE"

def generate_clusters(ad_text: str) -> Tuple[str, str, str]:
    """Generate cluster categories for an ad."""
    try:
        client = get_openai_client()
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": CLUSTER_SYSTEM_PROMPT},
                {"role": "user", "content": build_cluster_prompt(ad_text)},
            ],
            temperature=TEMPERATURE,
            max_tokens=80,
        )
        raw = (resp.choices[0].message.content or "").strip()
        # Parse the label line format
        clusters = parse_label_line(raw)
        return clusters[0], clusters[1], clusters[2]
    except Exception as e:
        print(f"[ERROR] Cluster generation failed: {e}")
        return "NONE", None, None

def process_ad_with_gpt(ad_text: str) -> Tuple[str, str, str, str]:
    """Process a single ad to generate summary and three ranked clusters."""
    summary = generate_summary(ad_text)
    cluster1, cluster2, cluster3 = generate_clusters(ad_text)
    return summary, cluster1, cluster2, cluster3

# ----------------------------
# Main processing functions
# ----------------------------
def label_ads_with_gpt(df: pd.DataFrame, max_workers: int = MAX_WORKERS) -> pd.DataFrame:
    """
    Add GPT-generated summaries and clusters to a DataFrame of ads.
    
    Args:
        df: DataFrame with ad data
        max_workers: Number of parallel workers for GPT API calls
    
    Returns:
        DataFrame with added summary and cluster columns
    """
    if COL_TEXT not in df.columns:
        print(f"[WARNING] Column {COL_TEXT} not found, skipping GPT labeling")
        return df
    
    # Clean and prepare data
    df = df.copy()
    df[COL_TEXT] = df[COL_TEXT].map(lambda x: compact_text(x if isinstance(x, str) else ""))
    
    # Remove empty texts
    df = df[df[COL_TEXT].str.len() > 0].reset_index(drop=True)
    
    if df.empty:
        print("[WARNING] No valid ad texts found for GPT labeling")
        return df
    
    # Deduplicate based on normalized text
    df["__norm__"] = df[COL_TEXT].map(lambda s: normalize_text(s).lower())
    df = df.drop_duplicates(subset=["__norm__"], keep="first").reset_index(drop=True)
    
    texts = df[COL_TEXT].tolist()
    print(f"[INFO] Processing {len(texts)} unique ads with GPT...")
    
    # Initialize result columns
    summaries = [None] * len(texts)
    cluster1_list = [None] * len(texts)
    cluster2_list = [None] * len(texts)
    cluster3_list = [None] * len(texts)
    
    # Parallel processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_ad_with_gpt, text): i for i, text in enumerate(texts)}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="GPT Labeling"):
            i = futures[future]
            try:
                summary, cluster1, cluster2, cluster3 = future.result()
                summaries[i] = summary
                cluster1_list[i] = cluster1
                cluster2_list[i] = cluster2
                cluster3_list[i] = cluster3
            except Exception as e:
                print(f"[ERROR] Failed to process ad {i}: {e}")
                summaries[i] = "NONE"
                cluster1_list[i] = "NONE"
                cluster2_list[i] = None
                cluster3_list[i] = None
    
    # Add results to DataFrame
    df[COL_SUMMARY] = summaries
    df[COL_CLUSTER_1] = cluster1_list
    df[COL_CLUSTER_2] = cluster2_list
    df[COL_CLUSTER_3] = cluster3_list
    
    # Clean up temporary column
    df = df.drop(columns=["__norm__"])
    
    print(f"[DONE] GPT labeling completed for {len(df)} ads")
    return df

def get_cluster_stats(df: pd.DataFrame) -> Dict[str, int]:
    """Get statistics on cluster distribution."""
    if COL_CLUSTER_1 not in df.columns:
        return {}
    
    cluster_counts = {}
    
    # Count all clusters from all three columns
    for col in [COL_CLUSTER_1, COL_CLUSTER_2, COL_CLUSTER_3]:
        if col in df.columns:
            for cluster in df[col]:
                if cluster and cluster != "NONE" and pd.notna(cluster):
                    cluster_counts[cluster] = cluster_counts.get(cluster, 0) + 1
    
    return dict(sorted(cluster_counts.items(), key=lambda x: x[1], reverse=True))

def print_cluster_stats(df: pd.DataFrame):
    """Print cluster statistics."""
    stats = get_cluster_stats(df)
    if not stats:
        print("[INFO] No cluster statistics available")
        return
    
    print("\n[CLUSTER STATISTICS]")
    print("-" * 50)
    for cluster, count in stats.items():
        print(f"{cluster}: {count}")
    print("-" * 50)
