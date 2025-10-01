import pandas as pd
from dateutil import tz
import json
import ast

REQUIRED = ["inputUrl", "totalCount", "pageID", "adArchiveID", "startDateFormatted"]

def flatten(items: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(items)
    return pd.json_normalize(df.to_dict(orient="records"), sep="/") if not df.empty else df

def ensure_columns(df: pd.DataFrame, url: str, total_count: int) -> pd.DataFrame:
    df = df.copy()
    if "inputUrl" not in df: df["inputUrl"] = url
    if "totalCount" not in df: df["totalCount"] = total_count
    if "pageID" not in df:
        df["pageID"] = df.get("pageId") or df.get("snapshot/page_id") or pd.NA
    if "adArchiveID" not in df:
        df["adArchiveID"] = df.get("adArchiveId") or df.get("snapshot/ad_archive_id") or pd.NA
    if "startDateFormatted" not in df:
        candidates = ["startDate","adDeliveryStartTime","created_time","snapshot/publish_date","snapshot/created_at"]
        c = next((c for c in candidates if c in df), None)
        df["startDateFormatted"] = pd.to_datetime(df[c], errors="coerce").dt.date.astype("string") if c else pd.NA
    else:
        df["startDateFormatted"] = pd.to_datetime(df["startDateFormatted"], errors="coerce").dt.date.astype("string")
    for col in REQUIRED:
        if col not in df: df[col] = pd.NA
        df[col] = df[col].astype("string")
    return df

def extract_card_body_text(cards_data):
    """
    Extract body text from snapshot/cards data.
    
    Args:
        cards_data: String representation of cards data (list of dicts)
    
    Returns:
        String containing the body text from the first card, or None if not found
    """
    # Handle different data types safely
    if cards_data is None:
        return None
    
    # Check if it's a pandas Series or array-like object
    if hasattr(cards_data, '__len__') and not isinstance(cards_data, (str, dict)):
        if len(cards_data) == 0:
            return None
        # If it's a Series or array, get the first element
        cards_data = cards_data.iloc[0] if hasattr(cards_data, 'iloc') else cards_data[0]
    
    # Now check if the processed data is None or empty
    if cards_data is None or (isinstance(cards_data, str) and not cards_data.strip()):
        return None
    
    try:
        # Try to parse as JSON first
        if isinstance(cards_data, str):
            cards = json.loads(cards_data)
        else:
            cards = cards_data
        
        # If it's a list of cards, get the first one
        if isinstance(cards, list) and len(cards) > 0:
            first_card = cards[0]
            if isinstance(first_card, dict) and 'body' in first_card:
                return first_card['body']
        
        # If it's a single card dict
        elif isinstance(cards, dict) and 'body' in cards:
            return cards['body']
            
    except (json.JSONDecodeError, ValueError, TypeError):
        # If JSON parsing fails, try to parse as Python literal
        try:
            if isinstance(cards_data, str):
                cards = ast.literal_eval(cards_data)
            else:
                cards = cards_data
                
            if isinstance(cards, list) and len(cards) > 0:
                first_card = cards[0]
                if isinstance(first_card, dict) and 'body' in first_card:
                    return first_card['body']
            elif isinstance(cards, dict) and 'body' in cards:
                return cards['body']
        except (ValueError, SyntaxError, TypeError):
            pass
    
    return None

def process_card_extraction(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process DataFrame to extract body text from cards when main body contains {{product.brand}}.
    
    Args:
        df: DataFrame with ad data
    
    Returns:
        DataFrame with updated body text from cards where applicable
    """
    df = df.copy()
    
    # Check if we have the required columns
    if 'snapshot/body/text' not in df.columns or 'snapshot/cards' not in df.columns:
        return df
    
    # Create a mask for rows where body text contains {{product.brand}}
    product_brand_mask = df['snapshot/body/text'].astype(str).str.contains(r'\{\{product\.brand\}\}', na=False)
    
    if not product_brand_mask.any():
        return df
    
    print(f"[INFO] Found {product_brand_mask.sum()} ads with {{product.brand}} in body text")
    
    # Extract body text from cards for rows that match the condition
    def extract_and_replace(row):
        if pd.isna(row['snapshot/body/text']) or '{{product.brand}}' not in str(row['snapshot/body/text']):
            return row['snapshot/body/text']
        
        card_body = extract_card_body_text(row['snapshot/cards'])
        if card_body:
            print(f"[CARD EXTRACT] Replaced {{product.brand}} with card body: {card_body[:100]}...")
            return card_body
        
        return row['snapshot/body/text']
    
    # Apply the extraction to matching rows
    df.loc[product_brand_mask, 'snapshot/body/text'] = df.loc[product_brand_mask].apply(extract_and_replace, axis=1)
    
    return df

def filter_recent(df: pd.DataFrame, tz_name: str, days_back: int) -> pd.DataFrame:
    tzinfo = tz.gettz(tz_name)
    now = pd.Timestamp.now(tz=tzinfo).normalize()
    keep_dates = {(now - pd.Timedelta(days=i)).date() for i in range(days_back)}
    dates = pd.to_datetime(df["startDateFormatted"], errors="coerce").dt.date
    return df[dates.isin(keep_dates)].copy()
