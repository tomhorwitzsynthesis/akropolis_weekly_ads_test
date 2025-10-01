import pandas as pd
from pathlib import Path

def load_excel(path: Path) -> pd.DataFrame:
    """Load data from Excel file, returning empty DataFrame if file doesn't exist."""
    if path.exists():
        return pd.read_excel(path, dtype=str)
    return pd.DataFrame()

def save_excel(df: pd.DataFrame, path: Path) -> None:
    """Save DataFrame to Excel file with proper formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(path, index=False, engine='openpyxl')

def deduplicate(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Remove duplicate rows based on specified keys."""
    for k in keys:
        if k not in df: df[k] = pd.NA
        df[k] = df[k].astype("string")
    return df.drop_duplicates(subset=keys, keep="first")

# Legacy function names for backward compatibility
def load_csv(path: Path) -> pd.DataFrame:
    """Legacy function - now loads Excel files."""
    return load_excel(path)

def save_csv(df: pd.DataFrame, path: Path) -> None:
    """Legacy function - now saves Excel files."""
    save_excel(df, path)
