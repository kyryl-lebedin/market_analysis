# implement
# - cleaning url from bollocks
# - cleaning duplicates in description and job name
# - write simple functoins
import pandas as pd
from urllib.parse import urlparse
from pathlib import Path
from typing import Dict, Optional


def extract_domain(url):
    if not url:
        return None
    try:
        parsed = urlparse(str(url))
        return parsed.netloc
    except:
        return None


def unique_domains(df: pd.DataFrame, print_results: bool = False) -> pd.Series:

    counts = df["home_url"].apply(extract_domain).dropna().value_counts()

    if print_results:
        for domain, count in counts.items():
            print(f"{domain}: {count}")

    return counts


def drop_rare_domains(
    df: pd.DataFrame, top_n: int, copy: bool = False
) -> Optional[pd.DataFrame]:
    """
    Keep only rows with domains that are in the top N most frequent domains.

    Args:
        df: DataFrame with 'home_url' column
        top_n: Number of top domains to keep
        copy: Whether to return a copy or modify in place

    Returns:
        DataFrame with only top N domains, or None if error
    """
    if copy:
        df = df.copy()

    # Get domain counts
    domain_counts = unique_domains(df)

    if top_n > len(domain_counts):
        print(
            f"top_n ({top_n}) is greater than the number of unique domains ({len(domain_counts)})"
        )
        return None

    # Get top N domains to keep
    top_domains = domain_counts.head(top_n).index.tolist()

    # Add domain column temporarily for filtering
    df["domain"] = df["home_url"].apply(extract_domain)

    # Filter to keep only rows with top domains
    filtered_df = df[df["domain"].isin(top_domains)].copy()

    # Remove the temporary domain column
    filtered_df = filtered_df.drop("domain", axis=1)

    return filtered_df


def strip_url(df: pd.DataFrame, copy: bool = False):
    """
    Strip main known domains to avoid difficulties for scraping
    """
    # basic version - just strip everything until ?

    if copy:
        df = df.copy()

    def strip(url: str) -> str:
        if not url or pd.isna(url):  # Handle None/NaN values
            return url
        if "?" in url:
            url = url.split("?")[0]
        return url

    df["home_url"] = df["home_url"].apply(strip)

    return df


PROJECT_ROOT = Path(__file__).parent.parent.parent
URL_DATA_DIR = PROJECT_ROOT / "data" / "url"
CLEANURL_DATA_DIR = PROJECT_ROOT / "data" / "clean_url"
path = URL_DATA_DIR / "data_scientist_gbraw_home_url.parquet"
df = pd.read_parquet(path)

filtered_df = drop_rare_domains(df, 4)
filtered_df = strip_url(filtered_df)
path = CLEANURL_DATA_DIR / "data_scientist_gbraw_clean_url.parquet"

filtered_df.to_parquet(path)
