"""
Home URL Processing Module

This module provides functionality to process job listings and extract home URLs
from redirect URLs. It uses concurrent processing with proxy support to handle
large datasets efficiently while respecting rate limits and avoiding blocks.

Key Features:
    - Concurrent processing of redirect URLs using ThreadPoolExecutor
    - Proxy support for web scraping with BrightData integration
    - Meta refresh redirect detection and handling
    - Comprehensive error handling and logging
    - Parquet file I/O for efficient data storage

Classes:
    HomeUrlProcessor: Main class for processing job URLs and extracting home URLs

Functions:
    configure_logging: Sets up logging configuration for the application
    main: Main execution function that orchestrates the URL processing workflow

"""

from concurrent.futures import ThreadPoolExecutor
import logging
from pathlib import Path
import pandas as pd
import os
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional
import requests
import uuid
import re
import certifi
import sys
from urllib.parse import urlparse

from urllib3.util import url


from job_pipeline.logging_conf import setup_logging, get_logger
from job_pipeline.config import LOGS_DIR
from job_pipeline.steps.io_utils import (
    adzuna_read_raw_bronze,
    adzuna_save_home_url_silver,
)
from job_pipeline.config import (
    LOGS_DIR,
    BD_HOST,
    BD_PASSWORD,
    BD_PORT,
    BD_USERNAME_BASE,
    BD_COUNTRY,
)

# set up logging

log = get_logger("pipeline")

if __name__ == "__main__":
    # if ingestion is run locally
    setup_logging(app_name="add_home_url", level="INFO", log_dir=LOGS_DIR)
    # Force the logger to use the configured "add_home_url" logger
    log = get_logger("add_home_url")
    log.info("Starting Home URL processing...")


#######################################################################################


class HomeUrlProcessor:
    """
    A processor for extracting home URLs from job listing redirect URLs.

    This class handles the concurrent processing of redirect URLs to extract
    the final home URLs of job postings. It uses proxy support and handles
    various types of redirects including meta refresh redirects.

    Attributes:
        BD_HOST (str): BrightData proxy host address
        BD_PORT (int): BrightData proxy port number
        BD_USERNAME_BASE (str): Base username for proxy authentication
        BD_PASSWORD (str): Password for proxy authentication
        BD_COUNTRY (str): Country code for proxy location
        CERT_PATH (str): Path to SSL certificate for residential proxy connections

    """

    def __init__(
        self,
        BD_HOST: str,
        BD_PORT: int,
        BD_USERNAME_BASE: str,
        BD_PASSWORD: str,
        BD_COUNTRY: str,
    ) -> None:
        """
        Initialize the HomeUrlProcessor with proxy configuration.

        Args:
            BD_HOST: BrightData proxy host address (e.g., "proxy.brightdata.com")
            BD_PORT: BrightData proxy port number (typically 33335)
            BD_USERNAME_BASE: Base username for proxy authentication
            BD_PASSWORD: Password for proxy authentication
            BD_COUNTRY: Country code for proxy location (e.g., "US", "GB")
            CERT_PATH: Path to SSL certificate file for secure proxy connections

        Raises:
            ValueError: If any required parameter is None or empty
        """
        self.BD_HOST = BD_HOST
        self.BD_PORT = BD_PORT
        self.BD_USERNAME_BASE = BD_USERNAME_BASE
        self.BD_PASSWORD = BD_PASSWORD
        self.BD_COUNTRY = BD_COUNTRY

    def add_home_urls(self, jobs: pd.DataFrame, max_workers: int) -> pd.DataFrame:
        """
        Process job listings to extract home URLs from redirect URLs.

        This method processes a DataFrame of job listings, extracting home URLs
        from their redirect URLs using concurrent processing. It handles the
        processing in batches to manage memory and avoid overwhelming the target
        servers.

        Args:
            jobs: DataFrame containing job listings with a 'redirect_url' column
            max_workers: Maximum number of concurrent threads for processing

        Returns:
            DataFrame: Original jobs DataFrame with an additional 'home_url' column
            containing the extracted home URLs

        Raises:
            KeyboardInterrupt: If processing is interrupted by user (Ctrl+C)
            Exception: For unexpected errors during batch processing

        Note:
            - Processing is done in batches to manage memory usage
            - Interrupted processing will fill remaining URLs with None
            - Progress is logged for each batch completion

        """

        # Make a copy to avoid the warning and ensure we're not modifying a slice
        jobs = jobs.copy()

        redirect_urls = jobs["redirect_url"]

        home_urls = []

        p1 = 0
        p2 = max_workers

        for i in range(
            len(redirect_urls) // max_workers
            + (1 if len(redirect_urls) % max_workers else 0)
        ):
            try:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    home_url_batch = list(
                        executor.map(self.get_home_url, redirect_urls[p1:p2])
                    )

                    home_urls += home_url_batch
                    log.info(
                        f"Processed batch {p1}-{min(p2, len(redirect_urls))} of {len(redirect_urls)}"
                    )
                    p1 += max_workers
                    p2 += max_workers
            except KeyboardInterrupt:
                log.info(
                    f"you interrupted, stopped on batch {p1}-{p2} of {len(redirect_urls)}"
                )
                # fill the rest with none (do i need to do it in pd or will it fill automatically?)
                # try without it and see if index matching works
                home_urls += [None] * min(0, len(redirect_urls) - len(home_urls))
                break
            except Exception as e:
                log.error(
                    f"unknown error in batch {p1}-{p2} of {len(redirect_urls)}: {e}"
                )
                break

        # update the dataframe with home jobs
        # see if index matching works
        jobs["home_url"] = home_urls
        return jobs

    def add_home_urls_robust(
        self,
        jobs: pd.DataFrame,
        max_workers: int,
        acceptable_fault_rate: float,
        max_tries: int,
        initial_process: bool,
    ) -> pd.DataFrame:

        if initial_process:
            processed_jobs = self.add_home_urls(jobs, max_workers)
        else:
            processed_jobs = jobs.copy()  # Avoid modifying original

        fails = processed_jobs[
            processed_jobs["home_url"] == "either blocked or something else (license)"
        ]

        non_adzuna_url_num = (
            processed_jobs["redirect_url"].str.contains("/land/", na=False).sum()
        )

        rate = len(fails) / non_adzuna_url_num if len(processed_jobs) > 0 else 0

        try:
            while rate > acceptable_fault_rate and max_tries:
                fail_indices = fails.index
                fails_upd = self.add_home_urls(fails, max_workers)

                # Update processed jobs indices
                processed_jobs.loc[fail_indices, "home_url"] = fails_upd["home_url"]

                fails = fails_upd[
                    fails_upd["home_url"]
                    == "either blocked or something else (license)"
                ]

                rate = len(fails) / non_adzuna_url_num
                max_tries -= 1

        except KeyboardInterrupt:
            return processed_jobs

        return processed_jobs

    def get_home_url(
        self,
        redirect_url: str,
        max_redirects: int = 10,
        timeout: int = 15,
    ) -> Optional[str]:
        """
        Extract the home URL from a redirect URL.

        This method follows redirects and extracts the final home URL. It handles
        both HTTP redirects and meta refresh redirects. For URLs containing '/land/',
        it performs web scraping to extract the final destination.

        Args:
            redirect_url: The redirect URL to process
            max_redirects: Maximum number of redirects to follow (default: 10)
            timeout: Request timeout in seconds (default: 15)

        Returns:
            Optional[str]: The extracted home URL, or None if extraction fails.
            Returns the original URL if it doesn't contain '/land/'.

        Note:
            - Only processes URLs containing '/land/' pattern
            - Uses realistic browser headers to avoid detection
            - Handles meta refresh redirects in HTML content
            - Returns specific error message for blocked/licensed content

        """
        if "/land/" in redirect_url:
            try:
                session = requests.Session()
                session.max_redirects = max_redirects

                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",  # not sure about this one
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Cache-Control": "max-age=0",
                }

                proxies = self._get_proxies()

                response = session.get(
                    redirect_url,
                    headers=headers,
                    timeout=timeout,
                    proxies=proxies,
                    allow_redirects=True,
                    verify=certifi.where(),  # use 'certificate' when using residential proxy, 'certifi.where()' when using datacenter proxy
                )

                content = response.text

                # check for meta refresh redirects
                meta_refresh_match = re.search(
                    r'<meta[^>]*http-equiv=["\']?refresh["\']?[^>]*content=["\']?\d+;\s*url=([^"\'>\s]+)',
                    content,
                    re.IGNORECASE,
                )
                if meta_refresh_match:
                    redirect_url = meta_refresh_match.group(1)

                    if "click.appcast" in redirect_url:
                        # For click.appcast URLs, we need to make another request to get the final redirect
                        try:
                            response2 = session.get(
                                redirect_url,
                                headers=headers,
                                timeout=timeout,
                                proxies=proxies,
                                allow_redirects=True,
                                verify=certifi.where(),
                            )
                            content2 = response2.text

                            # Check for JavaScript redirects in the click.appcast response
                            js_redirect_match = re.search(
                                r'navigateTo\([^,]+,\s*[^,]+,\s*["\']([^"\']+)["\']',
                                content2,
                                re.IGNORECASE,
                            )
                            if js_redirect_match:
                                return js_redirect_match.group(1)

                            # Check for other JavaScript redirect patterns
                            js_location_match = re.search(
                                r'window\.location\.(?:href|replace)\s*=\s*["\']([^"\']+)["\']',
                                content2,
                                re.IGNORECASE,
                            )
                            if js_location_match:
                                return js_location_match.group(1)

                        except Exception as e:
                            print(f"Error processing click.appcast redirect: {e}")
                            return "either blocked or something else (license)"

                    return redirect_url
                else:
                    # Check if original URL contains click.appcast and handle it directly
                    if "click.appcast" in redirect_url:
                        # Check for JavaScript redirects in the original content
                        js_redirect_match = re.search(
                            r'navigateTo\([^,]+,\s*[^,]+,\s*["\']([^"\']+)["\']',
                            content,
                            re.IGNORECASE,
                        )
                        if js_redirect_match:
                            return js_redirect_match.group(1)

                        # Check for other JavaScript redirect patterns
                        js_location_match = re.search(
                            r'window\.location\.(?:href|replace)\s*=\s*["\']([^"\']+)["\']',
                            content,
                            re.IGNORECASE,
                        )
                        if js_location_match:
                            return js_location_match.group(1)

                    # most likely
                    return "either blocked or something else (license)"

            # we don't really know the error behaviour, so will build up something, to catch ban and non found
            except Exception as e:
                print(f"Error trying to obtain home url for {redirect_url}: {e}")
                return None

        else:
            return redirect_url

    def _get_proxies(self) -> Dict[str, str]:
        """
        Generate proxy configuration for requests.

        Creates a unique proxy session using BrightData credentials with a
        randomly generated session ID to ensure proper session management.

        Returns:
            Dict[str, str]: Dictionary containing 'http' and 'https' proxy URLs
            with authentication credentials

        """
        user = f"{self.BD_USERNAME_BASE}-session-{uuid.uuid4().hex}"
        proxy_url = f"http://{user}:{self.BD_PASSWORD}@{self.BD_HOST}:{self.BD_PORT}"
        return {"http": proxy_url, "https": proxy_url}

    ############################# URL CLEANIG FUNCTIONALITY ################################

    def clean_urls(
        self,
        df: pd.DataFrame,
        # top_n: int,
        copy: bool = True,
        specific_domains: List = [],
    ) -> pd.DataFrame:

        if copy:
            df = df.copy()
        # df = self.drop_rare_domains(df, top_n, specific_domains=specific_domains)
        df = self.keep_specific_domains(df, specific_domains)
        df = self.strip_url(df)
        return df

    def extract_domain(self, url):
        if not url:
            return None
        try:
            parsed = urlparse(str(url))
            return parsed.netloc
        except:
            return None

    def unique_domains(self, df: pd.DataFrame, log_results: bool = True) -> pd.Series:

        counts = df["home_url"].apply(self.extract_domain).dropna().value_counts()

        if log_results:
            log.info("Unique domains:")
            for domain, count in counts.items():
                log.info(f"{domain}: {count}")

        return counts

    def keep_specific_domains(
        self, df: pd.DataFrame, specific_domains: List
    ) -> pd.DataFrame:
        df["domain"] = df["home_url"].apply(self.extract_domain)
        df = df[df["domain"].isin(specific_domains)]
        df = df.drop("domain", axis=1)
        log.info(f"Filtered dataframe to keep only specific domains {specific_domains}")
        return df

    def drop_rare_domains(
        self,
        df: pd.DataFrame,
        top_n: int,
        copy: bool = True,
        specific_domains: List = [],
        log_results: bool = True,
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
        domain_counts = self.unique_domains(df)

        if top_n > len(domain_counts):
            print(
                f"top_n ({top_n}) is greater than the number of unique domains ({len(domain_counts)})"
            )
            return None

        # Get top N domains to keep
        top_domains = domain_counts.head(top_n).index.tolist()

        if log_results:
            log.info("Top domains to keep:")
            for domain in top_domains:
                log.info(f"{domain}")

            log.info("Specific domains to exclude:")
            for domain in specific_domains:
                log.info(f"{domain}")

        if specific_domains:
            top_domains = [
                domain for domain in top_domains if domain not in specific_domains
            ]

        # Add domain column temporarily for filtering
        df["domain"] = df["home_url"].apply(self.extract_domain)

        # Filter to keep only rows with top domains
        filtered_df = df[df["domain"].isin(top_domains)].copy()

        # Remove the temporary domain column
        filtered_df = filtered_df.drop("domain", axis=1)

        log.info(
            "Filtered dataframe to keep only top domains and exclude specific domains"
        )

        return filtered_df

    def strip_url(self, df: pd.DataFrame, copy: bool = True):
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

        log.info("URLs stripped")

        return df


def main() -> None:
    """
    Main execution function for processing job URLs.

    This function orchestrates the complete workflow:
    1. Loads job data from a parquet file
    2. Initializes the HomeUrlProcessor with environment variables
    3. Processes URLs to extract home URLs
    4. Saves the results to a new parquet file

    The function reads configuration from environment variables and processes
    a file named "failed_once.parquet" from the raw data directory.

    Environment Variables Required:
        BD_HOST: BrightData proxy host
        BD_PORT: BrightData proxy port
        BD_USERNAME_BASE: Proxy username base
        BD_PASSWORD: Proxy password
        BD_COUNTRY: Proxy country code

    Raises:
        FileNotFoundError: If the input parquet file doesn't exist
        KeyError: If required environment variables are missing
        Exception: For other processing errors

    Note:
        - Input file: data/raw/failed_once.parquet
        - Output file: data/url/failed_once_home_url.parquet
        - Uses 50 concurrent workers for processing
    """
    # load file

    file_name = "adzuna_test_page_list_191306"
    jobs = adzuna_read_raw_bronze(file_name)

    url_processor = HomeUrlProcessor(
        BD_HOST=BD_HOST,
        BD_PORT=int(BD_PORT),
        BD_USERNAME_BASE=BD_USERNAME_BASE,
        BD_PASSWORD=BD_PASSWORD,
        BD_COUNTRY=BD_COUNTRY,
    )

    # url_jobs = url_processor.add_home_urls(jobs, max_workers=50)
    url_jobs = url_processor.add_home_urls_robust(jobs, 50, 0.01, 10, True)
    url_jobs = url_processor.clean_urls(
        url_jobs, specific_domains=["www.adzuna.co.uk", "www.linkedin.com"]
    )

    # save
    adzuna_save_home_url_silver(url_jobs, file_name)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"Error: {e}")
