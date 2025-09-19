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

# load environment variables
load_dotenv()


# intialize directories
PROJECT_ROOT = Path(__file__).parent.parent.parent
LOGS_DIR = PROJECT_ROOT / "logs"
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
URL_DATA_DIR = PROJECT_ROOT / "data" / "url"
CERT_PATH = PROJECT_ROOT / "certs" / "BrightData_SSL_certificate_(port 33335).crt"


# set up logging
log = logging.getLogger(__name__)


def configure_logging(
    level: str = "INFO", log_file: str = "add_home_url_adzuna.log"
) -> None:
    """
    Configure logging for the application.

    Sets up logging with both console and file output. Creates the logs directory
    if it doesn't exist and configures the logging format with timestamps.

    Args:
        level: Logging level (e.g., "INFO", "DEBUG", "WARNING", "ERROR")
        log_file: Name of the log file to create in the logs directory

    Note:
        - Logs are written to both console and file
        - Log format includes timestamp, level, logger name, and message
        - Logs directory is created automatically if it doesn't exist

    """

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOGS_DIR / log_file

    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_path, "a")],
    )


configure_logging()


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
        CERT_PATH: str,
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
        self.CERT_PATH = CERT_PATH

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

                    p1 += max_workers
                    p2 += max_workers

                    home_urls += home_url_batch
                    print(f"Processed batch {p1}-{p2} of {len(redirect_urls)}")
            except KeyboardInterrupt:
                log.info(
                    f"you interrupted, stopped on batch {p1}-{p2} of {len(redirect_urls)}"
                )
                # fill the rest with none (do i need to do it in pd or will it fill automatically?)
                # try without it and see if index matching works
                home_urls += [None] * max(0, len(redirect_urls) - len(home_urls))
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

                certificate = self.CERT_PATH
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
    name = "data_scientist_gb"
    path = RAW_DATA_DIR / (name + ".parquet")
    jobs = pd.read_parquet(path)

    url_processor = HomeUrlProcessor(
        BD_HOST=os.getenv("BD_HOST"),
        BD_PORT=int(os.getenv("BD_PORT")),
        BD_USERNAME_BASE=os.getenv("BD_USERNAME_BASE"),
        BD_PASSWORD=os.getenv("BD_PASSWORD"),
        BD_COUNTRY=os.getenv("BD_COUNTRY"),
        CERT_PATH=CERT_PATH,
    )

    # url_jobs = url_processor.add_home_urls(jobs, max_workers=50)
    url_jobs = url_processor.add_home_urls_robust(jobs, 100, 0.01, 10, True)

    # save url_jobs to data/url
    URL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = URL_DATA_DIR / (name + "raw_home_url.parquet")
    url_jobs.to_parquet(path)
    log.info(f"Saved processed data to {path}")

    # test = url_processor.get_home_url(
    #     "https://www.adzuna.co.uk/jobs/land/ad/5382334478?se=Pm-xqNWU8BGiM5twZbwYxg&utm_medium=api&utm_source=a6e6527f&v=ED9447DD603CADF66C94B171076D9175DA28D473"
    # )
    # print(test)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"Error: {e}")
