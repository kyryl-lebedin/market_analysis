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


def configure_logging(level="INFO", log_file="add_home_url_adzuna.log"):
    """
    Configures logging for the application.

    Args:
        level: Logging level (e.g., "INFO", "DEBUG", "WARNING")
        log_folder: Directory to save log files
        log_file: Name of the log file
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
    def __init__(
        self,
        BD_HOST: str,
        BD_PORT: int,
        BD_USERNAME_BASE: str,
        BD_PASSWORD: str,
        BD_COUNTRY: str,
        CERT_PATH: str,
    ) -> None:
        self.BD_HOST = BD_HOST
        self.BD_PORT = BD_PORT
        self.BD_USERNAME_BASE = BD_USERNAME_BASE
        self.BD_PASSWORD = BD_PASSWORD
        self.BD_COUNTRY = BD_COUNTRY
        self.CERT_PATH = CERT_PATH

    def add_home_urls(self, jobs: pd.DataFrame, max_workers: int) -> pd.DataFrame:

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

    def get_home_url(
        self,
        redirect_url: str,
        max_redirects: int = 10,
        timeout: int = 15,
    ) -> Optional[str]:
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
                    return redirect_url
                else:
                    # most likely
                    return "either blocked or something else (license)"

            # we don't really know the error behaviour, so will build up something, to catch ban and non found
            except Exception as e:
                print(f"Error trying to obtain home url for {redirect_url}: {e}")
                return None

        else:
            return redirect_url

    def _get_proxies(self):
        user = f"{self.BD_USERNAME_BASE}-session-{uuid.uuid4().hex}"
        proxy_url = f"http://{user}:{self.BD_PASSWORD}@{self.BD_HOST}:{self.BD_PORT}"
        return {"http": proxy_url, "https": proxy_url}


def main():
    # load file
    name = "failed_once"
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

    url_jobs = url_processor.add_home_urls(jobs, max_workers=50)

    # save url_jobs to data/url
    URL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = URL_DATA_DIR / (name + "_home_url.parquet")
    url_jobs.to_parquet(path)
    log.info(f"Saved processed data to {path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"Error: {e}")
