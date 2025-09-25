import pandas as pd
from typing import Optional, Dict
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from html import unescape
import json
import requests
import certifi
import uuid
from urllib.parse import urlparse


from job_pipeline.logging_conf import get_logger


# set up logging

log = get_logger(__name__)


#######################################################################################


class FullDescriptionProcessor:
    def __init__(
        self,
        BD_HOST: str,
        BD_PORT: int,
        BD_USERNAME_BASE: str,
        BD_PASSWORD: str,
        BD_COUNTRY: str,
    ) -> None:
        self.BD_HOST = BD_HOST
        self.BD_PORT = BD_PORT
        self.BD_USERNAME_BASE = BD_USERNAME_BASE
        self.BD_PASSWORD = BD_PASSWORD
        self.BD_COUNTRY = BD_COUNTRY

        # add arbitrary parser
        self.parsers = {
            "www.totaljobs.com": self.parse_totaljobs,
            "totaljobs.com": self.parse_totaljobs,
            "www.adzuna.co.uk": self.parse_adzuna,
            "adzuna.co.uk": self.parse_adzuna,
            "www.linkedin.com": self.parse_linkedin,
            "linkedin.com": self.parse_linkedin,
            "uk.linkedin.com": self.parse_linkedin,
        }

    def add_full_descriptions(
        self, jobs: pd.DataFrame, max_workers: int = 10, copy: bool = False
    ):

        if copy:
            jobs = jobs.copy()

        urls = jobs["home_url"]

        p1 = 0
        p2 = max_workers
        full_descriptions = []

        for i in range(
            len(urls) // max_workers + (1 if len(urls) % max_workers else 0)
        ):
            try:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    descriptions_batch = list(
                        executor.map(self.get_description, urls[p1:p2])
                    )

                    full_descriptions += descriptions_batch
                    log.info(
                        f"Processed batch {p1}-{min(p2, len(urls))} of {len(urls)}"
                    )
                    p1 += max_workers
                    p2 += max_workers
            except KeyboardInterrupt:
                full_descriptions += [None] * (len(urls) - len(full_descriptions))
                break
            except Exception as e:
                log.error(
                    f"unknown error in batch {p1}-{min(p2, len(urls))} of {len(urls)}: {e}"
                )
                break

        jobs["full_description"] = full_descriptions
        return jobs

    def add_full_descriptions_robust(
        self,
        jobs: pd.DataFrame,
        max_workers: int,
        acceptable_fault_rate: float,
        max_tries: int,
        initial_process: bool,
    ) -> pd.DataFrame:

        if initial_process:
            processed_jobs = self.add_full_descriptions(jobs, max_workers)
        else:
            processed_jobs = jobs.copy()

        fails = processed_jobs[processed_jobs["full_description"] == ""]

        rate = len(fails) / len(processed_jobs) if len(processed_jobs) > 0 else 0

        try:
            while rate > acceptable_fault_rate and max_tries:
                fail_indices = fails.index
                fails_upd = self.add_full_descriptions(fails, max_workers)

                processed_jobs.loc[fail_indices, "full_description"] = fails_upd[
                    "full_description"
                ]
                fails = fails_upd[fails_upd["full_description"] == ""]

                rate = len(fails) / len(processed_jobs)
                max_tries -= 1
        except KeyboardInterrupt:
            return processed_jobs

        return processed_jobs

    def get_description(
        self,
        url: str,
        max_redirects: int = 10,
        timeout: int = 15,  # Changed from tuple to int
    ) -> Optional[str]:
        try:
            session = requests.Session()
            session.max_redirects = max_redirects

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Cache-Control": "max-age=0",
            }

            proxies = self._get_proxies()
            response = session.get(
                url,
                headers=headers,
                timeout=timeout,
                proxies=proxies,
                allow_redirects=True,
                verify=certifi.where(),  # use 'certificate' when using residential proxy, 'certifi.where()' when using datacenter proxy
            )

            domain = urlparse(url).netloc.lower()

            description = self._parse_response(response.text, domain)
            # print(description)
            return description
        except Exception as e:
            log.error(f"error in get_html {url}: {e}")
            return None

    def _parse_response(self, html: str, domain: str):
        parser = self.parsers.get(domain)
        if parser:
            return parser(html)
        else:
            log.warning(f"No parser found for domain: {domain}")
            return ""

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

    def parse_adzuna(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")

        desc_section = soup.find("section", class_="adp-body")

        if not desc_section:
            return ""

        text = desc_section.get_text(separator="\n", strip=True)
        return text

    def parse_linkedin(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")

        desc_div = soup.find("div", class_="description__text")

        if not desc_div:
            return ""

        markup = desc_div.find("div", class_="show-more-less-html__markup")
        if not markup:
            return ""

        text = markup.get_text(separator="\n", strip=True)
        return text

    def parse_totaljobs(self, html: str) -> str:
        """
        Extracts job description text from a Totaljobs job page.
        Strategy:
        1) Try common DOM containers.
        2) Fall back to JSON-LD JobPosting.description (HTML), then strip tags.
        Returns plain text with line breaks.
        """
        soup = BeautifulSoup(html, "html.parser")

        # 1) Try likely DOM containers first (may work when server-rendered)
        dom = soup.select_one(
            '[data-at="jobad-description"], [data-at="jobad-content"], '
            ".job-description, #job-description"
        )
        if dom:
            return dom.get_text(separator="\n", strip=True)

        # 2) Fallback: JSON-LD <script type="application/ld+json"> with @type=JobPosting
        for s in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(s.string or "")
            except Exception:
                continue

            # Some pages embed a single object; others embed a list
            candidates = data if isinstance(data, list) else [data]
            for obj in candidates:
                if isinstance(obj, dict) and obj.get("@type") == "JobPosting":
                    desc_html = obj.get("description")
                    if not desc_html:
                        continue
                    # Unescape entities and strip HTML to plain text
                    desc_html = unescape(desc_html)
                    desc_text = BeautifulSoup(desc_html, "html.parser").get_text(
                        separator="\n", strip=True
                    )
                    if desc_text:
                        return desc_text

        # If everything fails
        return ""
