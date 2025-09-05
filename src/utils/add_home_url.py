import json
import requests
import time
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse, urljoin
import os
import re


def get_final_url(
    redirect_url: str, max_redirects: int = 10, timeout: int = 15
) -> Optional[str]:
    """
    Follow redirect chain to get the final destination URL.
    Handles both HTTP redirects and JavaScript/meta refresh redirects.

    Args:
        redirect_url: The initial redirect URL from Adzuna
        max_redirects: Maximum number of redirects to follow
        timeout: Request timeout in seconds

    Returns:
        Final destination URL or None if failed
    """
    try:
        # Use session to handle cookies and maintain connection
        session = requests.Session()
        session.max_redirects = max_redirects

        # Set headers to mimic a real browser
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Cache-Control": "max-age=0",
        }

        # First, try to follow HTTP redirects
        response = session.get(
            redirect_url, headers=headers, timeout=timeout, allow_redirects=True
        )

        final_url = response.url

        # If we're still on an Adzuna domain, try to extract redirect from page content
        if "adzuna.co.uk" in final_url or "adzuna.com" in final_url:
            # Look for JavaScript redirects or meta refresh
            content = response.text

            # Check for meta refresh redirect
            meta_refresh_match = re.search(
                r'<meta[^>]*http-equiv=["\']?refresh["\']?[^>]*content=["\']?\d+;\s*url=([^"\'>\s]+)',
                content,
                re.IGNORECASE,
            )
            if meta_refresh_match:
                redirect_url = meta_refresh_match.group(1)
                if not redirect_url.startswith("http"):
                    redirect_url = urljoin(final_url, redirect_url)
                return redirect_url

            # Check for JavaScript redirects
            js_redirect_patterns = [
                r'window\.location\s*=\s*["\']([^"\']+)["\']',
                r'window\.location\.href\s*=\s*["\']([^"\']+)["\']',
                r'location\.href\s*=\s*["\']([^"\']+)["\']',
                r'location\.replace\s*\(\s*["\']([^"\']+)["\']',
                r'window\.open\s*\(\s*["\']([^"\']+)["\']',
            ]

            for pattern in js_redirect_patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    redirect_url = match.group(1)
                    if not redirect_url.startswith("http"):
                        redirect_url = urljoin(final_url, redirect_url)
                    return redirect_url

            # Check for data attributes or other redirect indicators
            data_redirect_match = re.search(
                r'data-redirect-url=["\']([^"\']+)["\']', content, re.IGNORECASE
            )
            if data_redirect_match:
                redirect_url = data_redirect_match.group(1)
                if not redirect_url.startswith("http"):
                    redirect_url = urljoin(final_url, redirect_url)
                return redirect_url

        return final_url

    except requests.exceptions.RequestException as e:
        print(f"Error following redirect for {redirect_url}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error for {redirect_url}: {e}")
        return None


def add_home_urls_to_jobs(
    jobs_data: List[Dict[str, Any]], delay: float = 1.0
) -> List[Dict[str, Any]]:
    """
    Add home_url field to each job by following redirect URLs.

    Args:
        jobs_data: List of job dictionaries
        delay: Delay between requests to be respectful to servers

    Returns:
        Updated list of job dictionaries with home_url field
    """
    updated_jobs = []

    for i, job in enumerate(jobs_data):
        print(f"Processing job {i+1}/{len(jobs_data)}: {job.get('title', 'Unknown')}")

        # Create a copy of the job to avoid modifying the original
        updated_job = job.copy()

        redirect_url = job.get("redirect_url")
        if redirect_url:
            print(f"  Following redirect: {redirect_url}")
            home_url = get_final_url(redirect_url)

            if home_url:
                updated_job["home_url"] = home_url
                print(f"  Final URL: {home_url}")
            else:
                updated_job["home_url"] = None
                print(f"  Failed to resolve redirect")
        else:
            updated_job["home_url"] = None
            print(f"  No redirect URL found")

        updated_jobs.append(updated_job)

        # Add delay between requests to be respectful
        if i < len(jobs_data) - 1:  # Don't delay after the last request
            time.sleep(delay)

    return updated_jobs


def process_json_file(
    input_file: str, output_file: str = None, delay: float = 1.0
) -> str:
    """
    Process a JSON file containing job data and add home_url field.

    Args:
        input_file: Path to input JSON file
        output_file: Path to output JSON file (optional)
        delay: Delay between requests in seconds

    Returns:
        Path to the output file
    """
    # Read the input JSON file
    with open(input_file, "r", encoding="utf-8") as f:
        jobs_data = json.load(f)

    print(f"Loaded {len(jobs_data)} jobs from {input_file}")

    # Add home URLs
    updated_jobs = add_home_urls_to_jobs(jobs_data, delay=delay)

    # Determine output file path
    if not output_file:
        # Create data/interim directory if it doesn't exist
        interim_dir = "../../data/interim"
        os.makedirs(interim_dir, exist_ok=True)

        # Get the base filename without extension and directory
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        output_file = os.path.join(interim_dir, f"{base_name}_home_urls.json")

    # Save updated data
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(updated_jobs, f, indent=2, ensure_ascii=False)

    print(f"Updated data saved to {output_file}")

    return output_file


def main():
    """Main function to process the Adzuna jobs data"""
    import sys

    # Allow custom input file via command line argument
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = "data/raw/10_adzuna_jobs_20250905_184242.json"

    if not os.path.exists(input_file):
        print(f"Input file not found: {input_file}")
        print("Usage: python add_home_url.py [input_file.json]")
        return

    try:
        output_file = process_json_file(
            input_file, delay=1.5
        )  # Reasonable delay between requests
        print(f"Successfully processed {input_file} and saved to {output_file}")
    except Exception as e:
        print(f"Error processing file: {e}")


if __name__ == "__main__":
    main()
