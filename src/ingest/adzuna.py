from multiprocessing import Value
import os, requests, json, datetime
from dotenv import load_dotenv
from typing import Dict, List, Optional, Any
import time

load_dotenv()

ADZUNA_ID = os.getenv("ADZUNA_ID")
ADZUNA_KEY = os.getenv("ADZUNA_KEY")


class AdzunaAPI:
    """Adzuna API client for job search"""

    def __init__(self, app_id: str = None, app_key: str = None):

        self.app_id = ADZUNA_ID or app_id
        self.app_key = ADZUNA_KEY or app_key
        self.base_url = "https://api.adzuna.com/v1/api"

        if not self.app_id or not self.app_key:
            raise ValueError("No ADZUNA creds set.")

    def search_jobs(
        self,
        country: str = "us",
        location: Optional[str] = None,
        keywords: Optional[str] = None,
        category: Optional[str] = None,
        results_per_page: int = 20,
        page: int = 1,
        salary_min: Optional[int] = None,
        salary_max: Optional[int] = None,
        sort_by: str = "relevance",
    ) -> Dict[str, Any]:
        """Search for jobs using the Adzuna API.

        Args:
            country: Country code (e.g., 'us', 'gb', 'ca', 'au')
            location: Location to search in (e.g., 'New York', 'London')
            keywords: Job keywords to search for
            category: Job category (e.g., 'it-jobs', 'sales-jobs')
            results_per_page: Number of results per page (max 50)
            page: Page number to retrieve
            salary_min: Minimum salary filter
            salary_max: Maximum salary filter
            sort_by: Sort results by ('relevance', 'date', 'salary')

        Returns:
            Dictionary containing job search results
        """

        endpoint = f"{self.base_url}/jobs/{country}/search/{page}"

        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": min(results_per_page, 50),  # gotta figure that part out
            "sort_by": sort_by,
        }

        # optional parameters
        if location:
            params["where"] = location

        if keywords:
            params["what"] = keywords
        if category:
            params["category"] = category
        if salary_min:
            params["salary_min"] = salary_min
        if salary_max:
            params["salary_max"] = salary_max

        try:

            response = requests.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            return response.json()  # return our desired info
        except requests.exceptions.RequestException as e:
            print(f"Error making request to Adzuna API: {e}")
            return {
                "error": str(e)
            }  # return the error dict, to not flip up the func return
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {e}")
            return {"error": "Invalid JSON response"}

    def process_job_results(self, results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Turn messy API dict result into list with clean dict"""

        if "error" in results:
            return []

        jobs = results.get("results", [])
        processed_jobs = []

        for job in jobs:
            # cleaning method
            processed_job = {
                "id": job.get("id"),
                "title": job.get("title"),
                "company": job.get("company", {}).get("display_name"),
                "location": job.get("location", {}).get("display_name"),
                "description": job.get("description"),
                "salary_min": job.get("salary_min"),
                "salary_max": job.get("salary_max"),
                "salary_currency": job.get("salary_currency"),
                "created": job.get("created"),
                "redirect_url": job.get("redirect_url"),
                "category": job.get("category", {}).get("label"),
                "contract_type": job.get("contract_type"),
                "contract_time": job.get("contract_time"),
            }
            processed_jobs.append(processed_job)

        return processed_jobs

    def print_jobs(self, jobs: List[Dict[str, Any]]):
        for job in jobs:
            print(f"Title: {job['title']}")
            print(f"Company: {job['company']}")
            print(f"Location: {job['location']}")
            print(f"Description: {job['description']}")
            print(f"Redirect URL: {job['redirect_url']}")

    def save_jobs_to_file(
        self, results: List[Dict[str, Any]], filename: str = None
    ) -> str:
        """Save processed jobs as json file
        Returns path to saved file
        """

        if not filename:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{len(results)}_adzuna_jobs_{timestamp}.json"

        os.makedirs("data/raw", exist_ok=True)
        filepath = os.path.join("data/raw", filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        print(f"Results saved to {filepath}")

        return filepath


def main():
    try:
        api = AdzunaAPI()

        results = api.search_jobs(country="gb", keywords="data", results_per_page=10)

        processed_jobs = api.process_job_results(results)

        # api.print_jobs(processed_jobs)

        api.save_jobs_to_file(processed_jobs)

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
