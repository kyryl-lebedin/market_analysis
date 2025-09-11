from multiprocessing import Value
import os, requests, json, datetime
from dotenv import load_dotenv
from typing import Dict, List, Optional, Any
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

ADZUNA_ID = os.getenv("ADZUNA_ID")
ADZUNA_KEY = os.getenv("ADZUNA_KEY")


# api client
class AdzunaAPI:
    """Adzuna API client for job search"""

    def __init__(self, app_id: str = None, app_key: str = None):

        self.app_id = ADZUNA_ID or app_id
        self.app_key = ADZUNA_KEY or app_key
        self.base_url = "https://api.adzuna.com/v1/api"

        if not self.app_id or not self.app_key:
            raise ValueError("No ADZUNA creds set.")

    # fetches jobs and returns messy dict
    def search_jobs(
        self,
        country: str = "us",
        category: Optional[str] = None,
        results_per_page: int = 50,
        page: int = 1,
        pages: int = None,  # fetch specific number of pages
        sort_by: str = None,
        what_or: str = None,
        what_and: str = None,
        max_workers: int = None,
        all_jobs: bool = False,
        formated: bool = False,
    ) -> Dict[str, Any]:

        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": min(results_per_page, 50),
        }

        if what_or:
            params["what_or"] = what_or

        if what_and:
            params["what_and"] = what_and

        if sort_by:
            params["sort_by"] = sort_by

        if category:
            params["category"] = category

        # handle different ways to fetch jobs
        multiple = True if (all_jobs or pages) else False
        results = []

        # we don't know at this point what count, so we just keep count of how many have we done already
        if max_workers:

            folder = f"{what_or}_{what_and}_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}"
            os.makedirs(
                f"data/raw/{folder}",
                exist_ok=True,
            )

            job_counter = 0
            batch_num = 0

            page = 1
            num_per_batch = results_per_page * max_workers
            while True:
                job_counter += num_per_batch

                # run workers starting at page
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    batch = executor.map(
                        self._fetch_single_page,
                        [
                            f"{self.base_url}/jobs/{country}/search/{p}"
                            for p in range(page, page + max_workers)
                        ],
                        [params for i in range(max_workers)],
                        [p for p in range(page, page + max_workers)],
                    )
                    batch = list(batch)

                    batch = self._process_job_results(batch)
                    if not batch:
                        break
                    # print(batch)
                    self.save_jobs_to_file(
                        batch,
                        f"{folder}/batch_{batch_num}_{num_per_batch}({job_counter}).json",
                    )

                batch_num += 1
                page += max_workers

        if all_jobs:
            total = False
            results = []
            page_i = 1
            while True:
                endpoint = f"{self.base_url}/jobs/{country}/search/{page_i}"
                result = self._fetch_single_page(endpoint, params)

                # calcualte total number of pages in result
                if not total:
                    total = result.get("count", []) // 50 + (
                        1 if result.get("count", []) % 50 else 0
                    )

                if not result.get("results", []):
                    break
                print(f"{page_i}/{total}")
                results.append(result)
                page_i += 1
        elif pages:
            results = []
            for i in range(1, pages + 1):
                endpoint = f"{self.base_url}/jobs/{country}/search/{i}"
                results.append(self._fetch_single_page(endpoint, params))
                print(f"{i}/{pages}")
        else:  # case with single page
            endpoint = f"{self.base_url}/jobs/{country}/search/{page}"
            results.append(self._fetch_single_page(endpoint, params))

        # format results if needed and return final
        return self._process_job_results(results) if formated else results

    def _fetch_single_page(
        self, endpoint: str, params: Dict[str, Any], page: int = None
    ):
        try:
            response = requests.get(endpoint, params=params, timeout=30)
            response.raise_for_status()

            return response.json()  # return our desired info
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error: {e}")
            print(f"page: {page}")
            return {"error": str(e)}
        except requests.exceptions.RequestException as e:
            print(f"Error making request to Adzuna API: {e}")
            return {
                "error": str(e)
            }  # return the error dict, to not flip up the func return
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {e}")
            return {"error": "Invalid JSON response"}

    # clean data for json or printing
    def _process_job_results(
        self, results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Turn messy API dict result into list with clean dict"""

        for result in results:
            if "error" in result:
                continue

        all_results = []

        for result in results:
            jobs = result.get("results", [])
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
            all_results += processed_jobs
        return all_results

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


what_or_extensive_data = "data scientist,data science,data engineer,data engineering,big data engineer,data platform engineer,data platform,data architect,analytics engineer,data analyst,data analytics,business intelligence,bi analyst,bi developer,data visualization,data visualisation,analytics consultant,data science consultant,etl developer,etl engineer,sql developer,data warehouse engineer,data warehousing,dwh engineer,data modeler,data modeller,data quality engineer,data quality analyst,data governance,data steward,data ops,dataops,streaming data engineer,streaming engineer,kafka engineer,spark engineer,pyspark developer,databricks engineer,snowflake engineer,snowflake developer,dbt developer,machine learning engineer,ml engineer,ai engineer,ai developer,mlops engineer,ml ops,model ops,modelops,ml platform engineer,machine learning platform engineer,ml infrastructure engineer,research engineer,applied scientist,ai scientist,ml scientist,nlp engineer,natural language processing,nlp scientist,computer vision engineer,computer vision,cv engineer,speech recognition,speech engineer,audio ml engineer,deep learning engineer,deep learning,reinforcement learning,recommendation systems engineer,recommender systems,personalization engineer,information retrieval engineer,search engineer,relevance engineer,ranking engineer,generative ai,genai,large language models,llm engineer,llm developer,prompt engineer,prompt engineering,knowledge graph engineer,graph machine learning,quantitative analyst,quant analyst,decision scientist,product data scientist,insights analyst,statistician,time series analyst,forecasting analyst"
what_or_core_data = "data scientist,data engineer,machine learning,ai"

what_and_680 = "cat, or,and,in,of,it"
what_and_1411 = "cat,ll"

# write the wrapper extractor that would implement multiple threads


# script logic, calls the API client
def main():
    try:
        api = AdzunaAPI()

        results = api.search_jobs(
            country="gb",
            formated=True,
            # category="it-jobs",
            # all_jobs=True,
            what_or=what_or_core_data,
            # what_and=what_and_1411,
            results_per_page=50,
            # pages=3,
            max_workers=3,
        )

        # api.save_jobs_to_file(results, "filename")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
