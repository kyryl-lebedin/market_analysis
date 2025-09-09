import re
import sys
import os
import json
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime
import requests
import time
import random


def get_home_url(
    redirect_url: str, max_redirects: int = 10, timeout: int = 15
) -> Optional[str]:
    if "/land/" in redirect_url:

        try:
            session = requests.Session()
            session.max_redirects = max_redirects

            # sessioin setup
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

            response = session.get(
                redirect_url, headers=headers, timeout=timeout, allow_redirects=True
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

            # potentially can implement other methods as well, if needed

        except Exception as e:
            print(f"Error trying to obtain home url for {redirect_url}: {e}")

    else:
        return redirect_url


# returns json with home urls
# rewrite as genreator
def json_add_home_urls(
    data: List[Dict[str, Any]],
    delay: float = 1.0,
    save_rate: int = None,
) -> List[Dict[str, Any]]:
    # will update jobs in place
    updated_jobs = data.copy()
    jobs_num = len(data)
    if not save_rate:
        save_rate = jobs_num

    print(
        f"Total number of jobs: {jobs_num} \n Each copy will have such amount of jobs: {save_rate}"
    )

    count = save_rate
    local_copy = []
    for i, job in enumerate(updated_jobs):  # each job is dict with data
        count -= 1

        # get url for uodated job
        redirect_url = job.get("redirect_url")
        home_url = get_home_url(redirect_url)
        # update updated_jobs in place
        job["home_url"] = home_url
        # save job to local copy list
        local_copy.append(job)

        if not count:
            if save_rate != jobs_num:
                # yield batch subset
                yield local_copy
                local_copy = []
                count = save_rate
                print(f"{i+1}/{jobs_num}")
        # print(f"{i+1}/{jobs_num}")
    yield updated_jobs


# handles file input and output adds home urls to jobs
def file_add_home_urls(
    input_file: str,
    output_dir: str = "../../data/interim",
    delay: float = 1.0,
    source_dir: str = "../../data/raw/",
    save_rate: int = None,
) -> str:

    # home_dir = os.path.dirname(os.path.abspath(__file__))
    # input_file = os.path.join(home_dir, source_dir, input_file)
    script_dir = Path(__file__).parent
    input_path = script_dir / source_dir / input_file

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    n = len(data)

    if not save_rate:
        save_rate = n

    batches_num = n // save_rate + (1 if n % save_rate else 0)

    # create a folder in output dir

    dataset_folder = f"{n}_jobs_Adzuna_batch_home_url_{save_rate}_{datetime.now().strftime('%d_%m_%Y_%H-%M-%S')}"
    output_path = script_dir / output_dir / dataset_folder
    output_path.mkdir(parents=True, exist_ok=True)

    # save each batch to the folder
    for i, batch in enumerate(json_add_home_urls(data, delay, save_rate)):
        file_name = (
            f"batch_{i+1}.json"
            if i != batches_num and save_rate != n
            else f"{n}_jobs_Adzuna_{datetime.now().strftime('%d_%m_%Y_%H-%M-%S')}_home_url.json"
        )

        file_path = output_path / file_name

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(batch, f, indent=2, ensure_ascii=False)
    return str(output_path)


def main():
    if len(sys.argv) == 2:
        input_file = sys.argv[1]
    else:
        raise SystemExit("Correct usage: python add_home_url.py [input_file]")

    try:
        output_file_dir = file_add_home_urls(
            input_file,
            delay=1.5,
            save_rate=50,
        )
        print(f"Final dataset is saved into {output_file_dir}")
    except Exception as e:
        print(f"Error in main processing function: {e}")


if __name__ == "__main__":
    main()
