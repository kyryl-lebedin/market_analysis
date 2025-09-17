from __future__ import annotations

import json
from typing import List

import pandas as pd
import pytest

from ingest import adzuna


@pytest.fixture()
def api() -> adzuna.AdzunaAPI:
    """Return an ``AdzunaAPI`` instance with dummy credentials."""
    return adzuna.AdzunaAPI(app_id="id", app_key="key")


def test_constructor_requires_credentials() -> None:
    with pytest.raises(ValueError):
        adzuna.AdzunaAPI(app_id=None, app_key="key")

    with pytest.raises(ValueError):
        adzuna.AdzunaAPI(app_id="id", app_key=None)


def test_constructor_accepts_credentials(api: adzuna.AdzunaAPI) -> None:
    assert api.app_id == "id"
    assert api.app_key == "key"
    assert api.base_url.endswith("/api")


def test_search_jobs_single_page_formatted(monkeypatch: pytest.MonkeyPatch, api: adzuna.AdzunaAPI) -> None:
    captured_params: List[dict] = []

    def fake_fetch(endpoint: str, params: dict, page: int | None = None) -> dict:
        captured_params.append(params)
        assert endpoint.endswith("/search/2")
        assert params["results_per_page"] == 50  # capped from any larger value
        return {
            "results": [
                {
                    "id": "abc",
                    "title": "Data Scientist",
                    "company": {"display_name": "Acme"},
                    "location": {"display_name": "London"},
                    "description": "Work on ML.",
                    "salary_min": 50000,
                    "salary_max": 70000,
                    "salary_currency": "GBP",
                    "created": "2023-01-01",
                    "redirect_url": "http://example.com/job/abc",
                    "category": {"label": "IT"},
                    "contract_type": "permanent",
                    "contract_time": "full_time",
                }
            ]
        }

    monkeypatch.setattr(api, "_fetch_single_page", fake_fetch)

    results, page_errors = api.search_jobs(
        country="gb",
        results_per_page=200,
        page=2,
        formated=True,
    )

    assert page_errors == []
    assert captured_params and captured_params[0]["results_per_page"] == 50
    assert results == [
        {
            "id": "abc",
            "title": "Data Scientist",
            "company": "Acme",
            "location": "London",
            "description": "Work on ML.",
            "salary_min": 50000,
            "salary_max": 70000,
            "salary_currency": "GBP",
            "created": "2023-01-01",
            "redirect_url": "http://example.com/job/abc",
            "category": "IT",
            "contract_type": "permanent",
            "contract_time": "full_time",
        }
    ]


def test_search_jobs_multithreading_all_pages(monkeypatch: pytest.MonkeyPatch, api: adzuna.AdzunaAPI) -> None:
    calls: List[int] = []

    def fake_fetch(self: adzuna.AdzunaAPI, endpoint: str, params: dict, page: int) -> dict:
        calls.append(page)
        if page == 1:
            return {"count": 120, "results": []}
        return {
            "results": [
                {
                    "id": f"job-{page}",
                    "title": f"Role {page}",
                    "company": {"display_name": "Acme"},
                    "location": {"display_name": "Remote"},
                    "description": "Desc",
                    "salary_min": None,
                    "salary_max": None,
                    "salary_currency": "GBP",
                    "created": "2023-01-01",
                    "redirect_url": "http://example.com",
                    "category": {"label": "IT"},
                    "contract_type": None,
                    "contract_time": None,
                }
            ]
        }

    monkeypatch.setattr(adzuna.AdzunaAPI, "_fetch_single_page", fake_fetch, raising=False)

    results, errors = api.search_jobs(
        scope="all_pages",
        mode="multithreading",
        max_workers=2,
        formated=True,
    )

    assert errors == []
    assert calls[0] == 1
    assert set(calls) == {1, 2, 3}
    ids = sorted(job["id"] for job in results)
    assert ids == ["job-2", "job-3"]


def test_process_job_results_skips_errors(api: adzuna.AdzunaAPI) -> None:
    raw = [
        {
            "results": [
                {
                    "id": "1",
                    "title": "Role",
                    "company": {"display_name": "Acme"},
                    "location": {"display_name": "London"},
                    "description": "Desc",
                    "salary_min": None,
                    "salary_max": None,
                    "salary_currency": "GBP",
                    "created": "2023-01-01",
                    "redirect_url": "http://example.com",
                    "category": {"label": "IT"},
                    "contract_type": None,
                    "contract_time": None,
                }
            ]
        },
        {"error": "network"},
    ]

    cleaned = api._process_job_results(raw)
    assert len(cleaned) == 1
    assert cleaned[0]["company"] == "Acme"


def test_save_jobs_to_file_json(tmp_path: "Path", monkeypatch: pytest.MonkeyPatch, api: adzuna.AdzunaAPI) -> None:
    monkeypatch.setattr(adzuna, "DATA_DIR", tmp_path)

    payload = [
        {
            "id": "1",
            "title": "Role",
            "company": "Acme",
        }
    ]

    saved_path = api.save_jobs_to_file(payload, output_type="json")
    assert saved_path.suffix == ".json"
    assert saved_path.parent == tmp_path / "raw"

    with saved_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    assert data == payload


def test_save_jobs_to_file_parquet(monkeypatch: pytest.MonkeyPatch, tmp_path: "Path", api: adzuna.AdzunaAPI) -> None:
    monkeypatch.setattr(adzuna, "DATA_DIR", tmp_path)

    payload = [{"id": "1"}]
    captured_path: List[adzuna.Path] = []

    def fake_to_parquet(self: pd.DataFrame, path: adzuna.Path) -> None:
        captured_path.append(path)

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet, raising=False)

    saved_path = api.save_jobs_to_file(payload, filename="jobs.parquet", output_type="parquet")

    assert saved_path == tmp_path / "raw" / "jobs.parquet"
    assert captured_path and captured_path[0] == saved_path


def test_save_jobs_to_file_validation_errors(api: adzuna.AdzunaAPI) -> None:
    with pytest.raises(ValueError):
        api.save_jobs_to_file({"not": "a list"}, output_type="json")

    with pytest.raises(ValueError):
        api.save_jobs_to_file([], output_type="xml")
