"""
Scrapes all 11 job roles from LinkedIn via Apify in a single run.
Returns ALL jobs within the SCRAPE_HOURS window — pipeline categorises fresh vs older.
"""
import time
import requests
from config import APIFY_API_TOKEN, APIFY_ACTOR_ID, LINKEDIN_BASE, \
                   JOB_LIMIT_TOTAL, SCRAPE_HOURS, JOB_ROLES


def _build_urls(roles: list[str]) -> list[str]:
    tpr = SCRAPE_HOURS * 3600   # seconds
    return [LINKEDIN_BASE.format(role=r.replace(" ", "+"), tpr=tpr) for r in roles]


def scrape_jobs(roles: list[str] | None = None) -> tuple[list[dict], int]:
    """
    Scrape LinkedIn for all roles within the last SCRAPE_HOURS hours.
    Returns (all_unique_jobs, raw_count_before_dedup).
    Time-based categorisation (fresh vs older) is done in the pipeline.
    """
    if roles is None:
        roles = JOB_ROLES

    urls = _build_urls(roles)

    actor_id = APIFY_ACTOR_ID.replace("/", "~")
    run_url  = f"https://api.apify.com/v2/acts/{actor_id}/runs?token={APIFY_API_TOKEN}"
    payload  = {"urls": urls, "count": JOB_LIMIT_TOTAL, "scrapeCompany": False}

    resp = requests.post(run_url, json=payload, timeout=30)
    resp.raise_for_status()
    run_id = resp.json()["data"]["id"]

    # Poll until finished (max 10 minutes)
    status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_API_TOKEN}"
    for _ in range(60):
        time.sleep(10)
        run_data = requests.get(status_url, timeout=15).json()["data"]
        status   = run_data["status"]
        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Apify run ended with status: {status}")

    dataset_id = run_data["defaultDatasetId"]
    items_url  = (
        f"https://api.apify.com/v2/datasets/{dataset_id}/items"
        f"?token={APIFY_API_TOKEN}&format=json&limit={JOB_LIMIT_TOTAL}"
    )
    all_jobs = requests.get(items_url, timeout=30).json()
    if not isinstance(all_jobs, list):
        all_jobs = []

    raw_count = len(all_jobs)

    # Deduplicate by LinkedIn job ID
    seen, unique = set(), []
    for j in all_jobs:
        jid = str(j.get("id", ""))
        if jid and jid not in seen:
            seen.add(jid)
            unique.append(j)

    return unique, raw_count
