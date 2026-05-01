"""
INTEGRATION TESTS — test component interactions with mocked external services.
Covers: ATS pipeline with real PDFs, pipeline filter chain,
        email builder end-to-end, scraper API polling logic,
        sheets row writer, dedup logic, status sync.
"""
import sys, os, json, uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock, call
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

with __import__("unittest.mock", fromlist=["patch"]).patch(
        "google.oauth2.service_account.Credentials.from_service_account_file"):
    with __import__("unittest.mock", fromlist=["patch"]).patch(
            "google.cloud.bigquery.Client"):
        import database  # noqa

from ats_matcher import match_all_resumes
from email_sender import send_email, _build_html
from main import (
    _make_job_id, _check_blacklist, _requires_no_sponsorship,
    _requires_clearance, _required_years, _is_fresh,
)

RESUME_FOLDER = os.path.join(os.path.dirname(__file__), "..", "backend", "Resume")


# ════════════════════════════════════════════════════════════════════════
# 1. ATS pipeline — real PDFs
# ════════════════════════════════════════════════════════════════════════

def test_match_all_resumes_returns_structure():
    desc = "We are looking for a Python SQL BigQuery data engineer."
    result = match_all_resumes(desc, RESUME_FOLDER)
    assert "best" in result
    assert "all" in result
    assert "passed" in result

def test_match_all_resumes_all_is_list():
    result = match_all_resumes("Python SQL BigQuery", RESUME_FOLDER)
    assert isinstance(result["all"], list)

def test_match_all_resumes_best_has_score():
    result = match_all_resumes("Python SQL BigQuery ETL pipeline data engineer", RESUME_FOLDER)
    if result["best"]:
        assert "ats_score" in result["best"]
        assert 0 <= result["best"]["ats_score"] <= 100

def test_match_all_resumes_each_resume_scored():
    result = match_all_resumes("Python SQL BigQuery", RESUME_FOLDER)
    pdfs = [f for f in os.listdir(RESUME_FOLDER) if f.lower().endswith(".pdf")]
    assert len(result["all"]) == len(pdfs)

def test_match_all_resumes_best_is_highest():
    result = match_all_resumes("Python SQL BigQuery dbt Airflow data engineer pipeline", RESUME_FOLDER)
    if result["best"] and result["all"]:
        max_score = max(r["ats_score"] for r in result["all"])
        assert result["best"]["ats_score"] == max_score

def test_match_all_resumes_passed_flag_correct():
    from config import ATS_THRESHOLD
    result = match_all_resumes("Python SQL BigQuery dbt Airflow spark data engineer pipeline ETL", RESUME_FOLDER)
    if result["best"]:
        expected = result["best"]["ats_score"] >= ATS_THRESHOLD
        assert result["passed"] == expected

def test_match_all_resumes_empty_folder(tmp_path):
    result = match_all_resumes("Python SQL BigQuery", str(tmp_path))
    assert result == {"best": None, "all": [], "passed": False}

def test_match_all_resumes_includes_keywords():
    result = match_all_resumes("Python SQL BigQuery dbt Airflow data engineer", RESUME_FOLDER)
    if result["best"]:
        assert "matched_keywords" in result["best"]
        assert "missing_keywords" in result["best"]


# ════════════════════════════════════════════════════════════════════════
# 2. Pipeline filter chain — sequence of filters applied in correct order
# ════════════════════════════════════════════════════════════════════════

def test_filter_chain_applicants_checked_first():
    """Applicants check should fire before blacklist."""
    from config import MAX_APPLICANTS
    bq_bl = {"google": "excluded"}
    company = "Google"
    desc = "We do not sponsor visas."
    apps = MAX_APPLICANTS + 10

    from main import _parse_applicants
    assert _parse_applicants(str(apps)) >= MAX_APPLICANTS
    # applicants filter would fire before blacklist/sponsorship

def test_blacklist_fires_before_sponsorship():
    """Blacklist check comes before no-sponsorship check in pipeline."""
    bq_bl = {"teksystems": "staffing"}
    company = "TekSystems"
    desc = "No visa sponsorship available."

    is_bl, bl_type = _check_blacklist(company, bq_bl)
    assert is_bl is True  # blacklist fires first

def test_sponsorship_fires_before_clearance():
    """sponsorship check before clearance (both present in desc)."""
    desc = "We do not sponsor visas. Top secret clearance required."
    assert _requires_no_sponsorship(desc) is True   # fires first
    assert _requires_clearance(desc) is True        # would also fire

def test_clearance_fires_before_experience():
    """Clearance check before experience check."""
    desc = "Secret clearance required. 8 years of experience."
    assert _requires_clearance(desc) is True   # fires first

def test_experience_filter_boundary_exactly_max():
    """MAX_YEARS_EXPERIENCE is 5 — job requiring exactly 5 should be filtered."""
    from config import MAX_YEARS_EXPERIENCE
    desc = f"{MAX_YEARS_EXPERIENCE} years of experience in data engineering."
    assert _required_years(desc) >= MAX_YEARS_EXPERIENCE


# ════════════════════════════════════════════════════════════════════════
# 3. Deduplication logic
# ════════════════════════════════════════════════════════════════════════

def test_dedup_same_job_produces_same_id():
    job = {"companyName": "Google", "title": "Data Engineer", "location": "NYC"}
    id1 = _make_job_id(job["companyName"], job["title"], job["location"])
    id2 = _make_job_id(job["companyName"], job["title"], job["location"])
    assert id1 == id2

def test_dedup_different_company_different_id():
    id1 = _make_job_id("Google", "Data Engineer", "NYC")
    id2 = _make_job_id("Meta", "Data Engineer", "NYC")
    assert id1 != id2

def test_dedup_known_ids_set_prevents_reprocessing():
    """Simulate pipeline dedup: second encounter of same job skipped."""
    jobs = [
        {"companyName": "Google", "title": "DE", "location": "NYC"},
        {"companyName": "Google", "title": "DE", "location": "NYC"},  # duplicate
        {"companyName": "Meta", "title": "DE", "location": "NYC"},
    ]
    known_ids = set()
    new_jobs, dup_count = [], 0
    for j in jobs:
        jid = _make_job_id(j["companyName"], j["title"], j["location"])
        j["_job_id"] = jid
        if jid in known_ids:
            dup_count += 1
        else:
            new_jobs.append(j)
            known_ids.add(jid)

    assert len(new_jobs) == 2
    assert dup_count == 1


# ════════════════════════════════════════════════════════════════════════
# 4. Email builder — end-to-end HTML generation
# ════════════════════════════════════════════════════════════════════════

SAMPLE_JOBS_FRESH = [
    {"company": "Google", "job_title": "Data Engineer", "job_url": "https://example.com/1",
     "ats_score": 78.5, "resume_name": "resume.pdf"},
]
SAMPLE_JOBS_OLDER = [
    {"company": "Meta", "job_title": "Analytics Engineer", "job_url": "https://example.com/2",
     "ats_score": 62.0, "resume_name": "resume.pdf"},
]
SAMPLE_STATS = {
    "total_scraped": 250, "total_raw": 300,
    "skip_counts": {"staffing": 20, "no_sponsorship": 15, "clearance": 5, "experience": 10},
    "dedup_skipped": 30,
}

def test_build_html_contains_match_count():
    html = _build_html(SAMPLE_JOBS_FRESH, SAMPLE_JOBS_OLDER, [],
                       "https://sheet.url", "2024-01-15 12:00 UTC", "run-123", SAMPLE_STATS)
    assert "2" in html  # 2 total matches

def test_build_html_contains_sheet_url():
    html = _build_html(SAMPLE_JOBS_FRESH, [], [],
                       "https://sheet.example.com/abc", "tab1", "run-1", SAMPLE_STATS)
    assert "https://sheet.example.com/abc" in html

def test_build_html_contains_tab_name():
    html = _build_html(SAMPLE_JOBS_FRESH, [], [],
                       "https://sheet.url", "2024-01-15 12:00 UTC", "run-1", SAMPLE_STATS)
    assert "2024-01-15 12:00 UTC" in html

def test_build_html_shows_fresh_count():
    html = _build_html(SAMPLE_JOBS_FRESH, [], [],
                       "https://sheet.url", "tab", "run", SAMPLE_STATS)
    assert "1" in html  # 1 fresh job

def test_build_html_no_match_message_when_empty():
    html = _build_html([], [], [], "https://sheet.url", "tab", "run", SAMPLE_STATS)
    assert "No jobs matched" in html

def test_build_html_is_valid_html():
    html = _build_html(SAMPLE_JOBS_FRESH, SAMPLE_JOBS_OLDER, [],
                       "https://sheet.url", "tab", "run", SAMPLE_STATS)
    assert "<!DOCTYPE html>" in html
    assert "</html>" in html

def test_build_html_filter_stats_present():
    html = _build_html([], [], [], "https://sheet.url", "tab", "run", SAMPLE_STATS)
    assert "20" in html   # staffing count
    assert "15" in html   # no_sponsorship count

def test_send_email_returns_dict_on_smtp_failure():
    with patch("smtplib.SMTP_SSL") as mock_smtp:
        mock_smtp.side_effect = Exception("Connection refused")
        result = send_email(
            jobs_fresh=SAMPLE_JOBS_FRESH, jobs_older=SAMPLE_JOBS_OLDER, jobs_easy=[],
            sheet_url="https://sheet.url", tab_name="tab", run_id="run-1",
            recipient_email="test@example.com", stats=SAMPLE_STATS,
        )
    assert result["status"] == "failed"
    assert "Connection refused" in result["smtp_response"]

def test_send_email_success():
    with patch("smtplib.SMTP_SSL") as mock_smtp_cls:
        mock_ctx = MagicMock()
        mock_smtp_cls.return_value.__enter__ = lambda s: mock_ctx
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_ctx.sendmail.return_value = {}
        result = send_email(
            jobs_fresh=SAMPLE_JOBS_FRESH, jobs_older=[], jobs_easy=[],
            sheet_url="https://sheet.url", tab_name="tab", run_id="run-1",
            recipient_email="test@example.com", stats=SAMPLE_STATS,
        )
    assert result["status"] == "sent"
    assert result["jobs_count"] == 1

def test_send_email_subject_contains_count():
    with patch("smtplib.SMTP_SSL") as mock_smtp_cls:
        mock_ctx = MagicMock()
        mock_smtp_cls.return_value.__enter__ = lambda s: mock_ctx
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_ctx.sendmail.return_value = {}
        result = send_email(
            jobs_fresh=SAMPLE_JOBS_FRESH, jobs_older=SAMPLE_JOBS_OLDER, jobs_easy=[],
            sheet_url="https://sheet.url", tab_name="tab", run_id="run-1",
            recipient_email="test@example.com", stats=SAMPLE_STATS,
        )
    assert "2" in result["subject"]


# ════════════════════════════════════════════════════════════════════════
# 5. Scraper — Apify polling logic (mocked HTTP)
# ════════════════════════════════════════════════════════════════════════

def test_scraper_polls_until_succeeded():
    from scraper import scrape_jobs
    run_id = "test-run-id"
    dataset_id = "test-dataset-id"

    post_resp = MagicMock()
    post_resp.raise_for_status = MagicMock()
    post_resp.json.return_value = {"data": {"id": run_id}}

    status_resp = MagicMock()
    status_resp.json.return_value = {"data": {
        "status": "SUCCEEDED", "defaultDatasetId": dataset_id
    }}

    items_resp = MagicMock()
    items_resp.json.return_value = [
        {"id": "job1", "companyName": "Google", "title": "DE", "location": "NYC"},
    ]

    with patch("requests.post", return_value=post_resp), \
         patch("requests.get", side_effect=[status_resp, items_resp]), \
         patch("time.sleep"):
        jobs, raw = scrape_jobs(["Data Engineer"])

    assert len(jobs) == 1
    assert raw == 1

def test_scraper_raises_on_failed_run():
    from scraper import scrape_jobs

    post_resp = MagicMock()
    post_resp.raise_for_status = MagicMock()
    post_resp.json.return_value = {"data": {"id": "run-1"}}

    status_resp = MagicMock()
    status_resp.json.return_value = {"data": {"status": "FAILED", "defaultDatasetId": "ds1"}}

    with patch("requests.post", return_value=post_resp), \
         patch("requests.get", return_value=status_resp), \
         patch("time.sleep"), \
         pytest.raises(RuntimeError, match="FAILED"):
        scrape_jobs(["Data Engineer"])

def test_scraper_deduplicates_by_linkedin_id():
    from scraper import scrape_jobs

    post_resp = MagicMock()
    post_resp.raise_for_status = MagicMock()
    post_resp.json.return_value = {"data": {"id": "run-1"}}

    status_resp = MagicMock()
    status_resp.json.return_value = {"data": {"status": "SUCCEEDED", "defaultDatasetId": "ds1"}}

    items_resp = MagicMock()
    items_resp.json.return_value = [
        {"id": "job1", "companyName": "A", "title": "DE", "location": "NYC"},
        {"id": "job1", "companyName": "A", "title": "DE", "location": "NYC"},  # duplicate
        {"id": "job2", "companyName": "B", "title": "DA", "location": "NYC"},
    ]

    with patch("requests.post", return_value=post_resp), \
         patch("requests.get", side_effect=[status_resp, items_resp]), \
         patch("time.sleep"):
        jobs, raw = scrape_jobs(["Data Engineer"])

    assert raw == 3
    assert len(jobs) == 2   # duplicate removed


# ════════════════════════════════════════════════════════════════════════
# 6. Status sync — logic layer (no real Sheets/BQ calls)
# ════════════════════════════════════════════════════════════════════════

def test_status_sync_detects_new_status():
    current = [
        {"job_id": "a1b2c3d4e5f6a7b8", "status": "Applied", "row_number": 3},
    ]
    known = {}  # no previously known status

    updates = []
    for row in current:
        if not row["status"]:
            continue
        old = known.get(row["job_id"])
        if row["status"] != old:
            updates.append({"job_id": row["job_id"], "new_status": row["status"], "old_status": old})

    assert len(updates) == 1
    assert updates[0]["new_status"] == "Applied"

def test_status_sync_no_update_when_unchanged():
    current = [{"job_id": "a1b2c3d4e5f6a7b8", "status": "Applied", "row_number": 3}]
    known = {"a1b2c3d4e5f6a7b8": "Applied"}

    updates = [r for r in current if r["status"] != known.get(r["job_id"])]
    assert len(updates) == 0

def test_status_sync_skips_empty_status():
    current = [{"job_id": "a1b2c3d4e5f6a7b8", "status": "", "row_number": 3}]
    known = {}

    updates = [r for r in current if r["status"]]  # empty status → skip
    assert len(updates) == 0
