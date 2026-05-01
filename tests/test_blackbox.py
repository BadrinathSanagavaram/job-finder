"""
BLACK BOX TESTS — treat the system as a black box.
Verify observable outputs, config contracts, workflow correctness,
SQL query validity, and end-to-end pipeline behavior from outside in.
"""
import sys, os, yaml, json, re
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

with __import__("unittest.mock", fromlist=["patch"]).patch(
        "google.oauth2.service_account.Credentials.from_service_account_file"):
    with __import__("unittest.mock", fromlist=["patch"]).patch(
            "google.cloud.bigquery.Client"):
        import config  # noqa

ROOT    = os.path.join(os.path.dirname(__file__), "..")
BACKEND = os.path.join(ROOT, "backend")
DASH    = os.path.join(ROOT, "dashboard")


# ════════════════════════════════════════════════════════════════════════
# 1. GitHub Actions workflow
# ════════════════════════════════════════════════════════════════════════

def _load_workflow():
    path = os.path.join(ROOT, ".github", "workflows", "pipeline.yml")
    with open(path) as f:
        return yaml.safe_load(f)

def test_workflow_file_exists():
    path = os.path.join(ROOT, ".github", "workflows", "pipeline.yml")
    assert os.path.exists(path)

def _triggers(wf):
    # PyYAML parses `on:` as the boolean key True
    return wf.get(True, wf.get("on", {}))

def test_workflow_has_schedule():
    wf = _load_workflow()
    assert "schedule" in _triggers(wf)

def test_workflow_schedule_has_3_crons():
    wf = _load_workflow()
    crons = _triggers(wf)["schedule"]
    assert len(crons) >= 1

def test_workflow_cron_covers_correct_utc_hours():
    wf = _load_workflow()
    cron_expr = _triggers(wf)["schedule"][0]["cron"]
    # 7AM/12PM/5PM EST = 12,17,22 UTC
    assert "12" in cron_expr
    assert "17" in cron_expr
    assert "22" in cron_expr

def test_workflow_runs_weekdays_only():
    wf = _load_workflow()
    cron_expr = _triggers(wf)["schedule"][0]["cron"]
    assert "1-5" in cron_expr

def test_workflow_has_workflow_dispatch():
    wf = _load_workflow()
    assert "workflow_dispatch" in _triggers(wf)

def test_workflow_required_secrets_present():
    wf = _load_workflow()
    steps = wf["jobs"]["run-pipeline"]["steps"]
    run_step = next(s for s in steps if s.get("name") == "Run pipeline")
    env_block = run_step.get("env", {})
    required = {"APIFY_API_TOKEN", "GMAIL_APP_PASSWORD", "GMAIL_SENDER",
                "SHEET_EDITOR_EMAIL", "SHEET_FOLDER_ID"}
    for secret in required:
        assert secret in env_block, f"Missing secret: {secret}"

def test_workflow_credential_step_writes_both_files():
    wf = _load_workflow()
    steps = wf["jobs"]["run-pipeline"]["steps"]
    cred_step = next(s for s in steps if "credential" in s.get("name","").lower())
    run_script = cred_step["run"]
    assert "service_account.json" in run_script
    assert "token.json" in run_script

def test_workflow_python_version_is_311():
    wf = _load_workflow()
    steps = wf["jobs"]["run-pipeline"]["steps"]
    setup_step = next(s for s in steps if s.get("name","").startswith("Set up Python"))
    assert setup_step["with"]["python-version"] == "3.11"

def test_workflow_timeout_set():
    wf = _load_workflow()
    assert wf["jobs"]["run-pipeline"]["timeout-minutes"] == 20

def test_workflow_runs_main_py():
    wf = _load_workflow()
    steps = wf["jobs"]["run-pipeline"]["steps"]
    run_step = next(s for s in steps if s.get("name") == "Run pipeline")
    assert "main.py" in run_step["run"]


# ════════════════════════════════════════════════════════════════════════
# 2. Config / environment contracts
# ════════════════════════════════════════════════════════════════════════

def test_config_ats_threshold_sensible():
    from config import ATS_THRESHOLD
    assert 0 < ATS_THRESHOLD <= 100

def test_config_easy_apply_threshold_below_main():
    from config import ATS_THRESHOLD, EASY_APPLY_ATS_THRESHOLD
    assert EASY_APPLY_ATS_THRESHOLD < ATS_THRESHOLD

def test_config_fresh_hours_below_scrape_hours():
    from config import FRESH_HOURS, SCRAPE_HOURS
    assert FRESH_HOURS < SCRAPE_HOURS

def test_config_max_applicants_positive():
    from config import MAX_APPLICANTS
    assert MAX_APPLICANTS > 0

def test_config_max_years_experience_positive():
    from config import MAX_YEARS_EXPERIENCE
    assert MAX_YEARS_EXPERIENCE > 0

def test_config_job_roles_not_empty():
    from config import JOB_ROLES
    assert len(JOB_ROLES) > 0

def test_config_job_limit_positive():
    from config import JOB_LIMIT_TOTAL
    assert JOB_LIMIT_TOTAL > 0

def test_config_no_sponsorship_keywords_not_empty():
    from config import NO_SPONSORSHIP_KEYWORDS
    assert len(NO_SPONSORSHIP_KEYWORDS) > 0

def test_config_clearance_keywords_not_empty():
    from config import CLEARANCE_KEYWORDS
    assert len(CLEARANCE_KEYWORDS) > 0

def test_config_blacklisted_companies_not_empty():
    from config import BLACKLISTED_COMPANIES
    assert len(BLACKLISTED_COMPANIES) > 0

def test_config_bigquery_project_set():
    from config import BIGQUERY_PROJECT_ID
    assert BIGQUERY_PROJECT_ID == "job-finder-494904"

def test_config_max_tabs_per_sheet_reasonable():
    from config import MAX_TABS_PER_SHEET
    # Google Sheets supports up to ~200 tabs; 25 is safe
    assert 1 < MAX_TABS_PER_SHEET <= 200

def test_config_gcp_key_path_correct():
    from config import GCP_KEY_PATH
    assert GCP_KEY_PATH.endswith("service_account.json")


# ════════════════════════════════════════════════════════════════════════
# 3. Backend file structure
# ════════════════════════════════════════════════════════════════════════

EXPECTED_BACKEND_FILES = [
    "main.py", "ats_matcher.py", "scraper.py", "database.py",
    "sheets_manager.py", "email_sender.py", "sync_status.py",
    "config.py", "requirements.txt",
]

@pytest.mark.parametrize("fname", EXPECTED_BACKEND_FILES)
def test_backend_file_exists(fname):
    assert os.path.exists(os.path.join(BACKEND, fname)), f"Missing: {fname}"

def test_resume_folder_exists():
    from config import RESUME_FOLDER
    assert os.path.exists(RESUME_FOLDER)

def test_resume_folder_has_pdfs():
    from config import RESUME_FOLDER
    pdfs = [f for f in os.listdir(RESUME_FOLDER) if f.lower().endswith(".pdf")]
    assert len(pdfs) >= 1, "No PDF resumes found"

def test_requirements_txt_has_key_packages():
    req_path = os.path.join(BACKEND, "requirements.txt")
    content = open(req_path).read()
    for pkg in ["scikit-learn", "google-cloud-bigquery", "gspread", "pdfplumber", "requests"]:
        assert pkg in content, f"Missing package: {pkg}"


# ════════════════════════════════════════════════════════════════════════
# 4. Dashboard SQL queries — syntax spot-checks
# ════════════════════════════════════════════════════════════════════════

def _get_dashboard_source():
    with open(os.path.join(DASH, "app.py")) as f:
        return f.read()

def test_dashboard_no_range_reserved_keyword():
    """RANGE is a reserved BigQuery keyword and must not be used as an alias."""
    src = _get_dashboard_source()
    # Must not have "AS range" (case-insensitive), but allow "score_range"
    bad = re.findall(r'\bAS\s+range\b', src, re.IGNORECASE)
    assert len(bad) == 0, f"Found reserved keyword RANGE alias: {bad}"

def test_dashboard_uses_score_range_alias():
    src = _get_dashboard_source()
    assert "score_range" in src

def test_dashboard_no_bare_range_group_by():
    src = _get_dashboard_source()
    # GROUP BY range or ORDER BY range (without score_) should not exist
    bad_group = re.findall(r'GROUP BY\s+range\b', src, re.IGNORECASE)
    bad_order = re.findall(r'ORDER BY\s+range\b', src, re.IGNORECASE)
    assert len(bad_group) == 0
    assert len(bad_order) == 0

def test_dashboard_queries_use_ds_prefix():
    """All table references should use the {DS}. prefix to avoid cross-project issues."""
    src = _get_dashboard_source()
    # Should not have bare table names without DS prefix in FROM clauses
    bare = re.findall(r'FROM\s+`(?!{DS})[a-z_]+`', src)
    assert len(bare) == 0, f"Found bare table references: {bare}"

def test_dashboard_all_tabs_defined():
    src = _get_dashboard_source()
    assert 'st.tabs(' in src
    assert '"📊 Overview"' in src
    assert '"💼 Jobs"' in src
    assert '"📈 Analytics"' in src
    assert '"🚫 Blacklist"' in src

def test_dashboard_cache_ttl_set():
    src = _get_dashboard_source()
    assert "ttl=" in src

def test_dashboard_bq_client_cached():
    src = _get_dashboard_source()
    assert "@st.cache_resource" in src

def test_dashboard_no_hardcoded_private_key():
    """Private key must come from secrets, not hardcoded."""
    src = _get_dashboard_source()
    assert "BEGIN RSA PRIVATE KEY" not in src
    assert "BEGIN PRIVATE KEY" not in src

def test_dashboard_secret_fallback_logic():
    src = _get_dashboard_source()
    assert 'st.secrets["GCP_PRIVATE_KEY"]' in src

def test_dashboard_local_file_fallback_logic():
    src = _get_dashboard_source()
    assert "service_account.json" in src
    assert "os.path.exists" in src


# ════════════════════════════════════════════════════════════════════════
# 5. Pipeline run — black-box output contracts
# ════════════════════════════════════════════════════════════════════════

def test_run_pipeline_returns_dict_keys():
    """run_pipeline() must return a dict with run_id, fresh, older, easy_apply."""
    from unittest.mock import patch, MagicMock
    from main import run_pipeline

    mock_scrape = MagicMock(return_value=([], 0))
    mock_get_roles = MagicMock(return_value=["Data Engineer"])
    mock_get_recipients = MagicMock(return_value=[{
        "recipient_id": "user_1", "email_address": "test@example.com"
    }])
    mock_get_bl = MagicMock(return_value={})
    mock_get_existing = MagicMock(return_value=set())
    mock_get_active_ss = MagicMock(return_value=None)
    mock_add_tab = MagicMock(return_value={
        "sheet_id": "s1", "tab_name": "tab", "sheet_name": "Sheet",
        "sheet_url": "https://sheet.url", "created_at": "2024-01-01T00:00:00+00:00"
    })
    mock_send = MagicMock(return_value={
        "email_id": "e1", "run_id": "r1", "recipient_email": "test@example.com",
        "subject": "sub", "body_html": "", "jobs_count": 0,
        "sheet_url": "", "sent_at": "", "status": "sent", "smtp_response": ""
    })
    mock_insert_run = MagicMock()

    with patch("main.scrape_jobs", mock_scrape), \
         patch("main.get_active_roles", mock_get_roles), \
         patch("main.get_active_recipients", mock_get_recipients), \
         patch("main.get_blacklisted_companies", mock_get_bl), \
         patch("main.get_existing_job_ids", mock_get_existing), \
         patch("main.get_active_spreadsheet", mock_get_active_ss), \
         patch("main.add_sheet_tab", mock_add_tab), \
         patch("main.send_email", mock_send), \
         patch("main.insert_jobs"), \
         patch("main.insert_ats_results"), \
         patch("main.insert_job_keywords"), \
         patch("main.insert_sheet_entries"), \
         patch("main.insert_pipeline_run", mock_insert_run), \
         patch("main.insert_email_log"):
        result = run_pipeline(trigger_type="test")

    assert "run_id" in result
    assert "fresh" in result
    assert "older" in result
    assert "easy_apply" in result

def test_run_pipeline_always_calls_insert_pipeline_run():
    """Even on error, pipeline run must be logged to BigQuery."""
    from unittest.mock import patch, MagicMock
    from main import run_pipeline

    mock_insert_run = MagicMock()
    with patch("main.get_active_roles", side_effect=Exception("DB down")), \
         patch("main.insert_pipeline_run", mock_insert_run):
        run_pipeline(trigger_type="test")

    mock_insert_run.assert_called_once()
    run_data = mock_insert_run.call_args[0][0]
    assert run_data["status"] == "failed"

def test_run_pipeline_logs_error_message_on_exception():
    from unittest.mock import patch, MagicMock
    from main import run_pipeline

    mock_insert_run = MagicMock()
    with patch("main.get_active_roles", side_effect=Exception("DB connection error")), \
         patch("main.insert_pipeline_run", mock_insert_run):
        run_pipeline()

    run_data = mock_insert_run.call_args[0][0]
    assert "DB connection error" in run_data["error_message"]

def test_run_pipeline_no_active_recipient_raises():
    """No active recipient should cause failure and be logged."""
    from unittest.mock import patch, MagicMock
    from main import run_pipeline

    mock_insert_run = MagicMock()
    with patch("main.get_active_roles", return_value=["Data Engineer"]), \
         patch("main.get_active_recipients", return_value=[]), \
         patch("main.insert_pipeline_run", mock_insert_run):
        run_pipeline()

    run_data = mock_insert_run.call_args[0][0]
    assert run_data["status"] == "failed"


# ════════════════════════════════════════════════════════════════════════
# 6. Keyword / filter lists — black-box correctness
# ════════════════════════════════════════════════════════════════════════

def test_no_sponsorship_keywords_are_lowercase():
    from config import NO_SPONSORSHIP_KEYWORDS
    for kw in NO_SPONSORSHIP_KEYWORDS:
        assert kw == kw.lower(), f"Not lowercase: {kw}"

def test_clearance_keywords_are_lowercase():
    from config import CLEARANCE_KEYWORDS
    for kw in CLEARANCE_KEYWORDS:
        assert kw == kw.lower(), f"Not lowercase: {kw}"

def test_blacklisted_companies_are_lowercase():
    from config import BLACKLISTED_COMPANIES
    for co in BLACKLISTED_COMPANIES:
        assert co == co.lower(), f"Not lowercase: {co}"

def test_no_duplicate_no_sponsorship_keywords():
    from config import NO_SPONSORSHIP_KEYWORDS
    assert len(NO_SPONSORSHIP_KEYWORDS) == len(set(NO_SPONSORSHIP_KEYWORDS))

def test_no_duplicate_clearance_keywords():
    from config import CLEARANCE_KEYWORDS
    assert len(CLEARANCE_KEYWORDS) == len(set(CLEARANCE_KEYWORDS))

def test_no_duplicate_job_roles():
    from config import JOB_ROLES
    assert len(JOB_ROLES) == len(set(JOB_ROLES))

def test_blacklisted_job_boards_have_3_tuple_entries():
    from config import BLACKLISTED_JOB_BOARDS
    for entry in BLACKLISTED_JOB_BOARDS:
        assert len(entry) == 3, f"Entry should be (name, type, reason): {entry}"

def test_blacklisted_job_boards_valid_types():
    from config import BLACKLISTED_JOB_BOARDS
    valid_types = {"job_board", "excluded", "staffing"}
    for _, btype, _ in BLACKLISTED_JOB_BOARDS:
        assert btype in valid_types, f"Invalid blacklist type: {btype}"
