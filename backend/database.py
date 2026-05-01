"""
BigQuery data layer — append-only (free tier compatible).
All writes use load_table_from_json. All reads use SELECT queries.
"""
import json
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from config import BIGQUERY_PROJECT_ID, BIGQUERY_DATASET, GCP_KEY_PATH, MAX_TABS_PER_SHEET

_creds = Credentials.from_service_account_file(GCP_KEY_PATH)
client = bigquery.Client(project=BIGQUERY_PROJECT_ID, credentials=_creds)
DS     = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}"


def _load(table: str, rows: list[dict]):
    if not rows: return
    job = client.load_table_from_json(rows, f"{DS}.{table}",
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON))
    job.result()


def _query(sql: str) -> list:
    return list(client.query(sql).result())


# ── DIMENSION READS ───────────────────────────────────────────────────────────

def get_active_roles() -> list[str]:
    return [r.role_title for r in _query(
        f"SELECT role_title FROM `{DS}.dim_job_roles` WHERE is_active = TRUE")]


def get_active_recipients() -> list[dict]:
    return [dict(r) for r in _query(
        f"SELECT recipient_id, recipient_name, email_address "
        f"FROM `{DS}.dim_recipients` WHERE is_active = TRUE")]


def get_blacklisted_companies() -> dict:
    """
    Returns {company_name_lower: blacklist_type} for all active entries
    in dim_blacklisted_companies.
    """
    rows = _query(
        f"SELECT company_name, blacklist_type "
        f"FROM `{DS}.dim_blacklisted_companies` WHERE is_active = TRUE")
    return {r.company_name: r.blacklist_type for r in rows}


# ── DEDUPLICATION ─────────────────────────────────────────────────────────────

def get_existing_job_ids() -> set:
    return {r.job_id for r in _query(
        f"SELECT DISTINCT job_id FROM `{DS}.fact_jobs`")}


# ── ACTIVE SPREADSHEET TRACKING ───────────────────────────────────────────────

def get_active_spreadsheet() -> dict | None:
    rows = _query(f"""
        SELECT sheet_id, sheet_url, sheet_name, COUNT(DISTINCT tab_name) AS tab_count
        FROM `{DS}.fact_sheet_entries`
        GROUP BY sheet_id, sheet_url, sheet_name
        HAVING tab_count < {MAX_TABS_PER_SHEET}
        ORDER BY MAX(sheet_created_at) DESC
        LIMIT 1
    """)
    if not rows: return None
    r = rows[0]
    return {"sheet_id": r.sheet_id, "sheet_url": r.sheet_url,
            "sheet_name": r.sheet_name, "tab_count": r.tab_count}


# ── FACT INSERTS ──────────────────────────────────────────────────────────────

def insert_jobs(jobs: list[dict]):          _load("fact_jobs",          jobs)
def insert_ats_results(r: list[dict]):      _load("fact_ats_results",   r)
def insert_sheet_entries(e: list[dict]):    _load("fact_sheet_entries", e)
def insert_job_keywords(k: list[dict]):     _load("fact_job_keywords",  k)


# ── LOG INSERTS ───────────────────────────────────────────────────────────────

def insert_pipeline_run(run: dict):         _load("log_pipeline_runs",   [run])
def insert_email_log(log: dict):            _load("log_emails",          [log])
def insert_bq_sync(sync: dict):             _load("log_bq_sync",         [sync])
def insert_status_updates(u: list[dict]):   _load("log_status_updates",  u)


# ── SYNC QUERIES ──────────────────────────────────────────────────────────────

def get_open_sheets() -> list[dict]:
    return [dict(r) for r in _query(f"""
        SELECT DISTINCT sheet_id, tab_name, sheet_url
        FROM `{DS}.fact_sheet_entries`
        WHERE sheet_created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    """)]


def get_known_statuses(sheet_id: str, tab_name: str) -> dict:
    sql = f"""
    SELECT job_id, new_status FROM (
        SELECT job_id, new_status,
               ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY detected_at DESC) AS rn
        FROM `{DS}.log_status_updates`
        WHERE sheet_id = '{sheet_id}' AND tab_name = '{tab_name}'
    ) WHERE rn = 1
    """
    return {r.job_id: r.new_status for r in _query(sql)}
