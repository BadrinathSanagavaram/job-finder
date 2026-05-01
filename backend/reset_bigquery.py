"""
Drops all job_finder tables/views and recreates with latest schema.
Reseeds all dimension tables including dim_blacklisted_companies.
Run once: python3 reset_bigquery.py
"""
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from config import BIGQUERY_PROJECT_ID, BIGQUERY_DATASET, GCP_KEY_PATH
from datetime import datetime, timezone
import os, sys

creds  = Credentials.from_service_account_file(GCP_KEY_PATH)
client = bigquery.Client(project=BIGQUERY_PROJECT_ID, credentials=creds)
DS     = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}"
now    = datetime.now(timezone.utc).isoformat()

DROP_ORDER = [
    "v_job_applications_current",
    "fact_job_keywords", "fact_sheet_entries", "fact_ats_results", "fact_jobs",
    "log_status_updates", "log_bq_sync", "log_emails", "log_pipeline_runs",
    "dim_blacklisted_companies", "dim_job_roles", "dim_resumes", "dim_recipients",
]
for name in DROP_ORDER:
    client.delete_table(f"{DS}.{name}", not_found_ok=True)
    print(f"  Dropped : {name}")

F = bigquery.SchemaField

SCHEMAS = {
    "dim_recipients": [
        F("recipient_id",   "STRING"), F("recipient_name", "STRING"),
        F("email_address",  "STRING"), F("is_active",      "BOOLEAN"),
        F("created_at",     "TIMESTAMP"),
    ],
    "dim_resumes": [
        F("resume_id",     "STRING"), F("resume_name",   "STRING"),
        F("role_category", "STRING"), F("file_path",     "STRING"),
        F("is_active",     "BOOLEAN"), F("created_at",   "TIMESTAMP"),
    ],
    "dim_job_roles": [
        F("role_id",    "STRING"), F("role_title", "STRING"),
        F("is_active",  "BOOLEAN"), F("created_at", "TIMESTAMP"),
    ],
    "dim_blacklisted_companies": [
        F("company_name",   "STRING"),   # lowercase match key
        F("display_name",   "STRING"),   # original display name
        F("blacklist_type", "STRING"),   # 'staffing' | 'job_board' | 'excluded'
        F("reason",         "STRING"),
        F("added_at",       "TIMESTAMP"),
        F("is_active",      "BOOLEAN"),
    ],
    "fact_jobs": [
        F("job_id",              "STRING"),   F("run_id",              "STRING"),
        F("title",               "STRING"),   F("standardized_title",  "STRING"),
        F("company",             "STRING"),   F("location",            "STRING"),
        F("salary_raw",          "STRING"),   F("salary_min",          "FLOAT"),
        F("salary_max",          "FLOAT"),    F("salary_currency",     "STRING"),
        F("salary_period",       "STRING"),   F("employment_type",     "STRING"),
        F("seniority_level",     "STRING"),   F("industry",            "STRING"),
        F("workplace_type",      "STRING"),   F("job_url",             "STRING"),
        F("applicants_count",    "INTEGER"),  F("easy_apply",          "BOOLEAN"),
        F("posted_at",           "TIMESTAMP"),F("scraped_at",          "TIMESTAMP"),
        F("job_section",         "STRING"),   # 'fresh' | 'older'
        F("filter_reason",       "STRING"),   # 'passed'|'applicants'|'staffing'|'blacklisted'|'no_sponsorship'|'clearance'|'experience'|'ats_threshold'|'easy_apply_only'
        F("is_blacklisted",      "BOOLEAN"),  # True if from dim_blacklisted_companies or staffing list
        F("blacklist_type",      "STRING"),   # 'staffing'|'job_board'|'excluded'|null
        F("passed_ats_filter",   "BOOLEAN"),
        F("included_in_sheet",   "BOOLEAN"),
        F("description_text",    "STRING"),
    ],
    "fact_ats_results": [
        F("ats_id",             "STRING"), F("job_id",             "STRING"),
        F("run_id",             "STRING"), F("resume_id",          "STRING"),
        F("resume_name",        "STRING"), F("ats_score",          "FLOAT"),
        F("cosine_similarity",  "FLOAT"),  F("keyword_match_rate", "FLOAT"),
        F("matched_keywords",   "STRING"), F("missing_keywords",   "STRING"),
        F("total_job_keywords", "INTEGER"),F("passed_threshold",   "BOOLEAN"),
        F("created_at",         "TIMESTAMP"),
    ],
    "fact_sheet_entries": [
        F("entry_id",            "STRING"), F("run_id",              "STRING"),
        F("job_id",              "STRING"), F("ats_id",              "STRING"),
        F("resume_id",           "STRING"), F("resume_name",         "STRING"),
        F("company",             "STRING"), F("job_title",           "STRING"),
        F("job_url",             "STRING"), F("ats_score",           "FLOAT"),
        F("job_section",         "STRING"), # 'fresh'|'older'|'easy_apply'
        F("sheet_id",            "STRING"), F("tab_name",            "STRING"),
        F("sheet_name",          "STRING"), F("sheet_url",           "STRING"),
        F("sheet_created_at",    "TIMESTAMP"), F("bq_inserted_at",   "TIMESTAMP"),
        F("application_status",  "STRING"), F("status_updated_at",  "TIMESTAMP"),
        F("bq_status_synced_at", "TIMESTAMP"),
    ],
    "fact_job_keywords": [
        F("keyword_id",  "STRING"), F("job_id",      "STRING"),
        F("run_id",      "STRING"), F("resume_name", "STRING"),
        F("keyword",     "STRING"), F("is_matched",  "BOOLEAN"),
        F("created_at",  "TIMESTAMP"),
    ],
    "log_pipeline_runs": [
        F("run_id",                    "STRING"), F("trigger_type",           "STRING"),
        F("started_at",                "TIMESTAMP"), F("completed_at",        "TIMESTAMP"),
        F("duration_seconds",          "FLOAT"),  F("status",                 "STRING"),
        F("roles_searched",            "STRING"), F("total_jobs_scraped",     "INTEGER"),
        F("jobs_after_quality_filter", "INTEGER"),F("jobs_after_dedup",       "INTEGER"),
        F("jobs_after_ats_filter",     "INTEGER"),F("fresh_jobs_in_sheet",    "INTEGER"),
        F("older_jobs_in_sheet",       "INTEGER"),F("easy_apply_in_sheet",    "INTEGER"),
        F("skipped_staffing",          "INTEGER"),F("skipped_no_sponsor",     "INTEGER"),
        F("skipped_clearance",         "INTEGER"),F("skipped_experience",     "INTEGER"),
        F("skipped_duplicate",         "INTEGER"),F("skipped_applicants",     "INTEGER"),
        F("sheet_id",                  "STRING"), F("tab_name",               "STRING"),
        F("sheet_url",                 "STRING"), F("email_sent",             "BOOLEAN"),
        F("error_message",             "STRING"), F("error_traceback",        "STRING"),
        F("notes",                     "STRING"),
    ],
    "log_emails": [
        F("email_id",        "STRING"), F("run_id",          "STRING"),
        F("recipient_id",    "STRING"), F("recipient_email", "STRING"),
        F("subject",         "STRING"), F("body_html",       "STRING"),
        F("jobs_count",      "INTEGER"),F("sheet_url",       "STRING"),
        F("sent_at",         "TIMESTAMP"), F("status",       "STRING"),
        F("smtp_response",   "STRING"),
    ],
    "log_bq_sync": [
        F("sync_id",       "STRING"), F("run_id",        "STRING"),
        F("sheet_id",      "STRING"), F("synced_at",     "TIMESTAMP"),
        F("rows_synced",   "INTEGER"),F("status",        "STRING"),
        F("error_message", "STRING"),
    ],
    "log_status_updates": [
        F("update_id",   "STRING"), F("job_id",      "STRING"),
        F("sheet_id",    "STRING"), F("tab_name",    "STRING"),
        F("sheet_name",  "STRING"), F("sync_run_id", "STRING"),
        F("old_status",  "STRING"), F("new_status",  "STRING"),
        F("row_number",  "INTEGER"),F("detected_at", "TIMESTAMP"),
    ],
}

for table_name, schema in SCHEMAS.items():
    ref = client.dataset(BIGQUERY_DATASET).table(table_name)
    client.create_table(bigquery.Table(ref, schema=schema))
    print(f"  Created : {table_name}")

# View
view_sql = f"""
SELECT
    e.entry_id, e.run_id, e.job_id, e.company, e.job_title, e.job_url,
    e.ats_score, e.resume_name, e.job_section,
    e.sheet_id, e.tab_name, e.sheet_name, e.sheet_url, e.sheet_created_at,
    COALESCE(s.new_status, '') AS application_status,
    s.detected_at AS status_updated_at
FROM `{DS}.fact_sheet_entries` e
LEFT JOIN (
    SELECT job_id, sheet_id, new_status, detected_at
    FROM (
        SELECT job_id, sheet_id, new_status, detected_at,
               ROW_NUMBER() OVER (PARTITION BY job_id, sheet_id ORDER BY detected_at DESC) AS rn
        FROM `{DS}.log_status_updates`
    ) WHERE rn = 1
) s ON e.job_id = s.job_id AND e.sheet_id = s.sheet_id
"""
view_ref = client.dataset(BIGQUERY_DATASET).table("v_job_applications_current")
view = bigquery.Table(view_ref)
view.view_query = view_sql
client.create_table(view)
print("  Created : v_job_applications_current (view)")

def _seed(table, rows):
    if not rows: return
    job = client.load_table_from_json(rows, f"{DS}.{table}",
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON))
    job.result()

sys.path.insert(0, os.path.dirname(__file__))
from config import JOB_ROLES, RESUME_FOLDER, BLACKLISTED_COMPANIES, BLACKLISTED_JOB_BOARDS

_seed("dim_recipients", [
    {"recipient_id":"user_1","recipient_name":"Badrinath","email_address":"bsanagav@gmail.com","is_active":True,"created_at":now},
    {"recipient_id":"user_2","recipient_name":"Reserved","email_address":"","is_active":False,"created_at":now},
])
print("  Seeded  : dim_recipients")

resume_files = sorted([f for f in os.listdir(RESUME_FOLDER) if f.endswith(".pdf")]) if os.path.isdir(RESUME_FOLDER) else []
_seed("dim_resumes", [
    {"resume_id": f"res_{i:03d}", "resume_name": f,
     "role_category": ("data_engineer" if "data engineer" in f.lower() else
                       "data_analyst"  if "data analyst"  in f.lower() else
                       "bi_analyst"    if "bia"           in f.lower() else "general"),
     "file_path": os.path.join(RESUME_FOLDER, f), "is_active": True, "created_at": now}
    for i, f in enumerate(resume_files, 1)
])
print(f"  Seeded  : dim_resumes ({len(resume_files)} rows)")

_seed("dim_job_roles", [
    {"role_id": f"role_{i:02d}", "role_title": r, "is_active": True, "created_at": now}
    for i, r in enumerate(JOB_ROLES, 1)
])
print(f"  Seeded  : dim_job_roles ({len(JOB_ROLES)} rows)")

# Seed dim_blacklisted_companies
blacklist_rows = []
for name in BLACKLISTED_COMPANIES:
    blacklist_rows.append({
        "company_name":   name.lower(),
        "display_name":   name.title(),
        "blacklist_type": "staffing",
        "reason":         "Staffing / recruiting agency",
        "added_at":       now,
        "is_active":      True,
    })
for name, btype, reason in BLACKLISTED_JOB_BOARDS:
    blacklist_rows.append({
        "company_name":   name.lower(),
        "display_name":   name,
        "blacklist_type": btype,
        "reason":         reason,
        "added_at":       now,
        "is_active":      True,
    })
_seed("dim_blacklisted_companies", blacklist_rows)
print(f"  Seeded  : dim_blacklisted_companies ({len(blacklist_rows)} rows)")

print("\nBigQuery reset complete.")
