"""
Google Sheets manager — tab-based, OAuth2 user credentials.
Each pipeline run adds a new worksheet tab to the active spreadsheet.
After MAX_TABS_PER_SHEET tabs, a new spreadsheet is created.
Run auth_setup.py once to generate token.json before first use.
"""
import os
import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from datetime import datetime, timezone
from config import SHEET_EDITOR_EMAIL, SHEET_FOLDER_ID

BASE        = os.path.dirname(os.path.abspath(__file__))
TOKEN_FILE  = os.path.join(BASE, "token.json")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = [
    "Job ID", "Organization Name", "Job Title",
    "Job Link", "ATS Score", "Resume Name", "Status",
]

HEADER_FMT = {
    "backgroundColor": {"red": 0.192, "green": 0.306, "blue": 0.475},
    "textFormat":      {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
    "horizontalAlignment": "CENTER",
}

SECTION_FMT_FRESH = {
    "backgroundColor": {"red": 0.173, "green": 0.486, "blue": 0.357},
    "textFormat":      {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
    "horizontalAlignment": "LEFT",
}

SECTION_FMT_OLDER = {
    "backgroundColor": {"red": 0.588, "green": 0.467, "blue": 0.157},
    "textFormat":      {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
    "horizontalAlignment": "LEFT",
}


def _client() -> gspread.Client:
    if not os.path.exists(TOKEN_FILE):
        raise FileNotFoundError(
            "token.json not found. Run `python3 auth_setup.py` once to authorise Google access."
        )
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, scopes=SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return gspread.authorize(creds)


def _col_widths_request(sheet_id: int) -> list:
    widths = [140, 200, 240, 320, 90, 220, 120]
    return [{"updateDimensionProperties": {
        "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                  "startIndex": i, "endIndex": i + 1},
        "properties": {"pixelSize": w}, "fields": "pixelSize",
    }} for i, w in enumerate(widths)]


def _dropdown_request(sheet_id: int, start_row: int, end_row: int) -> dict:
    return {"setDataValidation": {
        "range": {"sheetId": sheet_id, "startRowIndex": start_row,
                  "endRowIndex": end_row, "startColumnIndex": 6, "endColumnIndex": 7},
        "rule": {"condition": {"type": "ONE_OF_LIST",
                               "values": [{"userEnteredValue": "Applied"},
                                          {"userEnteredValue": "Not Applied"}]},
                 "showCustomUi": True, "strict": True},
    }}


def _write_section(ws, ss, jobs: list[dict], section_label: str,
                   section_fmt: dict, data_start_row: int) -> int:
    """
    Write a section header + job rows. Returns the next available row index.
    data_start_row is 0-indexed for API requests, 1-indexed for gspread append.
    """
    if not jobs:
        return data_start_row

    # Section header (merged across all 7 columns)
    ws.append_row([section_label], value_input_option="RAW")
    section_row_idx = data_start_row      # 0-indexed for Sheets API
    ws.format(f"A{data_start_row + 1}:G{data_start_row + 1}", section_fmt)

    # Merge the section label across all 7 columns
    ss.batch_update({"requests": [{"mergeCells": {
        "range": {"sheetId": ws.id, "startRowIndex": section_row_idx,
                  "endRowIndex": section_row_idx + 1,
                  "startColumnIndex": 0, "endColumnIndex": 7},
        "mergeType": "MERGE_ALL",
    }}]})

    data_start_row += 1

    # Column headers
    ws.append_row(HEADERS, value_input_option="RAW")
    ws.format(f"A{data_start_row + 1}:G{data_start_row + 1}", HEADER_FMT)
    data_start_row += 1

    # Job rows
    rows = [[
        j["job_id"], j["company"], j["job_title"], j["job_url"],
        f"{j['ats_score']}%", j["resume_name"], "",
    ] for j in jobs]
    ws.append_rows(rows, value_input_option="RAW")

    job_end_row = data_start_row + len(rows)   # 0-indexed exclusive end

    # Dropdown on Status column for this section
    ss.batch_update({"requests": [
        _dropdown_request(ws.id, data_start_row, job_end_row)
    ]})

    return job_end_row


SECTION_FMT_EASY = {
    "backgroundColor": {"red": 0.122, "green": 0.471, "blue": 0.706},
    "textFormat":      {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
    "horizontalAlignment": "LEFT",
}


def add_sheet_tab(
    jobs_fresh: list[dict],
    jobs_older: list[dict],
    jobs_easy:  list[dict],
    active_ss:  dict | None = None,
) -> dict:
    """
    Add a new worksheet tab to the active spreadsheet (or create a new one).
    Three sections: Fresh (< 5 hrs), Older (5–24 hrs), Easy Apply (ATS ≥ 30%).
    active_ss: {sheet_id, sheet_url, sheet_name} or None → create new spreadsheet.
    Returns {sheet_id, tab_name, sheet_name, sheet_url, created_at}.
    """
    gc  = _client()
    now = datetime.now(timezone.utc)
    tab_name = now.strftime("%Y-%m-%d %H:%M UTC")

    if active_ss:
        ss = gc.open_by_key(active_ss["sheet_id"])
        ws = ss.add_worksheet(title=tab_name, rows=600, cols=7)
        sheet_id   = active_ss["sheet_id"]
        sheet_name = active_ss["sheet_name"]
        sheet_url  = active_ss["sheet_url"]
    else:
        sheet_name = f"Job Finder — from {now.strftime('%Y-%m-%d')}"
        ss = gc.create(sheet_name, folder_id=SHEET_FOLDER_ID)
        ss.share(SHEET_EDITOR_EMAIL, perm_type="user", role="writer", notify=True,
                 email_message="Your latest job matches are ready. Update the Status column after applying.")
        ws = ss.sheet1
        ws.update_title(tab_name)
        sheet_id  = ss.id
        sheet_url = ss.url

    ss.batch_update({"requests": _col_widths_request(ws.id)})

    current_row = 0

    total_jobs = len(jobs_fresh) + len(jobs_older) + len(jobs_easy)
    if total_jobs == 0:
        ws.append_row(["No jobs matched all filters in this run."], value_input_option="RAW")
    else:
        fresh_label = f"🟢  FRESH JOBS — Posted within last 5 hours  ({len(jobs_fresh)} jobs)"
        older_label = f"🟡  OLDER JOBS — Posted 5–24 hours ago  ({len(jobs_older)} jobs)"
        easy_label  = f"⚡  EASY APPLY — ATS ≥ 30%  ({len(jobs_easy)} jobs)"

        current_row = _write_section(ws, ss, jobs_fresh, fresh_label,
                                     SECTION_FMT_FRESH, current_row)
        if jobs_older:
            current_row += 1
            ws.append_row([""], value_input_option="RAW")
            current_row = _write_section(ws, ss, jobs_older, older_label,
                                         SECTION_FMT_OLDER, current_row)
        if jobs_easy:
            current_row += 1
            ws.append_row([""], value_input_option="RAW")
            current_row = _write_section(ws, ss, jobs_easy, easy_label,
                                         SECTION_FMT_EASY, current_row)

    return {
        "sheet_id":   sheet_id,
        "tab_name":   tab_name,
        "sheet_name": sheet_name,
        "sheet_url":  sheet_url,
        "created_at": now.isoformat(),
    }


def read_sheet_statuses(sheet_id: str, tab_name: str) -> list[dict]:
    """
    Read Job ID + Status columns from a specific tab.
    Returns list of {job_id, status, row_number}.
    """
    gc = _client()
    try:
        ss = gc.open_by_key(sheet_id)
        ws = ss.worksheet(tab_name)
    except Exception:
        return []

    all_rows = ws.get_all_values()
    results  = []
    for i, row in enumerate(all_rows, start=1):
        # Find rows where column A looks like a job_id (16-char hex)
        if not row or len(row) < 7:
            continue
        job_id = row[0].strip()
        status = row[6].strip()
        if len(job_id) == 16 and all(c in "0123456789abcdef" for c in job_id):
            results.append({"job_id": job_id, "status": status, "row_number": i})
    return results
