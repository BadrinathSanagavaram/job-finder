"""
Status sync job — runs every 3 hours independently.
Reads all open Google Sheet tabs from the last 7 days,
detects status changes, appends to log_status_updates.
"""
import uuid
import traceback
from datetime import datetime, timezone

from sheets_manager import read_sheet_statuses
from database       import (
    get_open_sheets, get_known_statuses,
    insert_status_updates, insert_bq_sync,
)


def sync_statuses(trigger_type: str = "scheduled"):
    sync_id    = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    print(f"\n{'─'*50}")
    print(f"STATUS SYNC: {sync_id}")
    print(f"Started   : {started_at.isoformat()}")

    total_updates = 0
    status        = "success"
    error_msg     = ""

    try:
        open_sheets = get_open_sheets()
        print(f"Tabs to check: {len(open_sheets)}")

        for sheet in open_sheets:
            sheet_id = sheet["sheet_id"]
            tab_name = sheet["tab_name"]
            print(f"  Checking: {tab_name}")

            current = read_sheet_statuses(sheet_id, tab_name)
            if not current:
                print(f"    → Empty or unreadable, skipping")
                continue

            known = get_known_statuses(sheet_id, tab_name)
            now   = datetime.now(timezone.utc).isoformat()

            updates = []
            for row in current:
                job_id     = row["job_id"]
                new_status = row["status"]

                if not new_status:
                    continue
                old_status = known.get(job_id, None)
                if new_status == old_status:
                    continue

                updates.append({
                    "update_id":   str(uuid.uuid4()),
                    "job_id":      job_id,
                    "sheet_id":    sheet_id,
                    "tab_name":    tab_name,
                    "sheet_name":  "",
                    "sync_run_id": sync_id,
                    "old_status":  old_status,
                    "new_status":  new_status,
                    "row_number":  row["row_number"],
                    "detected_at": now,
                })

            if updates:
                insert_status_updates(updates)
                total_updates += len(updates)
                print(f"    → {len(updates)} status update(s) synced")
            else:
                print(f"    → No changes detected")

    except Exception:
        status    = "failed"
        error_msg = traceback.format_exc()
        print(f"ERROR: {error_msg}")

    completed_at = datetime.now(timezone.utc)
    insert_bq_sync({
        "sync_id":       sync_id,
        "run_id":        sync_id,
        "sheet_id":      "multi",
        "synced_at":     completed_at.isoformat(),
        "rows_synced":   total_updates,
        "status":        status,
        "error_message": error_msg,
    })

    print(f"Completed : {completed_at.isoformat()}")
    print(f"Updated   : {total_updates} status row(s)")
    print(f"{'─'*50}\n")
    return {"sync_id": sync_id, "updates": total_updates}


if __name__ == "__main__":
    sync_statuses(trigger_type="manual")
