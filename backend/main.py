"""
Main pipeline — Mon–Fri 7 AM / 12 PM / 5 PM EST via GitHub Actions.
Scrape (24 hr) → Quality filter → Dedup → ATS → Sheet tab → Email → BigQuery
"""
import re, uuid, json, hashlib, traceback
from datetime import datetime, timezone, timedelta

from config import (
    RESUME_FOLDER, ATS_THRESHOLD, EASY_APPLY_ATS_THRESHOLD,
    MAX_APPLICANTS, ACTIVE_RECIPIENT_ID, FRESH_HOURS,
    BLACKLISTED_COMPANIES, BLACKLISTED_JOB_BOARDS,
    NO_SPONSORSHIP_KEYWORDS, CLEARANCE_KEYWORDS, MAX_YEARS_EXPERIENCE,
)
from scraper        import scrape_jobs
from ats_matcher    import match_all_resumes
from sheets_manager import add_sheet_tab
from email_sender   import send_email
from database       import (
    get_active_roles, get_active_recipients, get_blacklisted_companies,
    get_existing_job_ids, get_active_spreadsheet,
    insert_jobs, insert_ats_results, insert_sheet_entries,
    insert_job_keywords, insert_pipeline_run, insert_email_log,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_job_id(company: str, title: str, location: str) -> str:
    key = f"{company.lower().strip()}|{title.lower().strip()}|{location.lower().strip()}"
    return hashlib.md5(key.encode()).hexdigest()[:16]


def _parse_applicants(val) -> int:
    try:    return int(str(val).replace("+", "").strip())
    except: return 9999


def _is_fresh(posted_at_str: str) -> bool:
    if not posted_at_str: return False
    try:
        posted = datetime.fromisoformat(posted_at_str.replace("Z", "+00:00"))
        return posted >= datetime.now(timezone.utc) - timedelta(hours=FRESH_HOURS)
    except: return False


def _check_blacklist(company: str, bq_blacklist: dict) -> tuple[bool, str | None]:
    """Returns (is_blacklisted, blacklist_type)."""
    name = company.lower().strip()
    # Check BQ blacklist (includes staffing + job boards + excluded)
    for key, btype in bq_blacklist.items():
        if key in name:
            return True, btype
    return False, None


def _requires_no_sponsorship(desc: str) -> bool:
    t = desc.lower()
    return any(k in t for k in NO_SPONSORSHIP_KEYWORDS)


def _requires_clearance(desc: str) -> bool:
    t = desc.lower()
    return any(k in t for k in CLEARANCE_KEYWORDS)


def _required_years(desc: str) -> int:
    matches = re.findall(
        r'(\d+)\s*\+?\s*(?:to\s*\d+\s*)?years?\s+(?:of\s+)?(?:experience|exp)',
        desc.lower())
    return max((int(m) for m in matches), default=0)


# ── Pipeline ──────────────────────────────────────────────────────────────────

def run_pipeline(trigger_type: str = "manual"):
    run_id     = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    print(f"\n{'='*60}\nRUN  : {run_id}\nSTART: {started_at}\n{'='*60}")

    total_scraped = jobs_after_quality = jobs_after_dedup = jobs_after_ats = 0
    fresh_in_sheet = older_in_sheet = easy_apply_in_sheet = 0
    skip = {"staffing": 0, "blacklisted": 0, "no_sponsorship": 0,
            "clearance": 0, "experience": 0, "applicants": 0}
    dedup_skipped = 0
    sheet_meta  = {"sheet_id": "", "tab_name": "", "sheet_name": "", "sheet_url": ""}
    email_sent  = False
    error_msg   = error_tb = ""

    try:
        # ── 1. Config ─────────────────────────────────────────────────────────
        roles      = get_active_roles()
        recipients = [r for r in get_active_recipients()
                      if r["recipient_id"] == ACTIVE_RECIPIENT_ID]
        if not recipients:
            raise ValueError("No active recipient in dim_recipients")
        recipient    = recipients[0]
        bq_blacklist = get_blacklisted_companies()
        print(f"Roles: {len(roles)} | Blacklist: {len(bq_blacklist)} companies")

        # ── 2. Scrape ─────────────────────────────────────────────────────────
        print("\n[1/6] Scraping LinkedIn (24-hr window)...")
        all_jobs, raw_count = scrape_jobs(roles)
        total_scraped = len(all_jobs)
        print(f"  Raw: {raw_count} | Unique: {total_scraped}")

        # ── 3. Dedup ──────────────────────────────────────────────────────────
        print("\n[2/6] Deduplication...")
        known_ids = get_existing_job_ids()
        new_jobs, dup_jobs = [], []
        for j in all_jobs:
            jid = _make_job_id(j.get("companyName",""), j.get("title",""), j.get("location",""))
            j["_job_id"] = jid
            if jid in known_ids:
                dedup_skipped += 1
                dup_jobs.append(j)
            else:
                new_jobs.append(j)
                known_ids.add(jid)
        jobs_after_dedup = len(new_jobs)
        print(f"  Duplicates skipped: {dedup_skipped} | New: {jobs_after_dedup}")

        # ── 4. Filter + ATS scoring ───────────────────────────────────────────
        print(f"\n[3/6] Quality filter + ATS scoring...")
        now_iso     = datetime.now(timezone.utc).isoformat()
        fact_jobs   = []
        ats_rows    = []
        keyword_rows = []
        sheet_fresh  = []
        sheet_older  = []
        easy_apply_jobs = []
        seen_sheet_ids  = set()   # prevent intra-tab duplication across sections

        for j in new_jobs:
            jid     = j["_job_id"]
            apps    = _parse_applicants(j.get("applicantsCount", 9999))
            company = j.get("companyName", "")
            desc    = j.get("descriptionText", "")
            fresh   = _is_fresh(j.get("postedAt", ""))
            section = "fresh" if fresh else "older"
            easy    = bool(j.get("easyApply", False))

            # Salary
            sal     = j.get("salaryInsights", {}).get("compensationBreakdown", [{}])[0]
            sal_min = float(sal.get("minSalary", 0) or 0)
            sal_max = float(sal.get("maxSalary", 0) or 0)

            # Determine filter reason
            filter_reason = None
            is_bl, bl_type = _check_blacklist(company, bq_blacklist)

            if apps >= MAX_APPLICANTS:
                filter_reason = "applicants"; skip["applicants"] += 1
            elif is_bl:
                filter_reason = "blacklisted"
                skip["blacklisted"] += 1 if bl_type != "staffing" else 0
                skip["staffing"]    += 1 if bl_type == "staffing"  else 0
            elif _requires_no_sponsorship(desc):
                filter_reason = "no_sponsorship"; skip["no_sponsorship"] += 1
            elif _requires_clearance(desc):
                filter_reason = "clearance"; skip["clearance"] += 1
            elif _required_years(desc) >= MAX_YEARS_EXPERIENCE:
                filter_reason = "experience"; skip["experience"] += 1
            else:
                # Run ATS
                match  = match_all_resumes(desc, RESUME_FOLDER)
                best   = match["best"]
                passed = match["passed"]

                for res in match["all"]:
                    ats_rows.append({
                        "ats_id": str(uuid.uuid4()), "job_id": jid, "run_id": run_id,
                        "resume_id": "", "resume_name": res["resume_name"],
                        "ats_score": res["ats_score"],
                        "cosine_similarity":  res.get("cosine_similarity", 0),
                        "keyword_match_rate": res.get("keyword_match_rate", 0),
                        "matched_keywords":   json.dumps(res.get("matched_keywords", [])),
                        "missing_keywords":   json.dumps(res.get("missing_keywords", [])),
                        "total_job_keywords": res.get("total_job_keywords", 0),
                        "passed_threshold":   res["ats_score"] >= ATS_THRESHOLD,
                        "created_at": now_iso,
                    })
                    for kw in res.get("matched_keywords", []):
                        keyword_rows.append({"keyword_id": str(uuid.uuid4()), "job_id": jid,
                            "run_id": run_id, "resume_name": res["resume_name"],
                            "keyword": kw, "is_matched": True, "created_at": now_iso})
                    for kw in res.get("missing_keywords", []):
                        keyword_rows.append({"keyword_id": str(uuid.uuid4()), "job_id": jid,
                            "run_id": run_id, "resume_name": res["resume_name"],
                            "keyword": kw, "is_matched": False, "created_at": now_iso})

                if passed and best:
                    filter_reason = "passed"
                    entry = {"job_id": jid, "company": company,
                             "job_title": j.get("title",""), "job_url": j.get("link",""),
                             "ats_score": best["ats_score"], "resume_name": best["resume_name"],
                             "applicants_count": apps, "job_section": section}
                    seen_sheet_ids.add(jid)
                    if fresh: sheet_fresh.append(entry)
                    else:     sheet_older.append(entry)
                elif easy and best and best["ats_score"] >= EASY_APPLY_ATS_THRESHOLD:
                    # Easy Apply section — jobs that didn't clear main ATS bar
                    filter_reason = "easy_apply_only"
                    easy_apply_jobs.append({
                        "job_id": jid, "company": company,
                        "job_title": j.get("title",""), "job_url": j.get("link",""),
                        "ats_score": best["ats_score"], "resume_name": best["resume_name"],
                        "applicants_count": apps, "job_section": "easy_apply"
                    })
                else:
                    filter_reason = "ats_threshold"

            jobs_after_quality += 1 if filter_reason not in ("applicants",) else 0
            fact_jobs.append({
                "job_id": jid, "run_id": run_id,
                "title":              j.get("title", ""),
                "standardized_title": j.get("standardizedTitle", ""),
                "company":            company,
                "location":           j.get("location", ""),
                "salary_raw":         j.get("salary", ""),
                "salary_min":         sal_min, "salary_max": sal_max,
                "salary_currency":    sal.get("currencyCode", ""),
                "salary_period":      sal.get("payPeriod", ""),
                "employment_type":    j.get("employmentType", ""),
                "seniority_level":    j.get("seniorityLevel", ""),
                "industry":           j.get("industries", ""),
                "workplace_type":     ",".join(j.get("workplaceTypes", [])),
                "job_url":            j.get("link", ""),
                "applicants_count":   apps,
                "easy_apply":         easy,
                "posted_at":          j.get("postedAt", now_iso),
                "scraped_at":         now_iso,
                "job_section":        section,
                "filter_reason":      filter_reason,
                "is_blacklisted":     is_bl,
                "blacklist_type":     bl_type,
                "passed_ats_filter":  filter_reason in ("passed", "easy_apply_only"),
                "included_in_sheet":  filter_reason in ("passed", "easy_apply_only"),
                "description_text":   desc,
            })

        sheet_fresh.sort(key=lambda x: x["ats_score"], reverse=True)
        sheet_older.sort(key=lambda x: x["ats_score"], reverse=True)
        easy_apply_jobs.sort(key=lambda x: x["ats_score"], reverse=True)
        fresh_in_sheet      = len(sheet_fresh)
        older_in_sheet      = len(sheet_older)
        easy_apply_in_sheet = len(easy_apply_jobs)
        jobs_after_ats      = fresh_in_sheet + older_in_sheet + easy_apply_in_sheet
        print(f"  Fresh={fresh_in_sheet} | Older={older_in_sheet} | EasyApply={easy_apply_in_sheet}")

        # ── 5. Google Sheet tab ───────────────────────────────────────────────
        print("\n[4/6] Adding sheet tab...")
        active_ss  = get_active_spreadsheet()
        sheet_meta = add_sheet_tab(sheet_fresh, sheet_older, easy_apply_jobs, active_ss)
        print(f"  Spreadsheet : {sheet_meta['sheet_url']}")
        print(f"  Tab         : {sheet_meta['tab_name']}")

        # ── 6. BigQuery writes ────────────────────────────────────────────────
        print("\n[5/6] Writing to BigQuery...")
        insert_jobs(fact_jobs)
        insert_ats_results(ats_rows)
        insert_job_keywords(keyword_rows)

        all_sheet_jobs = sheet_fresh + sheet_older + easy_apply_jobs
        insert_sheet_entries([{
            "entry_id": str(uuid.uuid4()), "run_id": run_id,
            "job_id": j["job_id"], "ats_id": "", "resume_id": "",
            "resume_name": j["resume_name"], "company": j["company"],
            "job_title": j["job_title"], "job_url": j["job_url"],
            "ats_score": j["ats_score"], "job_section": j["job_section"],
            "sheet_id":   sheet_meta["sheet_id"],
            "tab_name":   sheet_meta["tab_name"],
            "sheet_name": sheet_meta["sheet_name"],
            "sheet_url":  sheet_meta["sheet_url"],
            "sheet_created_at":   sheet_meta["created_at"],
            "bq_inserted_at":     now_iso,
            "application_status": None, "status_updated_at": None,
            "bq_status_synced_at": None,
        } for j in all_sheet_jobs])
        print(f"  fact_jobs={len(fact_jobs)} | ats={len(ats_rows)} | "
              f"keywords={len(keyword_rows)} | sheet={len(all_sheet_jobs)}")

        # ── 7. Email ──────────────────────────────────────────────────────────
        print("\n[6/6] Sending email...")
        stats = {
            "total_scraped": total_scraped, "total_raw": raw_count,
            "skip_counts":   skip, "dedup_skipped": dedup_skipped,
        }
        email_log = send_email(
            jobs_fresh=sheet_fresh, jobs_older=sheet_older,
            jobs_easy=easy_apply_jobs,
            sheet_url=sheet_meta["sheet_url"], tab_name=sheet_meta["tab_name"],
            run_id=run_id, recipient_email=recipient["email_address"], stats=stats)
        insert_email_log({**email_log, "recipient_id": recipient["recipient_id"]})
        email_sent = email_log["status"] == "sent"
        print(f"  Email: {email_log['status']} → {recipient['email_address']}")

    except Exception:
        error_msg = traceback.format_exc().splitlines()[-1]
        error_tb  = traceback.format_exc()
        print(f"\nERROR: {error_msg}")

    completed_at = datetime.now(timezone.utc).isoformat()
    duration = (datetime.fromisoformat(completed_at) -
                datetime.fromisoformat(started_at)).total_seconds()

    insert_pipeline_run({
        "run_id": run_id, "trigger_type": trigger_type,
        "started_at": started_at, "completed_at": completed_at,
        "duration_seconds": round(duration, 2),
        "status": "success" if not error_msg else "failed",
        "roles_searched":            json.dumps(roles if "roles" in dir() else []),
        "total_jobs_scraped":        total_scraped,
        "jobs_after_quality_filter": jobs_after_quality,
        "jobs_after_dedup":          jobs_after_dedup,
        "jobs_after_ats_filter":     jobs_after_ats,
        "fresh_jobs_in_sheet":       fresh_in_sheet,
        "older_jobs_in_sheet":       older_in_sheet,
        "easy_apply_in_sheet":       easy_apply_in_sheet,
        "skipped_staffing":          skip["staffing"],
        "skipped_no_sponsor":        skip["no_sponsorship"],
        "skipped_clearance":         skip["clearance"],
        "skipped_experience":        skip["experience"],
        "skipped_duplicate":         dedup_skipped,
        "skipped_applicants":        skip["applicants"],
        "sheet_id":    sheet_meta.get("sheet_id", ""),
        "tab_name":    sheet_meta.get("tab_name", ""),
        "sheet_url":   sheet_meta.get("sheet_url", ""),
        "email_sent":  email_sent,
        "error_message":   error_msg,
        "error_traceback": error_tb,
        "notes": "",
    })

    print(f"\n{'='*60}")
    print(f"DONE | {duration:.1f}s | fresh={fresh_in_sheet} older={older_in_sheet} "
          f"easy={easy_apply_in_sheet} | email={'✓' if email_sent else '✗'}")
    print(f"{'='*60}\n")
    return {"run_id": run_id, "fresh": fresh_in_sheet,
            "older": older_in_sheet, "easy_apply": easy_apply_in_sheet}


if __name__ == "__main__":
    run_pipeline(trigger_type="manual")
