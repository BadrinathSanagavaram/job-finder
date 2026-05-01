"""
Email sender — Gmail SMTP, completely free.
Sends a visual summary dashboard (no cluttered table) with stat cards.
"""
import smtplib
import uuid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timezone
from config import GMAIL_SENDER, GMAIL_APP_PASSWORD


def _card(bg: str, border: str, number: str, label: str, sublabel: str = "") -> str:
    sub = f'<div style="font-size:11px;color:#6b7280;margin-top:2px">{sublabel}</div>' if sublabel else ""
    return f"""
    <td width="25%" style="padding:6px">
      <div style="background:{bg};border:1px solid {border};border-radius:10px;
                  padding:18px 12px;text-align:center">
        <div style="font-size:30px;font-weight:800;color:#1e293b;line-height:1">{number}</div>
        <div style="font-size:11px;font-weight:600;color:#475569;margin-top:6px;
                    text-transform:uppercase;letter-spacing:0.6px">{label}</div>
        {sub}
      </div>
    </td>"""


def _filter_row(icon: str, label: str, count: int) -> str:
    bar_width = min(count * 4, 180)
    color = "#ef4444" if count > 20 else "#f97316" if count > 10 else "#94a3b8"
    return f"""
    <tr>
      <td style="padding:7px 0;font-size:13px;color:#374151">{icon} {label}</td>
      <td style="padding:7px 12px">
        <div style="display:inline-block;background:{color};height:8px;width:{bar_width}px;
                    border-radius:4px;vertical-align:middle"></div>
      </td>
      <td style="padding:7px 0;font-size:13px;font-weight:700;color:#1e293b;
                 text-align:right">{count}</td>
    </tr>"""


def _build_html(jobs_fresh: list, jobs_older: list, jobs_easy: list,
                sheet_url: str, tab_name: str, run_id: str, stats: dict) -> str:
    now_str  = datetime.now(timezone.utc).strftime("%B %d, %Y · %H:%M UTC")
    all_jobs = jobs_fresh + jobs_older + jobs_easy
    avg_ats  = round(sum(j["ats_score"] for j in all_jobs) / len(all_jobs), 1) if all_jobs else 0
    skip     = stats.get("skip_counts", {})
    total_filtered = (skip.get("staffing", 0) + skip.get("no_sponsorship", 0) +
                      skip.get("clearance", 0) + skip.get("experience", 0) +
                      stats.get("dedup_skipped", 0))

    no_match_msg = ""
    if not all_jobs:
        no_match_msg = """
        <tr><td colspan="3">
          <div style="background:#fef9c3;border:1px solid #fde047;border-radius:8px;
                      padding:14px 18px;color:#854d0e;font-size:13px;margin:8px 0">
            No jobs matched all filters in this run. Filters are working correctly —
            try again at the next scheduled run.
          </div>
        </td></tr>"""

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width"></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:24px 0">
<tr><td align="center">
<table width="620" cellpadding="0" cellspacing="0"
       style="background:#ffffff;border-radius:14px;overflow:hidden;
              box-shadow:0 4px 24px rgba(0,0,0,0.09)">

  <!-- ── HEADER ── -->
  <tr>
    <td style="background:linear-gradient(135deg,#1e3a5f 0%,#2563eb 100%);padding:30px 32px">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td>
            <div style="font-size:11px;font-weight:600;color:#93c5fd;letter-spacing:1.2px;
                        text-transform:uppercase">Automated · Mon–Fri · 7 AM / 12 PM / 5 PM EST</div>
            <div style="font-size:22px;font-weight:800;color:#ffffff;margin-top:6px">
              Job Finder Results
            </div>
            <div style="font-size:13px;color:#bfdbfe;margin-top:4px">{now_str}</div>
          </td>
          <td align="right" style="vertical-align:top">
            <div style="background:rgba(255,255,255,0.15);border-radius:8px;padding:10px 16px;
                        text-align:center">
              <div style="font-size:28px;font-weight:800;color:#ffffff">{len(all_jobs)}</div>
              <div style="font-size:11px;color:#bfdbfe;font-weight:600">MATCHES</div>
            </div>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- ── STAT CARDS ── -->
  <tr>
    <td style="padding:24px 24px 8px">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          {_card("#eff6ff","#bfdbfe", str(stats.get("total_scraped",0)),  "Jobs Scraped",  f"24-hr window · {stats.get('total_raw',0)} raw")}
          {_card("#fef9c3","#fde047", str(total_filtered),                "Filtered Out",  "staffing · no-sponsor · clearance · exp")}
          {_card("#f0fdf4","#86efac", str(stats.get("dedup_skipped",0)),  "Duplicates",    "already seen in past runs")}
          {_card("#f5f3ff","#c4b5fd", f"{avg_ats}%",                      "Avg ATS Score", f"of {len(all_jobs)} matched jobs")}
        </tr>
      </table>
    </td>
  </tr>

  <!-- ── JOB SECTIONS ── -->
  <tr>
    <td style="padding:8px 24px 0">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td width="50%" style="padding:6px">
            <div style="background:#f0fdf4;border:1px solid #86efac;border-radius:10px;
                        padding:16px;text-align:center">
              <div style="font-size:26px;font-weight:800;color:#15803d">{len(jobs_fresh)}</div>
              <div style="font-size:11px;font-weight:600;color:#166534;text-transform:uppercase;
                          letter-spacing:0.6px;margin-top:4px">🟢 Fresh Jobs</div>
              <div style="font-size:11px;color:#6b7280;margin-top:2px">Posted &lt; 5 hours ago</div>
            </div>
          </td>
          <td width="50%" style="padding:6px">
            <div style="background:#fffbeb;border:1px solid #fcd34d;border-radius:10px;
                        padding:16px;text-align:center">
              <div style="font-size:26px;font-weight:800;color:#b45309">{len(jobs_older)}</div>
              <div style="font-size:11px;font-weight:600;color:#92400e;text-transform:uppercase;
                          letter-spacing:0.6px;margin-top:4px">🟡 Older Jobs</div>
              <div style="font-size:11px;color:#6b7280;margin-top:2px">Posted 5–24 hours ago</div>
            </div>
          </td>
        </tr>
        <tr>
          <td colspan="2" style="padding:6px">
            <div style="background:#eff6ff;border:1px solid #93c5fd;border-radius:10px;
                        padding:16px;text-align:center">
              <div style="font-size:26px;font-weight:800;color:#1d4ed8">{len(jobs_easy)}</div>
              <div style="font-size:11px;font-weight:600;color:#1e40af;text-transform:uppercase;
                          letter-spacing:0.6px;margin-top:4px">⚡ Easy Apply</div>
              <div style="font-size:11px;color:#6b7280;margin-top:2px">ATS ≥ 30% · One-click apply</div>
            </div>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- ── FILTER BREAKDOWN ── -->
  <tr>
    <td style="padding:20px 24px 0">
      <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:18px 20px">
        <div style="font-size:13px;font-weight:700;color:#1e293b;margin-bottom:12px">
          Filter Breakdown
        </div>
        <table width="100%" cellpadding="0" cellspacing="0">
          {no_match_msg}
          {_filter_row("🏢", "Staffing companies removed",  skip.get("staffing",       0))}
          {_filter_row("🚫", "No-sponsorship removed",      skip.get("no_sponsorship", 0))}
          {_filter_row("🔐", "Clearance required removed",  skip.get("clearance",      0))}
          {_filter_row("📅", "Experience ≥ 5 yrs removed",  skip.get("experience",     0))}
          {_filter_row("♻️", "Duplicates skipped",          stats.get("dedup_skipped", 0))}
        </table>
      </div>
    </td>
  </tr>

  <!-- ── SHEET LINK ── -->
  <tr>
    <td style="padding:20px 24px">
      <div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:10px;padding:18px 20px">
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr>
            <td>
              <div style="font-size:13px;font-weight:700;color:#1e3a5f">📊 Google Sheet</div>
              <div style="font-size:12px;color:#475569;margin-top:4px">
                Tab: <strong>{tab_name}</strong> &nbsp;·&nbsp;
                Update the <strong>Status</strong> column (Applied / Not Applied) after applying.
              </div>
            </td>
            <td align="right" style="white-space:nowrap;padding-left:16px">
              <a href="{sheet_url}"
                 style="background:#2563eb;color:#ffffff;padding:10px 20px;
                        border-radius:8px;text-decoration:none;font-size:13px;
                        font-weight:600;display:inline-block">
                Open Sheet →
              </a>
            </td>
          </tr>
        </table>
      </div>
    </td>
  </tr>

  <!-- ── FOOTER ── -->
  <tr>
    <td style="background:#f8fafc;border-top:1px solid #e2e8f0;padding:14px 24px">
      <div style="font-size:11px;color:#94a3b8;text-align:center">
        Run ID: {run_id} &nbsp;·&nbsp; Status sync runs every 3 hours automatically.
        &nbsp;·&nbsp; Job Finder © {datetime.now().year}
      </div>
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>"""


def send_email(jobs_fresh: list, jobs_older: list, jobs_easy: list,
               sheet_url: str, tab_name: str,
               run_id: str, recipient_email: str, stats: dict) -> dict:
    all_jobs = jobs_fresh + jobs_older + jobs_easy
    now      = datetime.now(timezone.utc)
    subject  = (f"[Job Finder] {len(all_jobs)} match{'es' if len(all_jobs) != 1 else ''} "
                f"({len(jobs_fresh)} fresh · {len(jobs_older)} older · {len(jobs_easy)} easy) · "
                f"{now.strftime('%b %d %H:%M UTC')}")
    body     = _build_html(jobs_fresh, jobs_older, jobs_easy, sheet_url, tab_name, run_id, stats)

    msg            = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = GMAIL_SENDER
    msg["To"]      = recipient_email
    msg.attach(MIMEText(body, "html"))

    smtp_response = ""
    status        = "sent"
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(GMAIL_SENDER, GMAIL_APP_PASSWORD)
            result = smtp.sendmail(GMAIL_SENDER, recipient_email, msg.as_string())
            smtp_response = str(result) if result else "250 OK"
    except Exception as e:
        status        = "failed"
        smtp_response = str(e)

    return {
        "email_id":        str(uuid.uuid4()),
        "run_id":          run_id,
        "recipient_email": recipient_email,
        "subject":         subject,
        "body_html":       body,
        "jobs_count":      len(all_jobs),
        "sheet_url":       sheet_url,
        "sent_at":         now.isoformat(),
        "status":          status,
        "smtp_response":   smtp_response,
    }
