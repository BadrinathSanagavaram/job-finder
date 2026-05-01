# 💼 Job Finder

> Scrapes LinkedIn 3× a day, scores every job against your resume, and sends you only the ones worth applying to — fully automated, zero manual work.

---

## How It Runs

```
⏰  GitHub Actions triggers at 7 AM / 12 PM / 5 PM EST  (Mon–Fri)
            │
            ▼
    🔍  Scrape LinkedIn  →  up to 300 jobs · 24-hr window
            │
            ▼
    🚫  Filter out noise
        ├─ Staffing agencies & job boards
        ├─ No visa sponsorship
        ├─ Security clearance required
        ├─ 5+ years experience
        └─ Already seen in a past run
            │
            ▼
    🤖  ATS Score  →  TF-IDF + keyword match against your PDF resumes
            │
            ▼
    📊  Google Sheet  →  jobs land here, sorted by score
    📧  Email digest  →  sent to your inbox
    🗄️  BigQuery      →  everything stored for the dashboard
            │
            ▼
    📈  Streamlit Dashboard  →  live, updates after every run
```

> Your laptop doesn't need to be on. GitHub's servers run the whole thing.

---

## Why This Stack

| Tool | Why we picked it |
|---|---|
| **Apify** | Reliable LinkedIn scraper — no brittle browser automation |
| **GitHub Actions** | Free serverless scheduler — no cloud VM, no cron server |
| **BigQuery** | Free tier handles this volume; SQL makes the dashboard trivial |
| **Google Sheets** | You already live here — easiest place to mark jobs Applied / Not Applied |
| **Streamlit** | Fastest way to ship a live dashboard, free hosting included |
| **TF-IDF + Keywords** | Accurate ATS scoring without paying for an LLM API |
| **Gmail SMTP** | Free, direct — no third-party email service needed |

---

## The Dashboard

Four tabs, all live from BigQuery:

| Tab | What's inside |
|---|---|
| 📊 **Overview** | Pipeline run history · totals at a glance |
| 💼 **Jobs** | Filter by section, status, and ATS score |
| 📈 **Analytics** | Filter breakdown · score distribution · top keywords |
| 🚫 **Blacklist** | Every company being filtered out, and why |

---

## The Email

After each run you get a digest — broken into three buckets:

- 🟢 **Fresh** — posted < 5 hours ago
- 🟡 **Older** — posted 5–24 hours ago
- ⚡ **Easy Apply** — ATS ≥ 30%, one-click apply on LinkedIn

---

## ATS Scoring Formula

```
Score = (TF-IDF cosine × 40%) + (keyword overlap × 50%) + (role relevance × 10%)
```

Jobs clearing **50%** go to the main sheet. Easy Apply jobs need **30%+**.

---

## Quick Setup

```bash
# 1. Install dependencies
pip install -r backend/requirements.txt

# 2. Add these 7 secrets to GitHub → Settings → Secrets
#    GCP_SERVICE_ACCOUNT   GOOGLE_OAUTH_TOKEN   APIFY_API_TOKEN
#    GMAIL_APP_PASSWORD     GMAIL_SENDER         SHEET_EDITOR_EMAIL
#    SHEET_FOLDER_ID

# 3. Push to main — Actions handles everything from here
```

---

## Tech Stack at a Glance

`Python` · `GitHub Actions` · `Apify` · `Google BigQuery` · `Google Sheets` · `Gmail SMTP` · `Streamlit` · `scikit-learn` · `pdfplumber`
