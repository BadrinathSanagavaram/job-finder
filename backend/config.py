import os
from dotenv import load_dotenv

BASE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE, ".env"))   # no-op if file absent (e.g. GitHub Actions)

APIFY_API_TOKEN  = os.getenv("APIFY_API_TOKEN")
APIFY_ACTOR_ID   = "curious_coder/linkedin-jobs-scraper"

BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID", "job-finder-494904")
BIGQUERY_DATASET    = "job_finder"
GCP_KEY_PATH        = os.path.join(BASE, "service_account.json")

GMAIL_SENDER        = os.getenv("GMAIL_SENDER")
GMAIL_APP_PASSWORD  = os.getenv("GMAIL_APP_PASSWORD")

RESUME_FOLDER       = os.path.join(BASE, "Resume")

SHEET_EDITOR_EMAIL  = os.getenv("SHEET_EDITOR_EMAIL",  "bsanagav@gmail.com")
SHEET_FOLDER_ID     = os.getenv("SHEET_FOLDER_ID",     "1kdofPcQOE4w4S3UBtz8n-wDEivxJiENX")
ACTIVE_RECIPIENT_ID = "user_1"
MAX_TABS_PER_SHEET  = 25

LINKEDIN_BASE = (
    "https://www.linkedin.com/jobs/search/"
    "?keywords={role}&location=United+States&f_TPR=r{tpr}&position=1&pageNum=0"
)

JOB_ROLES = [
    "Data Engineer", "Data Analytics Engineer", "Data Analyst",
    "Business Intelligence Analyst", "Business Intelligence Engineer",
    "Systems Analyst", "Systems Engineer", "Clinical Information Analyst",
    "Population Insights Analyst", "Reporting Analyst", "Data Reporting Analyst",
]

JOB_LIMIT_TOTAL        = 300
ATS_THRESHOLD          = 50.0
EASY_APPLY_ATS_THRESHOLD = 30.0   # lower bar for Easy Apply section
SCRAPE_HOURS           = 24
FRESH_HOURS            = 5
MAX_APPLICANTS         = 100
MAX_YEARS_EXPERIENCE   = 5
STATUS_SYNC_LOOKBACK_DAYS = 7

# ── Staffing / recruiting agencies (blacklist_type = 'staffing') ──────────────
BLACKLISTED_COMPANIES = [
    "robert half", "roberthalf", "teksystems", "tek systems", "mercor",
    "the ladders", "ladders", "kforce", "randstad", "apex systems",
    "insight global", "aerotek", "modis", "experis", "manpower", "hays",
    "adecco", "kelly services", "allegis", "disys", "staffmark", "cybercoders",
    "mastech", "staffing solutions", "staff management", "collabera",
    "infojini", "glocomms", "talentify", "talentbridge", "vaco",
    "acara solutions", "genesis10", "net2source", "eclaro",
    "itech solutions", "recruiting solutions", "hired", "toptal",
    "revature",
]

# ── Job boards / aggregators / excluded orgs (blacklist_type = 'job_board' | 'excluded') ─
# Each entry: (company_name, blacklist_type, reason)
BLACKLISTED_JOB_BOARDS = [
    ("Sundayy",         "job_board", "Job aggregator — not a direct employer"),
    ("RemoteHunter",    "job_board", "Job aggregator — not a direct employer"),
    ("Jobright.ai",     "job_board", "AI job board — not a direct employer"),
    ("FetchJobs.co",    "job_board", "Job aggregator — not a direct employer"),
    ("Netrolynx AI",    "job_board", "AI recruiting platform — not a direct employer"),
    ("Netrolynx",       "job_board", "AI recruiting platform — not a direct employer"),
]

# ── Sponsorship rejection keywords ────────────────────────────────────────────
NO_SPONSORSHIP_KEYWORDS = [
    "no sponsorship", "no visa sponsorship", "will not sponsor", "cannot sponsor",
    "unable to sponsor", "not able to sponsor", "does not sponsor",
    "sponsorship is not available", "sponsorship not available",
    "no h1b", "no h-1b", "no h 1b",
    "must be authorized to work in the u", "must be legally authorized",
    "must have unrestricted authorization", "authorized to work without sponsorship",
    "without the need for sponsorship", "without need for sponsorship",
    "without sponsorship", "us citizens only", "u.s. citizens only",
    "citizens and green card", "green card or us citizen",
    "us citizen or permanent resident", "u.s. citizen or permanent resident",
    "must be a us citizen", "must be a u.s. citizen",
    "permanent residents only", "no opt", "no cpt",
]

# ── Security clearance keywords ───────────────────────────────────────────────
CLEARANCE_KEYWORDS = [
    "secret clearance", "top secret", "ts/sci", "tssci", "security clearance",
    "obtain and maintain", "clearance required", "clearance type",
    "active clearance", "dod clearance", "dod secret", "public trust clearance",
    "confidential clearance", "sci clearance", "polygraph",
    "must hold a clearance", "clearance is required",
    "must have a clearance", "must possess a clearance", "must be clearable",
]
