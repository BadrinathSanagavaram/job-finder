"""
Job Finder Dashboard — Streamlit
Hosted free on Streamlit Community Cloud.
Connects to BigQuery using service account credentials.
"""
import json, os
import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

PROJECT = "job-finder-494904"
DATASET = "job_finder"
DS      = f"{PROJECT}.{DATASET}"

st.set_page_config(
    page_title="Job Finder Dashboard",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Auth: local file OR Streamlit Cloud secret ────────────────────────────────
@st.cache_resource
def _bq_client():
    sa_path = os.path.join(os.path.dirname(__file__), "..", "backend", "service_account.json")
    if os.path.exists(sa_path):
        creds = Credentials.from_service_account_file(sa_path)
    else:
        sa_info = {
            "type": "service_account",
            "project_id": "job-finder-494904",
            "private_key_id": "8540d60abd250527f7c31e4bce7b974d4d02f2ac",
            "private_key": st.secrets["GCP_PRIVATE_KEY"],
            "client_email": "bigquery-admin@job-finder-494904.iam.gserviceaccount.com",
            "client_id": "115263322098198221762",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bigquery-admin%40job-finder-494904.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com",
        }
        creds = Credentials.from_service_account_info(sa_info)
    return bigquery.Client(project=PROJECT, credentials=creds)

client = _bq_client()

@st.cache_data(ttl=300)   # refresh every 5 minutes
def q(sql: str) -> pd.DataFrame:
    return client.query(sql).to_dataframe()


# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="background:linear-gradient(135deg,#1e3a5f,#2563eb);
            padding:24px 32px;border-radius:12px;margin-bottom:24px">
  <h1 style="color:#fff;margin:0;font-size:26px;font-weight:800">💼 Job Finder Dashboard</h1>
  <p style="color:#bfdbfe;margin:6px 0 0;font-size:13px">
    Live monitoring · Pipeline runs Mon–Fri at 7 AM / 12 PM / 5 PM EST
  </p>
</div>
""", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "💼 Jobs", "📈 Analytics", "🚫 Blacklist"])

# ══ TAB 1 — OVERVIEW ══════════════════════════════════════════════════════════
with tab1:
    kpi = q(f"""
        SELECT
            COALESCE(SUM(total_jobs_scraped), 0)                      AS total_scraped,
            COALESCE(SUM(fresh_jobs_in_sheet + older_jobs_in_sheet
                         + easy_apply_in_sheet), 0)                   AS total_matched,
            COUNT(*)                                                   AS total_runs,
            MAX(started_at)                                            AS last_run
        FROM `{DS}.log_pipeline_runs` WHERE status = 'success'
    """)
    applied = q(f"""
        SELECT COUNT(DISTINCT job_id) AS cnt
        FROM `{DS}.log_status_updates` WHERE new_status = 'Applied'
    """)
    avg_ats = q(f"""
        SELECT ROUND(AVG(ats_score), 1) AS avg FROM `{DS}.fact_sheet_entries`
    """)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Scraped",  f"{int(kpi['total_scraped'].iloc[0] or 0):,}")
    c2.metric("ATS Matched",    f"{int(kpi['total_matched'].iloc[0] or 0):,}")
    c3.metric("Applied",        f"{int(applied['cnt'].iloc[0] or 0):,}")
    c4.metric("Avg ATS Score",  f"{avg_ats['avg'].iloc[0] or 0}%")
    c5.metric("Pipeline Runs",  f"{int(kpi['total_runs'].iloc[0] or 0):,}")

    st.divider()
    st.subheader("Recent Pipeline Runs")
    runs = q(f"""
        SELECT
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M UTC', started_at) AS run_time,
            status,
            total_jobs_scraped   AS scraped,
            fresh_jobs_in_sheet  AS fresh,
            older_jobs_in_sheet  AS older,
            easy_apply_in_sheet  AS easy_apply,
            skipped_staffing     AS staffing,
            skipped_no_sponsor   AS no_sponsor,
            skipped_clearance    AS clearance,
            skipped_experience   AS experience,
            skipped_duplicate    AS duplicate,
            CAST(ROUND(duration_seconds) AS INT64) AS dur_sec,
            CAST(email_sent AS STRING)   AS email
        FROM `{DS}.log_pipeline_runs`
        ORDER BY started_at DESC LIMIT 15
    """)
    if not runs.empty:
        st.dataframe(runs, use_container_width=True, hide_index=True)
    else:
        st.info("No pipeline runs yet.")

# ══ TAB 2 — JOBS ══════════════════════════════════════════════════════════════
with tab2:
    st.subheader("All Matched Jobs")
    col1, col2, col3 = st.columns(3)
    section_filter = col1.selectbox("Section", ["All", "fresh", "older", "easy_apply"])
    status_filter  = col2.selectbox("Status",  ["All", "Applied", "Not Applied", "Pending"])
    min_ats        = col3.slider("Min ATS Score", 0, 100, 30)

    where = [f"ats_score >= {min_ats}"]
    if section_filter != "All":
        where.append(f"job_section = '{section_filter}'")
    if status_filter == "Pending":
        where.append("(application_status IS NULL OR application_status = '')")
    elif status_filter != "All":
        where.append(f"application_status = '{status_filter}'")

    jobs = q(f"""
        SELECT
            company, job_title, job_section,
            ROUND(ats_score, 1)  AS ats_score,
            resume_name,
            COALESCE(NULLIF(application_status,''),'—') AS status,
            tab_name             AS sheet_tab,
            FORMAT_TIMESTAMP('%Y-%m-%d', sheet_created_at) AS date,
            job_url
        FROM `{DS}.v_job_applications_current`
        WHERE {" AND ".join(where)}
        ORDER BY sheet_created_at DESC, ats_score DESC
        LIMIT 500
    """)
    if not jobs.empty:
        st.dataframe(
            jobs,
            use_container_width=True,
            hide_index=True,
            column_config={
                "ats_score": st.column_config.ProgressColumn(
                    "ATS Score", min_value=0, max_value=100, format="%d%%"),
                "job_url":   st.column_config.LinkColumn("Link"),
            }
        )
        st.caption(f"{len(jobs)} jobs shown")
    else:
        st.info("No jobs match the current filters.")

# ══ TAB 3 — ANALYTICS ═════════════════════════════════════════════════════════
with tab3:
    col1, col2 = st.columns(2)

    with col1:
        filt = q(f"""
            SELECT
                SUM(skipped_staffing)   AS Staffing,
                SUM(skipped_no_sponsor) AS No_Sponsor,
                SUM(skipped_clearance)  AS Clearance,
                SUM(skipped_experience) AS Experience,
                SUM(skipped_duplicate)  AS Duplicate,
                SUM(skipped_applicants) AS Too_Many_Applicants
            FROM `{DS}.log_pipeline_runs` WHERE status = 'success'
        """)
        if not filt.empty:
            row = filt.iloc[0]
            labels = ["Staffing","No Sponsor","Clearance","Experience","Duplicate","Too Many Applicants"]
            values = [int(row[c] or 0) for c in ["Staffing","No_Sponsor","Clearance","Experience","Duplicate","Too_Many_Applicants"]]
            fig = px.bar(x=labels, y=values, title="Cumulative Filters (All Runs)",
                         labels={"x":"Filter","y":"Jobs Removed"},
                         color=labels,
                         color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        dist = q(f"""
            SELECT
                CASE
                    WHEN ats_score BETWEEN 30 AND 49 THEN '30–49'
                    WHEN ats_score BETWEEN 50 AND 59 THEN '50–59'
                    WHEN ats_score BETWEEN 60 AND 69 THEN '60–69'
                    WHEN ats_score BETWEEN 70 AND 79 THEN '70–79'
                    WHEN ats_score BETWEEN 80 AND 89 THEN '80–89'
                    ELSE '90–100'
                END AS range, COUNT(*) AS cnt
            FROM `{DS}.fact_ats_results`
            WHERE passed_threshold = TRUE
            GROUP BY range ORDER BY range
        """)
        if not dist.empty:
            fig = px.bar(dist, x="range", y="cnt",
                         title="ATS Score Distribution (Matched Jobs)",
                         labels={"range":"Score","cnt":"Count"},
                         color_discrete_sequence=["#2563eb"])
            st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        status_data = q(f"""
            SELECT COALESCE(NULLIF(application_status,''),'Pending') AS status,
                   COUNT(*) AS cnt
            FROM `{DS}.v_job_applications_current` GROUP BY status
        """)
        if not status_data.empty:
            fig = px.pie(status_data, values="cnt", names="status",
                         title="Application Status",
                         color_discrete_sequence=["#22c55e","#ef4444","#94a3b8"])
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        kws = q(f"""
            SELECT keyword, COUNT(*) AS freq
            FROM `{DS}.fact_job_keywords`
            WHERE is_matched = TRUE
            GROUP BY keyword ORDER BY freq DESC LIMIT 20
        """)
        if not kws.empty:
            fig = px.bar(kws, x="freq", y="keyword", orientation="h",
                         title="Top 20 Matched Keywords",
                         color_discrete_sequence=["#7c3aed"])
            fig.update_layout(yaxis={"categoryorder":"total ascending"})
            st.plotly_chart(fig, use_container_width=True)

    # Section breakdown
    st.subheader("Sheet Section Breakdown")
    sections = q(f"""
        SELECT job_section, COUNT(*) AS cnt
        FROM `{DS}.fact_sheet_entries` GROUP BY job_section
    """)
    if not sections.empty:
        fig = px.pie(sections, values="cnt", names="job_section",
                     title="Jobs by Sheet Section",
                     color_discrete_map={"fresh":"#22c55e","older":"#f59e0b","easy_apply":"#3b82f6"})
        st.plotly_chart(fig, use_container_width=True)

# ══ TAB 4 — BLACKLIST ═════════════════════════════════════════════════════════
with tab4:
    st.subheader("Company Blacklist")
    bl = q(f"""
        SELECT display_name AS Company, blacklist_type AS Type,
               reason AS Reason,
               FORMAT_TIMESTAMP('%Y-%m-%d', added_at) AS Added,
               is_active AS Active
        FROM `{DS}.dim_blacklisted_companies`
        ORDER BY blacklist_type, display_name
    """)
    if not bl.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Blacklisted", len(bl))
        col2.metric("Staffing Agencies", len(bl[bl["Type"]=="staffing"]))
        col3.metric("Job Boards / Other", len(bl[bl["Type"]!="staffing"]))
        st.dataframe(bl, use_container_width=True, hide_index=True)
    else:
        st.info("No blacklisted companies found.")

    st.caption("To add a company, insert a row into `dim_blacklisted_companies` in BigQuery.")
