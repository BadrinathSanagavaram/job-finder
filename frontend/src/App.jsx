import React, { useEffect, useState } from 'react';
import axios from 'axios';
import StatsCards from './components/StatsCards';
import { IndustryPieChart, ScoreDistributionChart } from './components/IndustryChart';
import JobsTable from './components/JobsTable';
import KeywordsTable from './components/KeywordsTable';

const API = 'http://localhost:5000/api/dashboard';

const MOCK = {
  runs:         { total_scraped: 99, total_matched: 34, total_emails: 8 },
  email_jobs:   { avg_jobs_per_email: 4.3 },
  industry:     [
    { industry: 'IT Services', count: 18 },
    { industry: 'Software Dev', count: 10 },
    { industry: 'Healthcare IT', count: 6 },
    { industry: 'Finance', count: 5 },
    { industry: 'Logistics', count: 3 },
  ],
  distribution: [
    { score_range: '50-59', count: 8 },
    { score_range: '60-69', count: 12 },
    { score_range: '70-79', count: 9 },
    { score_range: '80-89', count: 4 },
    { score_range: '90-100', count: 1 },
  ],
  matched_jobs: [
    { title: 'Senior Data Engineer', company: 'Forbes Technical Consulting', location: 'Remote', salary: '$65-$78/hr', job_url: 'https://www.linkedin.com/jobs/view/4407044096', applicants_count: 200, ats_score: 82, industry: 'Staffing', matched_keywords: '["python","sql","kafka","snowflake","airflow"]', missing_keywords: '["scala","flink"]' },
    { title: 'Data Engineer - Analytics', company: 'Dropbox', location: 'US Remote', salary: '$149K-$201K/yr', job_url: 'https://www.linkedin.com/jobs/view/4406580762', applicants_count: 200, ats_score: 76, industry: 'Software Dev', matched_keywords: '["spark","python","sql","databricks","airflow"]', missing_keywords: '["monte carlo"]' },
    { title: 'Data Engineer', company: 'Virtusa', location: 'Plano TX (Hybrid)', salary: '', job_url: 'https://www.linkedin.com/jobs/view/4405577986', applicants_count: 200, ats_score: 71, industry: 'IT Services', matched_keywords: '["python","spark","aws","kafka","sql"]', missing_keywords: '["iceberg","flink"]' },
    { title: 'Data Engineer', company: 'AARATECH', location: 'Remote', salary: '$65K-$75K/yr', job_url: 'https://www.linkedin.com/jobs/view/4406982929', applicants_count: 159, ats_score: 63, industry: 'IT Services', matched_keywords: '["python","sql","etl","aws","bigquery"]', missing_keywords: '["hl7","fhir"]' },
    { title: 'Data Engineer | Remote', company: 'Crossing Hurdles', location: 'Remote', salary: '$50-$120/hr', job_url: 'https://www.linkedin.com/jobs/view/4406949981', applicants_count: 45, ats_score: 58, industry: 'Staffing', matched_keywords: '["python","sql","airflow","dbt"]', missing_keywords: '["spark","kafka"]' },
  ],
};

export default function App() {
  const [data,    setData]    = useState(null);
  const [loading, setLoading] = useState(true);
  const [error,   setError]   = useState(null);
  const [tab,     setTab]     = useState('jobs');

  useEffect(() => {
    axios.get(API)
      .then(r => { setData(r.data.data); setLoading(false); })
      .catch(() => { setData(MOCK); setLoading(false); setError('Using mock data — connect backend for live data'); });
  }, []);

  const jobs = data?.matched_jobs || [];

  return (
    <div style={{ minHeight: '100vh', padding: '24px 32px', maxWidth: 1400, margin: '0 auto' }}>

      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 28, flexWrap: 'wrap', gap: 12 }}>
        <div>
          <h1 style={{ fontSize: 26, fontWeight: 800, color: '#e2e8f0' }}>
            Job Finder Dashboard
          </h1>
          <p style={{ color: '#64748b', fontSize: 13, marginTop: 4 }}>
            LinkedIn · Data Engineering · US · Last 3 hours · ATS &gt; 50%
          </p>
        </div>
        <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
          {error && (
            <span style={{ background: '#f59e0b22', color: '#f59e0b', border: '1px solid #f59e0b44', borderRadius: 6, padding: '4px 12px', fontSize: 12 }}>
              {error}
            </span>
          )}
          <div style={{
            background: '#22c55e22', color: '#22c55e', border: '1px solid #22c55e44',
            borderRadius: 20, padding: '4px 14px', fontSize: 12, fontWeight: 700,
          }}>
            ● Live
          </div>
        </div>
      </div>

      {loading ? (
        <div style={{ textAlign: 'center', padding: 80, color: '#475569' }}>Loading dashboard...</div>
      ) : (
        <>
          {/* KPI Cards */}
          <StatsCards data={data} />

          {/* Charts */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 24 }}>
            <IndustryPieChart data={data?.industry} />
            <ScoreDistributionChart data={data?.distribution} />
          </div>

          {/* Tabs */}
          <div style={{ display: 'flex', gap: 4, marginBottom: 16 }}>
            {[['jobs', 'Matched Jobs'], ['keywords', 'Keywords per Job']].map(([key, label]) => (
              <button key={key} onClick={() => setTab(key)} style={{
                padding: '8px 20px', borderRadius: 8, border: 'none', cursor: 'pointer',
                fontWeight: 600, fontSize: 13,
                background: tab === key ? '#6366f1' : '#1e293b',
                color: tab === key ? '#fff' : '#94a3b8',
                transition: 'all 0.15s',
              }}>{label}</button>
            ))}
          </div>

          {tab === 'jobs'     && <JobsTable     jobs={jobs} />}
          {tab === 'keywords' && <KeywordsTable jobs={jobs} />}
        </>
      )}
    </div>
  );
}
