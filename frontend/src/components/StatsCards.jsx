import React from 'react';

const Card = ({ label, value, sub, color }) => (
  <div style={{
    background: '#1e293b',
    border: `1px solid ${color}33`,
    borderRadius: 12,
    padding: '24px 20px',
    display: 'flex',
    flexDirection: 'column',
    gap: 8,
    boxShadow: `0 0 20px ${color}22`,
  }}>
    <span style={{ fontSize: 13, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: 1 }}>
      {label}
    </span>
    <span style={{ fontSize: 42, fontWeight: 800, color }}>{value ?? '—'}</span>
    {sub && <span style={{ fontSize: 12, color: '#64748b' }}>{sub}</span>}
  </div>
);

export default function StatsCards({ data }) {
  const runs   = data?.runs   || {};
  const emails = data?.email_jobs || {};
  return (
    <div style={{
      display: 'grid',
      gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
      gap: 16,
      marginBottom: 24,
    }}>
      <Card label="Jobs Scraped"          value={runs.total_scraped}  color="#6366f1" sub="Total across all runs" />
      <Card label="Jobs > 50% ATS Match"  value={runs.total_matched}  color="#22c55e" sub="Passed ATS threshold" />
      <Card label="Emails Sent"           value={runs.total_emails}   color="#f59e0b" sub="To recipient" />
      <Card label="Avg Jobs per Email"    value={emails.avg_jobs_per_email} color="#06b6d4" sub="Matched jobs per email" />
    </div>
  );
}
