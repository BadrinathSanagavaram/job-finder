import React, { useState } from 'react';

const badge = (score) => {
  const color = score >= 80 ? '#22c55e' : score >= 65 ? '#f59e0b' : '#6366f1';
  return (
    <span style={{
      background: `${color}22`, color, border: `1px solid ${color}44`,
      borderRadius: 20, padding: '2px 10px', fontSize: 12, fontWeight: 700,
    }}>
      {score}%
    </span>
  );
};

export default function JobsTable({ jobs }) {
  const [search, setSearch] = useState('');
  const [sortKey, setSortKey] = useState('ats_score');
  const [sortDir, setSortDir] = useState('desc');

  const filtered = (jobs || [])
    .filter(j =>
      j.title?.toLowerCase().includes(search.toLowerCase()) ||
      j.company?.toLowerCase().includes(search.toLowerCase()) ||
      j.industry?.toLowerCase().includes(search.toLowerCase())
    )
    .sort((a, b) => {
      const av = a[sortKey] ?? 0, bv = b[sortKey] ?? 0;
      return sortDir === 'desc' ? bv - av : av - bv;
    });

  const toggle = (key) => {
    if (sortKey === key) setSortDir(d => d === 'desc' ? 'asc' : 'desc');
    else { setSortKey(key); setSortDir('desc'); }
  };

  const th = (label, key) => (
    <th onClick={() => toggle(key)} style={{
      padding: '12px 16px', textAlign: 'left', cursor: 'pointer',
      color: sortKey === key ? '#6366f1' : '#94a3b8',
      fontSize: 12, textTransform: 'uppercase', letterSpacing: 1,
      background: '#0f172a', whiteSpace: 'nowrap',
    }}>
      {label} {sortKey === key ? (sortDir === 'desc' ? '↓' : '↑') : ''}
    </th>
  );

  return (
    <div style={{ background: '#1e293b', borderRadius: 12, border: '1px solid #334155', overflow: 'hidden' }}>
      <div style={{ padding: '16px 20px', borderBottom: '1px solid #334155', display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 12 }}>
        <h3 style={{ color: '#e2e8f0', fontWeight: 700 }}>
          Matched Jobs ({filtered.length})
        </h3>
        <input
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Search title / company / industry..."
          style={{
            background: '#0f172a', border: '1px solid #334155', borderRadius: 8,
            padding: '8px 14px', color: '#e2e8f0', fontSize: 14, width: 280,
            outline: 'none',
          }}
        />
      </div>
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              {th('Title', 'title')}
              {th('Company', 'company')}
              <th style={{ padding: '12px 16px', color: '#94a3b8', fontSize: 12, textTransform: 'uppercase', letterSpacing: 1, background: '#0f172a' }}>Location</th>
              {th('ATS Score', 'ats_score')}
              {th('Applicants', 'applicants_count')}
              <th style={{ padding: '12px 16px', color: '#94a3b8', fontSize: 12, textTransform: 'uppercase', letterSpacing: 1, background: '#0f172a' }}>Industry</th>
              <th style={{ padding: '12px 16px', color: '#94a3b8', fontSize: 12, textTransform: 'uppercase', letterSpacing: 1, background: '#0f172a' }}>Link</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((j, i) => (
              <tr key={i} style={{ borderTop: '1px solid #334155', transition: 'background 0.15s' }}
                onMouseEnter={e => e.currentTarget.style.background = '#0f172a'}
                onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
              >
                <td style={{ padding: '12px 16px', color: '#e2e8f0', fontWeight: 600, maxWidth: 220 }}>{j.title}</td>
                <td style={{ padding: '12px 16px', color: '#94a3b8' }}>{j.company}</td>
                <td style={{ padding: '12px 16px', color: '#94a3b8', fontSize: 13 }}>{j.location}</td>
                <td style={{ padding: '12px 16px' }}>{badge(j.ats_score)}</td>
                <td style={{ padding: '12px 16px', color: '#94a3b8', textAlign: 'center' }}>{j.applicants_count ?? 'N/A'}</td>
                <td style={{ padding: '12px 16px', color: '#64748b', fontSize: 12, maxWidth: 160 }}>{j.industry}</td>
                <td style={{ padding: '12px 16px' }}>
                  <a href={j.job_url} target="_blank" rel="noreferrer" style={{
                    background: '#6366f1', color: '#fff', padding: '5px 12px',
                    borderRadius: 6, fontSize: 12, fontWeight: 600,
                    textDecoration: 'none', whiteSpace: 'nowrap',
                  }}>Apply →</a>
                </td>
              </tr>
            ))}
            {filtered.length === 0 && (
              <tr><td colSpan={7} style={{ padding: 40, textAlign: 'center', color: '#475569' }}>No jobs found</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
