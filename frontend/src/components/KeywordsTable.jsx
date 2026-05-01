import React, { useState } from 'react';

const Pill = ({ word, color }) => (
  <span style={{
    background: `${color}22`, color, border: `1px solid ${color}44`,
    borderRadius: 20, padding: '2px 10px', fontSize: 11,
    fontWeight: 600, marginRight: 4, marginBottom: 4, display: 'inline-block',
  }}>{word}</span>
);

export default function KeywordsTable({ jobs }) {
  const [search, setSearch] = useState('');

  const filtered = (jobs || []).filter(j =>
    j.title?.toLowerCase().includes(search.toLowerCase()) ||
    j.company?.toLowerCase().includes(search.toLowerCase())
  );

  return (
    <div style={{ background: '#1e293b', borderRadius: 12, border: '1px solid #334155', overflow: 'hidden' }}>
      <div style={{ padding: '16px 20px', borderBottom: '1px solid #334155', display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 12 }}>
        <h3 style={{ color: '#e2e8f0', fontWeight: 700 }}>Keywords per Job</h3>
        <input
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Search title / company..."
          style={{
            background: '#0f172a', border: '1px solid #334155', borderRadius: 8,
            padding: '8px 14px', color: '#e2e8f0', fontSize: 14, width: 260, outline: 'none',
          }}
        />
      </div>
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              {['Job Title', 'Company', 'ATS Score', 'Matched Keywords', 'Missing Keywords'].map(h => (
                <th key={h} style={{
                  padding: '12px 16px', textAlign: 'left',
                  color: '#94a3b8', fontSize: 12, textTransform: 'uppercase',
                  letterSpacing: 1, background: '#0f172a', whiteSpace: 'nowrap',
                }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filtered.map((j, i) => {
              const matched = typeof j.matched_keywords === 'string'
                ? JSON.parse(j.matched_keywords) : (j.matched_keywords || []);
              const missing = typeof j.missing_keywords === 'string'
                ? JSON.parse(j.missing_keywords) : (j.missing_keywords || []);
              return (
                <tr key={i} style={{ borderTop: '1px solid #334155' }}
                  onMouseEnter={e => e.currentTarget.style.background = '#0f172a'}
                  onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
                >
                  <td style={{ padding: '12px 16px', color: '#e2e8f0', fontWeight: 600, maxWidth: 180 }}>{j.title}</td>
                  <td style={{ padding: '12px 16px', color: '#94a3b8' }}>{j.company}</td>
                  <td style={{ padding: '12px 16px', color: '#6366f1', fontWeight: 700 }}>{j.ats_score}%</td>
                  <td style={{ padding: '12px 16px', maxWidth: 300 }}>
                    <div style={{ display: 'flex', flexWrap: 'wrap' }}>
                      {matched.slice(0, 12).map(k => <Pill key={k} word={k} color="#22c55e" />)}
                      {matched.length > 12 && <Pill word={`+${matched.length - 12} more`} color="#22c55e" />}
                    </div>
                  </td>
                  <td style={{ padding: '12px 16px', maxWidth: 300 }}>
                    <div style={{ display: 'flex', flexWrap: 'wrap' }}>
                      {missing.slice(0, 8).map(k => <Pill key={k} word={k} color="#ef4444" />)}
                      {missing.length > 8 && <Pill word={`+${missing.length - 8} more`} color="#ef4444" />}
                    </div>
                  </td>
                </tr>
              );
            })}
            {filtered.length === 0 && (
              <tr><td colSpan={5} style={{ padding: 40, textAlign: 'center', color: '#475569' }}>No jobs found</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
