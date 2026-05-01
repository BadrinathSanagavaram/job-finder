import React from 'react';
import {
  PieChart, Pie, Cell, Tooltip, Legend, ResponsiveContainer,
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
} from 'recharts';

const COLORS = ['#6366f1','#22c55e','#f59e0b','#06b6d4','#ec4899','#8b5cf6','#14b8a6','#f97316'];

export function IndustryPieChart({ data }) {
  const chartData = (data || []).map(d => ({ name: d.industry || 'Unknown', value: d.count }));
  return (
    <div style={{ background: '#1e293b', borderRadius: 12, padding: 20, border: '1px solid #334155' }}>
      <h3 style={{ marginBottom: 16, color: '#94a3b8', fontSize: 14, textTransform: 'uppercase', letterSpacing: 1 }}>
        Jobs by Industry
      </h3>
      <ResponsiveContainer width="100%" height={280}>
        <PieChart>
          <Pie data={chartData} dataKey="value" nameKey="name" outerRadius={100} label={({ name, percent }) =>
            `${name.slice(0, 15)} ${(percent * 100).toFixed(0)}%`
          }>
            {chartData.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
          </Pie>
          <Tooltip contentStyle={{ background: '#0f172a', border: '1px solid #334155', borderRadius: 8 }} />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}

export function ScoreDistributionChart({ data }) {
  const chartData = (data || []).sort((a, b) => a.score_range.localeCompare(b.score_range));
  return (
    <div style={{ background: '#1e293b', borderRadius: 12, padding: 20, border: '1px solid #334155' }}>
      <h3 style={{ marginBottom: 16, color: '#94a3b8', fontSize: 14, textTransform: 'uppercase', letterSpacing: 1 }}>
        ATS Score Distribution
      </h3>
      <ResponsiveContainer width="100%" height={280}>
        <BarChart data={chartData} margin={{ top: 5, right: 10, left: -10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
          <XAxis dataKey="score_range" tick={{ fill: '#94a3b8', fontSize: 12 }} />
          <YAxis tick={{ fill: '#94a3b8', fontSize: 12 }} />
          <Tooltip contentStyle={{ background: '#0f172a', border: '1px solid #334155', borderRadius: 8 }} />
          <Bar dataKey="count" fill="#6366f1" radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
