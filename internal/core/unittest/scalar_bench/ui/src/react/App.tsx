import React, { useEffect } from 'react';
import { Link, NavLink, Route, Routes, useLocation } from 'react-router-dom';
import RunsPage from './pages/RunsPage';
import RunDetailPage from './pages/RunDetailPage';
import ComparePage from './pages/ComparePage';
import { SettingsBadge } from './components/SettingsBadge';

export default function App(): JSX.Element {
  const location = useLocation();
  useEffect(() => {
    // Scroll to top on route change
    window.scrollTo({ top: 0 });
  }, [location]);

  return (
    <div className="app-root">
      <header className="app-header">
        <div className="title">Milvus Scalar Bench</div>
        <div className="subtitle" style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap' }}>
          <span>Visualise scalar filter benchmark results from _artifacts/results</span>
          <SettingsBadge />
          <NavLink
            to="/runs"
            className={({ isActive }) => (isActive ? 'active' : '')}
            style={{
              display: 'inline-flex', alignItems: 'center', gap: '0.35rem',
              background: 'rgba(56, 189, 248, 0.16)', color: 'rgba(125, 211, 252, 0.95)',
              padding: '0.35rem 0.6rem', borderRadius: 999, fontSize: '0.75rem',
              border: '1px solid rgba(148, 163, 184, 0.2)'
            }}
          >Runs</NavLink>
        </div>
      </header>
      <main>
        <Routes>
          <Route path="/runs" element={<RunsPage />} />
          <Route path="/run/:runId" element={<RunDetailPage />} />
          <Route path="/compare" element={<ComparePage />} />
          <Route path="*" element={<RunsPage />} />
        </Routes>
      </main>
    </div>
  );
}

