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
        <div className="subtitle">
          Visualise scalar filter benchmark results from _artifacts/results
          <SettingsBadge />
        </div>
        <nav>
          <NavLink to="/runs" className={({ isActive }) => (isActive ? 'active' : '')}>Runs</NavLink>
          <NavLink to="/compare" className={({ isActive }) => (isActive ? 'active' : '')}>Compare</NavLink>
        </nav>
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

