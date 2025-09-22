import React from 'react'
import { HashRouter, Routes, Route, Navigate, Link } from 'react-router-dom'
import { RunsPage } from './pages/RunsPage'
import { RunDetailPage } from './pages/RunDetailPage'
import { ComparePage } from './pages/ComparePage'

export function App(): JSX.Element {
  return (
    <HashRouter>
      <div className="container">
        <header className="app-header">
          <Link to="/runs" className="brand">Scalar Bench</Link>
          <nav>
            <Link to="/runs">Runs</Link>
            <Link to="/compare">Compare</Link>
          </nav>
        </header>
        <main>
          <Routes>
            <Route path="/" element={<Navigate to="/runs" replace />} />
            <Route path="/runs" element={<RunsPage />} />
            <Route path="/run/:runId" element={<RunDetailPage />} />
            <Route path="/compare" element={<ComparePage />} />
          </Routes>
        </main>
      </div>
    </HashRouter>
  )
}

