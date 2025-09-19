import React, { useEffect, useState } from 'react';
import { DEFAULT_BASE_PATH, getBasePath, setBasePath } from '../utils/state';

export function SettingsBadge(): JSX.Element | null {
  const [path, setPath] = useState<string>(getBasePath());
  const [open, setOpen] = useState(false);

  useEffect(() => {
    const onChange = (e: Event) => {
      const ce = e as CustomEvent<string>;
      setPath(ce.detail);
    };
    document.addEventListener('scalar-bench:base-path-changed', onChange as EventListener);
    return () => document.removeEventListener('scalar-bench:base-path-changed', onChange as EventListener);
  }, []);

  return (
    <>
      <span
        className="badge"
        style={{ marginLeft: '0.75rem', cursor: 'pointer' }}
        title="Click to configure results directory"
        onClick={() => setOpen(true)}
      >
        Data root: {path}
      </span>
      {open && (
        <div
          onClick={(e) => {
            if (e.target === e.currentTarget) setOpen(false);
          }}
          style={{
            position: 'fixed', inset: 0, backdropFilter: 'blur(6px)',
            background: 'rgba(2, 6, 23, 0.6)', display: 'flex', alignItems: 'flex-start', justifyContent: 'center', paddingTop: '10vh', zIndex: 100,
          }}
        >
          <div className="settings-panel" id="settings-panel">
            <h3 style={{ margin: 0 }}>Results directory</h3>
            <p className="text-muted small">
              Provide the relative or absolute path that contains <code>index.json</code> and run folders. Default is <code>{DEFAULT_BASE_PATH}</code>.
            </p>
            <input type="text" defaultValue={path} placeholder="../_artifacts/results/" onChange={(e) => setPath(e.target.value)} />
            <div className="actions">
              <button className="ghost" onClick={() => setOpen(false)}>Cancel</button>
              <button className="primary" onClick={() => { setBasePath(path.trim() || DEFAULT_BASE_PATH); setOpen(false); }}>Save</button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

