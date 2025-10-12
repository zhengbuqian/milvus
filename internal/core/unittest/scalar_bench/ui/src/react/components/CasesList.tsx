import type { CaseInfo } from '../types/bundle';

export interface CasesListProps {
  cases: CaseInfo[];
  bundleId?: string;
  onCaseClick?: (caseId: string) => void;
}

export function CasesList({ cases, bundleId, onCaseClick }: CasesListProps): JSX.Element {
  if (!cases || cases.length === 0) {
    return <div className="empty-state">No cases found.</div>;
  }

  return (
    <div className="table-scroll">
      <table className="data-table">
        <thead>
          <tr>
            <th>Case Name</th>
            <th>Suites</th>
            <th>Total Tests</th>
            <th>Has Flamegraphs</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {cases.map((caseInfo) => (
            <tr key={caseInfo.case_id}>
              <td><strong>{caseInfo.case_name}</strong></td>
              <td>{caseInfo.suites.join(', ')}</td>
              <td className="numeric">{caseInfo.total_tests}</td>
              <td>{caseInfo.has_flamegraphs ? '✓' : '—'}</td>
              <td>
                <button
                  className="secondary"
                  style={{ padding: '0.25rem 0.5rem', fontSize: '0.85em' }}
                  onClick={() => {
                    if (onCaseClick) {
                      onCaseClick(caseInfo.case_id);
                    } else if (bundleId) {
                      // Navigate to case detail page
                      window.location.href = `#/bundle/${bundleId}/case/${caseInfo.case_id}`;
                    } else {
                      // Fallback to scroll to element
                      const element = document.getElementById(`case-${caseInfo.case_id}`);
                      if (element) {
                        element.scrollIntoView({ behavior: 'smooth', block: 'start' });
                      }
                    }
                  }}
                >
                  View Details
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
