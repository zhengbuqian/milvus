import { escapeHtml } from '../utils/format';

export interface MetaItemProps {
  label: string;
  value: string;
}

export function MetaItem({ label, value }: MetaItemProps): JSX.Element {
  return (
    <div className="meta-item">
      <span className="label">{label}</span>
      <span className="value" dangerouslySetInnerHTML={{ __html: escapeHtml(String(value)) }} />
    </div>
  );
}
