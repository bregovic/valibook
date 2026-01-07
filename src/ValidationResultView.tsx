import { useState } from 'react';

interface Issue {
    key: string;
    type: string;
    message?: string;
    column?: string;
    expected?: string;
    actual?: string;
}

interface Props {
    projectId: number;
    onBack: () => void;
}

export default function ValidationResultView({ projectId, onBack }: Props) {
    const [issues, setIssues] = useState<Issue[]>([]);
    const [loading, setLoading] = useState(false);
    const [ran, setRan] = useState(false);
    const [stats, setStats] = useState({ count: 0 });

    const runValidation = async () => {
        setLoading(true);
        try {
            const res = await fetch(`/api/projects/${projectId}/validate`, { method: 'POST' });
            const data = await res.json();
            if (data.error) {
                alert(data.error);
            } else {
                setIssues(data.issues);
                setStats({ count: data.issuesCount });
                setRan(true);
            }
        } catch (e) {
            console.error(e);
            alert('Validation failed to run');
        }
        setLoading(false);
    };

    const getBadgeClass = (type: string) => {
        if (type === 'value_mismatch') return 'badge danger';
        if (type === 'missing_row') return 'badge warning';
        if (type === 'codebook_violation') return 'badge warning';
        return 'badge';
    };

    return (
        <div className="validation-view fade-in">
            <div className="header-actions">
                <button onClick={onBack} className="secondary"> &larr; Back </button>
                <h2 style={{ border: 0, margin: 0, fontSize: '1.2rem' }}>Validation Report</h2>
                <button onClick={runValidation} className="primary" disabled={loading}>
                    {loading ? 'Running...' : 'Run Validation'}
                </button>
            </div>

            {loading && (
                <div style={{ textAlign: 'center', padding: '4rem', color: 'var(--text-muted)' }}>
                    <div className="spinner"></div>
                    <p>Processing files... This might take a moment.</p>
                </div>
            )}

            {!loading && ran && (
                <div className="fade-in">
                    <div className="summary" style={{ marginBottom: '2rem', padding: '1.5rem', borderRadius: 'var(--radius)', background: stats.count > 0 ? 'rgba(239, 68, 68, 0.1)' : 'rgba(16, 185, 129, 0.1)', border: '1px solid transparent', borderColor: stats.count > 0 ? 'var(--danger)' : 'var(--success)' }}>
                        {stats.count === 0 ? (
                            <div style={{ textAlign: 'center' }}>
                                <h3 style={{ color: 'var(--success)', margin: 0 }}>✅ Success!</h3>
                                <p>No data discrepancies found.</p>
                            </div>
                        ) : (
                            <div style={{ textAlign: 'center' }}>
                                <h3 style={{ color: 'var(--danger)', margin: 0 }}>Found {stats.count} issues</h3>
                                <p style={{ opacity: 0.8 }}>(Showing first {issues.length})</p>
                            </div>
                        )}
                    </div>

                    {issues.length > 0 && (
                        <div style={{ overflowX: 'auto' }}>
                            <table>
                                <thead>
                                    <tr>
                                        <th>Type</th>
                                        <th>Key</th>
                                        <th>Column</th>
                                        <th>Expected (Vzor)</th>
                                        <th>Actual (Export)</th>
                                        <th>Message</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {issues.map((issue, idx) => (
                                        <tr key={idx}>
                                            <td>
                                                <span className={getBadgeClass(issue.type)}>
                                                    {issue.type.replace('_', ' ')}
                                                </span>
                                            </td>
                                            <td><strong>{issue.key}</strong></td>
                                            <td>{issue.column || '-'}</td>
                                            <td style={{ color: 'var(--success)' }}>{issue.expected}</td>
                                            <td style={{ color: 'var(--danger)', fontWeight: 500 }}>{issue.actual}</td>
                                            <td style={{ color: 'var(--text-muted)' }}>{issue.message}</td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}
