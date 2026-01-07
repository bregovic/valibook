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

    return (
        <div className="validation-view">
            <div className="header-actions" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
                <button onClick={onBack} style={{ background: '#666' }}> Back </button>
                <h2>Validation Report</h2>
                <button onClick={runValidation} style={{ background: '#007bff' }}>Run Validation</button>
            </div>

            {loading && <p>Running validation... (This might take a moment)</p>}

            {!loading && ran && (
                <div>
                    <div className="summary" style={{ marginBottom: '1rem', padding: '1rem', background: stats.count > 0 ? '#fff3cd' : '#d4edda', border: '1px solid #ccc' }}>
                        {stats.count === 0 ? (
                            <strong style={{ color: 'green' }}>Success! No issues found.</strong>
                        ) : (
                            <strong style={{ color: '#856404' }}>Found {stats.count} issues. (Showing first {issues.length})</strong>
                        )}
                    </div>

                    {issues.length > 0 && (
                        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }}>
                            <thead>
                                <tr style={{ background: '#eee', textAlign: 'left' }}>
                                    <th style={{ padding: '8px' }}>Type</th>
                                    <th style={{ padding: '8px' }}>Key</th>
                                    <th style={{ padding: '8px' }}>Column</th>
                                    <th style={{ padding: '8px' }}>Expected</th>
                                    <th style={{ padding: '8px' }}>Actual</th>
                                    <th style={{ padding: '8px' }}>Message</th>
                                </tr>
                            </thead>
                            <tbody>
                                {issues.map((issue, idx) => (
                                    <tr key={idx} style={{ borderBottom: '1px solid #ddd' }}>
                                        <td style={{ padding: '8px' }}>
                                            <span style={{
                                                padding: '2px 6px', borderRadius: '4px', fontSize: '0.8em',
                                                background: issue.type === 'value_mismatch' ? '#f8d7da' : '#e2e3e5',
                                                color: issue.type === 'value_mismatch' ? '#721c24' : '#383d41'
                                            }}>
                                                {issue.type}
                                            </span>
                                        </td>
                                        <td style={{ padding: '8px' }}><strong>{issue.key}</strong></td>
                                        <td style={{ padding: '8px' }}>{issue.column || '-'}</td>
                                        <td style={{ padding: '8px', color: 'green' }}>{issue.expected}</td>
                                        <td style={{ padding: '8px', color: 'red' }}>{issue.actual}</td>
                                        <td style={{ padding: '8px' }}>{issue.message}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    )}
                </div>
            )}
        </div>
    );
}
