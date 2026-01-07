import { useState, useEffect } from 'react';

interface Column {
    id: number;
    column_name: string;
    sample_value: string;
}

interface Mapping {
    sourceColumnId: number | null;
    targetColumnId: number | null;
    isKey: boolean;
    codebookFileId: number | null;
    note?: string;
}

interface Props {
    projectId: number;
    files: any[]; // Passed from parent for convenience
    onBack: () => void;
    onNext: () => void;
}

export default function MappingView({ projectId, files, onBack, onNext }: Props) {
    const [sourceCols, setSourceCols] = useState<Column[]>([]);
    const [targetCols, setTargetCols] = useState<Column[]>([]);
    const [mappings, setMappings] = useState<Mapping[]>([]);
    const [loading, setLoading] = useState(false);
    const [codebookFiles, setCodebookFiles] = useState<any[]>([]);

    useEffect(() => {
        const sFile = files.find(f => f.file_type === 'source');
        const tFile = files.find(f => f.file_type === 'target');
        const cFiles = files.filter(f => f.file_type === 'codebook');

        if (sFile) setSourceCols(sFile.columns);
        if (tFile) setTargetCols(tFile.columns);
        setCodebookFiles(cFiles);

        if (sFile && tFile) {
            fetchMappings();
        }
    }, [files]);

    const fetchMappings = async () => {
        setLoading(true);
        try {
            // 1. Try to fetch existing mappings
            const res = await fetch(`/api/projects/${projectId}/mappings`);
            const existing = await res.json();

            if (existing.length > 0) {
                // Convert DB format to local state format
                const mappingState = existing.map((m: any) => {
                    let extra = {};
                    try { extra = JSON.parse(m.mapping_note || '{}'); } catch (e) { }
                    return {
                        sourceColumnId: m.source_column_id,
                        targetColumnId: m.target_column_id,
                        isKey: (extra as any).isKey || false,
                        codebookFileId: (extra as any).codebookFileId || null,
                        note: m.mapping_note
                    };
                });
                setMappings(mappingState);
            } else {
                // Initialize empty mappings based on source columns
                // We wait for user action or Auto-Map
            }
        } catch (e) {
            console.error(e);
        }
        setLoading(false);
    };

    const handleAutoMap = async () => {
        try {
            const res = await fetch(`/api/projects/${projectId}/auto-map`, { method: 'POST' });
            const data = await res.json();

            if (data.logs) {
                console.groupCollapsed('Auto-Map Debug Logs');
                data.logs.forEach((l: string) => console.log(l));
                console.groupEnd();
            }

            if (data.mappings && data.mappings.length > 0) {
                setMappings(data.mappings);
                alert(`Auto-mapped ${data.mappings.length} columns.`);
            } else {
                alert('Auto-map finished but found 0 matches.\n\nCheck browser console (F12) for detailed matching logs to see why.');
            }
        } catch (e) {
            console.error('Auto-map error:', e);
            alert('Auto-map failed. See console.');
        }
    };

    const updateMapping = (sourceId: number, updates: Partial<Mapping>) => {
        setMappings(prev => {
            const existing = prev.find(m => m.sourceColumnId === sourceId);
            if (existing) {
                // Update
                return prev.map(m => m.sourceColumnId === sourceId ? { ...m, ...updates } : m);
            } else {
                // Create new
                return [...prev, { sourceColumnId: sourceId, targetColumnId: null, isKey: false, codebookFileId: null, ...updates }];
            }
        });
    };

    const saveMappings = async () => {
        // Filter out empty mappings if necessary, or send as is (if null implies unmapped)
        // We send all relevant mappings.

        // Ensure we have an entry for everything we want to save. 
        // Actually, we store what is in state.

        // Serialize extra fields into 'note'
        const payload = mappings.filter(m => m.targetColumnId !== null || m.isKey || m.codebookFileId).map(m => ({
            sourceColumnId: m.sourceColumnId,
            targetColumnId: m.targetColumnId,
            note: JSON.stringify({ isKey: m.isKey, codebookFileId: m.codebookFileId })
        }));

        const res = await fetch(`/api/projects/${projectId}/mappings`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ mappings: payload })
        });

        if (res.ok) {
            // alert('Mappings saved!');
            onNext();
        } else {
            alert('Error saving mappings');
        }
    };

    return (
        <div className="mapping-view fade-in">
            <div className="header-actions">
                <button onClick={onBack} className="secondary"> &larr; Back </button>
                <h2 style={{ border: 0, margin: 0, fontSize: '1.2rem' }}>Column Mapping & Rules</h2>
                <div className="actions" style={{ margin: 0 }}>
                    <button onClick={handleAutoMap} className="secondary" style={{ color: 'var(--warning)', borderColor: 'var(--warning)' }}>Auto Map</button>
                    <button onClick={saveMappings} className="primary">Save & Validate &rarr;</button>
                </div>
            </div>

            {loading && <p style={{ textAlign: 'center', padding: '2rem', fontStyle: 'italic', color: 'var(--text-muted)' }}>Loading mappings...</p>}

            <div style={{ overflowX: 'auto' }}>
                <table>
                    <thead>
                        <tr>
                            <th style={{ width: '60px', textAlign: 'center' }}>Key</th>
                            <th>Source Column (Vzor)</th>
                            <th>Sample</th>
                            <th>Target Column (Export)</th>
                            <th>Validation Rule</th>
                        </tr>
                    </thead>
                    <tbody>
                        {sourceCols.map(sCol => {
                            const m = mappings.find(map => map.sourceColumnId === sCol.id);
                            const isKey = m?.isKey || false;
                            const targetId = m?.targetColumnId ?? '';
                            const codebookId = m?.codebookFileId ?? '';

                            return (
                                <tr key={sCol.id} className={isKey ? 'selected-key-row' : ''} style={isKey ? { background: 'rgba(99, 102, 241, 0.1)' } : {}}>
                                    <td style={{ textAlign: 'center' }}>
                                        <input
                                            type="radio"
                                            name="primary_key"
                                            checked={isKey}
                                            onChange={() => updateMapping(sCol.id, { isKey: true })}
                                            style={{ cursor: 'pointer', width: '1.2em', height: '1.2em' }}
                                        />
                                    </td>
                                    <td><strong>{sCol.column_name}</strong></td>
                                    <td style={{ color: 'var(--text-muted)', fontSize: '0.9em' }}>{sCol.sample_value}</td>
                                    <td>
                                        <select
                                            className="table-select"
                                            value={targetId}
                                            onChange={(e) => updateMapping(sCol.id, { targetColumnId: e.target.value ? parseInt(e.target.value) : null })}
                                            style={{ width: '100%' }}
                                        >
                                            <option value="">-- Unmapped --</option>
                                            {targetCols.map(tCol => (
                                                <option key={tCol.id} value={tCol.id}>{tCol.column_name}</option>
                                            ))}
                                        </select>
                                        {m?.targetColumnId && <div style={{ fontSize: '0.8em', color: 'var(--text-muted)', marginTop: '4px' }}>Sample: {targetCols.find(t => t.id === m.targetColumnId)?.sample_value}</div>}
                                    </td>
                                    <td>
                                        <select
                                            className="table-select"
                                            value={codebookId}
                                            onChange={(e) => updateMapping(sCol.id, { codebookFileId: e.target.value ? parseInt(e.target.value) : null })}
                                            style={{ width: '100%' }}
                                        >
                                            <option value="">-- No Validation --</option>
                                            {codebookFiles.map(cb => (
                                                <option key={cb.id} value={cb.id}>Check in: {cb.original_filename}</option>
                                            ))}
                                        </select>
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
