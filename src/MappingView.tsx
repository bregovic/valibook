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
        const res = await fetch(`/api/projects/${projectId}/auto-map`, { method: 'POST' });
        const data = await res.json();
        if (data.mappings) {
            // Merge with existing logic: exact overwrite or partial?
            // Let's just set them for now.
            setMappings(data.mappings);
            alert(`Auto-mapped ${data.mappings.length} columns.`);
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
        <div className="mapping-view">
            <div className="header-actions" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
                <button onClick={onBack} style={{ background: '#666' }}> Back </button>
                <h2>Column Mapping & Validation Rules</h2>
                <div>
                    <button onClick={handleAutoMap} style={{ marginRight: '10px', background: '#e0a800' }}>Auto Map</button>
                    <button onClick={saveMappings} style={{ background: '#28a745' }}>Save & Prepare Validation</button>
                </div>
            </div>

            {loading && <p>Loading...</p>}

            <table className="mapping-table" style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                    <tr style={{ background: '#eee', textAlign: 'left' }}>
                        <th style={{ padding: '10px' }}>Is Key?</th>
                        <th style={{ padding: '10px' }}>Source Column (Vzor)</th>
                        <th style={{ padding: '10px' }}>Sample</th>
                        <th style={{ padding: '10px' }}>Target Column (Export)</th>
                        <th style={{ padding: '10px' }}>Validation Rule</th>
                    </tr>
                </thead>
                <tbody>
                    {sourceCols.map(sCol => {
                        const m = mappings.find(map => map.sourceColumnId === sCol.id);
                        const isKey = m?.isKey || false;
                        const targetId = m?.targetColumnId ?? '';
                        const codebookId = m?.codebookFileId ?? '';

                        return (
                            <tr key={sCol.id} style={{ borderBottom: '1px solid #ddd', background: isKey ? '#e3f2fd' : 'white' }}>
                                <td style={{ padding: '10px', textAlign: 'center' }}>
                                    <input
                                        type="radio"
                                        name="primary_key"
                                        checked={isKey}
                                        onChange={() => updateMapping(sCol.id, { isKey: true })}
                                    />
                                </td>
                                <td style={{ padding: '10px' }}><strong>{sCol.column_name}</strong></td>
                                <td style={{ padding: '10px', color: '#666', fontSize: '0.9em' }}>{sCol.sample_value}</td>
                                <td style={{ padding: '10px' }}>
                                    <select
                                        value={targetId}
                                        onChange={(e) => updateMapping(sCol.id, { targetColumnId: e.target.value ? parseInt(e.target.value) : null })}
                                        style={{ padding: '5px', width: '100%' }}
                                    >
                                        <option value="">-- Unmapped --</option>
                                        {targetCols.map(tCol => (
                                            <option key={tCol.id} value={tCol.id}>
                                                {tCol.column_name}
                                            </option>
                                        ))}
                                    </select>
                                    {m?.targetColumnId && <div style={{ fontSize: '0.8em', color: '#888', marginTop: '4px' }}>Sample: {targetCols.find(t => t.id === m.targetColumnId)?.sample_value}</div>}
                                </td>
                                <td style={{ padding: '10px' }}>
                                    <select
                                        value={codebookId}
                                        onChange={(e) => updateMapping(sCol.id, { codebookFileId: e.target.value ? parseInt(e.target.value) : null })}
                                        style={{ padding: '5px', width: '100%' }}
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
    );
}
