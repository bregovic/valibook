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
    // Files grouped by type
    const sourceFiles = files.filter(f => f.file_type === 'source');
    const targetFiles = files.filter(f => f.file_type === 'target');
    const codebookFiles = files.filter(f => f.file_type === 'codebook');

    // Selection State
    const [selectedSourceFileId, setSelectedSourceFileId] = useState<number | null>(null);
    const [selectedTargetFileId, setSelectedTargetFileId] = useState<number | null>(null);

    // Columns for selected files
    const [sourceCols, setSourceCols] = useState<Column[]>([]);
    const [targetCols, setTargetCols] = useState<Column[]>([]);

    const [mappings, setMappings] = useState<Mapping[]>([]);
    const [loading, setLoading] = useState(false);

    // Initialize selection with first available files
    useEffect(() => {
        if (sourceFiles.length > 0 && !selectedSourceFileId) setSelectedSourceFileId(sourceFiles[0].id);
        if (targetFiles.length > 0 && !selectedTargetFileId) setSelectedTargetFileId(targetFiles[0].id);
    }, [files]);

    // Update columns when selection changes
    useEffect(() => {
        if (selectedSourceFileId) {
            const f = sourceFiles.find(f => f.id === selectedSourceFileId);
            if (f) setSourceCols(f.columns);
        } else {
            setSourceCols([]);
        }

        if (selectedTargetFileId) {
            const f = targetFiles.find(f => f.id === selectedTargetFileId);
            if (f) setTargetCols(f.columns);
        } else {
            setTargetCols([]);
        }
    }, [selectedSourceFileId, selectedTargetFileId, files]);

    // Fetch mappings whenever selection changes (or initial load)
    useEffect(() => {
        if (selectedSourceFileId && selectedTargetFileId) {
            fetchMappings();
        }
    }, [selectedSourceFileId, selectedTargetFileId]); // Re-fetch if pair changes

    const fetchMappings = async () => {
        setLoading(true);
        try {
            const res = await fetch(`/api/projects/${projectId}/mappings`);
            const existing = await res.json();

            if (existing.length > 0) {
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
            }
        } catch (e) {
            console.error(e);
        }
        setLoading(false);
    };

    const handleGlobalDiscovery = async () => {
        if (!confirm("This will scan ALL files and attempt to automatically find relationships and mappings. Existing mappings might be overwritten.\n\nContinue?")) return;

        setLoading(true);
        try {
            const res = await fetch(`/api/projects/${projectId}/auto-map`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({}) // No specific IDs = Global Discovery
            });
            const data = await res.json();

            if (data.mappings) {
                // Refresh local state
                await fetchMappings();
                alert(`Magic Discovery Complete!\n\nFound ${data.mappings.length} column relationships across all files.`);
            } else {
                alert('Discovery finished. Check console logs.');
            }
        } catch (e) {
            console.error(e);
            alert('Discovery failed');
        }
        setLoading(false);
    };

    const handleAutoMapPair = async () => {
        if (!selectedSourceFileId || !selectedTargetFileId) return;

        setLoading(true);
        try {
            const res = await fetch(`/api/projects/${projectId}/auto-map`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    sourceFileId: selectedSourceFileId,
                    targetFileId: selectedTargetFileId
                })
            });
            const data = await res.json();

            if (data.logs) {
                console.groupCollapsed('Auto-Map Pair Logs');
                data.logs.forEach((l: string) => console.log(l));
                console.groupEnd();
            }

            if (data.mappings && data.mappings.length > 0) {
                await fetchMappings(); // Refresh fully
                alert(`Auto-mapped ${data.mappings.length} columns for this pair.`);
            } else {
                alert('No matches found for this specific file pair.');
            }
        } catch (e) {
            console.error('Auto-map error:', e);
        }
        setLoading(false);
    };

    const updateMapping = (sourceId: number, updates: Partial<Mapping>) => {
        setMappings(prev => {
            const existing = prev.find(m => m.sourceColumnId === sourceId);
            if (existing) {
                return prev.map(m => m.sourceColumnId === sourceId ? { ...m, ...updates } : m);
            } else {
                return [...prev, { sourceColumnId: sourceId, targetColumnId: null, isKey: false, codebookFileId: null, ...updates }];
            }
        });
    };

    const saveMappings = async () => {
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
            onNext();
        } else {
            alert('Error saving mappings');
        }
    };

    return (
        <div className="mapping-view fade-in">
            <div className="header-actions" style={{ flexDirection: 'column', gap: '1rem', alignItems: 'stretch' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <button onClick={onBack} className="secondary"> &larr; Back </button>
                    <div style={{ textAlign: 'center' }}>
                        <h2 style={{ border: 0, margin: 0, fontSize: '1.2rem' }}>Relation & Column Mapping</h2>
                        <button onClick={handleGlobalDiscovery} className="magic-button" style={{ marginTop: '0.5rem', fontSize: '0.9rem', padding: '0.4rem 1rem' }}>
                            ✨ Magic Auto-Discover (Scan All Files)
                        </button>
                    </div>
                    <button onClick={saveMappings} className="primary">Save & Validate &rarr;</button>
                </div>

                {/* File Pair Selector */}
                <div style={{
                    display: 'grid',
                    gridTemplateColumns: '1fr auto 1fr',
                    gap: '1rem',
                    background: 'var(--surface-sunken)',
                    padding: '1rem',
                    borderRadius: '8px',
                    alignItems: 'center'
                }}>
                    <div>
                        <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: 600 }}>Source File (Vzor)</label>
                        <select
                            className="input-field"
                            style={{ width: '100%' }}
                            value={selectedSourceFileId || ''}
                            onChange={(e) => setSelectedSourceFileId(Number(e.target.value))}
                        >
                            {sourceFiles.map(f => <option key={f.id} value={f.id}>{f.original_filename}</option>)}
                        </select>
                    </div>

                    <div style={{ fontSize: '1.5rem', color: 'var(--text-muted)' }}>&rarr;</div>

                    <div>
                        <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: 600 }}>Target File (Export)</label>
                        <select
                            className="input-field"
                            style={{ width: '100%' }}
                            value={selectedTargetFileId || ''}
                            onChange={(e) => setSelectedTargetFileId(Number(e.target.value))}
                        >
                            {targetFiles.map(f => <option key={f.id} value={f.id}>{f.original_filename}</option>)}
                        </select>
                    </div>
                </div>

                <div style={{ textAlign: 'right' }}>
                    <button onClick={handleAutoMapPair} className="secondary" style={{ fontSize: '0.85em' }}>
                        Scan only this pair
                    </button>
                </div>
            </div>

            {loading && <p style={{ textAlign: 'center', padding: '2rem', fontStyle: 'italic', color: 'var(--text-muted)' }}>Loading mappings...</p>}

            <div style={{ overflowX: 'auto', marginTop: '1rem' }}>
                <table>
                    <thead>
                        <tr>
                            <th style={{ width: '60px', textAlign: 'center' }}>Key</th>
                            <th>Column in <em>{sourceFiles.find(f => f.id === selectedSourceFileId)?.original_filename}</em></th>
                            <th>Sample</th>
                            <th>Maps to <em>{targetFiles.find(f => f.id === selectedTargetFileId)?.original_filename}</em></th>
                            {codebookFiles.length > 0 && <th>Validation Rule</th>}
                        </tr>
                    </thead>
                    <tbody>
                        {sourceCols.length === 0 ? (
                            <tr><td colSpan={5} style={{ textAlign: 'center', padding: '2rem' }}>Select files above to begin mapping.</td></tr>
                        ) : sourceCols.map(sCol => {
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
                                        {/* Sample display below select */}
                                        {m?.targetColumnId && <div style={{ fontSize: '0.8em', color: 'var(--text-muted)', marginTop: '4px' }}>Sample: {targetCols.find(t => t.id === m.targetColumnId)?.sample_value}</div>}
                                    </td>
                                    {codebookFiles.length > 0 && (
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
                                    )}
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
