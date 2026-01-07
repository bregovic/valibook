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
    scopeFileId: number | null;
    setScopeFileId: (id: number | null) => void;
}

export default function MappingView({ projectId, files, onBack, onNext, scopeFileId, setScopeFileId }: Props) {
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

    const handleMappingChange = (targetId: number, newSourceId: number | null) => {
        setMappings(prev => {
            // Logic: Target can only have ONE Source. 
            // 1. Find if this target is already mapped to something.
            const existingMappingForTarget = prev.find(m => m.targetColumnId === targetId);

            let next = [...prev];

            // If we are unsetting (newSourceId is null), just remove the target from the mapping
            if (newSourceId === null) {
                if (existingMappingForTarget) {
                    // We can't just delete the object if it holds codebook info or isKey for source?
                    // But mapping is defined by the link. 
                    // Let's simplify: Remove the whole mapping object for now.
                    next = next.filter(m => m.targetColumnId !== targetId);
                }
                return next;
            }

            // If we are setting a NEW Source (switching source or creating new)
            // 1. Remove old mapping for this target
            if (existingMappingForTarget) {
                next = next.filter(m => m.targetColumnId !== targetId);
            }

            // 2. Check if the New Source is already mapped to something else? 
            // In theory one Source col could map to multiple Target cols (e.g. splitting), but usually 1:1.
            // Let's assume 1:1 for now to avoid confusion? No, let's allow 1:N but typically 1:1.

            // 3. Create or Update Source Mapping
            // Actually, we should check if an entry for this Source ID already exists (e.g. holding just metadata)
            // But with current logic, mappings are links.

            next.push({
                sourceColumnId: newSourceId,
                targetColumnId: targetId,
                isKey: false,
                codebookFileId: null
            });

            return next;
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

                {/* Validation Scope Configuration */}
                <div style={{ marginTop: '1rem', padding: '0.8rem', border: '1px solid var(--primary)', borderRadius: '6px', background: 'rgba(99, 102, 241, 0.05)' }}>
                    <div style={{ display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: '10px' }}>
                        <label style={{ fontWeight: 'bold', color: 'var(--primary)' }}>🎯 Validation Scope (Optional):</label>
                        <select
                            value={scopeFileId || ''}
                            onChange={(e) => setScopeFileId(e.target.value ? Number(e.target.value) : null)}
                            className="input-field"
                            style={{ flex: 1, minWidth: '200px' }}
                        >
                            <option value="">-- Validate ALL Records (Default) --</option>
                            {targetFiles.map(f => (
                                <option key={f.id} value={f.id}>Limit validation to IDs found in: {f.original_filename}</option>
                            ))}
                        </select>
                    </div>
                    <div style={{ fontSize: '0.8em', color: 'var(--text-muted)', marginTop: '4px' }}>
                        Use this if your Source contains more data than the Export. Validation will skip source rows that define a Key not present in the selected Master Export file.
                    </div>
                </div>
            </div>

            {loading && <p style={{ textAlign: 'center', padding: '2rem', fontStyle: 'italic', color: 'var(--text-muted)' }}>Loading mappings...</p>}

            <div style={{ overflowX: 'auto', marginTop: '1rem' }}>
                <table>
                    <thead>
                        <tr>
                            <th style={{ width: '60px', textAlign: 'center' }}>Key</th>
                            <th>Column in <em>{targetFiles.find(f => f.id === selectedTargetFileId)?.original_filename || 'Export'}</em></th>
                            <th>Sample (Export)</th>
                            <th>Maps to <em>{sourceFiles.find(f => f.id === selectedSourceFileId)?.original_filename || 'Source'}</em></th>
                            {codebookFiles.length > 0 && <th>Validation Rule</th>}
                        </tr>
                    </thead>
                    <tbody>
                        {targetCols.length === 0 ? (
                            <tr><td colSpan={5} style={{ textAlign: 'center', padding: '2rem' }}>Select files above to begin mapping.</td></tr>
                        ) : targetCols.map(tCol => {
                            // Find mapping where THIS target column is the target
                            // Logic: The DB stores Source -> Target. We need to find the pair.
                            const m = mappings.find(map => map.targetColumnId === tCol.id);

                            const isKey = m?.isKey || false;
                            const sourceId = m?.sourceColumnId ?? '';
                            // Note: If multiple sources map to same target (unlikely/invalid), we pick first.

                            const codebookId = m?.codebookFileId ?? '';

                            return (
                                <tr key={tCol.id} className={isKey ? 'selected-key-row' : ''} style={isKey ? { background: 'rgba(99, 102, 241, 0.1)' } : {}}>
                                    <td style={{ textAlign: 'center' }}>
                                        <input
                                            type="radio"
                                            name="primary_key"
                                            checked={isKey}
                                            onChange={() => {
                                                // To set key, we need an existing mapping record.
                                                // If no source is selected, we can't really set key on the relationship yet easily in this DB schema
                                                // unless we allow partial mappings.
                                                if (sourceId) updateMapping(Number(sourceId), { isKey: true });
                                                else alert("Please select a Source column first to mark as key.");
                                            }}
                                            style={{ cursor: 'pointer', width: '1.2em', height: '1.2em' }}
                                        />
                                    </td>
                                    <td><strong>{tCol.column_name}</strong></td>
                                    <td style={{ color: 'var(--text-muted)', fontSize: '0.9em' }}>{tCol.sample_value}</td>
                                    <td>
                                        <select
                                            className="table-select"
                                            value={sourceId}
                                            onChange={(e) => {
                                                const newSourceId = e.target.value ? parseInt(e.target.value) : null;

                                                // Function to update state needs to be clever now.
                                                // We are changing the Source ID for a fixed Target ID.
                                                // But state is indexed/managed by mappings objects.

                                                handleMappingChange(tCol.id, newSourceId);
                                            }}
                                            style={{ width: '100%' }}
                                        >
                                            <option value="">-- Unmapped --</option>
                                            {sourceCols.map(sCol => (
                                                <option key={sCol.id} value={sCol.id}>{sCol.column_name}</option>
                                            ))}
                                        </select>
                                        {/* Sample display below select */}
                                        {m?.sourceColumnId && <div style={{ fontSize: '0.8em', color: 'var(--text-muted)', marginTop: '4px' }}>Sample: {sourceCols.find(s => s.id === m.sourceColumnId)?.sample_value}</div>}
                                    </td>
                                    {codebookFiles.length > 0 && (
                                        <td>
                                            <select
                                                className="table-select"
                                                value={codebookId}
                                                onChange={(e) => {
                                                    const cbId = e.target.value ? parseInt(e.target.value) : null;
                                                    // We need to update the mapping record that contains this target ID
                                                    if (sourceId) updateMapping(Number(sourceId), { codebookFileId: cbId });
                                                    else alert("Map a source column first to add validation.");
                                                }}
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
