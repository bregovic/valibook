import { useState, useEffect } from 'react';

interface Relation {
    id: string;
    type: 'data' | 'reference';
    targetFileId: number;
    targetFileName: string;
    sourceFileId: number | null;
    sourceFileName: string | null;
    refFileId?: number;
    refFileName?: string;
    columnCount: number;
    isKey?: boolean;
}

interface ColumnMapping {
    sourceColumnId: number | null;
    targetColumnId: number;
    targetColumnName: string;
    sourceColumnName: string | null;
    isKey: boolean;
    codebookFileId: number | null;
    sample: string;
}

interface Props {
    projectId: number;
    files: any[];
    onBack: () => void;
    onNext: () => void;
    scopeFileId: number | null;
    setScopeFileId: (id: number | null) => void;
}

export default function MappingView({ projectId, files, onBack, onNext, scopeFileId, setScopeFileId }: Props) {
    const targetFiles = files.filter(f => f.file_type === 'target');

    const [relations, setRelations] = useState<Relation[]>([]);
    const [selectedRelation, setSelectedRelation] = useState<Relation | null>(null);
    const [columnMappings, setColumnMappings] = useState<ColumnMapping[]>([]);
    const [loading, setLoading] = useState(false);
    const [discoveryLogs, setDiscoveryLogs] = useState<string[]>([]);

    // Load existing mappings and build relations on mount
    useEffect(() => {
        loadRelations();
    }, [projectId]);

    const loadRelations = async () => {
        setLoading(true);
        try {
            const res = await fetch(`/api/projects/${projectId}/mappings`);
            const mappings = await res.json();

            // Group mappings by Target File -> Source File (or Reference)
            const relationMap = new Map<string, Relation>();

            for (const m of mappings) {
                let extra: any = {};
                try { extra = JSON.parse(m.mapping_note || '{}'); } catch (e) { }

                // Find target file
                const targetFile = files.find(f => f.columns?.some((c: any) => c.id === m.target_column_id));
                if (!targetFile) continue;

                const targetCol = targetFile.columns?.find((c: any) => c.id === m.target_column_id);

                if (extra.type === 'reference' && extra.codebookFileId) {
                    // Reference Relation
                    const refFile = files.find(f => f.id === extra.codebookFileId);
                    const key = `ref_${targetFile.id}_${extra.codebookFileId}_${targetCol?.column_name}`;

                    if (!relationMap.has(key)) {
                        relationMap.set(key, {
                            id: key,
                            type: 'reference',
                            targetFileId: targetFile.id,
                            targetFileName: targetFile.original_filename,
                            sourceFileId: null,
                            sourceFileName: null,
                            refFileId: extra.codebookFileId,
                            refFileName: refFile?.original_filename || 'Unknown',
                            columnCount: 1
                        });
                    }
                } else if (m.source_column_id) {
                    // Data Relation
                    const sourceFile = files.find(f => f.columns?.some((c: any) => c.id === m.source_column_id));
                    if (!sourceFile) continue;

                    const key = `data_${targetFile.id}_${sourceFile.id}`;

                    if (!relationMap.has(key)) {
                        relationMap.set(key, {
                            id: key,
                            type: 'data',
                            targetFileId: targetFile.id,
                            targetFileName: targetFile.original_filename,
                            sourceFileId: sourceFile.id,
                            sourceFileName: sourceFile.original_filename,
                            columnCount: 0
                        });
                    }

                    const rel = relationMap.get(key)!;
                    rel.columnCount++;
                    if (extra.isKey) rel.isKey = true;
                }
            }

            setRelations(Array.from(relationMap.values()));

            // Auto-select first relation
            if (relationMap.size > 0) {
                const first = Array.from(relationMap.values())[0];
                setSelectedRelation(first);
                loadColumnMappings(first, mappings);
            }
        } catch (e) {
            console.error(e);
        }
        setLoading(false);
    };

    const loadColumnMappings = (relation: Relation, allMappings?: any[]) => {
        const targetFile = files.find(f => f.id === relation.targetFileId);
        if (!targetFile) return;

        const mappingsToUse = allMappings || [];

        const cols: ColumnMapping[] = targetFile.columns.map((tc: any) => {
            // Find mapping for this target column
            const m = mappingsToUse.find((map: any) => map.target_column_id === tc.id);

            let sourceColName = null;
            let sourceColId = null;
            let isKey = false;
            let codebookFileId = null;

            if (m) {
                try {
                    const extra = JSON.parse(m.mapping_note || '{}');
                    isKey = extra.isKey || false;
                    codebookFileId = extra.codebookFileId || null;
                } catch (e) { }

                if (m.source_column_id) {
                    const sourceFile = files.find(f => f.id === relation.sourceFileId);
                    const sourceCol = sourceFile?.columns?.find((c: any) => c.id === m.source_column_id);
                    sourceColName = sourceCol?.column_name || null;
                    sourceColId = m.source_column_id;
                }
            }

            return {
                targetColumnId: tc.id,
                targetColumnName: tc.column_name,
                sourceColumnId: sourceColId,
                sourceColumnName: sourceColName,
                isKey,
                codebookFileId,
                sample: tc.sample_value || ''
            };
        });

        setColumnMappings(cols);
    };

    const handleDiscovery = async () => {
        if (!confirm("Scan all files and discover relationships automatically?\n\nExisting mappings will be replaced.")) return;

        setLoading(true);
        setDiscoveryLogs([]);

        try {
            const res = await fetch(`/api/projects/${projectId}/auto-map`, { method: 'POST' });
            const data = await res.json();

            if (data.logs) setDiscoveryLogs(data.logs);

            // Reload relations
            await loadRelations();

            alert(`Discovery complete!\n\nFound ${data.mappings?.length || 0} column mappings.`);
        } catch (e) {
            console.error(e);
            alert('Discovery failed');
        }
        setLoading(false);
    };

    const handleSaveAndValidate = async () => {
        // Mappings are already saved by Discovery, just proceed
        onNext();
    };

    const selectRelation = async (rel: Relation) => {
        setSelectedRelation(rel);

        // Reload mappings for this relation
        const res = await fetch(`/api/projects/${projectId}/mappings`);
        const mappings = await res.json();
        loadColumnMappings(rel, mappings);
    };

    const dataRelations = relations.filter(r => r.type === 'data');
    const refRelations = relations.filter(r => r.type === 'reference');

    return (
        <div className="mapping-view fade-in">
            {/* Header */}
            <div className="header-actions">
                <button onClick={onBack} className="secondary">&larr; Back</button>
                <h2 style={{ border: 0, margin: 0, fontSize: '1.2rem' }}>Discovered Relations</h2>
                <div style={{ display: 'flex', gap: '10px' }}>
                    <button onClick={handleDiscovery} className="magic-button" disabled={loading}>
                        ✨ {loading ? 'Scanning...' : 'Auto-Discover'}
                    </button>
                    <button onClick={handleSaveAndValidate} className="primary" disabled={relations.length === 0}>
                        Validate &rarr;
                    </button>
                </div>
            </div>

            {/* Scope Selection - Optional */}
            <div style={{ margin: '1rem 0', padding: '0.8rem', border: '1px dashed var(--border)', borderRadius: '6px', background: 'transparent' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
                    <label style={{ fontWeight: 500, color: 'var(--text-muted)', fontSize: '0.9rem' }}>🎯 Scope (Optional):</label>
                    <select
                        value={scopeFileId || ''}
                        onChange={(e) => setScopeFileId(e.target.value ? Number(e.target.value) : null)}
                        className="input-field"
                        style={{ flex: 1, minWidth: '200px' }}
                    >
                        <option value="">All Records (No Filter)</option>
                        {targetFiles.map(f => (
                            <option key={f.id} value={f.id}>Only records linked to: {f.original_filename}</option>
                        ))}
                    </select>
                </div>
                <div style={{ fontSize: '0.75em', color: 'var(--text-muted)', marginTop: '4px' }}>
                    Leave empty to validate everything. Select a file to skip source records not linked to that export.
                </div>
            </div>

            {loading && (
                <div style={{ textAlign: 'center', padding: '2rem' }}>
                    <div className="spinner"></div>
                    <p>Loading...</p>
                </div>
            )}

            {!loading && relations.length === 0 && (
                <div style={{ textAlign: 'center', padding: '3rem', color: 'var(--text-muted)' }}>
                    <h3>No Relations Found</h3>
                    <p>Click <strong>Auto-Discover</strong> to scan your files and find relationships.</p>
                </div>
            )}

            {!loading && relations.length > 0 && (
                <div style={{ display: 'grid', gridTemplateColumns: '280px 1fr', gap: '1.5rem', marginTop: '1rem' }}>
                    {/* Left Panel - Relations List */}
                    <div style={{ background: 'var(--bg-secondary)', borderRadius: '8px', padding: '1rem', maxHeight: '60vh', overflowY: 'auto' }}>
                        <h4 style={{ margin: '0 0 1rem 0', fontSize: '0.9rem', color: 'var(--text-muted)' }}>📊 Data Validations ({dataRelations.length})</h4>
                        {dataRelations.map(rel => (
                            <div
                                key={rel.id}
                                onClick={() => selectRelation(rel)}
                                style={{
                                    padding: '0.6rem',
                                    marginBottom: '0.5rem',
                                    borderRadius: '6px',
                                    cursor: 'pointer',
                                    background: selectedRelation?.id === rel.id ? 'var(--primary)' : 'white',
                                    color: selectedRelation?.id === rel.id ? 'white' : 'inherit',
                                    border: '1px solid var(--border)',
                                    fontSize: '0.85rem'
                                }}
                            >
                                <div style={{ fontWeight: 500 }}>{rel.targetFileName}</div>
                                <div style={{ fontSize: '0.75rem', opacity: 0.8 }}>→ {rel.sourceFileName} ({rel.columnCount} cols)</div>
                            </div>
                        ))}

                        {refRelations.length > 0 && (
                            <>
                                <h4 style={{ margin: '1.5rem 0 1rem 0', fontSize: '0.9rem', color: 'var(--text-muted)' }}>🔗 Reference Checks ({refRelations.length})</h4>
                                {refRelations.map(rel => (
                                    <div
                                        key={rel.id}
                                        onClick={() => selectRelation(rel)}
                                        style={{
                                            padding: '0.6rem',
                                            marginBottom: '0.5rem',
                                            borderRadius: '6px',
                                            cursor: 'pointer',
                                            background: selectedRelation?.id === rel.id ? 'var(--warning)' : 'white',
                                            color: selectedRelation?.id === rel.id ? 'white' : 'inherit',
                                            border: '1px solid var(--border)',
                                            fontSize: '0.85rem'
                                        }}
                                    >
                                        <div style={{ fontWeight: 500 }}>{rel.targetFileName}</div>
                                        <div style={{ fontSize: '0.75rem', opacity: 0.8 }}>→ {rel.refFileName}</div>
                                    </div>
                                ))}
                            </>
                        )}
                    </div>

                    {/* Right Panel - Column Mappings */}
                    <div style={{ background: 'white', borderRadius: '8px', border: '1px solid var(--border)', overflow: 'hidden' }}>
                        {selectedRelation ? (
                            <>
                                <div style={{ padding: '1rem', borderBottom: '1px solid var(--border)', background: 'var(--bg-secondary)' }}>
                                    <h4 style={{ margin: 0 }}>
                                        {selectedRelation.type === 'data' ? '📊' : '🔗'} {selectedRelation.targetFileName}
                                        <span style={{ fontWeight: 'normal', opacity: 0.7 }}> → {selectedRelation.sourceFileName || selectedRelation.refFileName}</span>
                                    </h4>
                                </div>
                                <div style={{ maxHeight: '50vh', overflowY: 'auto' }}>
                                    <table style={{ width: '100%' }}>
                                        <thead>
                                            <tr>
                                                <th style={{ width: '40px' }}>Key</th>
                                                <th>Export Column</th>
                                                <th>Sample</th>
                                                <th>Maps to Source</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {columnMappings.map(cm => (
                                                <tr key={cm.targetColumnId} style={{ background: cm.sourceColumnName ? 'rgba(16, 185, 129, 0.05)' : 'transparent' }}>
                                                    <td style={{ textAlign: 'center' }}>
                                                        {cm.isKey && <span title="Primary Key">🔑</span>}
                                                    </td>
                                                    <td style={{ fontWeight: 500 }}>{cm.targetColumnName}</td>
                                                    <td style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>{cm.sample}</td>
                                                    <td>
                                                        {cm.sourceColumnName ? (
                                                            <span style={{ color: 'var(--success)' }}>✓ {cm.sourceColumnName}</span>
                                                        ) : (
                                                            <span style={{ color: 'var(--text-muted)', fontStyle: 'italic' }}>-- Not Mapped --</span>
                                                        )}
                                                    </td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            </>
                        ) : (
                            <div style={{ padding: '3rem', textAlign: 'center', color: 'var(--text-muted)' }}>
                                Select a relation from the left panel
                            </div>
                        )}
                    </div>
                </div>
            )}

            {/* Discovery Logs */}
            {discoveryLogs.length > 0 && (
                <details style={{ marginTop: '1.5rem' }}>
                    <summary style={{ cursor: 'pointer', fontWeight: 500 }}>📋 Discovery Log ({discoveryLogs.length} entries)</summary>
                    <pre style={{ background: '#1e1e1e', color: '#d4d4d4', padding: '1rem', borderRadius: '6px', fontSize: '0.8rem', maxHeight: '200px', overflowY: 'auto' }}>
                        {discoveryLogs.join('\n')}
                    </pre>
                </details>
            )}
        </div>
    );
}
