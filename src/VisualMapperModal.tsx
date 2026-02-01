
import { useState, useEffect } from 'react';

// Define minimal types needed for this component, compatible with App.tsx
interface MapperColumn {
    id: string;
    columnName: string;
    sampleValue?: string;
}

interface TablePreview {
    tableName: string;
    columns: MapperColumn[];
    rowData: Record<string, string>; // columnId -> value
}

interface Props {
    projectId: string;
    tables: any[]; // Pass the full table objects from App
    onClose: () => void;
    onSave: () => void; // Trigger reload in App
}

export default function VisualMapperModal({ projectId, tables, onClose, onSave }: Props) {
    // Selection state
    const [sourceTable, setSourceTable] = useState<string>('');
    const [targetTable, setTargetTable] = useState<string>('');

    // Data state
    const [sourcePreview, setSourcePreview] = useState<TablePreview | null>(null);
    const [targetPreview, setTargetPreview] = useState<TablePreview | null>(null);
    const [loading, setLoading] = useState(false);

    // Mapping state
    const [selectedSourceCol, setSelectedSourceCol] = useState<string | null>(null);
    const [mappings, setMappings] = useState<Array<{ sourceColId: string, targetColId: string, sourceName: string, targetName: string }>>([]);

    // Fetch previews when tables are selected
    useEffect(() => {
        if (sourceTable && targetTable) {
            loadPreviews();
        }
    }, [sourceTable, targetTable]);

    const loadPreviews = async () => {
        setLoading(true);
        try {
            const [srcRes, tgtRes] = await Promise.all([
                fetch(`/api/projects/${projectId}/tables/${sourceTable}/preview-row`),
                fetch(`/api/projects/${projectId}/tables/${targetTable}/preview-row`)
            ]);

            const srcData = await srcRes.json();
            const tgtData = await tgtRes.json();

            setSourcePreview({
                tableName: sourceTable,
                columns: srcData.columns,
                rowData: srcData.rowData
            });

            setTargetPreview({
                tableName: targetTable,
                columns: tgtData.columns,
                rowData: tgtData.rowData
            });

            // Clear previous selections
            setMappings([]);
            setSelectedSourceCol(null);

        } catch (e) {
            console.error(e);
            alert("Nepodařilo se načíst náhledová data.");
        } finally {
            setLoading(false);
        }
    };

    const handleSourceClick = (colId: string) => {
        setSelectedSourceCol(colId);
    };

    const handleTargetClick = (colId: string, colName: string) => {
        if (!selectedSourceCol || !sourcePreview) return;

        // Check if already mapped
        if (mappings.some(m => m.targetColId === colId)) {
            // Optional: Allow re-mapping? For now, prevent duplicates on target
            alert("Tento cílový sloupec je již připojen.");
            return;
        }

        const sourceCol = sourcePreview.columns.find(c => c.id === selectedSourceCol);
        if (!sourceCol) return;

        // Add mapping
        setMappings([...mappings, {
            sourceColId: selectedSourceCol,
            targetColId: colId,
            sourceName: sourceCol.columnName,
            targetName: colName
        }]);

        setSelectedSourceCol(null); // Reset selection
    };

    const removeMapping = (index: number) => {
        setMappings(mappings.filter((_, i) => i !== index));
    };

    const handleSave = async () => {
        if (mappings.length === 0) {
            onClose();
            return;
        }

        setLoading(true);
        try {
            // Apply all mappings
            for (const m of mappings) {
                // 1. Link the columns
                await fetch(`/api/columns/${m.sourceColId}`, {
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ linkedToColumnId: m.targetColId })
                });

                // 2. Set target as Primary Key (as requested by logic: manual link implies key check)
                // Actually, let's keep it safe. If user is mapping "Controlled Values", they likely want to check against keys.
                // But the user request said "Controlled Values". 
                // Implicit behavior: The TARGET of the link is usually the REFERENCE (Primary Key).
                // Wait, based on the backend logic, `linkedToColumnId` is on the FOREIGN KEY pointing TO the PRIMARY KEY.
                // So: Source (Foreign Key) points to Target (Primary Key).
                // But in this UI: 
                // "Source Table" = "Zdrojová" (usually Reference/Book of Truth?)
                // "Target Table" = "Kontrolovaná" (usually what we are checking?)

                // USER REQUEST:
                // "vybrat postupně definované vazby... zobrazili by se náhodně vybrané dva záznamy... označit grafiky, jaké sloupce k sobě patří"
                // Usually "Reference/Source" (Truth) vs "Target/Monitored" (Check).

                // In Valibook schema:
                // `linkedToColumnId` means THIS column points TO THAT column.
                // Usually `TargetTable.col` -> `SourceTable.col`.

                // So if user selects:
                // Left: Reference Table (Truth)
                // Right: Checked Table (Target)
                // Link should be: Right(Checked).linkedTo = Left(Reference).id

                // Let's assume standard ETL flow: Source = Input (Truth), Target = Output (Check)? 
                // Actually in Valibook context: 
                // Project has 'SOURCE' (Truth) and 'TARGET' (Check) types.

                // If user puts "Reference" on Left and "Checked" on Right.
                // Mapping: Checked -> Reference.

                // We need to clarify direction.
                // Let's enforce: Left Side = REFERENCE (Keys/Truth), Right Side = CHECKED DATA.
                // Link = Right.linkedToColumnId = Left.id

                // Make sure to correctly orient the link.
                await fetch(`/api/columns/${m.targetColId}`, { // The "Right" side column
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ linkedToColumnId: m.sourceColId }) // Points to "Left" side
                });

                // Also set "Left" side as Primary Key if it isn't
                await fetch(`/api/columns/${m.sourceColId}`, {
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ isPrimaryKey: true })
                });
            }

            alert("Vazby byly úspěšně uloženy.");
            onSave();
            onClose();
        } catch (e) {
            console.error(e);
            alert("Chyba při ukládání vazeb.");
        } finally {
            setLoading(false);
        }
    };

    return (
        <div style={{
            position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
            background: 'rgba(255,255,255,0.98)', zIndex: 2000,
            display: 'flex', flexDirection: 'column'
        }}>
            {/* Header */}
            <div style={{
                padding: '16px 24px', borderBottom: '1px solid #e2e8f0',
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                background: 'white', boxShadow: '0 2px 4px rgba(0,0,0,0.05)'
            }}>
                <div>
                    <h2 style={{ margin: 0, color: '#1e293b' }}>🔗 Kontrolované hodnoty (Vizuální párování)</h2>
                    <p style={{ margin: '4px 0 0 0', color: '#64748b', fontSize: '0.9rem' }}>
                        Vyberte referenční (zdrojovou) tabulku a přiřaďte k ní kontrolované sloupce.
                    </p>
                </div>
                <div style={{ display: 'flex', gap: '12px' }}>
                    <button onClick={onClose} style={{ padding: '8px 16px', border: '1px solid #cbd5e1', background: 'white', borderRadius: '6px', cursor: 'pointer' }}>
                        Zavřít
                    </button>
                    <button onClick={handleSave} disabled={loading || mappings.length === 0}
                        style={{
                            padding: '8px 24px', background: '#3b82f6', color: 'white', border: 'none',
                            borderRadius: '6px', cursor: (loading || mappings.length === 0) ? 'not-allowed' : 'pointer',
                            fontWeight: 600
                        }}
                    >
                        {loading ? 'Ukládám...' : '✅ Použít vazby'}
                    </button>
                </div>
            </div>

            {/* Selection Bar */}
            <div style={{ padding: '16px 24px', background: '#f8fafc', borderBottom: '1px solid #e2e8f0', display: 'flex', gap: '2rem', alignItems: 'center' }}>
                <div style={{ flex: 1 }}>
                    <label style={{ display: 'block', marginBottom: '6px', fontWeight: 600, color: '#0f172a' }}>
                        1. Referenční tabulka (Zdroj pravdy)
                    </label>
                    <select
                        value={sourceTable}
                        onChange={(e) => setSourceTable(e.target.value)}
                        style={{ width: '100%', padding: '10px', borderRadius: '6px', border: '1px solid #cbd5e1', fontSize: '1rem' }}
                    >
                        <option value="">-- Vyberte referenční tabulku --</option>
                        {tables.map(t => <option key={t.tableName} value={t.tableName}>{t.tableName}</option>)}
                    </select>
                </div>

                <div style={{ fontSize: '1.5rem', color: '#94a3b8' }}>→</div>

                <div style={{ flex: 1 }}>
                    <label style={{ display: 'block', marginBottom: '6px', fontWeight: 600, color: '#0f172a' }}>
                        2. Kontrolovaná tabulka (Data k ověření)
                    </label>
                    <select
                        value={targetTable}
                        onChange={(e) => setTargetTable(e.target.value)}
                        style={{ width: '100%', padding: '10px', borderRadius: '6px', border: '1px solid #cbd5e1', fontSize: '1rem' }}
                    >
                        <option value="">-- Vyberte kontrolovanou tabulku --</option>
                        {tables.map(t => <option key={t.tableName} value={t.tableName}>{t.tableName}</option>)}
                    </select>
                </div>
            </div>

            {/* Main Mapping Area */}
            {loading ? (
                <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#64748b' }}>
                    Načítám ukázková data...
                </div>
            ) : (!sourcePreview || !targetPreview) ? (
                <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#94a3b8' }}>
                    vyberte obě tabulky pro zobrazení sloupců
                </div>
            ) : (
                <div style={{ flex: 1, display: 'flex', overflow: 'hidden' }}>
                    {/* Left Column (Reference) */}
                    <div style={{ flex: 1, overflowY: 'auto', borderRight: '1px solid #e2e8f0', background: '#fff' }}>
                        <div style={{ padding: '12px', background: '#f1f5f9', fontWeight: 600, position: 'sticky', top: 0, borderBottom: '1px solid #e2e8f0' }}>
                            Sloupce: {sourcePreview.tableName} (Zdroj)
                        </div>
                        {sourcePreview.columns.map(col => (
                            <div
                                key={col.id}
                                onClick={() => handleSourceClick(col.id)}
                                style={{
                                    padding: '12px 16px',
                                    borderBottom: '1px solid #f1f5f9',
                                    cursor: 'pointer',
                                    background: selectedSourceCol === col.id ? '#eff6ff' : 'transparent',
                                    borderLeft: selectedSourceCol === col.id ? '4px solid #3b82f6' : '4px solid transparent',
                                    transition: 'background 0.2s'
                                }}
                            >
                                <div style={{ fontWeight: 500, color: '#334155' }}>{col.columnName}</div>
                                <div style={{ fontSize: '0.85rem', color: '#64748b', marginTop: '2px', wordBreak: 'break-all' }}>
                                    {sourcePreview.rowData[col.id] || <span style={{ fontStyle: 'italic', opacity: 0.5 }}>{'{prázdné}'}</span>}
                                </div>
                            </div>
                        ))}
                    </div>

                    {/* Middle (Connections) */}
                    <div style={{ width: '300px', background: '#f8fafc', borderRight: '1px solid #e2e8f0', display: 'flex', flexDirection: 'column' }}>
                        <div style={{ padding: '12px', background: '#f1f5f9', fontWeight: 600, borderBottom: '1px solid #e2e8f0', textAlign: 'center' }}>
                            Vytvořené vazby ({mappings.length})
                        </div>
                        <div style={{ flex: 1, overflowY: 'auto', padding: '10px' }}>
                            {mappings.length === 0 ? (
                                <div style={{ padding: '20px', textAlign: 'center', color: '#94a3b8', fontSize: '0.9rem' }}>
                                    Klikněte na sloupec vlevo a poté na odpovídající sloupec vpravo.
                                </div>
                            ) : (
                                mappings.map((m, i) => (
                                    <div key={i} style={{
                                        background: 'white', padding: '10px', borderRadius: '6px',
                                        marginBottom: '8px', border: '1px solid #e2e8f0', boxShadow: '0 1px 2px rgba(0,0,0,0.05)'
                                    }}>
                                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                                            <span style={{ fontWeight: 600, color: '#3b82f6' }}>{m.sourceName}</span>
                                            <span style={{ color: '#94a3b8' }}>→</span>
                                            <span style={{ fontWeight: 600, color: '#ec4899' }}>{m.targetName}</span>
                                        </div>
                                        <button
                                            onClick={() => removeMapping(i)}
                                            style={{
                                                marginTop: '6px', width: '100%', padding: '4px',
                                                border: '1px solid #fee2e2', background: '#fff1f2', color: '#e11d48',
                                                borderRadius: '4px', cursor: 'pointer', fontSize: '0.8rem'
                                            }}
                                        >
                                            Odstranit
                                        </button>
                                    </div>
                                ))
                            )}
                        </div>
                    </div>

                    {/* Right Column (Checked) */}
                    <div style={{ flex: 1, overflowY: 'auto', background: '#fff' }}>
                        <div style={{ padding: '12px', background: '#f1f5f9', fontWeight: 600, position: 'sticky', top: 0, borderBottom: '1px solid #e2e8f0' }}>
                            Sloupce: {targetPreview.tableName} (Cíl)
                        </div>
                        {targetPreview.columns.map(col => {
                            const isMapped = mappings.some(m => m.targetColId === col.id);
                            return (
                                <div
                                    key={col.id}
                                    onClick={() => !isMapped && handleTargetClick(col.id, col.columnName)}
                                    style={{
                                        padding: '12px 16px',
                                        borderBottom: '1px solid #f1f5f9',
                                        cursor: isMapped ? 'default' : 'pointer',
                                        opacity: isMapped ? 0.5 : 1,
                                        background: isMapped ? '#f1f5f9' : (selectedSourceCol ? '#fff' : '#fafafa'),
                                        transition: 'background 0.2s',
                                        position: 'relative'
                                    }}
                                >
                                    <div style={{ fontWeight: 500, color: '#334155' }}>{col.columnName}</div>
                                    <div style={{ fontSize: '0.85rem', color: '#64748b', marginTop: '2px', wordBreak: 'break-all' }}>
                                        {targetPreview.rowData[col.id] || <span style={{ fontStyle: 'italic', opacity: 0.5 }}>{'{prázdné}'}</span>}
                                    </div>
                                    {selectedSourceCol && !isMapped && (
                                        <div style={{
                                            position: 'absolute', right: '10px', top: '50%', transform: 'translateY(-50%)',
                                            color: '#3b82f6', fontWeight: 600, fontSize: '1.2rem'
                                        }}>
                                            ← Připojit
                                        </div>
                                    )}
                                </div>
                            );
                        })}
                    </div>
                </div>
            )}
        </div>
    );
}
