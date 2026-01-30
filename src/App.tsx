import { useState, useEffect, useCallback } from 'react';
import './App.css';

const API_URL = import.meta.env.DEV ? 'http://localhost:3001/api' : '/api';

interface Column {
  id: string;
  columnName: string;
  columnIndex: number;
  isPrimaryKey: boolean;
  isRequired: boolean;
  uniqueCount: number | null;
  nullCount: number | null;
  sampleValues: string[] | null;
  linkedToColumnId: string | null;
}

interface TableData {
  tableName: string;
  tableType: 'SOURCE' | 'TARGET' | 'FORBIDDEN' | 'RANGE';
  rowCount: number | null;
  columns: Column[];
}

interface Project {
  id: string;
  name: string;
  description: string | null;
  createdAt: string;
  _count?: { columns: number };
}

interface LinkSuggestion {
  sourceColumnId: string;
  sourceColumn: string;
  sourceTable: string;
  targetColumnId: string;
  targetColumn: string;
  targetTable: string;
  matchPercentage: number;
  commonValues: number;
}

interface ValidationError {
  fkTable: string;
  fkColumn: string;
  pkTable: string;
  pkColumn: string;
  missingValues: string[];
  missingCount: number;
  totalFkValues: number;
}

interface ReconciliationError {
  sourceTable: string;
  sourceColumn: string;
  targetTable: string;
  targetColumn: string;
  joinKey: string;
  mismatches: { key: string; source: string; target: string }[];
  count: number | string;
}

interface ValidationResult {
  errors: ValidationError[];
  reconciliation?: ReconciliationError[];
  protocol?: string;
  summary: {
    totalChecks: number;
    passed: number;
    failed: number;
  };
}

function App() {
  const [projects, setProjects] = useState<Project[]>([]);
  const [selectedProject, setSelectedProject] = useState<Project | null>(null);
  const [tables, setTables] = useState<TableData[]>([]);
  const [newProjectName, setNewProjectName] = useState('');
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [statusText, setStatusText] = useState('');
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const [hideEmptyColumns, setHideEmptyColumns] = useState(false);
  const [showLinkedOnly, setShowLinkedOnly] = useState(false);
  const [uploadType, setUploadType] = useState<'SOURCE' | 'TARGET' | 'FORBIDDEN' | 'RANGE'>('TARGET');
  const [linkSuggestions, setLinkSuggestions] = useState<LinkSuggestion[]>([]);
  const [selectedSuggestionIds, setSelectedSuggestionIds] = useState<Set<string>>(new Set());
  const [detecting, setDetecting] = useState(false);
  const [validating, setValidating] = useState(false);
  const [validationResult, setValidationResult] = useState<ValidationResult | null>(null);

  // Load projects
  const loadProjects = useCallback(async () => {
    const res = await fetch(`${API_URL}/projects`);
    const data = await res.json();
    setProjects(data);
  }, []);

  // Load project tables
  const loadTables = useCallback(async (projectId: string) => {
    const res = await fetch(`${API_URL}/projects/${projectId}/tables`);
    const data = await res.json();
    setTables(data);
  }, []);

  useEffect(() => {
    loadProjects();
  }, [loadProjects]);

  useEffect(() => {
    if (selectedProject) {
      loadTables(selectedProject.id);
      setLinkSuggestions([]); // Clear suggestions when switching projects
    }
  }, [selectedProject, loadTables]);

  // Create project
  const createProject = async () => {
    if (!newProjectName.trim()) return;

    const res = await fetch(`${API_URL}/projects`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: newProjectName })
    });
    const project = await res.json();
    setProjects([project, ...projects]);
    setNewProjectName('');
    setSelectedProject(project);
  };

  // Delete project
  const deleteProject = async (e: React.MouseEvent, projectId: string) => {
    e.stopPropagation();
    if (!confirm('Opravdu chcete smazat tento projekt? Všechna data budou ztracena.')) return;

    setDeletingId(projectId);
    try {
      const res = await fetch(`${API_URL}/projects/${projectId}`, { method: 'DELETE' });
      if (res.ok) {
        setProjects(prev => prev.filter(p => p.id !== projectId));
        if (selectedProject?.id === projectId) {
          setSelectedProject(null);
          setTables([]);
        }
      }
    } catch (err) {
      alert('Failed to delete project');
    } finally {
      setDeletingId(null);
    }
  };

  // Upload files (supports multiple) with Progress
  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!e.target.files?.length || !selectedProject) return;

    setUploading(true);
    setUploadProgress(0);
    // Filter only valid Excel/CSV files
    const files = Array.from(e.target.files).filter(f => f.name.match(/\.(xlsx|xls|csv)$/i));
    const totalFiles = files.length;

    if (totalFiles === 0) {
      alert('Nebyly nalezeny žádné podporované soubory (xlsx, xls, csv).');
      setUploading(false);
      e.target.value = '';
      return;
    }

    const uploadSingleFile = (file: File, index: number, total: number, allowOverwrite = false): Promise<void> => {
      return new Promise((resolve) => {
        const formData = new FormData();
        formData.append('file', file);
        formData.append('tableType', uploadType);

        const url = `${API_URL}/projects/${selectedProject.id}/upload` + (allowOverwrite ? '?overwrite=true' : '');

        const xhr = new XMLHttpRequest();
        xhr.open('POST', url);

        xhr.upload.onprogress = (event) => {
          if (event.lengthComputable) {
            const percent = Math.round((event.loaded / event.total) * 100);
            setUploadProgress(percent);
            if (percent < 100) {
              setStatusText(`Nahrávám ${index}/${total}: ${file.name}`);
            } else {
              setStatusText(`Zpracovávám ${index}/${total}: ${file.name}...`);
            }
          }
        };

        xhr.onload = () => {
          if (xhr.status === 409) {
            try {
              const data = JSON.parse(xhr.responseText);
              if (confirm(`Tabulka '${data.tableName}' již existuje. Chcete ji přepsat? Stará data budou smazána.`)) {
                resolve(uploadSingleFile(file, index, total, true));
              } else {
                resolve();
              }
            } catch { resolve(); }
            return;
          }

          if (xhr.status >= 200 && xhr.status < 300) {
            resolve();
          } else {
            try {
              const res = JSON.parse(xhr.responseText);
              alert(`Error uploading ${file.name}: ${res.error}`);
            } catch {
              alert(`Error uploading ${file.name}: Server Error ${xhr.status}`);
            }
            resolve();
          }
        };

        xhr.onerror = () => {
          alert(`Network error uploading ${file.name}`);
          resolve();
        };

        xhr.send(formData);
      });
    };

    try {
      for (let i = 0; i < totalFiles; i++) {
        const file = files[i];
        setStatusText(`Nahrávám ${i + 1}/${totalFiles}: ${file.name}`);
        setUploadProgress(0);
        await uploadSingleFile(file, i + 1, totalFiles);
      }
      await loadTables(selectedProject.id);
      loadProjects();
    } finally {
      setUploading(false);
      setUploadProgress(0);
      setStatusText('');
      e.target.value = '';
    }
  };

  // Detect links automatically
  const detectLinks = async () => {
    if (!selectedProject) return;

    setDetecting(true);
    try {
      const res = await fetch(`${API_URL}/projects/${selectedProject.id}/detect-links`, {
        method: 'POST'
      });
      const data = await res.json();

      if (data.success) {
        setLinkSuggestions(data.suggestions);

        // Count new suggestions for alert
        const activeLinks = new Set<string>();
        tables.forEach(t => t.columns.forEach(c => {
          if (c.linkedToColumnId) activeLinks.add(`${c.id}|${c.linkedToColumnId}`);
        }));

        const newCount = data.suggestions.filter((s: LinkSuggestion) =>
          !activeLinks.has(`${s.sourceColumnId}|${s.targetColumnId}`)
        ).length;

        if (newCount === 0 && data.suggestions.length > 0) {
          alert('Všechny nalezené vazby jsou již nastaveny.');
        } else if (data.suggestions.length === 0) {
          alert('Nebyly nalezeny žádné nové vazby.');
        }
      }
    } catch (err) {
      alert('Detection failed: ' + (err as Error).message);
    } finally {
      setDetecting(false);
    }
  };

  // Helpers for selection
  const getSuggestionKey = (s: LinkSuggestion) => `${s.sourceColumnId}-${s.targetColumnId}`;

  const toggleSelection = (s: LinkSuggestion) => {
    const key = getSuggestionKey(s);
    const next = new Set(selectedSuggestionIds);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    setSelectedSuggestionIds(next);
  };

  const toggleSelectAll = () => {
    if (selectedSuggestionIds.size === linkSuggestions.length) {
      setSelectedSuggestionIds(new Set());
    } else {
      setSelectedSuggestionIds(new Set(linkSuggestions.map(getSuggestionKey)));
    }
  };

  const applySelectedSuggestions = async () => {
    const toApply = linkSuggestions.filter(s => selectedSuggestionIds.has(getSuggestionKey(s)));
    if (toApply.length === 0) return;

    for (const s of toApply) {
      await applyLink(s, true); // Skip reload for individual items
    }

    if (selectedProject) loadTables(selectedProject.id);
    setSelectedSuggestionIds(new Set());
  };

  // Apply link suggestion
  const applyLink = async (suggestion: LinkSuggestion, skipReload = false) => {
    await fetch(`${API_URL}/columns/${suggestion.sourceColumnId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ linkedToColumnId: suggestion.targetColumnId })
    });

    // Mark target as primary key if not already
    await fetch(`${API_URL}/columns/${suggestion.targetColumnId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ isPrimaryKey: true })
    });

    if (!skipReload && selectedProject) loadTables(selectedProject.id);

    // Remove applied suggestion
    setLinkSuggestions(prev => prev.filter(s => s.sourceColumnId !== suggestion.sourceColumnId));
  };

  // Toggle primary key
  const togglePrimaryKey = async (columnId: string, current: boolean) => {
    await fetch(`${API_URL}/columns/${columnId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ isPrimaryKey: !current })
    });
    if (selectedProject) loadTables(selectedProject.id);
  };

  // Set link between columns
  const setColumnLink = async (columnId: string, linkedToColumnId: string | null) => {
    await fetch(`${API_URL}/columns/${columnId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ linkedToColumnId })
    });
    if (selectedProject) loadTables(selectedProject.id);
  };

  // Get all primary key columns for linking
  const getPrimaryKeyColumns = () => {
    return tables
      .flatMap(t => t.columns.filter(c => c.isPrimaryKey))
      .map(c => ({
        id: c.id,
        label: `${tables.find(t => t.columns.includes(c))?.tableName}.${c.columnName}`
      }));
  };

  // Validate FK integrity
  const validateProject = async () => {
    if (!selectedProject) return;

    setValidating(true);
    try {
      const res = await fetch(`${API_URL}/projects/${selectedProject.id}/validate`, {
        method: 'POST'
      });
      const data = await res.json();

      if (data.success) {
        setValidationResult(data);
      }
    } catch (err) {
      alert('Validation failed: ' + (err as Error).message);
    } finally {
      setValidating(false);
    }
  };

  return (
    <div className="app">
      <header className="header">
        <div className="logo-section">
          <img src="/logo.png" alt="Valibook" className="app-logo" />
          <h1>Valibook</h1>
        </div>
        <p>Excel Validation Tool</p>
      </header>

      <div className="main-layout">
        {/* Sidebar - Projects */}
        <aside className="sidebar">
          <h2>Projekty</h2>

          <div className="new-project">
            <input
              type="text"
              placeholder="Název nového projektu..."
              value={newProjectName}
              onChange={(e) => setNewProjectName(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && createProject()}
            />
            <button onClick={createProject}>+</button>
          </div>

          <ul className="project-list">
            {projects.map(p => (
              <li
                key={p.id}
                className={selectedProject?.id === p.id ? 'active' : ''}
                onClick={() => setSelectedProject(p)}
              >
                <div style={{ flex: 1 }}>
                  <span className="project-name">{p.name}</span>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                  <span className="project-count">{p._count?.columns || 0}</span>
                  <button
                    className="delete-project-btn"
                    onClick={(e) => deleteProject(e, p.id)}
                    title="Smazat projekt"
                    disabled={deletingId === p.id}
                  >
                    {deletingId === p.id ? '⏳' : '🗑️'}
                  </button>
                </div>
              </li>
            ))}
          </ul>
        </aside>

        {/* Main content */}
        <main className="content">
          {!selectedProject ? (
            <div className="empty-state">
              <h2>Vyberte nebo vytvořte projekt</h2>
              <p>Začněte vytvořením nového validačního projektu v levém panelu.</p>
            </div>
          ) : (
            <>
              <div className="project-header">
                <h2>{selectedProject.name}</h2>

                <div className="upload-section">
                  <select
                    value={uploadType}
                    onChange={(e) => setUploadType(e.target.value as any)}
                  >
                    <option value="SOURCE">📗 Zdrojová tabulka</option>
                    <option value="TARGET">📕 Kontrolovaná tabulka</option>
                    <option value="FORBIDDEN">⛔ Zakázané hodnoty</option>
                    <option value="RANGE">🔢 Rozsah hodnot</option>
                  </select>

                  <div style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
                    <div style={{ display: 'flex', gap: '10px' }}>
                      <label className={`upload-btn ${uploading ? 'disabled' : ''}`} style={{ textAlign: 'center', position: 'relative', minWidth: '160px' }}>
                        {uploading ? (
                          <span style={{ fontSize: '0.85rem' }}>{statusText} <br /> ({uploadProgress}%)</span>
                        ) : '📁 Soubory'}
                        <input
                          type="file"
                          accept=".xlsx,.xls,.csv"
                          multiple
                          onChange={handleFileUpload}
                          disabled={uploading}
                        />
                      </label>

                      <label className={`upload-btn ${uploading ? 'disabled' : ''}`} style={{ textAlign: 'center', position: 'relative', minWidth: '160px' }}>
                        {uploading ? '...' : '📂 Složka'}
                        <input
                          type="file"
                          // @ts-ignore
                          webkitdirectory=""
                          // @ts-ignore
                          directory=""
                          multiple
                          onChange={handleFileUpload}
                          disabled={uploading}
                        />
                      </label>
                    </div>
                    {uploading && (
                      <div style={{ width: '100%', height: '4px', background: '#e5e7eb', borderRadius: '2px', overflow: 'hidden' }}>
                        <div style={{ width: `${uploadProgress}%`, height: '100%', background: '#3b82f6', transition: 'width 0.2s ease-out' }}></div>
                      </div>
                    )}
                  </div>

                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginLeft: '1rem' }}>
                    <input
                      type="checkbox"
                      id="hideEmpty"
                      checked={hideEmptyColumns}
                      onChange={(e) => setHideEmptyColumns(e.target.checked)}
                    />
                    <label htmlFor="hideEmpty" style={{ fontSize: '0.9rem', cursor: 'pointer', userSelect: 'none' }}>
                      Skrýt prázdné
                    </label>
                  </div>

                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <input
                      type="checkbox"
                      id="showLinked"
                      checked={showLinkedOnly}
                      onChange={(e) => setShowLinkedOnly(e.target.checked)}
                    />
                    <label htmlFor="showLinked" style={{ fontSize: '0.9rem', cursor: 'pointer', userSelect: 'none' }}>
                      Jen s vazbou
                    </label>
                  </div>

                  {tables.length > 0 && (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                      <div style={{ display: 'flex', gap: '10px' }}>
                        <button
                          className="detect-btn"
                          onClick={detectLinks}
                          disabled={detecting}
                        >
                          {detecting ? '🔍 Hledám...' : '🔍 Najít vazby'}
                        </button>
                        <button
                          className="validate-btn"
                          onClick={validateProject}
                          disabled={validating}
                        >
                          {validating ? '⏳ Validuji...' : '✓ Validovat'}
                        </button>
                      </div>
                      {(detecting || validating) && (
                        <div style={{ width: '100%', maxWidth: '300px' }}>
                          <div style={{
                            fontSize: '0.8rem',
                            color: '#666',
                            marginBottom: '4px'
                          }}>
                            {detecting && 'Analyzuji vzorky dat...'}
                            {validating && 'Kontroluji integritu a zakázané hodnoty...'}
                          </div>
                          <div style={{
                            width: '100%',
                            height: '6px',
                            background: '#e5e7eb',
                            borderRadius: '3px',
                            overflow: 'hidden'
                          }}>
                            <div style={{
                              width: '30%',
                              height: '100%',
                              background: 'linear-gradient(90deg, #3b82f6, #8b5cf6, #3b82f6)',
                              backgroundSize: '200% 100%',
                              animation: 'shimmer 1.5s infinite linear',
                              borderRadius: '3px'
                            }}></div>
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </div>

              {/* Validation Results */}
              {validationResult && (
                <div className={`validation-panel ${validationResult.summary.failed > 0 ? 'has-errors' : 'all-passed'}`}>
                  <div className="validation-header">
                    <h3>
                      {validationResult.summary.failed > 0 ? '❌ Nalezeny chyby' : '✅ Validace úspěšná'}
                    </h3>
                    <div className="validation-summary">
                      <span className="check-count">Kontrol: {validationResult.summary.totalChecks}</span>
                      <span className="passed-count">✓ {validationResult.summary.passed}</span>
                      <span className="failed-count">✗ {validationResult.summary.failed}</span>
                    </div>
                    <button className="close-btn" onClick={() => setValidationResult(null)}>×</button>
                  </div>

                  {validationResult.protocol && (
                    <pre className="validation-protocol">
                      {validationResult.protocol}
                    </pre>
                  )}

                  {validationResult.errors.length > 0 && (
                    <div className="validation-errors">
                      {validationResult.errors.map((err, i) => (
                        <div key={i} className="error-item">
                          <div className="error-header">
                            <strong>{err.fkTable}.{err.fkColumn}</strong>
                            <span className="arrow">→</span>
                            <strong>{err.pkTable}.{err.pkColumn}</strong>
                            <span className="error-count">{err.missingCount} chybějících hodnot</span>
                          </div>
                          <div className="missing-values">
                            Chybí: {err.missingValues.join(', ')}
                            {err.missingCount > 10 && ` ... a dalších ${err.missingCount - 10}`}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}

                  {/* Reconciliation Errors */}
                  {validationResult.reconciliation && validationResult.reconciliation.length > 0 && (
                    <div className="validation-errors" style={{ marginTop: '1rem' }}>
                      <h4 style={{ margin: '0 0 0.5rem 0', color: '#d97706' }}>⚠️ Neshody v datech (Reconciliation)</h4>
                      {validationResult.reconciliation.map((err, i) => (
                        <div key={i} className="error-item warning">
                          <div className="error-header">
                            <strong>{err.sourceTable}.{err.sourceColumn}</strong>
                            <span className="arrow">≠</span>
                            <strong>{err.targetTable}.{err.targetColumn}</strong>
                            <span className="error-count" style={{ background: '#fef3c7', color: '#d97706', borderColor: '#fcd34d' }}>{err.count} neshod</span>
                          </div>
                          <div className="missing-values">
                            <div style={{ marginBottom: '0.5rem', fontWeight: 600, fontSize: '0.75rem' }}>Spojeno přes klíč: {err.joinKey}</div>
                            <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
                              {err.mismatches.map((m, j) => (
                                <li key={j} style={{ borderBottom: '1px solid #eee', padding: '2px 0' }}>
                                  <span style={{ color: '#6b7280' }}>[{m.key}]</span>: <span style={{ color: '#ef4444' }}>"{m.source}"</span> vs <span style={{ color: '#10b981' }}>"{m.target}"</span>
                                </li>
                              ))}
                            </ul>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}

              {/* Link Suggestions */}
              {linkSuggestions.length > 0 && (
                <div className="suggestions-panel">
                  <div className="suggestions-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
                    <h3 style={{ margin: 0 }}>🔗 Navrhované vazby</h3>
                    <div style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
                      <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer', fontSize: '0.9rem' }}>
                        <input
                          type="checkbox"
                          checked={linkSuggestions.length > 0 && selectedSuggestionIds.size === linkSuggestions.length}
                          onChange={toggleSelectAll}
                        />
                        Vybrat vše
                      </label>
                      {selectedSuggestionIds.size > 0 && (
                        <button className="apply-btn" onClick={applySelectedSuggestions}>
                          Použít vybrané ({selectedSuggestionIds.size})
                        </button>
                      )}
                    </div>
                  </div>

                  <div className="suggestions-list">
                    <table className="suggestions-table" style={{ width: '100%', borderCollapse: 'collapse' }}>
                      <thead>
                        <tr>
                          <th style={{ width: '40px' }}>
                            <input
                              type="checkbox"
                              checked={linkSuggestions.length > 0 && selectedSuggestionIds.size === linkSuggestions.length}
                              onChange={toggleSelectAll}
                            />
                          </th>
                          <th>Zdrojový sloupec</th>
                          <th>Cílový sloupec</th>
                          <th>Shoda</th>
                          <th>Akce</th>
                        </tr>
                      </thead>
                      <tbody>
                        {linkSuggestions.filter(s => !tables.some(t => t.columns.some(c => c.id === s.sourceColumnId && c.linkedToColumnId === s.targetColumnId))).map((s, i) => (
                          <tr key={i} className="suggestion-item">
                            <td>
                              <input
                                type="checkbox"
                                checked={selectedSuggestionIds.has(getSuggestionKey(s))}
                                onChange={() => toggleSelection(s)}
                              />
                            </td>
                            <td><strong>{s.sourceTable}</strong>.<br />{s.sourceColumn}</td>
                            <td><strong>{s.targetTable}</strong>.<br />{s.targetColumn}</td>
                            <td>
                              <span className="match-badge">{s.matchPercentage}%</span>
                              <div style={{ fontSize: '0.75rem', color: '#666' }}>({s.commonValues} shod)</div>
                            </td>
                            <td>
                              <button className="apply-btn" onClick={() => applyLink(s)}>
                                ✓ Použít
                              </button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {tables.length === 0 ? (
                <div className="empty-state">
                  <p>Žádné tabulky. Nahrajte Excel soubor.</p>
                </div>
              ) : (
                <div className="tables-grid">
                  {tables.map(table => (
                    <div key={table.tableName} className={`table-card ${table.tableType.toLowerCase()}`}>
                      <div className="table-header">
                        <span className="table-type-badge">
                          {table.tableType === 'SOURCE' && '📗 Zdroj'}
                          {table.tableType === 'TARGET' && '📕 Kontrola'}
                        </span>
                        <h3>{table.tableName}</h3>
                        <span className="row-count">{table.rowCount} řádků</span>
                      </div>

                      <table className="columns-table">
                        <thead>
                          <tr>
                            <th>Sloupec</th>
                            <th>PK</th>
                            <th>Unikát</th>
                            <th>Prázdné</th>
                            <th>Vzorky</th>
                            <th>Vazba →</th>
                          </tr>
                        </thead>
                        <tbody>
                          {table.columns
                            .filter(col => (!hideEmptyColumns || (col.uniqueCount ?? 0) > 0) && (!showLinkedOnly || col.linkedToColumnId))
                            .map(col => (
                              <tr key={col.id}>
                                <td className="col-name">{col.columnName}</td>
                                <td>
                                  <button
                                    className={`pk-btn ${col.isPrimaryKey ? 'active' : ''}`}
                                    onClick={() => togglePrimaryKey(col.id, col.isPrimaryKey)}
                                    title="Primární klíč"
                                  >
                                    🔑
                                  </button>
                                </td>
                                <td className="stat">{col.uniqueCount}</td>
                                <td className="stat">{col.nullCount}</td>
                                <td className="samples">
                                  {col.sampleValues?.slice(0, 3).join(', ')}
                                  {(col.sampleValues?.length || 0) > 3 && '...'}
                                </td>
                                <td>
                                  <select
                                    value={col.linkedToColumnId || ''}
                                    onChange={(e) => setColumnLink(col.id, e.target.value || null)}
                                    style={{
                                      border: (linkSuggestions.some(s => s.sourceColumnId === col.id) && !col.linkedToColumnId) ? '2px solid #3b82f6' : '1px solid #e2e8f0'
                                    }}
                                  >
                                    <option value="">—</option>
                                    {(() => {
                                      const relevantSuggestions = linkSuggestions.filter(s => s.sourceColumnId === col.id);
                                      const suggestedTargets = new Set(relevantSuggestions.map(s => s.targetColumnId));
                                      const allPks = getPrimaryKeyColumns().filter(pk => pk.id !== col.id);

                                      const suggested = allPks.filter(pk => suggestedTargets.has(pk.id));
                                      const others = allPks.filter(pk => !suggestedTargets.has(pk.id));

                                      return (
                                        <>
                                          {suggested.length > 0 && (
                                            <optgroup label="✨ Doporučené">
                                              {suggested.map(pk => {
                                                const score = relevantSuggestions.find(s => s.targetColumnId === pk.id)?.matchPercentage;
                                                return (
                                                  <option key={pk.id} value={pk.id}>
                                                    {pk.label} ({score}%)
                                                  </option>
                                                );
                                              })}
                                            </optgroup>
                                          )}
                                          <optgroup label="Ostatní">
                                            {others.map(pk => (
                                              <option key={pk.id} value={pk.id}>{pk.label}</option>
                                            ))}
                                          </optgroup>
                                        </>
                                      );
                                    })()}
                                  </select>
                                </td>
                              </tr>
                            ))}
                        </tbody>
                      </table>
                    </div>
                  ))}
                </div>
              )}
            </>
          )}
        </main>
      </div>
    </div>
  );
}

export default App;
