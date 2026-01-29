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
  tableType: 'SOURCE' | 'TARGET';
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

interface ValidationResult {
  errors: ValidationError[];
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
  const [uploadType, setUploadType] = useState<'SOURCE' | 'TARGET'>('SOURCE');
  const [linkSuggestions, setLinkSuggestions] = useState<LinkSuggestion[]>([]);
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

  // Upload files (supports multiple)
  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!e.target.files?.length || !selectedProject) return;

    setUploading(true);
    const files = Array.from(e.target.files);

    try {
      for (const file of files) {
        const formData = new FormData();
        formData.append('file', file);
        formData.append('tableType', uploadType);

        const res = await fetch(`${API_URL}/projects/${selectedProject.id}/upload`, {
          method: 'POST',
          body: formData
        });
        const result = await res.json();

        if (!result.success) {
          alert(`Error uploading ${file.name}: ${result.error}`);
        }
      }

      loadTables(selectedProject.id);
      loadProjects();
    } catch (err) {
      alert('Upload failed: ' + (err as Error).message);
    } finally {
      setUploading(false);
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
      }
    } catch (err) {
      alert('Detection failed: ' + (err as Error).message);
    } finally {
      setDetecting(false);
    }
  };

  // Apply link suggestion
  const applyLink = async (suggestion: LinkSuggestion) => {
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

    if (selectedProject) loadTables(selectedProject.id);

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
        <h1>📊 Valibook</h1>
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
                <span className="project-name">{p.name}</span>
                <span className="project-count">{p._count?.columns || 0} sloupců</span>
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
                  </select>

                  <label className="upload-btn">
                    {uploading ? 'Nahrávám...' : '📁 Nahrát Excel'}
                    <input
                      type="file"
                      accept=".xlsx,.xls,.csv"
                      multiple
                      onChange={handleFileUpload}
                      disabled={uploading}
                    />
                  </label>

                  {tables.length > 0 && (
                    <>
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
                    </>
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
                </div>
              )}

              {/* Link Suggestions */}
              {linkSuggestions.length > 0 && (
                <div className="suggestions-panel">
                  <h3>🔗 Navrhované vazby</h3>
                  <div className="suggestions-list">
                    {linkSuggestions.map((s, i) => (
                      <div key={i} className="suggestion-item">
                        <span className="suggestion-text">
                          <strong>{s.sourceTable}.{s.sourceColumn}</strong>
                          <span className="arrow">→</span>
                          <strong>{s.targetTable}.{s.targetColumn}</strong>
                        </span>
                        <span className="match-badge">{s.matchPercentage}% shoda</span>
                        <button className="apply-btn" onClick={() => applyLink(s)}>
                          ✓ Použít
                        </button>
                      </div>
                    ))}
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
                          {table.columns.map(col => (
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
                                  disabled={col.isPrimaryKey}
                                >
                                  <option value="">—</option>
                                  {getPrimaryKeyColumns()
                                    .filter(pk => pk.id !== col.id)
                                    .map(pk => (
                                      <option key={pk.id} value={pk.id}>{pk.label}</option>
                                    ))
                                  }
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
