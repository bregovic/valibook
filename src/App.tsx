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
  tableType: 'SOURCE' | 'TARGET' | 'CODEBOOK';
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

function App() {
  const [projects, setProjects] = useState<Project[]>([]);
  const [selectedProject, setSelectedProject] = useState<Project | null>(null);
  const [tables, setTables] = useState<TableData[]>([]);
  const [newProjectName, setNewProjectName] = useState('');
  const [uploading, setUploading] = useState(false);
  const [uploadType, setUploadType] = useState<'SOURCE' | 'TARGET' | 'CODEBOOK'>('SOURCE');

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

  // Upload file
  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!e.target.files?.[0] || !selectedProject) return;

    setUploading(true);
    const formData = new FormData();
    formData.append('file', e.target.files[0]);
    formData.append('tableType', uploadType);

    try {
      const res = await fetch(`${API_URL}/projects/${selectedProject.id}/upload`, {
        method: 'POST',
        body: formData
      });
      const result = await res.json();

      if (result.success) {
        loadTables(selectedProject.id);
        loadProjects();
      } else {
        alert('Error: ' + result.error);
      }
    } catch (err) {
      alert('Upload failed: ' + (err as Error).message);
    } finally {
      setUploading(false);
      e.target.value = '';
    }
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
                    <option value="CODEBOOK">📘 Číselník</option>
                  </select>

                  <label className="upload-btn">
                    {uploading ? 'Nahrávám...' : '📁 Nahrát Excel'}
                    <input
                      type="file"
                      accept=".xlsx,.xls,.csv"
                      onChange={handleFileUpload}
                      disabled={uploading}
                    />
                  </label>
                </div>
              </div>

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
                          {table.tableType === 'CODEBOOK' && '📘 Číselník'}
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
