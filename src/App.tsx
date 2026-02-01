import { useState, useEffect, useCallback } from 'react';
import './App.css';
import VisualMapperModal from './VisualMapperModal';

const API_URL = import.meta.env.DEV ? 'http://localhost:3001/api' : '/api';

interface Column {
  id: string;
  columnName: string;
  columnIndex: number;
  isPrimaryKey: boolean;
  isValidationRange: boolean;
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

interface ForbiddenError {
  targetTable: string;
  column: string;
  forbiddenTable: string;
  forbiddenColumn: string;
  foundValues: string[];
  count: number;
}

interface AIRuleError {
  ruleId: string;
  table: string;
  column: string;
  ruleType: string;
  description: string;
  failedCount: number;
  samples: string[];
}

interface ValidationResult {
  errors: ValidationError[];
  reconciliation?: ReconciliationError[];
  forbidden?: ForbiddenError[];
  validationRules?: AIRuleError[];
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
  const [activeValidationTab, setActiveValidationTab] = useState<'SUMMARY' | 'INTEGRITY' | 'FORBIDDEN' | 'RULES' | 'RECONCILE' | 'PROTOCOL'>('SUMMARY');
  const [validationSearchTerm, setValidationSearchTerm] = useState('');
  const [manualLinkTarget, setManualLinkTarget] = useState<{ table: string; column: string } | null>(null);
  const [showManualLinkModal, setShowManualLinkModal] = useState(false);
  const [manualLinkSource, setManualLinkSource] = useState<{ table: string; column: string } | null>(null);
  const [showRuleFailureModal, setShowRuleFailureModal] = useState(false);
  const [selectedRuleFailures, setSelectedRuleFailures] = useState<any[]>([]);
  const [loadingFailures, setLoadingFailures] = useState(false);
  const [activeRuleTitle, setActiveRuleTitle] = useState('');
  const [selectedRuleIds, setSelectedRuleIds] = useState<Set<string>>(new Set());

  // AI States
  const [showAIModal, setShowAIModal] = useState(false);
  /* REMOVED UNUSED apiKey STATE */
  const [aiPassword, setAiPassword] = useState('Heslo123');
  const [aiResult, setAiResult] = useState('');
  const [generatingAI, setGeneratingAI] = useState(false);

  // System Config
  const [showSettingsModal, setShowSettingsModal] = useState(false);
  const [showVisualMapper, setShowVisualMapper] = useState(false);
  const [hasSystemKey, setHasSystemKey] = useState(false);
  const [newSystemKey, setNewSystemKey] = useState('');

  const checkConfig = useCallback(async () => {
    try {
      const res = await fetch(`${API_URL}/config`);
      const data = await res.json();
      setHasSystemKey(data.hasOpenAIKey);
    } catch (e) { console.error(e); }
  }, []);

  useEffect(() => { checkConfig(); }, [checkConfig]);

  const saveSettings = async () => {
    try {
      const res = await fetch(`${API_URL}/config`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key: 'OPENAI_API_KEY', value: newSystemKey, password: aiPassword })
      });
      const data = await res.json();
      if (data.success) {
        alert('Nastavení uloženo.');
        setShowSettingsModal(false);
        setNewSystemKey('');
        checkConfig();
      } else {
        alert('Chyba: ' + data.error);
      }
    } catch (e) { alert('Chyba spojení.'); }
  };

  // Generate Rules Logic
  const generateAIRules = async () => {
    if (!selectedProject || tables.length === 0) return;
    setGeneratingAI(true);
    setAiResult('Příprava analýzy...');

    try {
      const tableNames = tables.map((t: TableData) => t.tableName);
      const BATCH_SIZE = 1; // Safest possible batch size for poor connections/slow API
      let totalCreated = 0;

      for (let i = 0; i < tableNames.length; i += BATCH_SIZE) {
        const subset = tableNames.slice(i, i + BATCH_SIZE);
        setAiResult(`Analyzuji tabulky ${i + 1} až ${Math.min(i + BATCH_SIZE, tableNames.length)} z ${tableNames.length}...`);

        const res = await fetch(`${API_URL}/projects/${selectedProject.id}/ai-suggest-rules`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            password: aiPassword,
            subsetTableNames: subset
          })
        });

        if (!res.ok) {
          const errData = await res.json();
          throw new Error(errData.error || `Chyba při zpracování dávky ${i / BATCH_SIZE + 1}`);
        }

        const data = await res.json();
        totalCreated += data.count || 0;
      }

      setAiResult(`✅ Úspěch! Vygenerováno celkem ${totalCreated} pravidel.`);
      setTimeout(() => {
        setShowAIModal(false);
        setAiResult('');
        alert(`Pravidla byla vygenerována (${totalCreated}). Spusťte prosím VALIDACI pro kontrolu.`);
      }, 3000);

    } catch (err) {
      console.error(err);
      setAiResult(`❌ Chyba: ${(err as Error).message}`);
    } finally {
      setGeneratingAI(false);
    }
  };

  const deleteRule = async (id: string) => {
    if (!id || !confirm('Opravdu chcete toto pravidlo smazat?')) return;
    try {
      const res = await fetch(`${API_URL}/rules/${id}`, { method: 'DELETE' });
      const data = await res.json();
      if (data.success) {
        // Update local state to remove the rule block
        if (validationResult) {
          setValidationResult({
            ...validationResult,
            validationRules: validationResult.validationRules?.filter(r => r.ruleId !== id)
          });
        }
      }
    } catch (e) { alert('Chyba při mazání.'); }
  };

  const fetchRuleFailures = async (rule: AIRuleError) => {
    setActiveRuleTitle(`${rule.table}.${rule.column} (${rule.ruleType})`);
    setLoadingFailures(true);
    setShowRuleFailureModal(true);
    setSelectedRuleFailures([]);
    try {
      const res = await fetch(`${API_URL}/rules/${rule.ruleId}/failures`);
      const data = await res.json();
      setSelectedRuleFailures(data.failures || []);
    } catch (e) { console.error(e); }
    setLoadingFailures(false);
  };

  const deleteSelectedRules = async () => {
    if (selectedRuleIds.size === 0 || !confirm(`Opravdu chcete smazat ${selectedRuleIds.size} vybraných pravidel?`)) return;
    try {
      const res = await fetch(`${API_URL}/rules/bulk-delete`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ids: Array.from(selectedRuleIds) })
      });
      const data = await res.json();
      if (data.success) {
        if (validationResult) {
          setValidationResult({
            ...validationResult,
            validationRules: validationResult.validationRules?.filter(r => !selectedRuleIds.has(r.ruleId))
          });
        }
        setSelectedRuleIds(new Set());
      }
    } catch (e) { alert('Chyba při mazání.'); }
  };

  const removeColumnLink = async (tableName: string, columnName: string) => {
    if (!selectedProject || !confirm(`Opravdu chcete zrušit vazbu pro ${tableName}.${columnName}?`)) return;
    try {
      const table = tables.find(t => t.tableName === tableName);
      const col = table?.columns.find(c => c.columnName === columnName);
      if (!col) return;

      const res = await fetch(`${API_URL}/columns/${col.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ linkedToColumnId: null })
      });
      if (res.ok) {
        alert('Vazba byla zrušena. Spusťte prosím znovu VALIDACI.');
        loadTables(selectedProject.id);
      }
    } catch (e) { alert('Chyba při rušení vazby.'); }
  };

  const deleteForbiddenTable = async (tableName: string) => {
    if (!selectedProject || !confirm(`Opravdu chcete smazat tabulku zakázaných hodnot: ${tableName}?`)) return;
    try {
      const res = await fetch(`${API_URL}/projects/${selectedProject.id}/tables/${tableName}`, { method: 'DELETE' });
      const data = await res.json();
      if (data.success) {
        alert('Tabulka byla smazána.');
        loadTables(selectedProject.id);
      }
    } catch (e) { alert('Chyba při mazání tabulky.'); }
  };

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
      setValidationResult(null); // Clear validation result when switching projects
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
  const detectLinks = async (mode: 'KEYS' | 'VALUES') => {
    if (!selectedProject) return;

    setDetecting(true);
    try {
      const res = await fetch(`${API_URL}/projects/${selectedProject.id}/detect-links?mode=${mode}`, {
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
    // 1. Apply the main link
    await fetch(`${API_URL}/columns/${suggestion.sourceColumnId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ linkedToColumnId: suggestion.targetColumnId })
    });

    // Mark target as primary key IF it looks like a key (high match) - heuristic
    // Or just strictly if the suggestion came from 'KEYS' mode? We don't track mode.
    // We'll trust that if it's 90%+ match, it's likely a key context or accurate value.
    if (suggestion.matchPercentage >= 90) {
      await fetch(`${API_URL}/columns/${suggestion.targetColumnId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ isPrimaryKey: true })
      });
    }

    // 2. AUTO-CASCADE: Find other suggestions between the SAME tables and apply them automatically
    // This solves the user's issue: "I linked the key, but other columns didn't link automatically"
    const cascadeLinks = linkSuggestions.filter(s =>
      s.sourceTable === suggestion.sourceTable &&
      s.targetTable === suggestion.targetTable &&
      s.sourceColumnId !== suggestion.sourceColumnId // Don't re-apply self
    );

    for (const cascade of cascadeLinks) {
      await fetch(`${API_URL}/columns/${cascade.sourceColumnId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ linkedToColumnId: cascade.targetColumnId })
      });
    }

    if (!skipReload && selectedProject) loadTables(selectedProject.id);

    // Remove applied suggestion AND cascaded suggestions
    const appliedIds = new Set([suggestion.sourceColumnId, ...cascadeLinks.map(c => c.sourceColumnId)]);
    setLinkSuggestions(prev => prev.filter(s => !appliedIds.has(s.sourceColumnId)));
  };

  // Toggle validation range
  const toggleValidationRange = async (columnId: string, current: boolean) => {
    await fetch(`${API_URL}/columns/${columnId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ isValidationRange: !current })
    });
    if (selectedProject) loadTables(selectedProject.id);
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

  // Get all columns for manual linking
  const getAllColumns = () => {
    return tables.flatMap(t =>
      t.columns.map(c => ({
        id: c.id,
        tableName: t.tableName,
        columnName: c.columnName,
        label: `${t.tableName}.${c.columnName}`
      }))
    );
  };

  const [manualLinkType, setManualLinkType] = useState<'KEY' | 'VALUE'>('KEY');

  // Create manual link
  const createManualLink = async () => {
    if (!manualLinkSource || !manualLinkTarget) return;

    // Find source and target column IDs
    const allCols = getAllColumns();
    const sourceCol = allCols.find(c =>
      c.tableName === manualLinkSource.table && c.columnName === manualLinkSource.column
    );
    const targetCol = allCols.find(c =>
      c.tableName === manualLinkTarget.table && c.columnName === manualLinkTarget.column
    );

    if (!sourceCol || !targetCol) {
      alert('Sloupec nenalezen');
      return;
    }

    await setColumnLink(sourceCol.id, targetCol.id);

    // Only mark target as PK if user selected 'Referenční klíč'
    if (manualLinkType === 'KEY') {
      await fetch(`${API_URL}/columns/${targetCol.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ isPrimaryKey: true })
      });
    } else {
      // Ensure it is NOT primary key if it's a value check
      await fetch(`${API_URL}/columns/${targetCol.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ isPrimaryKey: false })
      });
    }

    setShowManualLinkModal(false);
    setManualLinkSource(null);
    setManualLinkTarget(null);
    if (selectedProject) loadTables(selectedProject.id);
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
          <button
            onClick={() => setShowSettingsModal(true)}
            style={{ background: 'transparent', border: 'none', cursor: 'pointer', fontSize: '1.2rem', padding: '0 8px' }}
            title="Globální nastavení (API Key)"
          >
            ⚙️
          </button>
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
                      <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap' }}>

                        <button
                          className="detect-btn"
                          style={{
                            background: '#3b82f6',
                            color: 'white',
                            border: 'none',
                            padding: '8px 16px',
                            borderRadius: '6px',
                            cursor: 'pointer',
                            fontWeight: 500,
                            display: 'flex',
                            alignItems: 'center',
                            gap: '6px'
                          }}
                          onClick={() => setShowVisualMapper(true)}
                        >
                          🔗 Kontrolované hodnoty
                        </button>
                        <button
                          className="detect-btn"
                          style={{ background: '#f59e0b', borderColor: '#d97706' }}
                          onClick={() => detectLinks('KEYS')}
                          disabled={detecting}
                        >
                          {detecting ? '🔍 ...' : '🔑 Najít Klíče'}
                        </button>
                        <button
                          className="detect-btn"
                          onClick={() => detectLinks('VALUES')}
                          disabled={detecting}
                        >
                          {detecting ? '🔍 ...' : '📋 Najít Hodnoty'}
                        </button>
                        <button
                          className="validate-btn"
                          onClick={validateProject}
                          disabled={validating}
                        >
                          {validating ? '⏳ Validuji...' : '✓ Validovat'}
                        </button>
                        <button
                          style={{
                            background: '#10b981',
                            color: 'white',
                            border: 'none',
                            padding: '8px 16px',
                            borderRadius: '6px',
                            cursor: 'pointer',
                            fontWeight: 500
                          }}
                          onClick={() => setShowManualLinkModal(true)}
                        >
                          ➕ Ruční vazba
                        </button>
                        <button
                          style={{
                            background: '#8b5cf6', // Violet
                            color: 'white',
                            border: 'none',
                            padding: '8px 16px',
                            borderRadius: '6px',
                            cursor: 'pointer',
                            fontWeight: 500
                          }}
                          onClick={() => setShowAIModal(true)}
                        >
                          ✨ AI Pravidla
                        </button>
                      </div>
                      {(validating || detecting) && (
                        <div style={{ width: '100%', maxWidth: '300px' }}>
                          <div style={{
                            fontSize: '0.8rem',
                            color: '#666',
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
                    <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
                      <span style={{ fontSize: '1.5rem' }}>{validationResult.summary.failed > 0 ? '📊' : '✅'}</span>
                      <h3 style={{ margin: 0 }}>Výsledky validace</h3>
                    </div>
                    <button className="close-btn" onClick={() => setValidationResult(null)}>×</button>
                  </div>

                  {/* Tabs Navigation */}
                  <div className="validation-tabs">
                    <button
                      className={`tab-btn ${activeValidationTab === 'SUMMARY' ? 'active' : ''}`}
                      onClick={() => setActiveValidationTab('SUMMARY')}
                    >
                      🏠 Přehled
                    </button>

                    {validationResult.errors.length > 0 && (
                      <button
                        className={`tab-btn error ${activeValidationTab === 'INTEGRITY' ? 'active' : ''}`}
                        onClick={() => setActiveValidationTab('INTEGRITY')}
                      >
                        🔗 Integrita <span className="tab-badge">{validationResult.errors.length}</span>
                      </button>
                    )}

                    {validationResult.forbidden && validationResult.forbidden.length > 0 && (
                      <button
                        className={`tab-btn error ${activeValidationTab === 'FORBIDDEN' ? 'active' : ''}`}
                        onClick={() => setActiveValidationTab('FORBIDDEN')}
                      >
                        ⛔ Zakázané <span className="tab-badge">{validationResult.forbidden.length}</span>
                      </button>
                    )}

                    {validationResult.validationRules && validationResult.validationRules.length > 0 && (
                      <button
                        className={`tab-btn error ${activeValidationTab === 'RULES' ? 'active' : ''}`}
                        onClick={() => setActiveValidationTab('RULES')}
                      >
                        ✨ AI Pravidla <span className="tab-badge">{validationResult.validationRules.length}</span>
                      </button>
                    )}

                    {validationResult.reconciliation && validationResult.reconciliation.length > 0 && (
                      <button
                        className={`tab-btn error ${activeValidationTab === 'RECONCILE' ? 'active' : ''}`}
                        onClick={() => setActiveValidationTab('RECONCILE')}
                      >
                        ⚖️ Rekonsiliace <span className="tab-badge">{validationResult.reconciliation.length}</span>
                      </button>
                    )}

                    <button
                      className={`tab-btn ${activeValidationTab === 'PROTOCOL' ? 'active' : ''}`}
                      onClick={() => setActiveValidationTab('PROTOCOL')}
                    >
                      📝 Protokol
                    </button>
                  </div>

                  {/* Search Bar for Results */}
                  {activeValidationTab !== 'SUMMARY' && activeValidationTab !== 'PROTOCOL' && (
                    <div className="validation-filter-bar" style={{ padding: '0 1rem 1rem 1rem' }}>
                      <input
                        type="text"
                        placeholder="🔍 Filtrovat výsledky (tabulka, sloupec, hodnota...)"
                        value={validationSearchTerm}
                        onChange={(e) => setValidationSearchTerm(e.target.value)}
                        style={{ width: '100%', padding: '0.75rem', borderRadius: '8px', border: '1px solid #e2e8f0', outline: 'none' }}
                      />
                    </div>
                  )}

                  {/* Tab Content */}
                  <div className="validation-content">

                    {/* 1. SUMMARY / DASHBOARD */}
                    {activeValidationTab === 'SUMMARY' && (
                      <div className="validation-dashboard-view">
                        <div className="validation-dashboard">
                          <div className="stat-card total">
                            <span className="stat-value">{validationResult.summary.totalChecks}</span>
                            <span className="stat-label">Celkem kontrol</span>
                          </div>
                          <div className="stat-card passed">
                            <span className="stat-value">{validationResult.summary.passed}</span>
                            <span className="stat-label">Úspěšné</span>
                          </div>
                          <div className="stat-card failed">
                            <span className="stat-value">{validationResult.summary.failed}</span>
                            <span className="stat-label">Selhalo</span>
                          </div>
                          <div className="stat-card">
                            <span className="stat-value" style={{ color: validationResult.summary.failed === 0 ? '#10b981' : '#f59e0b' }}>
                              {Math.round((validationResult.summary.passed / (validationResult.summary.totalChecks || 1)) * 100)}%
                            </span>
                            <span className="stat-label">Kvalita dat</span>
                          </div>
                        </div>

                        <div style={{ background: '#f8fafc', padding: '1.5rem', borderRadius: '12px', border: '1px solid #e2e8f0' }}>
                          <h4 style={{ marginTop: 0, marginBottom: '0.5rem' }}>Závěrečné shrnutí</h4>
                          <p style={{ color: '#475569', fontSize: '0.9rem' }}>
                            {validationResult.summary.failed === 0
                              ? 'Všechny kontroly proběhly v pořádku. Data jsou konzistentní a připravená k dalšímu zpracování.'
                              : `Bylo nalezeno ${validationResult.summary.failed} problematických oblastí. Doporučujeme projít jednotlivé záložky a opravit chyby ve zdrojových souborech.`}
                          </p>
                        </div>
                      </div>
                    )}

                    {/* 2. INTEGRITY (Foreign Keys) */}
                    {activeValidationTab === 'INTEGRITY' && (
                      <div className="validation-errors">
                        {validationResult.errors
                          .filter((err: ValidationError) =>
                            !validationSearchTerm ||
                            err.fkTable.toLowerCase().includes(validationSearchTerm.toLowerCase()) ||
                            err.fkColumn.toLowerCase().includes(validationSearchTerm.toLowerCase()) ||
                            err.pkTable.toLowerCase().includes(validationSearchTerm.toLowerCase()) ||
                            err.missingValues.some((v: string) => v.toLowerCase().includes(validationSearchTerm.toLowerCase()))
                          )
                          .map((err: ValidationError, i: number) => (
                            <div key={i} className="error-item">
                              <div className="error-header">
                                <strong>{err.fkTable}.{err.fkColumn}</strong>
                                <span className="arrow">→</span>
                                <strong>{err.pkTable}.{err.pkColumn}</strong>
                                <span className="error-count">{err.missingCount} chybějících (sirotků)</span>
                              </div>
                              <div className="missing-values">
                                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                                  <div style={{ flex: 1 }}>
                                    <div style={{ marginBottom: '0.5rem', fontSize: '0.75rem', color: '#94a3b8' }}>Tyto hodnoty neexistují v číselníku {err.pkTable}:</div>
                                    {err.missingValues.join(', ')}
                                    {err.missingCount > 10 && ` ... a dalších ${err.missingCount - 10}`}
                                  </div>
                                  <button
                                    onClick={() => removeColumnLink(err.fkTable, err.fkColumn)}
                                    className="secondary-btn"
                                    style={{ padding: '4px 8px', fontSize: '0.7rem', color: '#dc2626', borderColor: '#fecaca', background: '#fff', marginLeft: '1rem' }}
                                  >
                                    Zrušit vazbu
                                  </button>
                                </div>
                              </div>
                            </div>
                          ))}
                      </div>
                    )}

                    {/* 3. FORBIDDEN VALUES */}
                    {activeValidationTab === 'FORBIDDEN' && (
                      <div className="validation-errors">
                        {validationResult.forbidden?.filter((err: ForbiddenError) =>
                          !validationSearchTerm ||
                          err.targetTable.toLowerCase().includes(validationSearchTerm.toLowerCase()) ||
                          err.column.toLowerCase().includes(validationSearchTerm.toLowerCase()) ||
                          err.forbiddenTable.toLowerCase().includes(validationSearchTerm.toLowerCase()) ||
                          err.foundValues.some((v: string) => v.toLowerCase().includes(validationSearchTerm.toLowerCase()))
                        ).map((err: ForbiddenError, i: number) => (
                          <div key={i} className="error-item warning">
                            <div className="error-header">
                              <strong>{err.targetTable}.{err.column}</strong>
                              <span className="arrow">∩</span>
                              <strong>{err.forbiddenTable}.{err.forbiddenColumn}</strong>
                              <span className="error-count" style={{ background: '#fef3c7', color: '#d97706', borderColor: '#fcd34d' }}>{err.count} zakázaných hodnot</span>
                            </div>
                            <div className="missing-values">
                              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                                <div style={{ flex: 1 }}>
                                  Nalezeny hodnoty z blacklistu: {err.foundValues.join(', ')}
                                  {err.count > 10 && ` ... a dalších ${err.count - 10}`}
                                </div>
                                <button
                                  onClick={() => deleteForbiddenTable(err.forbiddenTable)}
                                  className="secondary-btn"
                                  style={{ padding: '4px 8px', fontSize: '0.7rem', color: '#dc2626', borderColor: '#fecaca', background: '#fff', marginLeft: '1rem' }}
                                >
                                  Smazat blacklist
                                </button>
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}

                    {/* 4. AI RULES */}
                    {activeValidationTab === 'RULES' && (
                      <div className="validation-errors">
                        {validationResult.validationRules && validationResult.validationRules.length > 0 && (
                          <div style={{ marginBottom: '1rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                            <label style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer', fontSize: '0.9rem' }}>
                              <input
                                type="checkbox"
                                checked={validationResult.validationRules.length > 0 && selectedRuleIds.size === validationResult.validationRules.length}
                                onChange={(e) => {
                                  if (e.target.checked) {
                                    setSelectedRuleIds(new Set(validationResult.validationRules?.map(r => r.ruleId)));
                                  } else {
                                    setSelectedRuleIds(new Set());
                                  }
                                }}
                                style={{ width: '18px', height: '18px' }}
                              />
                              Vybrat všechna pravidla
                            </label>
                            <button
                              onClick={deleteSelectedRules}
                              disabled={selectedRuleIds.size === 0}
                              className="secondary-btn"
                              style={{
                                background: selectedRuleIds.size > 0 ? '#fee2e2' : '#f3f4f6',
                                color: selectedRuleIds.size > 0 ? '#dc2626' : '#9ca3af',
                                borderColor: selectedRuleIds.size > 0 ? '#fecaca' : '#e5e7eb'
                              }}
                            >
                              Smazat vybrané ({selectedRuleIds.size})
                            </button>
                          </div>
                        )}
                        {validationResult.validationRules?.filter((err: AIRuleError) =>
                          !validationSearchTerm ||
                          err.table.toLowerCase().includes(validationSearchTerm.toLowerCase()) ||
                          err.column.toLowerCase().includes(validationSearchTerm.toLowerCase()) ||
                          err.description?.toLowerCase().includes(validationSearchTerm.toLowerCase()) ||
                          err.samples?.some((v: string) => v.toLowerCase().includes(validationSearchTerm.toLowerCase()))
                        ).map((err: AIRuleError, i: number) => (
                          <div key={i} className="rule-error error-item" style={{ display: 'flex', gap: '12px', alignItems: 'flex-start' }}>
                            <div style={{ paddingTop: '12px' }}>
                              <input
                                type="checkbox"
                                checked={selectedRuleIds.has(err.ruleId)}
                                onChange={(e) => {
                                  const newSet = new Set(selectedRuleIds);
                                  if (e.target.checked) newSet.add(err.ruleId);
                                  else newSet.delete(err.ruleId);
                                  setSelectedRuleIds(newSet);
                                }}
                                style={{ width: '18px', height: '18px', cursor: 'pointer' }}
                              />
                            </div>
                            <div style={{ flex: 1 }}>
                              <div className="rule-error-header">
                                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                                  <span className="rule-type-label">{err.ruleType}</span>
                                  <strong>{err.table}.{err.column}</strong>
                                </div>
                                <span
                                  className="error-count"
                                  style={{ borderColor: '#ddd', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
                                  onClick={() => fetchRuleFailures(err)}
                                  title="Klikněte pro zobrazení záznamů"
                                >
                                  🔍 {err.failedCount} vadných řádků
                                </span>
                              </div>
                              <div style={{ padding: '1rem' }}>
                                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: '1rem' }}>
                                  <div className="rule-description" style={{ flex: 1 }}>{err.description || 'Validace podle AI pravidla.'}</div>
                                  <button
                                    onClick={() => deleteRule(err.ruleId)}
                                    className="secondary-btn"
                                    style={{ padding: '4px 8px', fontSize: '0.7rem', color: '#dc2626', borderColor: '#fecaca', background: '#fff' }}
                                  >
                                    Smazat
                                  </button>
                                </div>
                                <div style={{ marginTop: '0.75rem' }}>
                                  <div style={{ fontSize: '0.7rem', textTransform: 'uppercase', color: '#94a3b8', marginBottom: '0.25rem' }}>Ukázka chyb:</div>
                                  <div className="missing-values" style={{ background: '#fafafa', border: '1px solid #eee' }}>
                                    {err.samples?.join(', ') || 'Není k dispozici'}
                                  </div>
                                </div>
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}

                    {/* 5. RECONCILIATION */}
                    {activeValidationTab === 'RECONCILE' && (
                      <div className="validation-errors">
                        {validationResult.reconciliation?.map((err, i) => (
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
                                  <li key={j} style={{ borderBottom: '1px solid #eee', padding: '4px 0', fontSize: '0.85rem' }}>
                                    <span style={{ color: '#64748b' }}>[{m.key}]</span>: <span style={{ color: '#ef4444', textDecoration: 'line-through' }}>"{m.source}"</span> → <span style={{ color: '#10b981', fontWeight: 600 }}>"{m.target}"</span>
                                  </li>
                                ))}
                              </ul>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}

                    {/* 6. PROTOCOL */}
                    {activeValidationTab === 'PROTOCOL' && (
                      <pre className="validation-protocol" style={{ maxHeight: '500px' }}>
                        {validationResult.protocol}
                      </pre>
                    )}

                  </div>
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

              {tables.filter(t => t.tableType === uploadType).length === 0 ? (
                <div className="empty-state" style={{ padding: '4rem 2rem' }}>
                  <p style={{ color: '#64748b', fontSize: '1.1rem' }}>
                    {tables.length === 0
                      ? 'Žádné tabulky. Nahrajte Excel soubor.'
                      : `V režimu '${uploadType === 'SOURCE' ? 'Zdrojová tabulka' :
                        uploadType === 'TARGET' ? 'Kontrolovaná tabulka' :
                          uploadType === 'FORBIDDEN' ? 'Zakázané hodnoty' : 'Rozsah hodnot'
                      }' nejsou zatím žádné tabulky.`}
                  </p>
                </div>
              ) : (
                <div className="tables-grid">
                  {tables.filter(table => table.tableType === uploadType).map(table => (
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
                            <th title="Primární klíč">PK</th>
                            <th title="Určuje rozsah validace (scope)">Rozsah</th>
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
                                <td>
                                  <button
                                    className={`pk-btn ${col.isValidationRange ? 'active' : ''}`}
                                    onClick={() => toggleValidationRange(col.id, col.isValidationRange)}
                                    style={{
                                      background: col.isValidationRange ? '#fef3c7' : '#f9fafb',
                                      borderColor: col.isValidationRange ? '#f59e0b' : '#e5e7eb',
                                      filter: col.isValidationRange ? 'none' : 'grayscale(100%) opacity(0.5)'
                                    }}
                                    title="Rozsah validace"
                                  >
                                    🎯
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

      {/* Manual Link Modal */}
      {
        showManualLinkModal && (
          <div style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: 'rgba(0,0,0,0.5)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 1000
          }}>
            <div style={{
              background: 'white',
              borderRadius: '12px',
              padding: '24px',
              width: '400px',
              maxWidth: '90vw',
              boxShadow: '0 20px 40px rgba(0,0,0,0.3)'
            }}>
              <h3 style={{ marginBottom: '20px' }}>➕ Vytvořit ruční vazbu</h3>

              {/* Link Type Selection */}
              <div style={{ marginBottom: '20px' }}>
                <label style={{ display: 'block', fontWeight: 500, marginBottom: '8px' }}>Typ vazby:</label>
                <div style={{ display: 'flex', gap: '15px' }}>
                  <label style={{ display: 'flex', alignItems: 'center', gap: '6px', cursor: 'pointer' }}>
                    <input
                      type="radio"
                      name="linkType"
                      value="KEY"
                      checked={manualLinkType === 'KEY'}
                      onChange={() => setManualLinkType('KEY')}
                    />
                    <span>🔑 Referenční klíč (PK/FK)</span>
                  </label>
                  <label style={{ display: 'flex', alignItems: 'center', gap: '6px', cursor: 'pointer' }}>
                    <input
                      type="radio"
                      name="linkType"
                      value="VALUE"
                      checked={manualLinkType === 'VALUE'}
                      onChange={() => setManualLinkType('VALUE')}
                    />
                    <span>📋 Kontrola hodnoty (Value)</span>
                  </label>
                </div>
              </div>

              {/* Source selection */}
              <div style={{ marginBottom: '16px' }}>
                <label style={{ display: 'block', fontWeight: 500, marginBottom: '6px' }}>
                  Zdrojový sloupec (FK):
                </label>
                <select
                  style={{ width: '100%', padding: '8px', marginBottom: '8px', boxSizing: 'border-box' }}
                  value={manualLinkSource?.table || ''}
                  onChange={(e) => setManualLinkSource({ table: e.target.value, column: '' })}
                >
                  <option value="">-- Vyberte tabulku --</option>
                  {tables.map(t => (
                    <option key={t.tableName} value={t.tableName}>{t.tableName}</option>
                  ))}
                </select>
                <select
                  style={{ width: '100%', padding: '8px', boxSizing: 'border-box' }}
                  value={manualLinkSource?.column || ''}
                  onChange={(e) => setManualLinkSource(prev => prev ? { ...prev, column: e.target.value } : null)}
                  disabled={!manualLinkSource?.table}
                >
                  <option value="">-- Vyberte sloupec --</option>
                  {manualLinkSource?.table && tables
                    .find(t => t.tableName === manualLinkSource.table)
                    ?.columns.map(c => (
                      <option key={c.id} value={c.columnName}>{c.columnName}</option>
                    ))
                  }
                </select>
              </div>

              {/* Target selection */}
              <div style={{ marginBottom: '20px' }}>
                <label style={{ display: 'block', fontWeight: 500, marginBottom: '6px' }}>
                  Cílový sloupec (PK):
                </label>
                <select
                  style={{ width: '100%', padding: '8px', marginBottom: '8px', boxSizing: 'border-box' }}
                  value={manualLinkTarget?.table || ''}
                  onChange={(e) => setManualLinkTarget({ table: e.target.value, column: '' })}
                >
                  <option value="">-- Vyberte tabulku --</option>
                  {tables.map(t => (
                    <option key={t.tableName} value={t.tableName}>{t.tableName}</option>
                  ))}
                </select>
                <select
                  style={{ width: '100%', padding: '8px', boxSizing: 'border-box' }}
                  value={manualLinkTarget?.column || ''}
                  onChange={(e) => setManualLinkTarget(prev => prev ? { ...prev, column: e.target.value } : null)}
                  disabled={!manualLinkTarget?.table}
                >
                  <option value="">-- Vyberte sloupec --</option>
                  {manualLinkTarget?.table && tables
                    .find(t => t.tableName === manualLinkTarget.table)
                    ?.columns.map(c => (
                      <option key={c.id} value={c.columnName}>{c.columnName}</option>
                    ))
                  }
                </select>
              </div>

              {/* Actions */}
              <div style={{ display: 'flex', gap: '12px', justifyContent: 'flex-end' }}>
                <button
                  style={{
                    padding: '10px 20px',
                    border: '1px solid #ddd',
                    borderRadius: '6px',
                    background: 'white',
                    cursor: 'pointer'
                  }}
                  onClick={() => {
                    setShowManualLinkModal(false);
                    setManualLinkSource(null);
                    setManualLinkTarget(null);
                  }}
                >
                  Zrušit
                </button>
                <button
                  style={{
                    padding: '10px 20px',
                    border: 'none',
                    borderRadius: '6px',
                    background: '#10b981',
                    color: 'white',
                    cursor: 'pointer',
                    fontWeight: 500
                  }}
                  onClick={createManualLink}
                  disabled={!manualLinkSource?.column || !manualLinkTarget?.column}
                >
                  ✓ Vytvořit vazbu
                </button>
              </div>
            </div>
          </div>
        )
      }

      {/* Settings Modal */}
      {
        showSettingsModal && (
          <div style={{
            position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
            background: 'rgba(0,0,0,0.5)', display: 'flex', justifyContent: 'center', alignItems: 'center', zIndex: 1100
          }}>
            <div style={{
              background: 'white', padding: '24px', borderRadius: '12px', width: '400px', maxWidth: '90%',
              boxShadow: '0 4px 20px rgba(0,0,0,0.2)'
            }}>
              <h3 style={{ margin: '0 0 16px', color: '#1f2937' }}>⚙️ Globální Nastavení</h3>
              <div style={{ marginBottom: '15px' }}>
                <label style={{ display: 'block', marginBottom: '4px', fontWeight: 500 }}>OpenAI API Key</label>
                <input
                  type="password"
                  placeholder={hasSystemKey ? "******** (Nastaveno)" : "sk-..."}
                  value={newSystemKey}
                  onChange={e => setNewSystemKey(e.target.value)}
                  style={{ width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px' }}
                />
                <small style={{ color: '#666', display: 'block', marginTop: '4px' }}>
                  Klíč bude bezpečně uložen v databázi a nebude se zobrazovat v UI.
                </small>
              </div>
              <div style={{ marginBottom: '20px' }}>
                <label style={{ display: 'block', marginBottom: '4px', fontWeight: 500 }}>Potvrzení heslem</label>
                <input
                  type="password"
                  placeholder="Heslo"
                  value={aiPassword}
                  onChange={e => setAiPassword(e.target.value)}
                  style={{ width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px' }}
                />
              </div>
              <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '8px' }}>
                <button
                  onClick={() => setShowSettingsModal(false)}
                  style={{ padding: '8px 16px', background: '#f3f4f6', border: 'none', borderRadius: '6px', cursor: 'pointer' }}
                >
                  Zavřít
                </button>
                <button
                  onClick={saveSettings}
                  style={{ background: '#3b82f6', color: 'white', border: 'none', padding: '8px 16px', borderRadius: '6px', cursor: 'pointer' }}
                >
                  Uložit nastavení
                </button>
              </div>
            </div>
          </div>
        )
      }

      {/* AI Rules Modal */}
      {
        showAIModal && (
          <div style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: 'rgba(0,0,0,0.5)',
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            zIndex: 1000
          }}>
            <div style={{
              background: 'white',
              padding: '24px',
              borderRadius: '12px',
              width: '400px',
              maxWidth: '90%',
              boxShadow: '0 4px 20px rgba(0,0,0,0.2)'
            }}>
              <h3 style={{ margin: '0 0 16px', color: '#1f2937' }}>✨ AI Návrh Pravidel</h3>

              {!aiResult || aiResult.startsWith('Analyzuji') ? (
                <>
                  <p style={{ margin: '0 0 12px', color: '#4b5563', fontSize: '0.9rem' }}>
                    Analýza struktury a generování pravidel probíhá pomocí OpenAI.
                  </p>

                  <div style={{ marginBottom: '12px' }}>
                    <label style={{ display: 'block', marginBottom: '4px', fontSize: '0.85rem', fontWeight: 500 }}>Stav API Klíče</label>
                    {hasSystemKey ? (
                      <div style={{ padding: '8px', background: '#d1fae5', color: '#065f46', borderRadius: '6px', fontSize: '0.9rem', display: 'flex', alignItems: 'center', gap: '6px' }}>
                        ✅ Klíč je nastaven v systému.
                      </div>
                    ) : (
                      <div style={{ padding: '8px', background: '#fee2e2', color: '#b91c1c', borderRadius: '6px', fontSize: '0.9rem' }}>
                        ⚠️ Chybí API Klíč! Nastavte ho v ⚙️ Nastavení (ikona vpravo nahoře).
                      </div>
                    )}
                  </div>

                  <div style={{ marginBottom: '20px' }}>
                    <label style={{ display: 'block', marginBottom: '4px', fontSize: '0.85rem', fontWeight: 500 }}>Bezpečnostní heslo</label>
                    <input
                      type="password"
                      value={aiPassword}
                      onChange={(e) => setAiPassword(e.target.value)}
                      placeholder="Heslo"
                      style={{ width: '100%', padding: '8px', borderRadius: '6px', border: '1px solid #d1d5db' }}
                    />
                  </div>

                  <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '8px' }}>
                    <button
                      onClick={() => setShowAIModal(false)}
                      style={{ padding: '8px 16px', background: '#f3f4f6', border: 'none', borderRadius: '6px', cursor: 'pointer' }}
                      disabled={generatingAI}
                    >
                      Zrušit
                    </button>
                    <button
                      onClick={generateAIRules}
                      disabled={generatingAI || !hasSystemKey}
                      style={{
                        padding: '8px 16px',
                        background: (generatingAI || !hasSystemKey) ? '#9ca3af' : '#8b5cf6',
                        color: 'white',
                        border: 'none',
                        borderRadius: '6px',
                        cursor: (generatingAI || !hasSystemKey) ? 'not-allowed' : 'pointer'
                      }}
                    >
                      {generatingAI ? 'Generuji...' : '✨ Vygenerovat'}
                    </button>
                  </div>
                </>
              ) : (
                <div style={{ textAlign: 'center' }}>
                  <p style={{ marginBottom: '20px', fontSize: '1.1rem', color: aiResult.startsWith('✅') ? 'green' : 'red' }}>
                    {aiResult}
                  </p>
                  <div style={{ display: 'flex', justifyContent: 'center', gap: '8px' }}>
                    <button
                      onClick={() => { setShowAIModal(false); setAiResult(''); }}
                      style={{ padding: '8px 16px', background: '#f3f4f6', border: 'none', borderRadius: '6px', cursor: 'pointer' }}
                    >
                      Zavřít
                    </button>
                    {aiResult.startsWith('❌') && (
                      <button
                        onClick={() => setAiResult('')}
                        style={{ padding: '8px 16px', background: '#3b82f6', color: 'white', border: 'none', borderRadius: '6px', cursor: 'pointer' }}
                      >
                        Zkusit znovu
                      </button>
                    )}
                  </div>
                </div>
              )}
            </div>
          </div>
        )
      }

      {/* Rule Failure Modal */}
      {
        showRuleFailureModal && (
          <div className="modal-overlay" style={{
            position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
            background: 'rgba(0,0,0,0.5)', display: 'flex', justifyContent: 'center',
            alignItems: 'center', zIndex: 1100
          }}>
            <div style={{
              background: 'white', padding: '24px', borderRadius: '12px',
              width: '800px', maxWidth: '95%', maxHeight: '80vh', display: 'flex', flexDirection: 'column',
              boxShadow: '0 4px 20px rgba(0,0,0,0.2)'
            }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
                <h3 style={{ margin: 0 }}>🔍 Detailní výpis chyb: {activeRuleTitle}</h3>
                <button
                  onClick={() => setShowRuleFailureModal(false)}
                  style={{ border: 'none', background: 'none', fontSize: '1.5rem', cursor: 'pointer' }}
                >
                  &times;
                </button>
              </div>

              <div style={{ overflowY: 'auto', flex: 1, border: '1px solid #eee', borderRadius: '8px' }}>
                {loadingFailures ? (
                  <div style={{ padding: '2rem', textAlign: 'center' }}>Načítám záznamy...</div>
                ) : selectedRuleFailures.length === 0 ? (
                  <div style={{ padding: '2rem', textAlign: 'center' }}>Žádné záznamy nebyly nalezeny (nebo se vyskytla chyba).</div>
                ) : (
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' }}>
                    <thead style={{ position: 'sticky', top: 0, background: '#f8fafc' }}>
                      <tr>
                        <th style={{ padding: '8px', textAlign: 'left', borderBottom: '2px solid #e2e8f0' }}>Řádek</th>
                        <th style={{ padding: '8px', textAlign: 'left', borderBottom: '2px solid #e2e8f0' }}>Hodnota</th>
                        {selectedRuleFailures[0].val_0 !== undefined && (
                          <th style={{ padding: '8px', textAlign: 'left', borderBottom: '2px solid #e2e8f0' }}>Detaily</th>
                        )}
                      </tr>
                    </thead>
                    <tbody>
                      {selectedRuleFailures.map((f, idx) => (
                        <tr key={idx} style={{ background: idx % 2 === 0 ? '#fff' : '#f9fafb' }}>
                          <td style={{ padding: '8px', borderBottom: '1px solid #edf2f7', color: '#64748b' }}>#{f.rowIndex + 1}</td>
                          <td style={{ padding: '8px', borderBottom: '1px solid #edf2f7', fontWeight: 600 }}>{f.value || f.primary_val}</td>
                          {f.val_0 !== undefined && (
                            <td style={{ padding: '8px', borderBottom: '1px solid #edf2f7', fontSize: '0.8rem' }}>
                              {Object.entries(f)
                                .filter(([key]) => key.startsWith('val_'))
                                .map(([key, val]) => `${key}: ${val}`).join(' | ')
                              }
                            </td>
                          )}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>

              <div style={{ marginTop: '1rem', color: '#64748b', fontSize: '0.8rem' }}>
                Zobrazeno prvních {selectedRuleFailures.length} záznamů.
              </div>
            </div>
          </div>
        )
      }
      {/* Visual Mapper Modal */}
      {
        showVisualMapper && selectedProject && (
          <VisualMapperModal
            projectId={selectedProject.id}
            tables={tables}
            onClose={() => setShowVisualMapper(false)}
            onSave={() => loadTables(selectedProject.id)}
          />
        )
      }
    </div >
  )
}

export default App;
