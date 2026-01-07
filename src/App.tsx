import { useState, useEffect } from 'react';
import './App.css';
import MappingView from './MappingView';

import ValidationResultView from './ValidationResultView';

interface Project {
  id: number;
  name: string;
  description: string;
}

function App() {
  const [projects, setProjects] = useState<Project[]>([]);
  const [selectedProjectId, setSelectedProjectId] = useState<number | null>(null);
  const [newProjectName, setNewProjectName] = useState('');

  // New State for View Mode
  const [viewMode, setViewMode] = useState<'detail' | 'mapping' | 'validation'>('detail');
  const [validationScopeFileId, setValidationScopeFileId] = useState<number | null>(null);

  useEffect(() => {
    fetchProjects();
  }, []);

  const fetchProjects = async () => {
    const res = await fetch('/api/projects');
    const data = await res.json();
    setProjects(data);
  };

  const createProject = async () => {
    if (!newProjectName) return;
    await fetch('/api/projects', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: newProjectName })
    });
    setNewProjectName('');
    fetchProjects();
  };

  interface FileInfo {
    id: number;
    file_type: 'source' | 'target' | 'codebook' | 'exclusion';
    original_filename: string;
    columns: { id: number, column_name: string, sample_value: string }[];
  }

  const [projectFiles, setProjectFiles] = useState<FileInfo[]>([]);

  useEffect(() => {
    // Reset view mode when project changes
    setViewMode('detail');
    setProjectFiles([]);
    if (selectedProjectId) {
      fetchProjectDetails(selectedProjectId);
    }
  }, [selectedProjectId]);

  const fetchProjectDetails = async (id: number) => {
    const res = await fetch(`/api/projects/${id}/details`);
    const data = await res.json();
    setProjectFiles(data.files);
  };

  // Handle multiple file uploads
  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>, type: 'source' | 'target' | 'codebook' | 'exclusion') => {
    if (!selectedProjectId || !e.target.files) return;

    // Iterate over all selected files
    for (let i = 0; i < e.target.files.length; i++) {
      const file = e.target.files[i];

      // Check for duplicates
      const isDuplicate = projectFiles.some(pf => pf.original_filename === file.name);
      if (isDuplicate) {
        alert(`File '${file.name}' is already uploaded in this project.\nSkipping upload.`);
        continue;
      }

      const formData = new FormData();
      formData.append('file', file);
      formData.append('fileType', type);

      // Upload each file individually (simplest for current API structure)
      // Alternatively, backend could be updated to accept array of files, but single calls are safer for progress/errors.
      try {
        const res = await fetch(`/api/projects/${selectedProjectId}/files`, {
          method: 'POST',
          body: formData
        });
        const data = await res.json();

        if (data.error) {
          alert(`Error uploading file ${file.name}: ${data.error}`);
        } else if (data.warning) {
          alert(`Warning for file ${file.name}: ${data.warning}`);
        }
      } catch (err) {
        console.error('Upload error', err);
      }
    }

    // Refresh project details after all uploads
    fetchProjectDetails(selectedProjectId);
  };

  const handleDeleteFile = async (fileId: number) => {
    if (!confirm('Are you sure you want to delete this file?')) return;
    await fetch(`/api/files/${fileId}`, { method: 'DELETE' });
    if (selectedProjectId) fetchProjectDetails(selectedProjectId);
  };

  const handleBulkDelete = async (type?: 'source' | 'target' | 'codebook' | 'exclusion') => {
    const msg = type
      ? `Are you sure you want to delete ALL ${type.toUpperCase()} files?\nThis will remove related mappings.`
      : 'Are you sure you want to delete ALL files in this project?';

    if (!confirm(msg)) return;

    const query = type ? `?type=${type}` : '';
    try {
      await fetch(`/api/projects/${selectedProjectId}/files${query}`, { method: 'DELETE' });
      fetchProjectDetails(selectedProjectId!);
    } catch (e) {
      alert('Failed to delete files');
    }
  };

  /* Helper to render upload cards */
  const renderFileSection = (type: 'source' | 'target' | 'codebook' | 'exclusion', title: string) => {
    const files = projectFiles.filter(f => f.file_type === type);

    return (
      <div className="upload-card">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem', borderBottom: '1px solid var(--surface-border)', paddingBottom: '0.5rem' }}>
          <h3 style={{ margin: 0, border: 0, padding: 0 }}>{title}</h3>
          {files.length > 0 && (
            <button
              onClick={() => handleBulkDelete(type)}
              style={{ fontSize: '0.8rem', padding: '0.2rem 0.5rem', background: 'none', border: '1px solid var(--danger)', color: 'var(--danger)', borderRadius: '4px', cursor: 'pointer' }}
              title="Delete all files in this section"
            >
              Clear All
            </button>
          )}
        </div>

        {files.length > 0 ? (
          <div className="file-list">
            {files.map(file => (
              <div key={file.id} className="file-info fade-in" style={{ marginBottom: '0.5rem', display: 'flex', alignItems: 'center', gap: '10px' }}>
                {/* Left side: Icon + Text (flexible width) */}
                <div style={{ flex: 1, minWidth: 0, overflow: 'hidden' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                    <span style={{ flexShrink: 0 }}>✅</span>
                    <strong style={{
                      whiteSpace: 'nowrap',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis'
                    }} title={file.original_filename}>
                      {file.original_filename}
                    </strong>
                  </div>
                  <div style={{ fontSize: '0.85rem', marginTop: '0.1rem', opacity: 0.7, color: 'var(--text)' }}>
                    {file.columns?.length || 0} columns detected
                  </div>
                </div>
                {/* Right side: Delete button (fixed width) */}
                <button
                  onClick={() => handleDeleteFile(file.id)}
                  style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: '1.2rem' }}
                  title="Delete file"
                >
                  🗑️
                </button>
              </div>
            ))}
            {/* Allow adding more files even if some exist */}
            <div style={{ marginTop: '1rem', borderTop: '1px dashed var(--border)', paddingTop: '0.5rem' }}>
              <small>Add more:</small>
              <input className="file-input" type="file" multiple onChange={(e) => handleFileUpload(e, type)} />
            </div>
          </div>
        ) : (
          <div>
            <input className="file-input" type="file" multiple onChange={(e) => handleFileUpload(e, type)} />
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="app-container fade-in">
      <header className="app-header">
        <div className="logo-section">
          <img src="/logo.png" alt="ValiBook Logo" className="app-logo" />
        </div>
        <div className="user-section">
          {/* Placeholder for future user profile */}
        </div>
      </header>

      {!selectedProjectId ? (
        <div className="project-select-card">
          <h2>Select a Project</h2>
          <div className="project-list-items">
            {projects.map(p => (
              <div key={p.id} onClick={() => setSelectedProjectId(p.id)} className="project-item">
                <div style={{ display: 'flex', flexDirection: 'column' }}>
                  <span style={{ fontWeight: 600 }}>{p.name}</span>
                  {p.description && <span style={{ fontSize: '0.85rem', opacity: 0.7 }}>{p.description}</span>}
                </div>
                <span style={{ fontSize: '0.9rem', color: 'var(--primary)' }}>Open &rarr;</span>
              </div>
            ))}
            {projects.length === 0 && <p style={{ textAlign: 'center', opacity: 0.5, padding: '1rem' }}>No projects yet.</p>}
          </div>

          <div style={{ marginTop: '2rem', borderTop: '1px solid var(--border)', paddingTop: '1rem' }}>
            <h3>Create New Project</h3>
            <div className="new-project-form">
              <input
                type="text"
                placeholder="Project Name"
                value={newProjectName}
                onChange={(e) => setNewProjectName(e.target.value)}
              />
              <button className="primary" onClick={createProject} disabled={!newProjectName}>Create</button>
            </div>
          </div>
        </div>
      ) : (
        <>
          {viewMode === 'detail' && (
            <div className="detail-view fade-in">
              <div className="header-actions">
                <button className="secondary" onClick={() => { setSelectedProjectId(null); setViewMode('detail'); }}>&larr; Back to Projects</button>
                <h2 style={{ border: 0, margin: 0, fontSize: '1.2rem' }}>Project: {projects.find(p => p.id === selectedProjectId)?.name}</h2>
                <button
                  className="primary"
                  onClick={() => setViewMode('mapping')}
                  disabled={!projectFiles.find(f => f.file_type === 'source') || !projectFiles.find(f => f.file_type === 'target')}
                >
                  Proceed to Mapping &rarr;
                </button>
              </div>

              <div className="upload-grid">
                {renderFileSection('source', '1. Source File (Source of Truth)')}
                {renderFileSection('target', '2. Target File (Export to Check)')}
              </div>

              <div className="upload-grid" style={{ marginTop: '1rem' }}>
                {renderFileSection('exclusion', '3. Exclusion List (Forbidden Values - Optional)')}
              </div>
            </div>
          )}

          {viewMode === 'mapping' && (
            <MappingView
              projectId={selectedProjectId}
              files={projectFiles}
              onBack={() => setViewMode('detail')}
              onNext={() => setViewMode('validation')}
              scopeFileId={validationScopeFileId}
              setScopeFileId={setValidationScopeFileId}
            />
          )}

          {viewMode === 'validation' && (
            <ValidationResultView
              projectId={selectedProjectId!}
              onBack={() => setViewMode('mapping')}
              scopeFileId={validationScopeFileId}
            />
          )}
        </>
      )}
    </div>
  );
}

export default App;
