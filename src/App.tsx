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
    file_type: 'source' | 'target' | 'codebook';
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

  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>, type: 'source' | 'target' | 'codebook') => {
    if (!selectedProjectId || !e.target.files?.[0]) return;

    const formData = new FormData();
    formData.append('file', e.target.files[0]);
    formData.append('fileType', type);

    const res = await fetch(`/api/projects/${selectedProjectId}/files`, {
      method: 'POST',
      body: formData
    });

    if (res.ok) {
      fetchProjectDetails(selectedProjectId);
    } else {
      alert('Upload failed');
    }
  };

  const renderFileSection = (type: 'source' | 'target' | 'codebook', title: string) => {
    const file = projectFiles.find(f => f.file_type === type);
    return (
      <div className="upload-sect">
        <h3>{title}</h3>
        {file ? (
          <div>
            <p><strong>Loaded:</strong> {file.original_filename}</p>
            <details>
              <summary>View Columns ({file.columns.length})</summary>
              <ul style={{ textAlign: 'left', maxHeight: '200px', overflowY: 'auto' }}>
                {file.columns.map(c => (
                  <li key={c.id}>
                    {c.column_name} <span style={{ color: '#888', fontSize: '0.8em' }}>({c.sample_value})</span>
                  </li>
                ))}
              </ul>
            </details>
          </div>
        ) : (
          <input type="file" onChange={(e) => handleFileUpload(e, type)} />
        )}
      </div>
    );
  };

  return (
    <div className="container">
      <h1>Export Validator</h1>

      {!selectedProjectId ? (
        <div className="project-list">
          <div className="card">
            <h2>New Project</h2>
            <input
              value={newProjectName}
              onChange={(e) => setNewProjectName(e.target.value)}
              placeholder="Project Name"
            />
            <button onClick={createProject}>Create</button>
          </div>

          <div className="list">
            <h2>Recent Projects</h2>
            {projects.map(p => (
              <div key={p.id} className="project-item" onClick={() => setSelectedProjectId(p.id)}>
                <strong>{p.name}</strong>
                <p>{p.description}</p>
              </div>
            ))}
          </div>
        </div>
      ) : (
        <>
          {viewMode === 'detail' && (
            <div className="project-detail">
              <button onClick={() => setSelectedProjectId(null)}>Back to Projects</button>
              <h2>Project #{selectedProjectId}</h2>

              {renderFileSection('source', '1. Source File (Source of Truth / Vzor)')}
              {renderFileSection('target', '2. Target File (Export to Check)')}
              {renderFileSection('codebook', '3. Codebooks (Optional - for value validation)')}

              <div className="status">
                {projectFiles.filter(f => f.file_type === 'source' || f.file_type === 'target').length >= 2 && (
                  <button
                    style={{ marginTop: '1rem', background: '#28a745' }}
                    onClick={() => setViewMode('mapping')}
                  >
                    Proceed to Mapping & Validation &gt;
                  </button>
                )}
              </div>
            </div>
          )}

          {viewMode === 'mapping' && (
            <MappingView
              projectId={selectedProjectId}
              files={projectFiles}
              onBack={() => setViewMode('detail')}
              onNext={() => setViewMode('validation')}
            />
          )}

          {viewMode === 'validation' && (
            <ValidationResultView
              projectId={selectedProjectId!}
              onBack={() => setViewMode('mapping')}
            />
          )}
        </>
      )}
    </div>
  );
}

export default App;
