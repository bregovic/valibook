CREATE TABLE IF NOT EXISTS validation_projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS imported_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    original_filename TEXT NOT NULL,
    file_type TEXT CHECK(file_type IN ('source', 'target', 'codebook')), 
    stored_filename TEXT,
    uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(project_id) REFERENCES validation_projects(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS file_columns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL,
    column_name TEXT NOT NULL,
    column_index INTEGER NOT NULL,
    sample_value TEXT,
    FOREIGN KEY(file_id) REFERENCES imported_files(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS column_mappings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    source_column_id INTEGER, 
    target_column_id INTEGER, 
    mapping_note TEXT,
    FOREIGN KEY(project_id) REFERENCES validation_projects(id) ON DELETE CASCADE,
    FOREIGN KEY(source_column_id) REFERENCES file_columns(id),
    FOREIGN KEY(target_column_id) REFERENCES file_columns(id)
);

CREATE TABLE IF NOT EXISTS validation_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    column_mapping_id INTEGER NOT NULL,
    rule_type TEXT NOT NULL, -- 'exact_match', 'exists_in_codebook', 'format_regex', 'not_empty'
    parameter TEXT,
    severity TEXT DEFAULT 'error', 
    FOREIGN KEY(column_mapping_id) REFERENCES column_mappings(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS validation_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    row_index INTEGER,
    column_mapping_id INTEGER,
    error_message TEXT,
    actual_value TEXT,
    expected_value TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(project_id) REFERENCES validation_projects(id) ON DELETE CASCADE
);
