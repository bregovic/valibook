import express from 'express';
import cors from 'cors';
import path from 'path';
import multer from 'multer';
import { initDatabase } from './database.js';
import db from './database.js';
import * as XLSX from 'xlsx';
import fs from 'fs';

const app = express();
const PORT = 3001;

app.use(cors());
app.use(express.json());

// Initialize Database
initDatabase();

// Multer setup for file uploads
const UPLOADS_DIR = process.env.UPLOADS_DIR || 'uploads/';
if (!fs.existsSync(UPLOADS_DIR)) {
    fs.mkdirSync(UPLOADS_DIR, { recursive: true });
}

const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, UPLOADS_DIR);
    },
    filename: (req, file, cb) => {
        const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1e9);
        cb(null, uniqueSuffix + '-' + file.originalname);
    }
});
const upload = multer({ storage: storage });

// --- API ROUTES ---

// Health Check endpoint
app.get('/health', (req, res) => {
    res.status(200).send('OK');
});

// 1. Get all projects
app.get('/api/projects', (req, res) => {
    try {
        const projects = db.prepare('SELECT * FROM validation_projects ORDER BY created_at DESC').all();
        res.json(projects);
    } catch (error) {
        res.status(500).json({ error: (error as Error).message });
    }
});

// 2. Create new project
app.post('/api/projects', (req, res) => {
    const { name, description } = req.body;
    if (!name) return res.status(400).json({ error: 'Project name is required' });

    try {
        const info = db.prepare('INSERT INTO validation_projects (name, description) VALUES (?, ?)').run(name, description || '');
        res.json({ id: info.lastInsertRowid, name, description });
    } catch (error) {
        res.status(500).json({ error: (error as Error).message });
    }
});

// 3. Upload File & Analyze Columns
app.post('/api/projects/:id/files', upload.single('file'), (req, res) => {
    const projectId = req.params.id;
    const fileType = req.body.fileType; // 'source' | 'target' | 'codebook'

    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    if (!['source', 'target', 'codebook'].includes(fileType)) {
        return res.status(400).json({ error: 'Invalid file type' });
    }

    try {
        const filePath = req.file.path;

        // Insert file record
        const fileStmt = db.prepare('INSERT INTO imported_files (project_id, original_filename, file_type, stored_filename) VALUES (?, ?, ?, ?)');
        const fileInfo = fileStmt.run(projectId, req.file.originalname, fileType, filePath);
        const fileId = fileInfo.lastInsertRowid;

        // Parse Columns using XLSX
        const workbook = XLSX.readFile(filePath);
        const sheetName = workbook.SheetNames[0];
        const sheet = workbook.Sheets[sheetName];

        // Convert to JSON (header: 1 means array of arrays)
        const jsonData: any[][] = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });

        if (jsonData.length > 0) {
            const headers = jsonData[0];
            const firstRow = jsonData.length > 1 ? jsonData[1] : [];

            const insertCol = db.prepare('INSERT INTO file_columns (file_id, column_name, column_index, sample_value) VALUES (?, ?, ?, ?)');
            const insertMany = db.transaction((columns) => {
                for (const col of columns) insertCol.run(col.fileId, col.name, col.index, col.sample);
            });

            const columnsToInsert = headers.map((h, idx) => ({
                fileId,
                name: String(h || `Column ${idx + 1}`),
                index: idx,
                sample: String(firstRow[idx] || '')
            }));

            insertMany(columnsToInsert);
        }

        res.json({ success: true, fileId });
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// 4. Get Project Files and Columns
app.get('/api/projects/:id/details', (req, res) => {
    try {
        const files = db.prepare('SELECT * FROM imported_files WHERE project_id = ?').all(req.params.id) as any[];

        for (const file of files) {
            file.columns = db.prepare('SELECT * FROM file_columns WHERE file_id = ? ORDER BY column_index').all(file.id);
        }

        res.json({ files });
    } catch (error) {
        res.status(500).json({ error: (error as Error).message });
    }
});

// 5. Get Mappings
app.get('/api/projects/:id/mappings', (req, res) => {
    try {
        const mappings = db.prepare(`
            SELECT m.*, 
                   sc.column_name as source_name, sc.sample_value as source_sample,
                   tc.column_name as target_name, tc.sample_value as target_sample
            FROM column_mappings m
            LEFT JOIN file_columns sc ON m.source_column_id = sc.id
            LEFT JOIN file_columns tc ON m.target_column_id = tc.id
            WHERE m.project_id = ?
        `).all(req.params.id);
        res.json(mappings);
    } catch (error) {
        res.status(500).json({ error: (error as Error).message });
    }
});

// 6. Save Mappings
app.post('/api/projects/:id/mappings', (req, res) => {
    const projectId = req.params.id;
    const { mappings } = req.body; // Array of { sourceColumnId, targetColumnId, note }

    if (!Array.isArray(mappings)) {
        return res.status(400).json({ error: 'Mappings must be an array' });
    }

    try {
        const deleteStmt = db.prepare('DELETE FROM column_mappings WHERE project_id = ?');
        const insertStmt = db.prepare('INSERT INTO column_mappings (project_id, source_column_id, target_column_id, mapping_note) VALUES (?, ?, ?, ?)');

        const saveTransaction = db.transaction((mappingList) => {
            deleteStmt.run(projectId);
            for (const m of mappingList) {
                insertStmt.run(projectId, m.sourceColumnId, m.targetColumnId, m.note || '');
            }
        });

        saveTransaction(mappings);
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: (error as Error).message });
    }
});

// 7. Auto-Map Columns (Helper)
app.post('/api/projects/:id/auto-map', (req, res) => {
    const projectId = req.params.id;
    try {
        // Get source and target files
        const sourceFile = db.prepare("SELECT id FROM imported_files WHERE project_id = ? AND file_type = 'source' LIMIT 1").get(projectId) as any;
        const targetFile = db.prepare("SELECT id FROM imported_files WHERE project_id = ? AND file_type = 'target' LIMIT 1").get(projectId) as any;

        if (!sourceFile || !targetFile) {
            return res.status(400).json({ error: 'Source or Target file missing' });
        }

        const sourceCols = db.prepare('SELECT * FROM file_columns WHERE file_id = ?').all(sourceFile.id) as any[];
        const targetCols = db.prepare('SELECT * FROM file_columns WHERE file_id = ?').all(targetFile.id) as any[];

        const newMappings = [];

        // Simple name matching (case-insensitive, ignoring underscores/spaces)
        const normalize = (s: string) => s.toLowerCase().replace(/[^a-z0-9]/g, '');

        for (const sCol of sourceCols) {
            const sNameNorm = normalize(sCol.column_name);
            const match = targetCols.find(tCol => normalize(tCol.column_name) === sNameNorm);

            if (match) {
                newMappings.push({
                    sourceColumnId: sCol.id,
                    targetColumnId: match.id,
                    note: 'Auto-mapped by name'
                });
            }
        }

        res.json({ mappings: newMappings });
    } catch (error) {
        res.status(500).json({ error: (error as Error).message });
    }
});

// 8. Validate Project
app.post('/api/projects/:id/validate', (req, res) => {
    const projectId = req.params.id;

    try {
        // Fetch files
        const sourceFile = db.prepare("SELECT * FROM imported_files WHERE project_id = ? AND file_type = 'source' LIMIT 1").get(projectId) as any;
        const targetFile = db.prepare("SELECT * FROM imported_files WHERE project_id = ? AND file_type = 'target' LIMIT 1").get(projectId) as any;

        if (!sourceFile || !targetFile) {
            return res.status(400).json({ error: 'Missing source or target file' });
        }

        // Fetch Mappings
        const mappings = db.prepare('SELECT * FROM column_mappings WHERE project_id = ?').all(projectId) as any[];

        // Identify Key Column
        let keyMapping = mappings.find(m => {
            try { return JSON.parse(m.mapping_note || '{}').isKey; } catch (e) { return false; }
        });

        // If no key explicitly marked, try to find a mapped column named 'id' or similar, or fail
        // For this MVP, we REQUIRE a key.
        if (!keyMapping) {
            return res.status(400).json({ error: 'No Primary Key defined in mappings. Please select a Key column.' });
        }

        // Load Mapping Configs
        const config = mappings.map(m => {
            const note = JSON.parse(m.mapping_note || '{}');
            return {
                sourceId: m.source_column_id,
                targetId: m.target_column_id,
                isKey: note.isKey || false,
                codebookId: note.codebookFileId || null
            };
        });

        const sourceColInfo = db.prepare('SELECT * FROM file_columns WHERE file_id = ?').all(sourceFile.id) as any[];
        const targetColInfo = db.prepare('SELECT * FROM file_columns WHERE file_id = ?').all(targetFile.id) as any[];

        // Helper to get column index by ID
        const getIdx = (colId: number, info: any[]) => info.find(c => c.id === colId)?.column_index;

        const sourceKeyIdx = getIdx(keyMapping.source_column_id, sourceColInfo);
        const targetKeyIdx = getIdx(keyMapping.target_column_id, targetColInfo);

        if (sourceKeyIdx === undefined || targetKeyIdx === undefined) {
            return res.status(400).json({ error: 'Key columns not found in file definitions' });
        }

        // Load Data (Memory intensive for large files, but ok for MVP)
        const readSheet = (path: string) => {
            const wb = XLSX.readFile(path);
            return XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]], { header: 1, defval: '' }) as any[][];
        };

        const sourceRows = readSheet(sourceFile.stored_filename);
        const targetRows = readSheet(targetFile.stored_filename);

        // Remove Headers
        const sourceHeader = sourceRows.shift();
        const targetHeader = targetRows.shift();

        // Index Data by Key
        const sourceMap = new Map<string, any[]>();
        sourceRows.forEach((row, i) => sourceMap.set(String(row[sourceKeyIdx]), row));

        const targetMap = new Map<string, any[]>();
        targetRows.forEach((row, i) => targetMap.set(String(row[targetKeyIdx]), row));

        // Codebook Caches
        const codebookCache = new Map<number, Set<string>>();
        const getCodebookValues = (cbId: number) => {
            if (!codebookCache.has(cbId)) {
                const cbFile = db.prepare("SELECT * FROM imported_files WHERE id = ?").get(cbId) as any;
                if (cbFile) {
                    const rows = readSheet(cbFile.stored_filename) as any[][];
                    // Assume codebook values are in first column, start from row 1 (skip header)
                    const values = new Set(rows.slice(1).map(r => String(r[0])));
                    codebookCache.set(cbId, values);
                } else {
                    codebookCache.set(cbId, new Set());
                }
            }
            return codebookCache.get(cbId)!;
        };

        const results: { key: string; type: string; message?: string; column?: string; expected?: string; actual?: string }[] = [];

        // 1. Check Missing in Target
        for (const [key, sRow] of sourceMap) {
            if (!targetMap.has(key)) {
                results.push({
                    key,
                    type: 'missing_row',
                    message: `Row with Key ${key} missing in Target file`
                });
            } else {
                // Compare Rows
                const tRow = targetMap.get(key)!;

                for (const cfg of config) {
                    if (cfg.targetId) {
                        const sIdx = getIdx(cfg.sourceId, sourceColInfo);
                        const tIdx = getIdx(cfg.targetId, targetColInfo);

                        if (sIdx !== undefined && tIdx !== undefined) {
                            const sVal = String(sRow[sIdx]).trim();
                            const tVal = String(tRow[tIdx]).trim();

                            if (sVal !== tVal) {
                                results.push({
                                    key,
                                    type: 'value_mismatch',
                                    column: sourceColInfo.find(c => c.id === cfg.sourceId)?.column_name,
                                    expected: sVal,
                                    actual: tVal
                                });
                            }

                            // Check Codebook
                            if (cfg.codebookId) {
                                const allowed = getCodebookValues(cfg.codebookId);
                                if (!allowed.has(tVal) && tVal !== '') {
                                    results.push({
                                        key,
                                        type: 'codebook_violation',
                                        column: sourceColInfo.find(c => c.id === cfg.sourceId)?.column_name,
                                        message: `Value '${tVal}' not found in codebook`,
                                        actual: tVal
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        // 2. Check Extra in Target (optional)
        for (const [key] of targetMap) {
            if (!sourceMap.has(key)) {
                results.push({
                    key,
                    type: 'extra_row',
                    message: `Row with Key ${key} extra in Target file`
                });
            }
        }

        // Save results to DB (validation_results) 
        // For MVP, just returning JSON

        // Log last run
        const deleteRes = db.prepare('DELETE FROM validation_results WHERE project_id = ?');
        const insertRes = db.prepare('INSERT INTO validation_results (project_id, column_mapping_id, error_message, actual_value, expected_value) VALUES (?, ?, ?, ?, ?)');

        db.transaction(() => {
            deleteRes.run(projectId);
            for (const r of results) {
                // We'll simplify the schema usage here slightly for speed
                insertRes.run(projectId, null, `${r.type}: ${r.message || ''} (Key: ${r.key})`, r.actual || '', r.expected || '');
            }
        })();

        res.json({ success: true, issuesCount: results.length, limit: 100, issues: results.slice(0, 100) });

    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// Serve static frontend in production
if (process.env.NODE_ENV === 'production') {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));

    app.get(/.*/, (req, res) => {
        res.sendFile(path.join(distPath, 'index.html'));
    });
}

app.listen(Number(PORT), '0.0.0.0', () => {
    console.log(`Server running on http://0.0.0.0:${PORT}`);
});
