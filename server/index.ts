import express from 'express';
import cors from 'cors';
import path from 'path';
import multer from 'multer';
import db from './db.js';
import * as XLSX_PKG from 'xlsx';
const { readFile, utils } = (XLSX_PKG as any).default ?? XLSX_PKG;
import fs from 'fs';

const app = express();
const PORT = process.env.PORT || 8080;

app.use(cors());
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Initialize Database
db.init();

// Debug health check
app.get('/api/health', (req, res) => {
    res.json({ status: 'ok', time: new Date().toISOString() });
});

// Multer setup for file uploads
const isWindows = process.platform === 'win32';
const UPLOADS_DIR = process.env.UPLOADS_DIR || (isWindows ? 'uploads/' : '/tmp/uploads/');

if (!fs.existsSync(UPLOADS_DIR)) {
    fs.mkdirSync(UPLOADS_DIR, { recursive: true });
}

// GLOBAL DEBUG LOG BUFFER for diagnosing 500 errors
const serverLogs: string[] = [];
function serverLog(msg: string) {
    const time = new Date().toISOString().split('T')[1].substring(0, 8);
    const line = `[${time}] ${msg}`;
    console.log(line);
    serverLogs.push(line);
    if (serverLogs.length > 200) serverLogs.shift(); // Keep last 200 lines
}

app.get('/api/debug/logs', (req, res) => {
    res.json({ logs: serverLogs });
});

const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, UPLOADS_DIR);
    },
    filename: (req, file, cb) => {
        // Sanitize filename to avoid filesystem issues
        const safeName = file.originalname.replace(/[^a-z0-9.]/gi, '_');
        const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1e9);
        cb(null, uniqueSuffix + '-' + safeName);
    }
});
const upload = multer({ storage: storage });

// --- API ROUTES ---

// Health Check endpoint
app.get('/health', (req, res) => {
    res.status(200).send('OK');
});

// 1. Get all projects
app.get('/api/projects', async (req, res) => {
    try {
        const projects = await db.query('SELECT * FROM validation_projects ORDER BY created_at DESC');
        res.json(projects);
    } catch (error) {
        res.status(500).json({ error: (error as Error).message });
    }
});

// 2. Create new project
app.post('/api/projects', async (req, res) => {
    const { name, description } = req.body;
    if (!name) return res.status(400).json({ error: 'Project name is required' });

    try {
        const result = await db.run('INSERT INTO validation_projects (name, description) VALUES (?, ?)', [name, description || '']);
        res.json({ id: result.id, name, description });
    } catch (error) {
        res.status(500).json({ error: (error as Error).message });
    }
});

// 2b. Delete project (cascade delete all related data)
app.delete('/api/projects/:id', async (req, res) => {
    const projectId = req.params.id;

    try {
        // Delete in order of dependencies
        await db.run('DELETE FROM validation_results WHERE project_id = ?', [projectId]);
        await db.run('DELETE FROM column_mappings WHERE project_id = ?', [projectId]);

        // Get files to delete their columns
        const files = await db.query('SELECT id FROM imported_files WHERE project_id = ?', [projectId]);
        for (const file of files) {
            await db.run('DELETE FROM file_columns WHERE file_id = ?', [file.id]);
        }

        await db.run('DELETE FROM imported_files WHERE project_id = ?', [projectId]);
        await db.run('DELETE FROM validation_projects WHERE id = ?', [projectId]);

        res.json({ success: true, message: 'Project deleted' });
    } catch (error) {
        res.status(500).json({ error: (error as Error).message });
    }
});

// 3. Upload File & Analyze Columns
app.post('/api/projects/:id/files', upload.single('file'), async (req, res) => {
    const projectId = req.params.id;
    const fileType = req.body.fileType; // 'source' | 'target' | 'codebook'

    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    if (!['source', 'target', 'codebook', 'exclusion'].includes(fileType)) {
        return res.status(400).json({ error: 'Invalid file type' });
    }

    try {
        const filePath = req.file.path;

        // 1. Insert into DB (Metadata only)
        const fileInfo = await db.run(
            'INSERT INTO imported_files (project_id, original_filename, file_type, stored_filename) VALUES (?, ?, ?, ?)',
            [projectId, req.file.originalname, fileType, filePath]
        );
        const fileId = fileInfo.id;

        // 2. Parse Excel & Batch Insert Rows
        let jsonData: any[][] = [];
        let parseWarning = null;
        try {
            if (!fs.existsSync(filePath)) throw new Error(`File not found: ${filePath}`);

            // Basic size check logic removed (we handle via batch)
            const stats = fs.statSync(filePath);
            if (stats.size === 0) throw new Error('File is empty');

            const wb = readFile(filePath);
            const sheet = wb.Sheets[wb.SheetNames[0]];
            jsonData = utils.sheet_to_json(sheet, { header: 1, defval: '' });

            // BLOB STORAGE: Update file_data with full JSON content
            if (jsonData.length > 0) {
                // Optimization: Store as stringified JSON in DB
                // Postgres TEXT column can hold up to 1GB, so 50MB is fine.
                await db.run('UPDATE imported_files SET file_data = ? WHERE id = ?', [JSON.stringify(jsonData), fileId]);
            }

        } catch (parseErr) {
            console.error('Error parsing/saving Excel:', parseErr);
            parseWarning = `Failed: ${(parseErr as Error).message}`;
        }


        // 3. Insert column definitions
        if (jsonData && jsonData.length > 0) {
            const headers = jsonData[0];
            const firstRow = jsonData.length > 1 ? jsonData[1] : [];

            for (let idx = 0; idx < headers.length; idx++) {
                const name = String(headers[idx] || `Column ${idx + 1}`).trim();
                const sample = String(firstRow[idx] || '').substring(0, 100);

                await db.run('INSERT INTO file_columns (file_id, column_name, column_index, sample_value) VALUES (?, ?, ?, ?)',
                    [fileId, name, idx, sample]);
            }
        } else if (!parseWarning) {
            parseWarning = 'Excel sheet appears to be empty or could not be parsed.';
        }

        // Clean up temp file (optional - data is now in DB)
        try {
            fs.unlinkSync(filePath);
        } catch (e) {
            console.log('Could not delete temp file:', filePath);
        }

        res.json({ success: true, fileId, warning: parseWarning });
    } catch (error) {
        console.error('Upload critical error:', error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// 3.1 Delete File
app.delete('/api/files/:id', async (req, res) => {
    try {
        const fileId = req.params.id;
        // Get file info first to delete physical file
        const file = await db.get('SELECT * FROM imported_files WHERE id = ?', [fileId]);

        if (file) {
            // Manual Cleanup of Dependencies
            await db.run(`
                DELETE FROM column_mappings 
                WHERE source_column_id IN (SELECT id FROM file_columns WHERE file_id = ?) 
                   OR target_column_id IN (SELECT id FROM file_columns WHERE file_id = ?)
            `, [fileId, fileId]);

            await db.run('DELETE FROM file_columns WHERE file_id = ?', [fileId]);
            await db.run('DELETE FROM imported_files WHERE id = ?', [fileId]);

            // Try delete physical file
            if (file.stored_filename && fs.existsSync(file.stored_filename)) {
                try {
                    fs.unlinkSync(file.stored_filename);
                } catch (e) {
                    console.warn('Could not delete file from disk', e);
                }
            }
        }
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: (error as Error).message });
    }
});

// 3.2 Bulk Delete Files
app.delete('/api/projects/:id/files', async (req, res) => {
    const projectId = req.params.id;
    const fileType = req.query.type; // 'source', 'target', 'codebook' or undefined (all)

    try {
        let query = "SELECT * FROM imported_files WHERE project_id = ?";
        const params = [projectId];

        if (fileType) {
            query += " AND file_type = ?";
            params.push(String(fileType));
        }

        const filesToDelete = await db.query(query, params);

        for (const file of filesToDelete) {
            const fileId = file.id;

            // Cleanup DB References
            await db.run(`
                DELETE FROM column_mappings 
                WHERE source_column_id IN (SELECT id FROM file_columns WHERE file_id = ?) 
                   OR target_column_id IN (SELECT id FROM file_columns WHERE file_id = ?)
            `, [fileId, fileId]);

            await db.run('DELETE FROM file_columns WHERE file_id = ?', [fileId]);
            await db.run('DELETE FROM imported_files WHERE id = ?', [fileId]);

            // Cleanup Disk
            if (file.stored_filename && fs.existsSync(file.stored_filename)) {
                try { fs.unlinkSync(file.stored_filename); } catch (e) { }
            }
        }

        res.json({ success: true, count: filesToDelete.length });
    } catch (error) {
        console.error("Bulk delete error:", error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// 4. Get Project Files and Columns
app.get('/api/projects/:id/details', async (req, res) => {
    try {
        const files = await db.query('SELECT * FROM imported_files WHERE project_id = ?', [req.params.id]);

        for (const file of files) {
            file.columns = await db.query('SELECT * FROM file_columns WHERE file_id = ? ORDER BY column_index', [file.id]);
        }

        res.json({ files });
    } catch (error) {
        res.status(500).json({ error: (error as Error).message });
    }
});

// 5. Get Mappings
app.get('/api/projects/:id/mappings', async (req, res) => {
    try {
        const mappings = await db.query(`
            SELECT m.*, 
                   sc.column_name as source_name, sc.sample_value as source_sample,
                   tc.column_name as target_name, tc.sample_value as target_sample
            FROM column_mappings m
            LEFT JOIN file_columns sc ON m.source_column_id = sc.id
            LEFT JOIN file_columns tc ON m.target_column_id = tc.id
            WHERE m.project_id = ?
        `, [req.params.id]);
        res.json(mappings);
    } catch (error) {
        res.status(500).json({ error: (error as Error).message });
    }
});

// 6. Save Mappings
app.post('/api/projects/:id/mappings', async (req, res) => {
    const projectId = req.params.id;
    const { mappings } = req.body; // Array of { sourceColumnId, targetColumnId, note }

    if (!Array.isArray(mappings)) {
        return res.status(400).json({ error: 'Mappings must be an array' });
    }

    try {
        await db.run('DELETE FROM column_mappings WHERE project_id = ?', [projectId]);

        for (const m of mappings) {
            await db.run('INSERT INTO column_mappings (project_id, source_column_id, target_column_id, mapping_note) VALUES (?, ?, ?, ?)',
                [projectId, m.sourceColumnId, m.targetColumnId, m.note || '']);
        }

        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: (error as Error).message });
    }
});

// 7. Auto-Map & Global Discovery (Smart Metadata Mode)
app.post('/api/projects/:id/auto-map', async (req, res) => {
    // Force JSON response
    res.setHeader('Content-Type', 'application/json');

    const projectId = req.params.id;
    const { sourceFileId, targetFileId } = req.body;

    const debugLogs: string[] = [];
    const log = (msg: string) => {
        debugLogs.push(msg);
        serverLog(`[AutoMap ${projectId}] ${msg}`);
    };

    try {
        log(`Starting Metadata-Based Auto-Map for project ${projectId}`);

        // Fetch files (metadata)
        const allFiles = await db.query("SELECT id, project_id, original_filename, file_type FROM imported_files WHERE project_id = ?", [projectId]);

        const targets = allFiles.filter((f: any) => f.file_type === 'target');
        const sources = allFiles.filter((f: any) => f.file_type === 'source');
        const codebooks = allFiles.filter((f: any) => f.file_type === 'codebook');
        const potentialSources = [...sources, ...codebooks];

        log(`Found ${targets.length} targets and ${potentialSources.length} sources.`);

        const newMappings = [];
        const normalize = (s: string) => String(s).toLowerCase().replace(/[^a-z0-9]/g, '');

        // Loop Targets
        for (const tFile of targets) {
            log(`Analyzing Target: ${tFile.original_filename}`);

            // Fetch Columns (Metadata Only - NO BLOB LOADING)
            const targetCols = await db.query('SELECT * FROM file_columns WHERE file_id = ?', [tFile.id]);

            // Find Best Source File based on Column Name Matches
            let bestSource = null;
            let bestScore = 0;
            let bestMappings: any[] = [];

            if (targetCols.length > 2000) {
                serverLog(`Warning: Too many columns (${targetCols.length}). Truncating to 2000.`);
                targetCols.length = 2000;
            }

            for (const sFile of potentialSources) {
                const sourceCols = await db.query('SELECT * FROM file_columns WHERE file_id = ?', [sFile.id]);
                if (sourceCols.length > 2000) sourceCols.length = 2000;

                let fileScore = 0;
                let fileMappings = [];

                // Compare Columns
                for (let i = 0; i < targetCols.length; i++) {
                    // YIELD to event loop to prevent watchdog kill
                    if (i % 50 === 0) await new Promise(r => setImmediate(r));

                    const tCol = targetCols[i];
                    // Find best match in source cols
                    let bestColMatch = null;
                    let bestColScore = 0;

                    for (const sCol of sourceCols) {
                        let score = 0;
                        const n1 = normalize(tCol.column_name);
                        const n2 = normalize(sCol.column_name);

                        // Exact match
                        if (n1 === n2) score = 1.0;
                        // Partial match (if long enough)
                        else if (n1.length > 3 && n2.length > 3 && (n1.includes(n2) || n2.includes(n1))) score = 0.6;

                        // ID/Code heuristic
                        if ((n1 === 'id' || n1.endsWith('id')) && (n2 === 'id' || n2.endsWith('id'))) score += 0.2;

                        if (score > bestColScore && score > 0.5) {
                            bestColScore = score;
                            bestColMatch = sCol;
                        }
                    }

                    if (bestColMatch) {
                        fileScore += bestColScore;
                        fileMappings.push({
                            sourceColumnId: bestColMatch.id,
                            targetColumnId: tCol.id,
                            sourceColName: bestColMatch.column_name,
                            score: bestColScore,
                            codebookFileId: sFile.file_type === 'codebook' ? sFile.id : null
                        });
                    }
                }

                if (fileMappings.length > 0) {
                    // Normalize score by file size (optional, but raw score is fine for now)
                    if (fileScore > bestScore) {
                        bestScore = fileScore;
                        bestSource = sFile;
                        bestMappings = fileMappings;
                    }
                }
            }

            if (bestSource && bestMappings.length > 0) {
                log(` => Matched with ${bestSource.original_filename} (Score: ${bestScore.toFixed(1)})`);

                // Add mappings
                if (keyCandidate) {
                    log(`   Selected Primary Key: ${keyCandidate.sourceColName}`);
                } else {
                    log(`   WARNING: No suitable Primary Key found for this mapping!`);
                }

                const bindings = bestMappings.map(m => ({
                    sourceColumnId: m.sourceColumnId,
                    targetColumnId: m.targetColumnId,
                    note: JSON.stringify({
                        isKey: (keyCandidate && m.sourceColumnId === keyCandidate.sourceColumnId) || false,
                        codebookFileId: m.codebookFileId,
                        autoDiscovered: true,
                        strategy: 'metadata_name_match'
                    })
                }));

                // Safe Array Append
                for (const b of bindings) newMappings.push(b);
            }
        }

        // SAVE to DB
        if (newMappings.length > 0) {
            serverLog(`Saving ${newMappings.length} mappings to DB...`);
            await db.run('DELETE FROM column_mappings WHERE project_id = ?', [projectId]);

            // Batch insert or sequential
            for (const m of newMappings) {
                await db.run('INSERT INTO column_mappings (project_id, source_column_id, target_column_id, mapping_note) VALUES (?, ?, ?, ?)',
                    [projectId, m.sourceColumnId, m.targetColumnId, m.note]);
            }
            serverLog(`Saved successfully.`);
        }

        res.json({ mappings: newMappings, logs: debugLogs });

    } catch (e) {
        log(`Error: ${(e as Error).message}`);
        res.status(500).json({ error: (e as Error).message, logs: debugLogs });
    }
});

// 8. Validate Project (Multi-file Support)
app.post('/api/projects/:id/validate', async (req, res) => {
    const projectId = req.params.id;
    const { scopeFileId } = req.body;

    try {
        serverLog(`Starting Validation for Project ${projectId}`);
        // Fetch Metadata Only
        const allFiles = await db.query("SELECT id, project_id, original_filename, file_type, stored_filename FROM imported_files WHERE project_id = ?", [projectId]);

        const getFile = (id: number) => allFiles.find((f: any) => f.id === id);

        // Fetch all mappings
        const mappings = await db.query('SELECT * FROM column_mappings WHERE project_id = ?', [projectId]);

        if (mappings.length === 0) {
            return res.status(400).json({ error: 'No mappings defined. Please map columns first.' });
        }

        // Fetch all column definitions
        const allColumns = await db.query(
            `SELECT fc.*, f.file_type 
             FROM file_columns fc 
             JOIN imported_files f ON fc.file_id = f.id 
             WHERE f.project_id = ? `,
            [projectId]
        );

        const getCol = (id: number) => allColumns.find((c: any) => c.id === id);

        // Helper to get cached sheet data from DB
        const sheetCache = new Map<number, any[][]>();
        const readSheet = async (file: any) => {
            if (!file) return [];
            if (sheetCache.has(file.id)) return sheetCache.get(file.id)!;

            try {
                // BLOB FETCH
                const res = await db.query('SELECT file_data FROM imported_files WHERE id = ?', [file.id]);
                if (res && res[0] && res[0].file_data) {
                    const data = JSON.parse(res[0].file_data);
                    sheetCache.set(file.id, data);
                    return data;
                } else {
                    // Fallback
                    const rowsRes = await db.query('SELECT row_data FROM imported_file_rows WHERE file_id = ? ORDER BY row_index ASC', [file.id]);
                    if (rowsRes && rowsRes.length > 0) {
                        const data = rowsRes.map(r => JSON.parse(r.row_data));
                        sheetCache.set(file.id, data);
                        return data;
                    }
                }
            } catch (e) { console.error("Error fetching DB rows", e); }

            return [];
        };

        // PREPARE SCOPE
        let allowedScopeKeys: Set<string> | null = null;
        let scopeKeyColName: string = '';

        if (scopeFileId) {
            // Find mapping where Target is ScopeFile AND isKey=true
            // (Standard Key Logic: SourceKey -> TargetKey)
            let scopeMapping = null;

            // Search all mappings to find which column in ScopeFile is the Key
            for (const m of mappings) {
                const tCol = getCol(m.target_column_id);
                if (tCol && tCol.file_id === scopeFileId) {
                    try {
                        if (JSON.parse(m.mapping_note || '{}').isKey) {
                            scopeMapping = m;
                            break;
                        }
                    } catch (e) { }
                }
            }

            if (scopeMapping) {
                const tCol = getCol(scopeMapping.target_column_id);
                const sCol = getCol(scopeMapping.source_column_id);
                scopeKeyColName = sCol ? sCol.column_name : ''; // Use Source Name as the "Global Key Name" (e.g. AccountNum)

                const scopeFile = getFile(scopeFileId);
                const scopeRows = await readSheet(scopeFile); // AWAIT and pass file object
                // Assume Header at 0
                const keyIdx = tCol.column_index;
                allowedScopeKeys = new Set();
                for (let i = 1; i < scopeRows.length; i++) {
                    const val = String(scopeRows[i][keyIdx]).trim();
                    if (val) allowedScopeKeys.add(val);
                }
                console.log(`Validation Scope Enabled: ${scopeKeyColName} (${allowedScopeKeys.size} allowed IDs)`);
            }
        }


        // Group mappings by Source File
        const filePairs = new Map<number, { targetFileId: number, mappings: any[] }>();

        for (const m of mappings) {
            const sCol = getCol(m.source_column_id);
            if (!sCol) continue;
            const sourceFileId = sCol.file_id;
            // Identify target file
            let targetFileId = null;
            if (m.target_column_id) {
                const tCol = getCol(m.target_column_id);
                if (tCol) targetFileId = tCol.file_id;
            }
            if (targetFileId) {
                if (!filePairs.has(sourceFileId)) filePairs.set(sourceFileId, { targetFileId, mappings: [] });
                const group = filePairs.get(sourceFileId);
                if (group && group.targetFileId === targetFileId) group.mappings.push(m);
            }
        }

        const results: any[] = [];
        const codebookCache = new Map<number, Set<string>>();

        const getCodebookValues = async (cbId: number) => {
            if (!codebookCache.has(cbId)) {
                const cbFile = getFile(cbId);
                if (cbFile) {
                    const rows = await readSheet(cbFile) as any[][]; // AWAIT and pass file object
                    let keyIdx = 0; // Default for Codebooks (Col A)

                    // If referencing another Export/Source file (Consistency Check), we must use its defined PRIMARY KEY column
                    if (cbFile.file_type !== 'codebook') {
                        for (const m of mappings) {
                            const tCol = getCol(m.target_column_id);
                            if (tCol && tCol.file_id === cbId) {
                                try {
                                    if (JSON.parse(m.mapping_note || '{}').isKey) {
                                        keyIdx = tCol.column_index;
                                        break;
                                    }
                                } catch (e) { }
                            }
                        }
                    }

                    const values = new Set<string>();
                    for (let i = 1; i < rows.length; i++) {
                        const val = String(rows[i][keyIdx]).trim();
                        if (val) values.add(val);
                    }
                    codebookCache.set(cbId, values);
                } else { codebookCache.set(cbId, new Set()); }
            }
            return codebookCache.get(cbId)!;
        };

        // Iterate over each file pair and validate
        for (const [sFileId, group] of filePairs) {
            const sFile = getFile(sFileId);
            const tFile = getFile(group.targetFileId);

            if (!sFile || !tFile) continue;

            const fileLabel = `${sFile.original_filename} -> ${tFile.original_filename}`;

            // Find Key Mapping
            const keyMapping = group.mappings.find((m: any) => {
                try { return JSON.parse(m.mapping_note || '{}').isKey; } catch (e) { return false; }
            });

            if (!keyMapping) {
                results.push({ key: 'setup', type: 'error', message: `No Primary Key defined for ${fileLabel}`, file: fileLabel });
                continue;
            }

            const sourceKeyIdx = getCol(keyMapping.source_column_id)?.column_index;
            const targetKeyIdx = getCol(keyMapping.target_column_id)?.column_index;

            // Load Data
            const sourceRows = await readSheet(sFile); // AWAIT and pass file object
            const targetRows = await readSheet(tFile); // AWAIT and pass file object

            const sourceHeaders = sourceRows[0]; // Needed for finding Scope Column by name

            // DETERMINE SCOPE COLUMN IN SOURCE (FK Check)
            let sourceScopeColIdx = -1;
            if (allowedScopeKeys && scopeKeyColName) {
                // Find the column index in the source file that matches the scopeKeyColName
                if (sourceHeaders) {
                    sourceScopeColIdx = sourceHeaders.findIndex((header: string) => String(header).trim().toLowerCase() === scopeKeyColName.toLowerCase());
                }
            }

            // Build Maps & Check Duplicates
            const sourceMap = new Map<string, any[]>();
            let duplicatesFound = 0;
            let exampleDuplicate = '';

            // Fill Source Map (with Filtering)
            for (let i = 1; i < sourceRows.length; i++) {
                const r = sourceRows[i];

                // FILTER: Check if this row belongs to scope
                if (allowedScopeKeys && sourceScopeColIdx !== -1 && sourceScopeColIdx < r.length) {
                    const fkVal = String(r[sourceScopeColIdx]).trim();
                    if (!allowedScopeKeys.has(fkVal)) continue; // SKIP row out of scope
                }

                const k = String(r[sourceKeyIdx]).trim();
                if (!k) continue; // Skip empty keys

                if (sourceMap.has(k)) {
                    duplicatesFound++;
                    if (!exampleDuplicate) exampleDuplicate = k;
                } else {
                    sourceMap.set(k, r);
                }
            }

            if (duplicatesFound > 0) {
                results.push({
                    key: 'setup',
                    type: 'error',
                    message: `Primary Key '${getCol(keyMapping.source_column_id)?.column_name}' is NOT unique. Found ${duplicatesFound} duplicates (e.g. '${exampleDuplicate}'). Validation aborted for this file.`,
                    file: fileLabel
                });
                continue; // Skip comparing this file pair
            }

            const targetMap = new Map<string, any[]>();
            // Target is just loaded as is
            for (let i = 1; i < targetRows.length; i++) {
                const r = targetRows[i];
                const k = String(r[targetKeyIdx]).trim();
                if (k) targetMap.set(k, r);
            }

            // 1. Check Missing Records in Target
            for (const [key, sRow] of sourceMap) {
                if (!targetMap.has(key)) {
                    results.push({
                        key, type: 'missing_row', message: `Row missing in Target`, file: fileLabel, expected: 'Present', actual: 'Missing'
                    });
                    continue;
                }

                // Compare Columns
                const tRow = targetMap.get(key)!;

                for (const m of group.mappings) {
                    if (m === keyMapping) continue; // Skip the key mapping itself

                    const sColDef = getCol(m.source_column_id);
                    const tColDef = getCol(m.target_column_id);

                    if (!sColDef || !tColDef) continue;

                    const sVal = String(sRow[sColDef.column_index]).trim();
                    const tVal = String(tRow[tColDef.column_index]).trim();
                    const sColName = sColDef.column_name;

                    let cbId = null;
                    try { cbId = JSON.parse(m.mapping_note || '{}').codebookFileId; } catch (e) { }

                    if (cbId) {
                        const validValues = await getCodebookValues(cbId);
                        if (!validValues.has(tVal) && tVal !== '') {
                            results.push({ key, type: 'codebook_violation', message: `Value '${tVal}' not in codebook`, file: fileLabel, column: sColName, actual: tVal });
                        }
                    }

                    if (sVal !== tVal) {
                        results.push({
                            key, type: 'value_mismatch', message: 'Value mismatch', file: fileLabel, column: sColName, expected: sVal, actual: tVal
                        });
                    }
                }
            }
        } // end loop

        // ===========================================
        // EXCLUSION LIST VALIDATION
        // Check if export files contain forbidden values from 'exclusion' files
        // ===========================================
        const exclusionFiles = allFiles.filter((f: any) => f.file_type === 'exclusion');

        if (exclusionFiles.length > 0) {
            console.log(`[Validation] Checking ${exclusionFiles.length} exclusion file(s)...`);

            // Build exclusion sets (column name -> forbidden values)
            const exclusionMap = new Map<string, Set<string>>();

            for (const exFile of exclusionFiles) {
                const exRows = await readSheet(exFile); // AWAIT and pass file object
                if (exRows.length < 2) continue;

                const headers = exRows[0] as string[];

                // Each column in exclusion file is a separate exclusion list
                for (let colIdx = 0; colIdx < headers.length; colIdx++) {
                    const colName = String(headers[colIdx]).trim().toLowerCase();
                    if (!colName) continue;

                    if (!exclusionMap.has(colName)) {
                        exclusionMap.set(colName, new Set());
                    }

                    // Add all values from this column
                    for (let r = 1; r < exRows.length; r++) {
                        const val = String(exRows[r][colIdx]).trim();
                        if (val) exclusionMap.get(colName)!.add(val);
                    }
                }
            }

            console.log(`[Validation] Loaded exclusion lists for columns: ${Array.from(exclusionMap.keys()).join(', ')}`);

            // Check each Target (Export) file against exclusions
            const targetFiles = allFiles.filter((f: any) => f.file_type === 'target');

            for (const tFile of targetFiles) {
                const tRows = await readSheet(tFile);
                if (tRows.length < 2) continue;

                const headers = tRows[0] as string[];

                // Check each column that has an exclusion list
                for (let colIdx = 0; colIdx < headers.length; colIdx++) {
                    const colName = String(headers[colIdx]).trim().toLowerCase();

                    if (!exclusionMap.has(colName)) continue;

                    const forbiddenValues = exclusionMap.get(colName)!;

                    // Check each row in export
                    for (let r = 1; r < tRows.length; r++) {
                        const val = String(tRows[r][colIdx]).trim();

                        if (val && forbiddenValues.has(val)) {
                            results.push({
                                key: `row_${r}`,
                                type: 'exclusion_violation',
                                message: `Forbidden value '${val}' found in column '${headers[colIdx]}'`,
                                file: tFile.original_filename,
                                column: headers[colIdx],
                                actual: val
                            });
                        }
                    }
                }
            }
        }

        // Clean & Save Results
        await db.run('DELETE FROM validation_results WHERE project_id = ?', [projectId]);

        // Batch insert (simplified loop)
        const stmt = 'INSERT INTO validation_results (project_id, column_mapping_id, error_message, actual_value, expected_value) VALUES (?, ?, ?, ?, ?)';

        for (const r of results) {
            const fullMessage = `[${r.file || 'unknown'}] ${r.type}: ${r.column ? r.column + ' - ' : ''}${r.message} (Key: ${r.key})`;
            await db.run(stmt, [
                projectId,
                null,
                fullMessage,
                r.actual || '',
                r.expected || ''
            ]);
        }

        res.json({ success: true, issuesCount: results.length, limit: 100, issues: results.slice(0, 100) });


    } catch (error) {
        console.error('Validation Error:', error);
        res.status(500).json({ error: (error as Error).message });
    }
});

app.get('/api/debug/rows/:fileId', async (req, res) => {
    try {
        const fileId = req.params.fileId;
        const countRes = await db.query('SELECT COUNT(*) as c FROM imported_file_rows WHERE file_id = ?', [fileId]);
        res.json({ count: countRes[0].c });
    } catch (e) {
        res.status(500).json({ error: (e as Error).message });
    }
});

// Serve static frontend in production
if (process.env.NODE_ENV === 'production') {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));

    // Catch-all for SPA routing - use middleware instead of route pattern
    app.use((req, res, next) => {
        if (req.path.startsWith('/api')) {
            return next(); // Let API routes 404 properly
        }
        res.sendFile(path.join(distPath, 'index.html'));
    });
}

app.listen(Number(PORT), '0.0.0.0', () => {
    console.log(`Server running on http://0.0.0.0:${PORT}`);
    serverLog(`SERVER RESTART detected. Time: ${new Date().toISOString()}`);
});

