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

            // BATCH INSERT ROWS
            const BATCH_SIZE = 500;
            for (let i = 0; i < jsonData.length; i += BATCH_SIZE) {
                const chunk = jsonData.slice(i, i + BATCH_SIZE);
                if (chunk.length === 0) break;

                const placeholders = chunk.map(() => '(?, ?, ?)').join(',');
                const values = [];
                for (let c = 0; c < chunk.length; c++) {
                    values.push(fileId, i + c, JSON.stringify(chunk[c]));
                }

                // Use query for batch insert (db.run expects RETURNING id for single row usually)
                await db.query(`INSERT INTO imported_file_rows (file_id, row_index, row_data) VALUES ${placeholders}`, values);
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

// 7. Auto-Map & Global Discovery (Smart Discovery)
app.post('/api/projects/:id/auto-map', async (req, res) => {
    // Force JSON response
    res.setHeader('Content-Type', 'application/json');

    const projectId = req.params.id;
    const { sourceFileId, targetFileId } = req.body; // Optional: restrict to pair if user wants

    const debugLogs: string[] = [];
    const log = (msg: string) => {
        debugLogs.push(msg);
        serverLog(`[AutoMap ${projectId}] ${msg}`);
    };

    try {
        log(`Starting auto-map for project ${projectId}`);

        // 1. Fetch all files
        let allFiles: any[];
        try {
            allFiles = await db.query("SELECT * FROM imported_files WHERE project_id = ?", [projectId]);
            log(`Found ${allFiles.length} files`);
        } catch (dbErr: any) {
            log(`DB Error: ${dbErr.message}`);
            return res.status(500).json({ error: `Database error: ${dbErr.message}`, logs: debugLogs });
        }

        const targets = allFiles.filter((f: any) => f.file_type === 'target' && (!targetFileId || f.id === targetFileId));
        const sources = allFiles.filter((f: any) => f.file_type === 'source' && (!sourceFileId || f.id === sourceFileId));
        const codebooks = allFiles.filter((f: any) => f.file_type === 'codebook');

        // Combined potential sources (Source of Truth + Codebooks)
        const potentialSources = [...sources, ...codebooks];

        if (targets.length === 0 || potentialSources.length === 0) {
            return res.json({ mappings: [], logs: ["Not enough files to perform discovery."] });
        }

        // Helper: Load Column Data for a file (ASYNC now, but we need to fetch inside)
        // Since loadFileData was synchronous-ish in loop, we need to adapt.
        // Actually the loop awaits db calls, so we can make loadFileData async.
        const fileDataCache = new Map<number, { headers: any[], colData: Map<number, Set<string>>, rowsCount: number }>();

        const loadFileData = async (file: any, limit: number = 0) => {
            if (fileDataCache.has(file.id)) return fileDataCache.get(file.id)!;

            let rows: any[][] = [];
            try {
                // Fetch rows from DB with optional limit
                let sql = 'SELECT row_data FROM imported_file_rows WHERE file_id = ? ORDER BY row_index ASC';
                const params = [file.id];
                if (limit > 0) {
                    sql += ' LIMIT ?';
                    params.push(limit);
                }

                const rowsRes = await db.query(sql, params);
                if (rowsRes && rowsRes.length > 0) {
                    rows = rowsRes.map(r => JSON.parse(r.row_data));
                } else {
                    // Fallback or empty
                    log(`Warning: No DB rows for file ${file.original_filename}.`);
                    return null;
                }
            } catch (e) {
                log(`Error fetching DB rows for ${file.original_filename}: ${(e as Error).message}`);
                return null;
            }

            if (!rows || rows.length < 2) return null;

            const headers = rows[0];
            const colData = new Map<number, Set<string>>();

            // Sample data (optimize: taking max 200 rows for signature analysis)
            // We already limited DB fetch if limit > 0
            const sampleLimit = Math.min(rows.length, 200);
            for (let r = 1; r < sampleLimit; r++) {
                rows[r].forEach((val: any, idx: number) => {
                    if (!colData.has(idx)) colData.set(idx, new Set());
                    const s = String(val).trim();
                    if (s) colData.get(idx)!.add(s);
                });
            }

            const result = { headers, colData, rowsCount: rows.length }; // Note: rowsCount is sample size if limited
            fileDataCache.set(file.id, result);
            return result;
        };

        const newMappings = [];
        const normalize = (s: string) => String(s).toLowerCase().replace(/[^a-z0-9]/g, '');

        log(`Starting Discovery. Targets: ${targets.length}, Sources: ${potentialSources.length}`);

        // DISCOVERY LOOP
        for (const tFile of targets) {
            log(`Analyzing Target File: ${tFile.original_filename}`);
            const tData = await loadFileData(tFile, 100);
            if (!tData) continue;

            const targetColsDB = await db.query('SELECT * FROM file_columns WHERE file_id = ?', [tFile.id]);

            // We want to find the BEST matching Source file for this Target file
            // Score = number of column matches
            let bestSourceFile = null;
            let bestSourceScore = 0;
            let bestSourceMappings: any[] = [];

            for (const sFile of potentialSources) {
                const sData = await loadFileData(sFile, 100);
                if (!sData) continue;

                const sourceColsDB = await db.query('SELECT * FROM file_columns WHERE file_id = ?', [sFile.id]);

                let fileMatchScore = 0;
                let fileMappings = [];

                // Compare Columns
                for (let tColIdx = 0; tColIdx < targetColsDB.length; tColIdx++) {
                    const tCol = targetColsDB[tColIdx];
                    // Yield to event loop every 10 columns to prevent blocking
                    if (tColIdx % 10 === 0) await new Promise(resolve => setImmediate(resolve));

                    const tVals = tData.colData.get(tCol.column_index);
                    if (!tVals || tVals.size === 0) continue;

                    let bestColMatch = null;
                    let bestColScore = 0;

                    for (const sCol of sourceColsDB) {
                        const sVals = sData.colData.get(sCol.column_index);
                        if (!sVals || sVals.size === 0) continue;

                        // Similarity Score
                        // 1. Header Name Similarity
                        let nameScore = 0;
                        if (normalize(tCol.column_name) === normalize(sCol.column_name)) nameScore = 0.4;
                        else if (normalize(tCol.column_name).includes(normalize(sCol.column_name))) nameScore = 0.2;

                        // 2. Data Content Overlap (Intersection)
                        let hits = 0;
                        let sampleSize = 0;
                        // Check samples from Target against Source set
                        for (const val of tVals) {
                            sampleSize++;
                            if (sVals.has(val)) hits++;
                        }

                        const overlapScore = sampleSize > 0 ? (hits / sampleSize) : 0; // 0.0 - 1.0

                        const totalScore = overlapScore + nameScore;

                        if (totalScore > bestColScore && totalScore > 0.5) { // Threshold
                            bestColScore = totalScore;
                            bestColMatch = sCol;
                        }
                    }

                    if (bestColMatch) {
                        fileMatchScore += bestColScore;
                        fileMappings.push({
                            sourceColumnId: bestColMatch.id,
                            targetColumnId: tCol.id,
                            sourceColName: bestColMatch.column_name, // Capture name for Key guessing
                            score: bestColScore,
                            isKey: false,
                            codebookFileId: sFile.file_type === 'codebook' ? sFile.id : null
                        });
                    }
                } // end column loop

                // Normalize file score by number of columns mapped
                // Favor files where MANY columns match
                if (fileMappings.length > 0) {
                    log(` -> Match Candidate: ${sFile.original_filename} (Score: ${fileMatchScore.toFixed(1)}, Mapped Cols: ${fileMappings.length})`);
                    if (fileMatchScore > bestSourceScore) {
                        bestSourceScore = fileMatchScore;
                        bestSourceFile = sFile;
                        bestSourceMappings = fileMappings;
                    }
                }
            } // end source file loop

            if (bestSourceFile && bestSourceMappings.length > 0) {
                log(` => WINNER for ${tFile.original_filename} is ${bestSourceFile.original_filename}`);

                // Identify PRIMARY KEY (Improved Heuristic)
                // 1. Must be a MAPPED column (exists in both files)
                // 2. Must have HIGH UNIQUENESS in both files (>90%)
                // 3. Prefer ID-like names

                const tData = await loadFileData(tFile, 100);
                const sData = await loadFileData(bestSourceFile, 100);

                let keyCandidate: any = null;
                let bestKeyScore = 0;

                for (const m of bestSourceMappings) {
                    // Find column data
                    const tCol = targetColsDB.find((c: any) => c.id === m.targetColumnId);
                    const sCol = await db.query('SELECT * FROM file_columns WHERE id = ?', [m.sourceColumnId]);

                    if (!tCol || !sCol[0]) continue;

                    const tVals = tData?.colData.get(tCol.column_index);
                    const sVals = sData?.colData.get(sCol[0].column_index);

                    if (!tVals || !sVals) continue;

                    // Calculate uniqueness (unique values / total values)
                    const tUniqueness = tData?.rowsCount ? tVals.size / tData.rowsCount : 0;
                    const sUniqueness = sData?.rowsCount ? sVals.size / sData.rowsCount : 0;

                    // Both must have high uniqueness (>85%)
                    if (tUniqueness < 0.85 || sUniqueness < 0.85) continue;

                    // Calculate key score
                    let keyScore = (tUniqueness + sUniqueness) / 2; // Average uniqueness

                    // Bonus for ID-like names
                    const colName = m.sourceColName.toLowerCase();
                    if (['id', 'key', 'recid', 'accountnum', 'accountnumber', 'code', 'cislo'].includes(colName)) {
                        keyScore += 0.3;
                    } else if (/id$|^id|_id|num$|code$/.test(colName)) {
                        keyScore += 0.15;
                    }

                    if (keyScore > bestKeyScore) {
                        bestKeyScore = keyScore;
                        keyCandidate = m;
                        log(`   Key candidate: ${m.sourceColName} (score: ${keyScore.toFixed(2)}, uniqueness: T=${(tUniqueness * 100).toFixed(0)}% S=${(sUniqueness * 100).toFixed(0)}%)`);
                    }
                }

                if (keyCandidate) {
                    log(`   Selected Primary Key: ${keyCandidate.sourceColName}`);
                } else {
                    log(`   WARNING: No suitable Primary Key found for this mapping!`);
                }

                newMappings.push(...bestSourceMappings.map(m => ({
                    sourceColumnId: m.sourceColumnId,
                    targetColumnId: m.targetColumnId,
                    // Store metadata in note as JSON for frontend compatibility
                    note: JSON.stringify({
                        isKey: (keyCandidate && m.sourceColumnId === keyCandidate.sourceColumnId) || false,
                        codebookFileId: m.codebookFileId,
                        autoDiscovered: true,
                        score: m.score.toFixed(2)
                    })
                })));
            }

            // --- B. FIND REFERENCES (Target -> Other Targets for Consistency Check) ---
            for (const tCol of targetColsDB) {
                // Only check ID-like columns
                if (!/id|code|num|cislo|kod/i.test(tCol.column_name)) continue;

                const tVals = tData.colData.get(tCol.column_index);
                if (!tVals || tVals.size === 0) continue;

                for (const otherTarget of targets) {
                    if (otherTarget.id === tFile.id) continue;

                    const otherData = await loadFileData(otherTarget, 100);
                    if (!otherData) continue;

                    const otherCols = await db.query('SELECT * FROM file_columns WHERE file_id = ?', [otherTarget.id]);

                    for (const oCol of otherCols) {
                        // Strict Name Match for Key columns
                        if (normalize(tCol.column_name) !== normalize(oCol.column_name)) continue;

                        const oVals = otherData.colData.get(oCol.column_index);
                        if (!oVals) continue;

                        // Check if oCol is unique enough to be a PK (>90% unique values)
                        if (oVals.size < (otherData.rowsCount * 0.9)) continue;

                        // Check overlap
                        let hits = 0, sample = 0;
                        for (const val of tVals) {
                            sample++;
                            if (oVals.has(val)) hits++;
                        }

                        if (sample > 0 && (hits / sample) > 0.7) {
                            log(`Found Reference: ${tFile.original_filename}.${tCol.column_name} -> ${otherTarget.original_filename}`);
                            newMappings.push({
                                sourceColumnId: null,
                                targetColumnId: tCol.id,
                                note: JSON.stringify({
                                    isKey: false,
                                    codebookFileId: otherTarget.id,
                                    refColumnId: oCol.id,
                                    autoDiscovered: true,
                                    type: 'reference'
                                })
                            });
                            break;
                        }
                    }
                }
            }
        }

        // SAVE to DB (Magic Apply)
        if (newMappings.length > 0) {
            // Clear old mappings to avoid conflicts/duplicates
            await db.run('DELETE FROM column_mappings WHERE project_id = ?', [projectId]);

            for (const m of newMappings) {
                // Skip reference-only mappings (no source column) - they are stored in note
                if (!m.sourceColumnId) continue;

                await db.run('INSERT INTO column_mappings (project_id, source_column_id, target_column_id, mapping_note) VALUES (?, ?, ?, ?)',
                    [projectId, m.sourceColumnId, m.targetColumnId, m.note]);
            }
        }

        res.json({ mappings: newMappings, logs: debugLogs });

    } catch (error) {
        serverLog(`Auto-map error: ${error}`);
        console.error('Auto-map error:', error);
        // Ensure we always return JSON, not HTML
        res.setHeader('Content-Type', 'application/json');
        res.status(500).json({ error: String((error as Error).message || error), logs: debugLogs });
    }
});

// 8. Validate Project (Multi-file Support)
app.post('/api/projects/:id/validate', async (req, res) => {
    const projectId = req.params.id;
    const { scopeFileId } = req.body;

    try {
        const mem = process.memoryUsage();
        serverLog(`Starting Auto-Map Project ${projectId}. Heap: ${Math.round(mem.heapUsed / 1024 / 1024)}MB`);
        // Fetch all project files
        const allFiles = await db.query("SELECT * FROM imported_files WHERE project_id = ?", [projectId]);
        serverLog(`Found ${allFiles.length} files for project ${projectId}`);
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
                const rowsRes = await db.query('SELECT row_data FROM imported_file_rows WHERE file_id = ? ORDER BY row_index ASC', [file.id]);
                if (rowsRes && rowsRes.length > 0) {
                    const data = rowsRes.map(r => JSON.parse(r.row_data));
                    sheetCache.set(file.id, data);
                    return data;
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

