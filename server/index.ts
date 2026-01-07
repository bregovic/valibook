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
app.use(express.json());

// Initialize Database
db.init();

// Multer setup for file uploads
const isWindows = process.platform === 'win32';
const UPLOADS_DIR = process.env.UPLOADS_DIR || (isWindows ? 'uploads/' : '/tmp/uploads/');

if (!fs.existsSync(UPLOADS_DIR)) {
    fs.mkdirSync(UPLOADS_DIR, { recursive: true });
}

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

// 3. Upload File & Analyze Columns
app.post('/api/projects/:id/files', upload.single('file'), async (req, res) => {
    const projectId = req.params.id;
    const fileType = req.body.fileType; // 'source' | 'target' | 'codebook'

    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    if (!['source', 'target', 'codebook'].includes(fileType)) {
        return res.status(400).json({ error: 'Invalid file type' });
    }

    try {
        const filePath = req.file.path;

        // 1. Insert into DB
        const fileInfo = await db.run('INSERT INTO imported_files (project_id, original_filename, file_type, stored_filename) VALUES (?, ?, ?, ?)',
            [projectId, req.file.originalname, fileType, filePath]);
        const fileId = fileInfo.id;

        // 2. Parse Columns (Robust)
        let parseWarning = null;
        try {
            if (!fs.existsSync(filePath)) {
                throw new Error(`File not found at path: ${filePath}`);
            }

            const stats = fs.statSync(filePath);
            if (stats.size === 0) {
                throw new Error('File is empty (0 bytes)');
            }

            const workbook = readFile(filePath);
            if (!workbook.SheetNames.length) {
                throw new Error('Excel file has no sheets');
            }

            const sheetName = workbook.SheetNames[0];
            const sheet = workbook.Sheets[sheetName];

            // Convert to JSON (header: 1 means array of arrays)
            const jsonData: any[][] = utils.sheet_to_json(sheet, { header: 1, defval: '' });

            if (jsonData && jsonData.length > 0) {
                const headers = jsonData[0];
                // If only 1 row, samples will be empty but headers are valid
                const firstRow = jsonData.length > 1 ? jsonData[1] : [];

                for (let idx = 0; idx < headers.length; idx++) {
                    const name = String(headers[idx] || `Column ${idx + 1}`).trim();
                    const sample = String(firstRow[idx] || '').substring(0, 100);

                    await db.run('INSERT INTO file_columns (file_id, column_name, column_index, sample_value) VALUES (?, ?, ?, ?)',
                        [fileId, name, idx, sample]);
                }
            } else {
                parseWarning = 'Excel sheet appears to be empty or could not be parsed as array.';
            }
        } catch (parseErr) {
            console.error('Error parsing Excel columns:', parseErr);
            parseWarning = `Failed to parse columns: ${(parseErr as Error).message}`;
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
            // Manual Cleanup of Dependencies (to avoid FK errors if Cascade not set)
            // 1. Delete mappings referencing columns of this file
            await db.run(`
                DELETE FROM column_mappings 
                WHERE source_column_id IN (SELECT id FROM file_columns WHERE file_id = ?) 
                   OR target_column_id IN (SELECT id FROM file_columns WHERE file_id = ?)
            `, [fileId, fileId]);

            // 2. Delete columns
            await db.run('DELETE FROM file_columns WHERE file_id = ?', [fileId]);

            // 3. Delete file record
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
    const projectId = req.params.id;
    const { sourceFileId, targetFileId } = req.body; // Optional: restrict to pair if user wants

    const debugLogs: string[] = [];
    const log = (msg: string) => { console.log(msg); debugLogs.push(msg); };

    try {
        // 1. Fetch all files
        const allFiles = await db.query("SELECT * FROM imported_files WHERE project_id = ?", [projectId]);
        const targets = allFiles.filter((f: any) => f.file_type === 'target' && (!targetFileId || f.id === targetFileId));
        const sources = allFiles.filter((f: any) => f.file_type === 'source' && (!sourceFileId || f.id === sourceFileId));
        const codebooks = allFiles.filter((f: any) => f.file_type === 'codebook');

        // Combined potential sources (Source of Truth + Codebooks)
        const potentialSources = [...sources, ...codebooks];

        if (targets.length === 0 || potentialSources.length === 0) {
            return res.json({ mappings: [], logs: ["Not enough files to perform discovery."] });
        }

        // Helper: Load Column Data for a file
        const fileDataCache = new Map<number, { headers: any[], colData: Map<number, Set<string>>, rowsCount: number }>();

        const loadFileData = (file: any) => {
            if (fileDataCache.has(file.id)) return fileDataCache.get(file.id)!;

            const filePath = file.stored_filename;
            if (!fs.existsSync(filePath)) {
                log(`Skipping missing file: ${file.original_filename}`);
                return null;
            }

            try {
                const wb = readFile(filePath);
                const sheet = wb.Sheets[wb.SheetNames[0]];
                const rows = utils.sheet_to_json(sheet, { header: 1, defval: '' }) as any[][];

                if (rows.length < 2) return null;

                const headers = rows[0];
                const colData = new Map<number, Set<string>>();

                // Sample data (optimize: taking max 200 rows for signature analysis)
                const limit = Math.min(rows.length, 200);
                for (let r = 1; r < limit; r++) {
                    rows[r].forEach((val: any, idx: number) => {
                        if (!colData.has(idx)) colData.set(idx, new Set());
                        const s = String(val).trim();
                        if (s) colData.get(idx)!.add(s);
                    });
                }

                const result = { headers, colData, rowsCount: rows.length };
                fileDataCache.set(file.id, result);
                return result;
            } catch (e) {
                log(`Error reading ${file.original_filename}: ${e}`);
                return null;
            }
        };

        const newMappings = [];
        const normalize = (s: string) => String(s).toLowerCase().replace(/[^a-z0-9]/g, '');

        log(`Starting Discovery. Targets: ${targets.length}, Sources: ${potentialSources.length}`);

        // DISCOVERY LOOP
        for (const tFile of targets) {
            log(`Analyzing Target File: ${tFile.original_filename}`);
            const tData = loadFileData(tFile);
            if (!tData) continue;

            const targetColsDB = await db.query('SELECT * FROM file_columns WHERE file_id = ?', [tFile.id]);

            // We want to find the BEST matching Source file for this Target file
            // Score = number of column matches
            let bestSourceFile = null;
            let bestSourceScore = 0;
            let bestSourceMappings: any[] = [];

            for (const sFile of potentialSources) {
                const sData = loadFileData(sFile);
                if (!sData) continue;

                const sourceColsDB = await db.query('SELECT * FROM file_columns WHERE file_id = ?', [sFile.id]);

                let fileMatchScore = 0;
                let fileMappings = [];

                // Compare Columns
                for (const tCol of targetColsDB) {
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
                            score: bestColScore,
                            isKey: false, // will determine later
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

                newMappings.push(...bestSourceMappings.map(m => ({
                    sourceColumnId: m.sourceColumnId,
                    targetColumnId: m.targetColumnId,
                    note: `Auto-Discovered (Score: ${m.score.toFixed(2)})`,
                    codebookFileId: m.codebookFileId
                })));
            }
        }

        res.json({ mappings: newMappings, logs: debugLogs });

    } catch (error) {
        console.error('Auto-map error:', error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// 8. Validate Project (Multi-file Support)
app.post('/api/projects/:id/validate', async (req, res) => {
    const projectId = req.params.id;

    try {
        // Fetch all project files
        const allFiles = await db.query("SELECT * FROM imported_files WHERE project_id = ?", [projectId]);
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

        // Group mappings by Source File to identify pairs
        const filePairs = new Map<number, { targetFileId: number, mappings: any[] }>();

        for (const m of mappings) {
            const sCol = getCol(m.source_column_id);
            if (!sCol) continue;

            const sourceFileId = sCol.file_id;

            // Identify target file from target column (if mapped)
            let targetFileId = null;
            if (m.target_column_id) {
                const tCol = getCol(m.target_column_id);
                if (tCol) targetFileId = tCol.file_id;
            }

            if (targetFileId) {
                if (!filePairs.has(sourceFileId)) {
                    filePairs.set(sourceFileId, { targetFileId, mappings: [] });
                }
                const group = filePairs.get(sourceFileId);
                // Ensure consistency (1 Source -> 1 Target assumption per file for simplicity)
                if (group && group.targetFileId === targetFileId) {
                    group.mappings.push(m);
                }
            }
        }

        const results: any[] = [];

        // Helper to get cached codebook values
        const codebookCache = new Map<number, Set<string>>();
        const readSheet = (path: string) => {
            if (!fs.existsSync(path)) return [];
            const wb = readFile(path);
            return utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]], { header: 1, defval: '' }) as any[][];
        };

        const getCodebookValues = async (cbId: number) => {
            if (!codebookCache.has(cbId)) {
                const cbFile = getFile(cbId);
                if (cbFile) {
                    const rows = readSheet(cbFile.stored_filename) as any[][];
                    const values = new Set(rows.slice(1).map(r => String(r[0])));
                    codebookCache.set(cbId, values);
                } else {
                    codebookCache.set(cbId, new Set());
                }
            }
            return codebookCache.get(cbId)!;
        };

        // Iterate over each file pair and validate
        for (const [sFileId, group] of filePairs) {
            const sFile = getFile(sFileId);
            const tFile = getFile(group.targetFileId);

            if (!sFile || !tFile) continue;

            const fileLabel = `${sFile.original_filename} -> ${tFile.original_filename} `;

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
            const sourceRows = readSheet(sFile.stored_filename);
            const targetRows = readSheet(tFile.stored_filename);

            // Remove headers
            sourceRows.shift();
            targetRows.shift();

            // Index Maps
            const sourceMap = new Map<string, any[]>();
            sourceRows.forEach(r => {
                const k = String(r[sourceKeyIdx]).trim();
                if (k) sourceMap.set(k, r);
            });

            const targetMap = new Map<string, any[]>();
            targetRows.forEach(r => {
                const k = String(r[targetKeyIdx]).trim();
                if (k) targetMap.set(k, r);
            });

            // 1. Check Missing Records
            for (const [key, sRow] of sourceMap) {
                if (!targetMap.has(key)) {
                    results.push({
                        key,
                        type: 'missing_row',
                        message: `Row missing in Target`,
                        file: fileLabel,
                        expected: 'Present',
                        actual: 'Missing'
                    });
                } else {
                    // 2. Check Value Mismatches
                    const tRow = targetMap.get(key)!;

                    for (const m of group.mappings) {
                        if (m.source_column_id === keyMapping.source_column_id) continue;
                        if (!m.target_column_id) continue;

                        const sColDef = getCol(m.source_column_id);
                        const tColDef = getCol(m.target_column_id);

                        if (!sColDef || !tColDef) continue;

                        let cbId = null;
                        try { cbId = JSON.parse(m.mapping_note || '{}').codebookFileId; } catch (e) { }

                        const sIdx = sColDef.column_index;
                        const tIdx = tColDef.column_index;

                        const sVal = String(sRow[sIdx] || '').trim();
                        const tVal = String(tRow[tIdx] || '').trim();

                        if (sVal !== tVal) {
                            results.push({
                                key,
                                type: 'value_mismatch',
                                column: `${sColDef.column_name} `,
                                expected: sVal,
                                actual: tVal,
                                file: fileLabel
                            });
                        }

                        if (cbId) {
                            const validValues = await getCodebookValues(cbId);
                            if (!validValues.has(tVal) && tVal !== '') {
                                results.push({
                                    key,
                                    type: 'codebook_violation',
                                    column: tColDef.column_name,
                                    message: `Value not in codebook`,
                                    expected: 'Valid Code',
                                    actual: tVal,
                                    file: fileLabel
                                });
                            }
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
            await db.run(stmt, [projectId, null, `${r.file ? '[' + r.file + '] ' : ''}${r.type}: ${r.message || ''} `, r.actual || '', r.expected || '']);
        }

        res.json({ success: true, issuesCount: results.length, limit: 100, issues: results.slice(0, 100) });

    } catch (error) {
        console.error('Validation Error:', error);
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

