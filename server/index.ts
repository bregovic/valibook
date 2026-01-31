import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import multer from 'multer';
import XLSX from 'xlsx';
import fs from 'fs';
import prisma from './database.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

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
const upload = multer({
    storage: storage,
    limits: { fileSize: 50 * 1024 * 1024 } // 50MB limit
});

// ============================================
// PROJECTS API
// ============================================

// Get all projects
app.get('/api/projects', async (req, res) => {
    try {
        const projects = await prisma.project.findMany({
            orderBy: { createdAt: 'desc' },
            include: {
                _count: {
                    select: { columns: true }
                }
            }
        });
        res.json(projects);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// Create new project
app.post('/api/projects', async (req, res) => {
    const { name, description } = req.body;
    if (!name) return res.status(400).json({ error: 'Project name is required' });

    try {
        const project = await prisma.project.create({
            data: { name, description }
        });
        res.json(project);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// Get project details with columns
app.get('/api/projects/:id', async (req, res) => {
    try {
        const project = await prisma.project.findUnique({
            where: { id: req.params.id },
            include: {
                columns: {
                    orderBy: [
                        { tableName: 'asc' },
                        { columnIndex: 'asc' }
                    ],
                    include: {
                        linkedToColumn: {
                            select: { id: true, tableName: true, columnName: true }
                        }
                    }
                }
            }
        });

        if (!project) {
            return res.status(404).json({ error: 'Project not found' });
        }

        res.json(project);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// Delete project
app.delete('/api/projects/:id', async (req, res) => {
    const { id } = req.params;
    try {
        // 1. Unlink columns to prevent FK constraint errors (self-relation)
        await prisma.column.updateMany({
            where: { projectId: id },
            data: { linkedToColumnId: null }
        });

        // 2. Delete project (Cascade takes care of columns and values)
        await prisma.project.delete({
            where: { id }
        });
        res.json({ success: true });
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// ============================================
// COLUMNS API - Upload & Analyze Excel
// ============================================

// Upload Excel file and extract columns with data
app.post('/api/projects/:projectId/upload', upload.single('file'), async (req, res) => {
    const { projectId } = req.params;
    const tableType = req.body.tableType as 'SOURCE' | 'TARGET' | 'FORBIDDEN' | 'RANGE';

    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    if (!['SOURCE', 'TARGET', 'FORBIDDEN', 'RANGE'].includes(tableType)) {
        return res.status(400).json({ error: 'Invalid table type.' });
    }

    try {
        const filePath = req.file.path;
        const tableName = req.file.originalname.replace(/\.[^/.]+$/, ''); // Remove extension
        const overwrite = req.query.overwrite === 'true';

        // Check for duplicate table
        const existingCount = await prisma.column.count({
            where: { projectId, tableName }
        });

        if (existingCount > 0) {
            if (!overwrite) {
                fs.unlinkSync(filePath); // Clean up uploaded file
                return res.status(409).json({ error: 'Table already exists', tableName, requiresConfirmation: true });
            }

            // Delete existing
            await prisma.column.deleteMany({
                where: { projectId, tableName }
            });
        }

        // Parse Excel
        const workbook = XLSX.readFile(filePath);
        const sheetName = workbook.SheetNames[0];
        const sheet = workbook.Sheets[sheetName];
        const jsonData: any[][] = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });

        if (jsonData.length === 0) {
            return res.status(400).json({ error: 'Excel file is empty' });
        }

        const headers = jsonData[0] as string[];
        const dataRows = jsonData.slice(1);
        const rowCount = dataRows.length;

        // Process each column
        const createdColumns = [];

        for (let colIndex = 0; colIndex < headers.length; colIndex++) {
            const columnName = String(headers[colIndex] || `Column_${colIndex + 1}`);

            // Extract all values for this column
            const allValues = dataRows.map(row => String(row[colIndex] ?? ''));

            // Calculate statistics
            const nonEmptyValues = allValues.filter(v => v !== '');
            const uniqueValues = [...new Set(nonEmptyValues)];
            const nullCount = allValues.filter(v => v === '').length;

            // Get 100 random sample values for better detection
            const shuffled = [...uniqueValues].sort(() => 0.5 - Math.random());
            const sampleValues = shuffled.slice(0, 100);

            // Basic Profiling
            let minLength: number | null = null;
            let maxLength: number | null = null;
            let detectedType: string = 'STRING';
            let pattern: string | null = null;

            if (nonEmptyValues.length > 0) {
                const lengths = nonEmptyValues.map(v => v.length);
                minLength = Math.min(...lengths);
                maxLength = Math.max(...lengths);

                // Detect Type
                const isNumeric = nonEmptyValues.every(v => !isNaN(Number(v)) && v.trim() !== '');
                // Simple date detection (very basic)
                const isDate = nonEmptyValues.every(v => !isNaN(Date.parse(v)) && (v.includes('-') || v.includes('/')) && v.length >= 8);

                if (isNumeric) detectedType = 'NUMBER';
                else if (isDate) detectedType = 'DATE';

                // Detect common patterns (e.g. VAT)
                if (detectedType === 'STRING') {
                    const vatRegex = /^[A-Z]{2}[0-9]+$/;
                    if (nonEmptyValues.every(v => vatRegex.test(v))) {
                        pattern = '^[A-Z]{2}[0-9]+$';
                    }
                }
            }

            // Create column record
            const column = await prisma.column.create({
                data: {
                    projectId,
                    tableName,
                    columnName,
                    columnIndex: colIndex,
                    tableType,
                    rowCount,
                    uniqueCount: uniqueValues.length,
                    nullCount,
                    sampleValues,
                    // Profiling stats
                    minLength,
                    maxLength,
                    dataType: detectedType,
                    pattern
                }
            });

            // Store all values in column_values table
            if (allValues.length > 0) {
                await prisma.columnValue.createMany({
                    data: allValues.map((value, rowIndex) => ({
                        columnId: column.id,
                        rowIndex,
                        value
                    }))
                });
            }

            createdColumns.push({
                id: column.id,
                columnName,
                rowCount,
                uniqueCount: uniqueValues.length,
                sampleValues
            });
        }

        // Clean up uploaded file
        fs.unlinkSync(filePath);

        res.json({
            success: true,
            tableName,
            tableType,
            rowCount,
            columns: createdColumns
        });

    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// ============================================
// COLUMN OPERATIONS API
// ============================================

// Update column (set primary key, required, etc.)
app.get('/api/columns/:id', async (req, res) => {
    try {
        const column = await prisma.column.findUnique({
            where: { id: req.params.id },
            include: { linkedToColumn: true }
        });
        if (!column) return res.status(404).json({ error: 'Column not found' });
        res.json(column);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

app.patch('/api/columns/:id', async (req, res) => {
    const { id } = req.params;
    const { isPrimaryKey, isValidationRange, isRequired, linkedToColumnId } = req.body;

    try {
        const column = await prisma.column.update({
            where: { id },
            data: {
                ...(isPrimaryKey !== undefined && { isPrimaryKey }),
                ...(isValidationRange !== undefined && { isValidationRange }),
                ...(isRequired !== undefined && { isRequired }),
                ...(linkedToColumnId !== undefined && { linkedToColumnId })
            }
        });
        res.json(column);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// Get column values (for inspection)
app.get('/api/columns/:id/values', async (req, res) => {
    const { id } = req.params;
    const limit = parseInt(req.query.limit as string) || 100;
    const offset = parseInt(req.query.offset as string) || 0;

    try {
        const values = await prisma.columnValue.findMany({
            where: { columnId: id },
            orderBy: { rowIndex: 'asc' },
            take: limit,
            skip: offset
        });

        const total = await prisma.columnValue.count({
            where: { columnId: id }
        });

        res.json({ values, total, limit, offset });
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// Delete column (and its values)
app.delete('/api/columns/:id', async (req, res) => {
    const { id } = req.params;

    try {
        await prisma.column.delete({
            where: { id }
        });
        res.json({ success: true });
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// Get all unique tables in a project (grouped)
app.get('/api/projects/:projectId/tables', async (req, res) => {
    const { projectId } = req.params;

    try {
        const columns = await prisma.column.findMany({
            where: { projectId },
            orderBy: [
                { tableName: 'asc' },
                { columnIndex: 'asc' }
            ]
        });

        // Group by tableName
        const tables: Record<string, any> = {};
        for (const col of columns) {
            if (!tables[col.tableName]) {
                tables[col.tableName] = {
                    tableName: col.tableName,
                    tableType: col.tableType,
                    rowCount: col.rowCount,
                    columns: []
                };
            }
            tables[col.tableName].columns.push({
                id: col.id,
                columnName: col.columnName,
                columnIndex: col.columnIndex,
                isPrimaryKey: col.isPrimaryKey,
                isValidationRange: col.isValidationRange,
                isRequired: col.isRequired,
                uniqueCount: col.uniqueCount,
                nullCount: col.nullCount,
                sampleValues: col.sampleValues,
                linkedToColumnId: col.linkedToColumnId
            });
        }

        res.json(Object.values(tables));
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// Delete specific validation rule
app.delete('/api/rules/:id', async (req, res) => {
    try {
        await prisma.validationRule.delete({
            where: { id: req.params.id }
        });
        res.json({ success: true });
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// Bulk delete rules
app.post('/api/rules/bulk-delete', async (req, res) => {
    const { ids } = req.body;
    if (!Array.isArray(ids)) return res.status(400).json({ error: 'IDs must be an array' });
    try {
        await prisma.validationRule.deleteMany({
            where: { id: { in: ids } }
        });
        res.json({ success: true });
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// Delete entire table
app.delete('/api/projects/:projectId/tables/:tableName', async (req, res) => {
    const { projectId, tableName } = req.params;
    try {
        await prisma.column.deleteMany({
            where: { projectId, tableName }
        });
        res.json({ success: true });
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// Get failing records for a rule
app.get('/api/rules/:id/failures', async (req, res) => {
    try {
        const rule = await prisma.validationRule.findUnique({
            where: { id: req.params.id },
            include: { column: true }
        });
        if (!rule) return res.status(404).json({ error: 'Rule not found' });

        const col = rule.column;
        const limit = 100;
        let failures: any[] = [];

        if (rule.type === 'REGEX' && rule.value) {
            failures = await prisma.$queryRawUnsafe(`
                SELECT "rowIndex", value 
                FROM "column_values"
                WHERE "columnId" = '${col.id}'
                  AND value != ''
                  AND NOT (value ~ '${rule.value.replace(/'/g, "''")}')
                LIMIT ${limit}
            `);
        } else if (rule.type === 'NOT_NULL') {
            failures = await prisma.columnValue.findMany({
                where: { columnId: col.id, value: '' },
                take: limit,
                select: { rowIndex: true, value: true }
            });
        } else if (rule.type === 'UNIQUE') {
            failures = await prisma.$queryRawUnsafe(`
                SELECT "rowIndex", value 
                FROM "column_values"
                WHERE "columnId" = '${col.id}'
                  AND value IN (
                    SELECT value FROM "column_values" 
                    WHERE "columnId" = '${col.id}' 
                    GROUP BY value HAVING COUNT(*) > 1
                  )
                LIMIT ${limit}
            `);
        } else if (rule.type === 'MATH_EQUATION' && rule.value) {
            const colNames = rule.value.match(/\[(.*?)\]/g)?.map(m => m.replace(/[\[\]]/g, '')) || [];
            const tableCols = await prisma.column.findMany({
                where: { tableName: col.tableName, projectId: col.projectId }
            });
            const nameToId: Record<string, string> = {};
            tableCols.forEach(tc => nameToId[tc.columnName] = tc.id);

            if (colNames.every(n => nameToId[n])) {
                let query = `SELECT cv0."rowIndex", cv0.value as primary_val, `;
                colNames.forEach((n, idx) => {
                    query += `cv${idx}.value as val_${idx}${idx < colNames.length - 1 ? ', ' : ''}`;
                });
                query += ` FROM "column_values" cv0 `;
                for (let i = 1; i < colNames.length; i++) {
                    query += `JOIN "column_values" cv${i} ON cv0."rowIndex" = cv${i}."rowIndex" AND cv${i}."columnId" = '${nameToId[colNames[i]]}' `;
                }
                query += `WHERE cv0."columnId" = '${nameToId[colNames[0]]}'`;

                let sqlExpr = rule.value.replace(/\[(.*?)\]/g, (match, name) => {
                    const idx = colNames.indexOf(name);
                    return `CAST(NULLIF(val_${idx}, '') AS DOUBLE PRECISION)`;
                }).replace('=', '!=');

                failures = await prisma.$queryRawUnsafe(`
                     WITH rows AS (${query})
                     SELECT * FROM rows 
                     WHERE ABS((${sqlExpr.split('!=')[0]}) - (${sqlExpr.split('!=')[1]})) > 0.1
                     LIMIT ${limit}
                 `);
            }
        }

        res.json({ failures });
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// ============================================
// DATA PROFILING EXPORT (Safe for AI)
// ============================================
app.get('/api/projects/:projectId/profile', async (req, res) => {
    const { projectId } = req.params;
    try {
        const columns = await prisma.column.findMany({
            where: { projectId },
            orderBy: [{ tableName: 'asc' }, { columnIndex: 'asc' }]
        });

        const profileReport: any = {
            projectId,
            generatedAt: new Date().toISOString(),
            tables: {}
        };

        // Group by table
        for (const col of columns) {
            if (!profileReport.tables[col.tableName]) {
                profileReport.tables[col.tableName] = [];
            }
            profileReport.tables[col.tableName].push({
                column: col.columnName,
                type: col.dataType || 'STRING',
                stats: {
                    rows: col.rowCount,
                    unique: col.uniqueCount,
                    nulls: col.nullCount,
                    nullRatio: col.rowCount ? (col.nullCount || 0) / col.rowCount : 0,
                    minLength: col.minLength,
                    maxLength: col.maxLength,
                    pattern: col.pattern
                }
            });
        }

        res.json(profileReport);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// ============================================
// SYSTEM CONFIG API (Global Settings)
// ============================================
app.get('/api/config', async (req, res) => {
    try {
        const config = await prisma.systemConfig.findUnique({
            where: { key: 'OPENAI_API_KEY' }
        });
        res.json({ hasOpenAIKey: !!config && !!config.value && config.value.length > 0 });
    } catch (error) {
        res.status(500).json({ error: (error as Error).message }); // Handle DB not ready
    }
});

app.post('/api/config', async (req, res) => {
    const { key, value, password } = req.body;
    if (password !== 'Heslo123') return res.status(403).json({ error: 'Nesprávné bezpečnostní heslo.' });

    try {
        if (!key || !value) return res.status(400).json({ error: 'Chybí klíč nebo hodnota.' });

        await prisma.systemConfig.upsert({
            where: { key },
            update: { value },
            create: { key, value }
        });
        res.json({ success: true });
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// ============================================
// AI SUGGEST RULES (OpenAI Integration)
// ============================================
app.post('/api/projects/:projectId/ai-suggest-rules', async (req, res) => {
    const { projectId } = req.params;
    const { apiKey, password } = req.body;

    // 1. Simple Security Gate
    if (password !== 'Heslo123') {
        return res.status(403).json({ error: 'Neplatné heslo pro AI funkce.' });
    }

    // 2. Resolve API Key (User provided OR System stored)
    let finalApiKey = apiKey;
    if (!finalApiKey) {
        const config = await prisma.systemConfig.findUnique({ where: { key: 'OPENAI_API_KEY' } });
        finalApiKey = config?.value;
    }

    if (!finalApiKey) {
        return res.status(400).json({ error: 'Chybí OpenAI API Key. Nastavte ho v globálním nastavení nebo zadejte ručně.' });
    }

    try {
        const { subsetTableNames } = req.body;

        // 2. Generate Profile
        const columns = await prisma.column.findMany({
            where: {
                projectId,
                ...(subsetTableNames && { tableName: { in: subsetTableNames } })
            },
            orderBy: [{ tableName: 'asc' }, { columnIndex: 'asc' }]
        });

        // Simplified profile for AI token limits
        const tablesProfile: Record<string, any[]> = {};
        const columnIdMap: Record<string, string> = {};
        const idToNameMap: Record<string, string> = {};

        // First pass: Build ID to Name map
        for (const col of columns) {
            idToNameMap[col.id] = `${col.tableName}.${col.columnName}`;
        }

        // Second pass: Build profile
        for (const col of columns) {
            if (col.rowCount && col.nullCount === col.rowCount) continue;
            if (!tablesProfile[col.tableName]) tablesProfile[col.tableName] = [];

            const key = `${col.tableName}.${col.columnName}`;
            columnIdMap[key] = col.id;

            const rawSamples = (col.sampleValues as string[]) || [];
            const safeSamples = rawSamples.slice(0, 3).map(s => String(s).substring(0, 100));

            tablesProfile[col.tableName].push({
                column: col.columnName,
                type: col.dataType || 'STRING',
                isPrimaryKey: col.isPrimaryKey,
                linkedTo: col.linkedToColumnId ? idToNameMap[col.linkedToColumnId] : null,
                sample: safeSamples,
                unique: col.uniqueCount,
                nulls: col.nullCount
            });
        }

        // 3. Call OpenAI for this (potentially pre-chunked) set
        const tableNamesToProcess = Object.keys(tablesProfile);
        if (tableNamesToProcess.length === 0) {
            return res.json({ success: true, rules: [] });
        }

        console.log(`Processing AI Rule Suggestion for: ${tableNamesToProcess.join(', ')}`);

        const prompt = `
            Analyze schema and suggest validation rules.
            SCHEMA: ${JSON.stringify(tablesProfile)}
            INSTRUCTIONS: VAT, Emails, Phone, IDs, Required, Amounts, Currencies, Dates.
            Rules: REGES, NOT_NULL, UNIQUE, MATH_EQUATION (A*B=C), CURRENCY_CONSISTENCY.
            Output ONLY valid JSON array.
            FORMAT: [{"table":"T","column":"C","type":"REGEX","value":"...","description":"...","severity":"ERROR"}]
        `;

        const aiRes = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${finalApiKey}`
            },
            body: JSON.stringify({
                model: "gpt-4o-mini",
                messages: [
                    { role: "system", content: "You are a Data Quality assistant. Output JSON array only." },
                    { role: "user", content: prompt }
                ],
                temperature: 0.1
            })
        });

        if (!aiRes.ok) {
            const errText = await aiRes.text();
            throw new Error(`OpenAI Error: ${errText}`);
        }

        const aiData = await aiRes.json();
        let content = aiData.choices[0].message.content;
        content = content.replace(/```json/g, '').replace(/```/g, '').trim();

        const suggestedRules = JSON.parse(content);
        const rulesToSave = [];

        for (const rule of suggestedRules) {
            const key = `${rule.table}.${rule.column}`;
            const columnId = columnIdMap[key];

            if (columnId) {
                rulesToSave.push({
                    columnId,
                    type: rule.type,
                    value: rule.value || null,
                    severity: rule.severity || 'ERROR',
                    description: rule.description
                });
            }
        }

        let savedCount = 0;
        if (rulesToSave.length > 0) {
            // Use transaction or createMany for speed
            const result = await prisma.validationRule.createMany({
                data: rulesToSave
            });
            savedCount = result.count;
        }

        res.json({
            success: true,
            count: savedCount,
            rules: [] // Backward compatibility for old frontend
        });

    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message, success: false });
    }
});

// ============================================
// AUTO-DETECT LINKS - Find columns with overlapping values
// ============================================
app.post('/api/projects/:projectId/detect-links', async (req, res) => {
    const { projectId } = req.params;
    const mode = req.query.mode as string | undefined; // 'KEYS' | 'VALUES'

    try {
        // Get all columns
        const columns = await prisma.column.findMany({
            where: { projectId },
            select: {
                id: true,
                tableName: true,
                columnName: true,
                tableType: true,
                isPrimaryKey: true,
                uniqueCount: true,
                rowCount: true
            }
        });

        // For each column, get 10 random non-empty sample values
        const columnSamplesMap = new Map<string, string[]>();

        for (const col of columns) {
            // Get 30 random unique samples to be extra sure
            const samples = await prisma.$queryRaw<Array<{ value: string }>>`
                SELECT value FROM (
                    SELECT DISTINCT value 
                    FROM "column_values" 
                    WHERE "columnId" = ${col.id} 
                      AND value != '' 
                      AND value IS NOT NULL
                ) AS distinct_vals
                ORDER BY random() 
                LIMIT 50
            `;
            columnSamplesMap.set(col.id, samples.map(s => s.value));
        }

        // Find potential links using sample matching
        const suggestions: Array<{
            sourceColumnId: string;
            sourceColumn: string;
            sourceTable: string;
            targetColumnId: string;
            targetColumn: string;
            targetTable: string;
            matchPercentage: number;
            commonValues: number;
            sampleSize: number;
        }> = [];

        // Track seen pairs to avoid duplicates
        const seenPairs = new Set<string>();

        // For each column A (TARGET only), we check its samples against ALL other columns B
        for (const colA of columns) {
            // Only take samples from TARGET tables - as requested by user
            if (colA.tableType !== 'TARGET') continue;

            const samplesA = columnSamplesMap.get(colA.id);
            if (!samplesA || samplesA.length === 0) continue;

            for (const colB of columns) {
                if (colA.id === colB.id) continue;
                if (colA.tableName.toLowerCase() === colB.tableName.toLowerCase()) continue; // Strict Case-Insensitive self-reference check

                // OPTIMIZATION: In 'KEYS' mode, the TARGET candidate (colB) MUST be unique-ish.
                const uniquenessB = (colB.uniqueCount ?? 0) / Math.max(colB.rowCount ?? 1, 1);
                if (mode === 'KEYS' && uniquenessB < 0.80) continue;

                // Match based on string pair (sorted IDs)
                const pairKey = [colA.id, colB.id].sort().join('|');
                if (seenPairs.has(pairKey)) continue;

                // HEURISTIC: Robust Number Matching
                // 1. Normalize SAMPLES (Target) in JS: remove all whitespace, replace ',' with '.'
                const normalizedSamples = samplesA.map(s => String(s).replace(/\s/g, '').replace(',', '.'));

                // 2. Normalize DB VALUES (Source) in SQL and compare against normalized samples
                // We handle standard space, non-breaking space (chr 160), and comma/dot replacement
                const matchRes = await prisma.$queryRawUnsafe<Array<{ match_count: bigint }>>(`
                    SELECT COUNT(DISTINCT value) as match_count
                    FROM "column_values"
                    WHERE "columnId" = $1 
                      AND (
                        -- Exact match
                        value IN (${samplesA.map((_, i) => `$${i + 2}`).join(',')})
                        OR 
                        -- Normalized match (Remove spaces/NBSP, replace comma with dot)
                        REPLACE(REPLACE(REPLACE(value, ' ', ''), chr(160), ''), ',', '.') IN (${normalizedSamples.map(s => `'${s}'`).join(',')})
                      )
                `, colB.id, ...samplesA);

                const sampleMatchCount = Number(matchRes[0].match_count);
                const sampleMatchPct = Math.round((sampleMatchCount / samplesA.length) * 100);
                const namesSimilar = colA.columnName.trim().toLowerCase() === colB.columnName.trim().toLowerCase();

                // LOGIC SPLIT: KEY CANDIDATE vs VALUE CANDIDATE
                // User Request: "Find Reference Keys" strictly for unique non-duplicate values.

                const uniquenessA = (colA.uniqueCount ?? 0) / Math.max(colA.rowCount ?? 1, 1);
                // RELAXED UNIQUENESS: 80% is enough to be considered a potential key (allows for some dirty data)
                const isKeyCandidate = uniquenessA > 0.80;

                // FILTER BY MODE: 
                // KEYS mode: Only show high-uniqueness candidates
                // VALUES mode: Show everything that matches well (don't exclude keys, they can be values too)
                if (mode === 'KEYS' && !isKeyCandidate) continue;

                let isMatch = false;

                if (isKeyCandidate) {
                    // KEY STRATEGY: 90% match required (was 97%)
                    if (sampleMatchPct >= 90) {
                        isMatch = true;
                    }
                    // Fallback: If in KEYS mode, and match is very high (98%), accept even if uniqueness is lower (handled by isKeyCandidate check above)
                }

                if (!isKeyCandidate || mode === 'VALUES') {
                    // VALUE STRATEGY: Relaxed thresholds
                    // 50% threshold for general matches (was 60%) to catch dirtier data
                    // 40% threshold if names match
                    if (sampleMatchPct >= 50 || (namesSimilar && sampleMatchPct >= 40)) {
                        isMatch = true;
                    }
                }
                if (sampleMatchPct >= 60 || (namesSimilar && sampleMatchPct >= 40)) {
                    isMatch = true;
                }
            }

            if (isMatch) {
                // To get exact commonValues for UI, we do a full intersect (only for likely candidates)
                const overlapRes = await prisma.$queryRaw<Array<{ overlap_count: bigint }>>`
                        SELECT COUNT(*) as overlap_count
                        FROM (
                            SELECT DISTINCT value FROM "column_values" WHERE "columnId" = ${colA.id} AND value != ''
                            INTERSECT
                            SELECT DISTINCT value FROM "column_values" WHERE "columnId" = ${colB.id} AND value != ''
                        ) AS overlap
                    `;
                const commonValues = Number(overlapRes[0].overlap_count);
                const matchPctA = Math.round((commonValues / Math.max(colA.uniqueCount || 1, 1)) * 100);
                const matchPctB = Math.round((commonValues / Math.max(colB.uniqueCount || 1, 1)) * 100);
                const bestMatchPct = Math.max(matchPctA, matchPctB, sampleMatchPct);

                // Check uniqueness: at least one side must have 90%+ unique values
                const sourceUniqueRatio = (colA.uniqueCount ?? 0) / Math.max(colA.rowCount ?? 1, 1);
                const targetUniqueRatio = (colB.uniqueCount ?? 0) / Math.max(colB.rowCount ?? 1, 1);
                const isSourceUnique = sourceUniqueRatio >= 0.9 && (colA.rowCount ?? 0) > 0;
                const isTargetUnique = targetUniqueRatio >= 0.9 && (colB.rowCount ?? 0) > 0;

                if (isSourceUnique || isTargetUnique) {
                    // Mark this pair as seen
                    seenPairs.add(pairKey);

                    // Determine direction: FK (less unique) -> PK (more unique)
                    const shouldReverse = targetUniqueRatio < sourceUniqueRatio;

                    if (shouldReverse) {
                        suggestions.push({
                            sourceColumnId: colB.id,
                            sourceColumn: colB.columnName,
                            sourceTable: colB.tableName,
                            targetColumnId: colA.id,
                            targetColumn: colA.columnName,
                            targetTable: colA.tableName,
                            matchPercentage: bestMatchPct,
                            commonValues: commonValues,
                            sampleSize: samplesA.length
                        });
                    } else {
                        suggestions.push({
                            sourceColumnId: colA.id,
                            sourceColumn: colA.columnName,
                            sourceTable: colA.tableName,
                            targetColumnId: colB.id,
                            targetColumn: colB.columnName,
                            targetTable: colB.tableName,
                            matchPercentage: bestMatchPct,
                            commonValues: commonValues,
                            sampleSize: samplesA.length
                        });
                    }
                }
            }
        }
    }





                // Sort by match count (highest first)
                suggestions.sort((a, b) => b.commonValues - a.commonValues || b.matchPercentage - a.matchPercentage);

    res.json({
        success: true,
        suggestions: suggestions.slice(0, 100) // Limit to top 100
    });

} catch (error) {
    console.error(error);
    res.status(500).json({ error: (error as Error).message });
}
        });

// ============================================
// VALIDATE PROJECT - Check FK integrity
// ============================================
app.post('/api/projects/:projectId/validate', async (req, res) => {
    const { projectId } = req.params;

    try {
        // Get all columns with links
        const allLinks = await prisma.column.findMany({
            where: {
                projectId,
                linkedToColumnId: { not: null }
            },
            include: {
                linkedToColumn: true
            }
        });

        // Separate RANGE filters from actual foreign key links
        const rangeFilters = new Map<string, { colId: string, rangeColId: string }>();
        const fkColumns = [];

        for (const col of allLinks) {
            if (col.linkedToColumn?.tableType === 'RANGE' || col.linkedToColumn?.isValidationRange) {
                rangeFilters.set(col.tableName, { colId: col.id, rangeColId: col.linkedToColumnId! });
            } else {
                fkColumns.push(col);
            }
        }

        // Recursive range filter propagation
        // If TabC -> TabB -> TabA (Range), then TabC should also be filtered by Range.
        const extendedFilters = new Map(rangeFilters);
        let changed = true;
        while (changed) {
            changed = false;
            for (const col of allLinks) {
                if (!extendedFilters.has(col.tableName)) {
                    const targetFilter = extendedFilters.get(col.linkedToColumn!.tableName);
                    // Only propagate if the link is to the same column that defines the range (or recursively through PKs)
                    if (targetFilter && (targetFilter.colId === col.linkedToColumnId || col.linkedToColumn?.isPrimaryKey)) {
                        extendedFilters.set(col.tableName, { colId: col.id, rangeColId: targetFilter.rangeColId });
                        changed = true;
                    }
                }
            }
        }

        // 1. INTEGRITY CHECKS (Orphans) - SQL Optimized
        const integrityErrors = [];
        const reconciliationErrors = [];
        const forbiddenErrors: any[] = [];
        const allChecks: Array<{ type: string; label: string; status: 'OK' | 'ERROR'; checked: number; failed: number; }> = [];

        for (const fkCol of fkColumns) {
            if (!fkCol.linkedToColumn) continue;

            // Smart logic for SOURCE tables:
            // 1. If it's TARGET, always check.
            // 2. If it's SOURCE, only check if it is constrained by a Range Filter.
            //    (Otherwise we flag all non-exported source records as errors, which is spam.)
            const filter = extendedFilters.get(fkCol.tableName);

            if (fkCol.tableType === 'SOURCE' && !filter) continue;

            // CRITICAL FIX: Skip Integrity (Orphan) checks for "Value Links" (Amounts, etc.)
            // We only want to check "Missing Keys" for actual Reference Keys (PK/FK).
            // Value checks (Reconciliation) are handled in the next section.
            if (!fkCol.linkedToColumn.isPrimaryKey) continue;

            let checkedCount = 0;
            let orphans: any[] = [];
            let totalOrphans = 0;

            if (filter) {
                // FILTERED Integrity Check
                const countRes = await prisma.$queryRaw<Array<{ count: bigint }>>`
                    SELECT COUNT(DISTINCT v.value) as count
                    FROM "column_values" v
                    JOIN "column_values" f ON f."rowIndex" = v."rowIndex" AND f."columnId" = ${filter.colId}
                    WHERE v."columnId" = ${fkCol.id}
                      AND v.value != ''
                      AND EXISTS (
                        SELECT 1 FROM "column_values" rv 
                        WHERE rv."columnId" = ${filter.rangeColId} AND rv.value = f.value
                      )
                `;
                checkedCount = Number(countRes[0].count);

                const orphanRes = await prisma.$queryRaw<Array<{ value: string }>>`
                    SELECT DISTINCT v.value
                    FROM "column_values" v
                    JOIN "column_values" f ON f."rowIndex" = v."rowIndex" AND f."columnId" = ${filter.colId}
                    WHERE v."columnId" = ${fkCol.id}
                      AND v.value != ''
                      AND EXISTS (
                        SELECT 1 FROM "column_values" rv 
                        WHERE rv."columnId" = ${filter.rangeColId} AND rv.value = f.value
                      )
                      AND NOT EXISTS (
                        SELECT 1 FROM "column_values" target
                        WHERE target."columnId" = ${fkCol.linkedToColumnId} AND target.value = v.value
                      )
                    LIMIT 11
                `;
                orphans = orphanRes;

                if (orphans.length > 0) {
                    const totalRes = await prisma.$queryRaw<Array<{ count: bigint }>>`
                        SELECT COUNT(DISTINCT v.value) as count
                        FROM "column_values" v
                        JOIN "column_values" f ON f."rowIndex" = v."rowIndex" AND f."columnId" = ${filter.colId}
                        WHERE v."columnId" = ${fkCol.id}
                          AND v.value != ''
                          AND EXISTS (
                            SELECT 1 FROM "column_values" rv 
                            WHERE rv."columnId" = ${filter.rangeColId} AND rv.value = f.value
                          )
                          AND NOT EXISTS (
                            SELECT 1 FROM "column_values" target
                            WHERE target."columnId" = ${fkCol.linkedToColumnId} AND target.value = v.value
                          )
                    `;
                    totalOrphans = Number(totalRes[0].count);
                }
            } else {
                // UNFILTERED (Standard) check
                checkedCount = await prisma.columnValue.count({
                    where: { columnId: fkCol.id, value: { not: '' } }
                });

                orphans = await prisma.$queryRaw<Array<{ value: string }>>`
                    SELECT v.value 
                    FROM "column_values" v
                    WHERE v."columnId" = ${fkCol.id}
                      AND v.value != ''
                      AND NOT EXISTS (
                        SELECT 1 FROM "column_values" target
                        WHERE target."columnId" = ${fkCol.linkedToColumnId}
                          AND target.value = v.value
                      )
                    LIMIT 11
                `;

                if (orphans.length > 0) {
                    const countRes = await prisma.$queryRaw<Array<{ count: bigint }>>`
                        SELECT COUNT(*) as count
                        FROM "column_values" v
                        WHERE v."columnId" = ${fkCol.id}
                          AND v.value != ''
                          AND NOT EXISTS (
                            SELECT 1 FROM "column_values" target
                            WHERE target."columnId" = ${fkCol.linkedToColumnId}
                              AND target.value = v.value
                          )
                    `;
                    totalOrphans = Number(countRes[0].count);
                }
            }

            if (orphans.length > 0) {
                integrityErrors.push({
                    fkTable: fkCol.tableName,
                    fkColumn: fkCol.columnName,
                    pkTable: fkCol.linkedToColumn!.tableName,
                    pkColumn: fkCol.linkedToColumn!.columnName,
                    missingValues: orphans.slice(0, 10).map(o => o.value),
                    missingCount: totalOrphans,
                    totalFkValues: checkedCount
                });

                allChecks.push({
                    type: 'INTEGRITA',
                    label: `${fkCol.tableName}.${fkCol.columnName} -> ${fkCol.linkedToColumn!.tableName}${filter ? ' (Filtrováno rozsahem)' : ''}`,
                    status: 'ERROR',
                    checked: checkedCount,
                    failed: totalOrphans
                });
            } else {
                allChecks.push({
                    type: 'INTEGRITA',
                    label: `${fkCol.tableName}.${fkCol.columnName} -> ${fkCol.linkedToColumn!.tableName}${filter ? ' (Filtrováno rozsahem)' : ''}`,
                    status: 'OK',
                    checked: checkedCount,
                    failed: 0
                });
            }
        }

        // 2. RECONCILIATION CHECKS (Value Mismatches)
        // Group by Table Pair to identify Join Keys vs Value Checks
        // Group by Table Pair to identify Join Keys vs Value Checks
        const tablePairs = new Map<string, { keyLinks: any[], valueLinks: any[] }>();

        for (const col of fkColumns) {
            if (!col.linkedToColumn) continue;
            const pairKey = `${col.tableName}|${col.linkedToColumn.tableName}`;

            if (!tablePairs.has(pairKey)) {
                tablePairs.set(pairKey, { keyLinks: [], valueLinks: [] });
            }
            const group = tablePairs.get(pairKey)!;

            // Definition: If target column is marked as Primary Key (🔑), it's part of the JOIN KEY.
            // Otherwise it's a value to be reconciliated (checked).
            if (col.linkedToColumn.isPrimaryKey) {
                group.keyLinks.push(col);
            } else {
                group.valueLinks.push(col);
            }
        }

        for (const [pairKey, group] of tablePairs.entries()) {
            // We need at least one key to join. If no keys defined, we can't reconcile.
            if (group.keyLinks.length === 0 || group.valueLinks.length === 0) continue;

            // Use the first key for the base join, and others as additional AND conditions
            const primaryKeyLink = group.keyLinks[0];
            const otherKeyLinks = group.keyLinks.slice(1);

            // CRITICAL CHECK: Target Key MUST be unique to avoid Cartesian Product explosion
            // If the user selected a "Key" column that actually contains duplicates (e.g. Currency 'CZK'),
            // the JOIN will produce millions of rows and crash the server (500).
            const targetCol = primaryKeyLink.linkedToColumn!;
            if ((targetCol.rowCount ?? 0) > 0 && (targetCol.uniqueCount ?? 0) < (targetCol.rowCount ?? 0) * 0.99) {
                // If distinct values are significantly less than total rows (< 99%), it's dangerous.
                allChecks.push({
                    type: 'KONFIGURACE',
                    label: `Klíč: ${primaryKeyLink.tableName}.${primaryKeyLink.columnName} -> ${targetCol.tableName}.${targetCol.columnName}`,
                    status: 'ERROR',
                    checked: 0,
                    failed: 1
                });
                // Add a "System Error" to the list so user sees it
                reconciliationErrors.push({
                    linkId: primaryKeyLink.id,
                    sourceTable: primaryKeyLink.tableName,
                    sourceColumn: primaryKeyLink.columnName,
                    targetTable: targetCol.tableName,
                    targetColumn: targetCol.columnName,
                    failureCount: 1,
                    samples: ['CRITICAL: Cílový klíč obsahuje duplicity. Nelze bezpečně napárovat řádky. Opravte data nebo zvolte unikátní klíč.']
                });
                continue; // SKIP the potentially exploding join
            }

            const filterS = extendedFilters.get(primaryKeyLink.tableName);
            const filterT = extendedFilters.get(primaryKeyLink.linkedToColumn.tableName);

            // Construct Join SQL dynamically for Composite Keys
            let joinSQL = `
                FROM "column_values" s_key0
                JOIN "column_values" t_key0 
                    ON s_key0.value = t_key0.value 
                    AND t_key0."columnId" = '${primaryKeyLink.linkedToColumnId}'
            `;

            // Add joins for additional key components
            otherKeyLinks.forEach((kLink, idx) => {
                const i = idx + 1;
                joinSQL += `
                    JOIN "column_values" s_key${i} ON s_key${i}."rowIndex" = s_key0."rowIndex" AND s_key${i}."columnId" = '${kLink.id}'
                    JOIN "column_values" t_key${i} ON t_key${i}."rowIndex" = t_key0."rowIndex" AND t_key${i}."columnId" = '${kLink.linkedToColumnId}' AND s_key${i}.value = t_key${i}.value
                `;
            });

            // Filters logic
            joinSQL += `
                ${filterS ? `JOIN "column_values" fs ON fs."rowIndex" = s_key0."rowIndex" AND fs."columnId" = '${filterS.colId}'` : ''}
                ${filterT ? `JOIN "column_values" ft ON ft."rowIndex" = t_key0."rowIndex" AND ft."columnId" = '${filterT.colId}'` : ''}
                WHERE s_key0."columnId" = '${primaryKeyLink.id}'
                  AND s_key0.value != ''
                  ${filterS ? `AND EXISTS (SELECT 1 FROM "column_values" rv WHERE rv."columnId" = '${filterS.rangeColId}' AND rv.value = fs.value)` : ''}
                  ${filterT ? `AND EXISTS (SELECT 1 FROM "column_values" rv WHERE rv."columnId" = '${filterT.rangeColId}' AND rv.value = ft.value)` : ''}
            `;

            // Get Checked Count (Joined rows)
            const joinedCountRes = await prisma.$queryRawUnsafe<Array<{ count: bigint }>>(`
                SELECT COUNT(*) as count
                ${joinSQL}
            `);
            const checkedCount = Number(joinedCountRes[0].count);

            for (const valCol of group.valueLinks) {
                const mismatches = await prisma.$queryRawUnsafe<Array<{ key: string, source: string, target: string }>>(`
                    SELECT 
                        s_key0.value as key,
                        s_val.value as source,
                        t_val.value as target
                    ${joinSQL}
                    JOIN "column_values" s_val ON s_val."rowIndex" = s_key0."rowIndex" AND s_val."columnId" = '${valCol.id}'
                    JOIN "column_values" t_val ON t_val."rowIndex" = t_key0."rowIndex" AND t_val."columnId" = '${valCol.linkedToColumnId}'
                    AND s_val.value != t_val.value
                    LIMIT 11
                `);

                if (mismatches.length > 0) {
                    const mismatchCountRes = await prisma.$queryRawUnsafe<Array<{ count: bigint }>>(`
                        SELECT COUNT(*) as count
                        ${joinSQL}
                        JOIN "column_values" s_val ON s_val."rowIndex" = s_key0."rowIndex" AND s_val."columnId" = '${valCol.id}'
                        JOIN "column_values" t_val ON t_val."rowIndex" = t_key0."rowIndex" AND t_val."columnId" = '${valCol.linkedToColumnId}'
                        AND s_val.value != t_val.value
                    `);
                    const count = Number(mismatchCountRes[0].count);

                    reconciliationErrors.push({
                        linkId: valCol.id,
                        sourceTable: valCol.tableName,
                        sourceColumn: valCol.columnName,
                        targetTable: valCol.linkedToColumn!.tableName,
                        targetColumn: valCol.linkedToColumn!.columnName,
                        failureCount: count,
                        samples: mismatches.map(m => `Key:${m.key || 'N/A'} | '${m.source}' vs '${m.target}'`)
                    });

                    allChecks.push({
                        type: 'REKONSILIACE',
                        label: `${valCol.tableName}.${valCol.columnName} vs ${valCol.linkedToColumn!.tableName}.${valCol.linkedToColumn!.columnName}`,
                        status: 'ERROR',
                        checked: checkedCount,
                        failed: count
                    });
                } else {
                    allChecks.push({
                        type: 'REKONSILIACE',
                        label: `${valCol.tableName}.${valCol.columnName} vs ${valCol.linkedToColumn!.tableName}.${valCol.linkedToColumn!.columnName}`,
                        status: 'OK',
                        checked: checkedCount,
                        failed: 0
                    });
                }

            }
        }

        // 3. FORBIDDEN VALUES CHECK - Progressive processing per forbidden column
        const forbiddenColumns = await prisma.column.findMany({
            where: { projectId, tableType: 'FORBIDDEN' }
        });

        if (forbiddenColumns.length > 0) {
            // Get candidate columns (TARGET only - skip SOURCE as per user request)
            const candidateColumns = await prisma.column.findMany({
                where: {
                    projectId,
                    tableType: 'TARGET'
                }
            });

            // Process each forbidden column one by one to prevent timeout
            for (const fCol of forbiddenColumns) {
                // For each forbidden column, check against ALL candidate columns
                for (const tCol of candidateColumns) {
                    // Double check type to be 100% sure we skip SOURCE
                    if (tCol.tableType !== 'TARGET') continue;

                    // Skip if same table
                    if (fCol.tableName === tCol.tableName) continue;

                    const filter = extendedFilters.get(tCol.tableName);
                    // Find intersection with normalization (LOWER + TRIM)
                    const intersection = await prisma.$queryRawUnsafe<Array<{
                        match_count: bigint;
                        sample_values: string;
                    }>>(`
                        WITH matched AS(
                    SELECT f_raw.value as val
                            FROM(
                        SELECT DISTINCT LOWER(TRIM(f.value)) as norm_val, f.value
                                FROM "column_values" f
                                WHERE f."columnId" = '${fCol.id}'
                                  AND f.value != ''
                    ) f_raw
                            JOIN(
                        SELECT DISTINCT LOWER(TRIM(t.value)) as norm_val
                                FROM "column_values" t
                                ${filter ? `JOIN "column_values" rf ON rf."rowIndex" = t."rowIndex" AND rf."columnId" = '${filter.colId}'` : ''}
                                WHERE t."columnId" = '${tCol.id}'
                                  AND t.value != ''
                                  ${filter ? `AND EXISTS (SELECT 1 FROM "column_values" rv WHERE rv."columnId" = '${filter.rangeColId}' AND rv.value = rf.value)` : ''}
                            ) t_raw ON f_raw.norm_val = t_raw.norm_val
                        )
SELECT
    (SELECT COUNT(*) FROM matched) as match_count,
    (SELECT STRING_AGG(val, ', ') FROM(SELECT val FROM matched ORDER BY val LIMIT 10) sub) as sample_values
                    `);

                    if (intersection.length > 0 && Number(intersection[0].match_count) > 0) {
                        const failedCount = Number(intersection[0].match_count);
                        const sampleValues = intersection[0].sample_values
                            ? intersection[0].sample_values.split(', ').slice(0, 10)
                            : [];

                        forbiddenErrors.push({
                            forbiddenTable: fCol.tableName,
                            forbiddenColumn: fCol.columnName,
                            targetTable: tCol.tableName,
                            column: tCol.columnName,
                            foundValues: sampleValues,
                            count: failedCount
                        });

                        allChecks.push({
                            type: 'ZAKÁZANÉ',
                            label: `${tCol.tableName}.${tCol.columnName} obsahuje ${failedCount}x zakázanou hodnotu z ${fCol.tableName} (${sampleValues.join(', ')})`,
                            status: 'ERROR',
                            checked: 0,
                            failed: failedCount
                        });

                        console.log(`  ❌ Found ${failedCount} forbidden values in ${tCol.tableName}.${tCol.columnName} `);
                    }
                }
            }

            console.log(`[Forbidden Check]Complete.Found ${forbiddenErrors.length} issues.`);
        }

        // 4. AI VALIDATION RULES CHECK
        const rules = await prisma.validationRule.findMany({
            where: { column: { projectId } },
            include: { column: true }
        });

        const ruleErrors: any[] = [];

        for (const rule of rules) {
            const col = rule.column;
            let failedCount = 0;
            let sampleFailures: string[] = [];

            const filter = extendedFilters.get(col.tableName);
            if (rule.type === 'REGEX' && rule.value) {
                try {
                    if (filter) {
                        const failures = await prisma.$queryRaw<Array<{ value: string }>>`
                            SELECT v.value FROM "column_values" v
                            JOIN "column_values" f ON f."rowIndex" = v."rowIndex" AND f."columnId" = ${filter.colId}
                            WHERE v."columnId" = ${col.id}
                              AND v.value != ''
                              AND EXISTS(SELECT 1 FROM "column_values" rv WHERE rv."columnId" = ${filter.rangeColId} AND rv.value = f.value)
                              AND NOT(v.value ~${rule.value})
                            LIMIT 10
    `;

                        if (failures.length > 0) {
                            const countRes = await prisma.$queryRaw<Array<{ count: bigint }>>`
                                SELECT COUNT(*) as count FROM "column_values" v
                                JOIN "column_values" f ON f."rowIndex" = v."rowIndex" AND f."columnId" = ${filter.colId}
                                WHERE v."columnId" = ${col.id}
                                  AND v.value != ''
                                  AND EXISTS(SELECT 1 FROM "column_values" rv WHERE rv."columnId" = ${filter.rangeColId} AND rv.value = f.value)
                                  AND NOT(v.value ~${rule.value})
    `;
                            failedCount = Number(countRes[0].count);
                            sampleFailures = failures.map(f => f.value);
                        }
                    } else {
                        const failures = await prisma.$queryRaw<Array<{ value: string }>>`
                            SELECT value FROM "column_values"
                            WHERE "columnId" = ${col.id}
                              AND value != ''
                              AND NOT(value ~${rule.value})
                            LIMIT 10
    `;

                        if (failures.length > 0) {
                            const countRes = await prisma.$queryRaw<Array<{ count: bigint }>>`
                                SELECT COUNT(*) as count FROM "column_values"
                                WHERE "columnId" = ${col.id}
                                  AND value != ''
                                  AND NOT(value ~${rule.value})
                            `;
                            failedCount = Number(countRes[0].count);
                            sampleFailures = failures.map(f => f.value);
                        }
                    }
                } catch (err) {
                    console.error(`Regex check failed for ${rule.value}: `, err);
                }
            } else if (rule.type === 'NOT_NULL') {
                if (filter) {
                    const countRes = await prisma.$queryRaw<Array<{ count: bigint }>>`
                        SELECT COUNT(*) as count FROM "column_values" v
                        JOIN "column_values" f ON f."rowIndex" = v."rowIndex" AND f."columnId" = ${filter.colId}
                        WHERE v."columnId" = ${col.id}
AND(v.value IS NULL OR v.value = '')
                          AND EXISTS(SELECT 1 FROM "column_values" rv WHERE rv."columnId" = ${filter.rangeColId} AND rv.value = f.value)
    `;
                    failedCount = Number(countRes[0].count);
                } else {
                    failedCount = col.nullCount || 0;
                }
                if (failedCount > 0) {
                    sampleFailures = ['(Empty values)'];
                }
            } else if (rule.type === 'UNIQUE') {
                if (filter) {
                    const duplicates = await prisma.$queryRaw<Array<{ value: string, count: bigint }>>`
                        SELECT v.value, COUNT(*) as count
                        FROM "column_values" v
                        JOIN "column_values" f ON f."rowIndex" = v."rowIndex" AND f."columnId" = ${filter.colId}
                        WHERE v."columnId" = ${col.id}
                          AND v.value != ''
                          AND EXISTS(SELECT 1 FROM "column_values" rv WHERE rv."columnId" = ${filter.rangeColId} AND rv.value = f.value)
                        GROUP BY v.value
                        HAVING COUNT(*) > 1
                        LIMIT 11
    `;
                    if (duplicates.length > 0) {
                        const totalRes = await prisma.$queryRaw<Array<{ count: bigint }>>`
                           SELECT SUM(sub.dupe_count) as count FROM(
        SELECT COUNT(*) as dupe_count
                             FROM "column_values" v
                             JOIN "column_values" f ON f."rowIndex" = v."rowIndex" AND f."columnId" = ${filter.colId}
                             WHERE v."columnId" = ${col.id}
                               AND v.value != ''
                               AND EXISTS(SELECT 1 FROM "column_values" rv WHERE rv."columnId" = ${filter.rangeColId} AND rv.value = f.value)
                             GROUP BY v.value
                             HAVING COUNT(*) > 1
    ) AS sub
                        `;
                        failedCount = Number(totalRes[0].count);
                        sampleFailures = duplicates.slice(0, 10).map(d => `${d.value} (${d.count}x)`);
                    }
                } else {
                    // Approximate check using metadata
                    if (col.uniqueCount !== col.rowCount) {
                        // Get duplicates
                        const duplicates = await prisma.$queryRaw<Array<{ value: string, count: bigint }>>`
                            SELECT value, COUNT(*) as count
                            FROM "column_values"
                            WHERE "columnId" = ${col.id}
                            GROUP BY value
                            HAVING COUNT(*) > 1
                            LIMIT 5
                         `;
                        failedCount = col.rowCount! - col.uniqueCount!;
                        sampleFailures = duplicates.map(d => `${d.value} (${d.count}x)`);
                    }
                }
            } else if (rule.type === 'MATH_EQUATION' && rule.value) {
                // Rule format: "[ColA] * [ColB] = [ColC]"
                try {
                    const parts = rule.value.split('=');
                    const leftSide = parts[0].trim();
                    const rightSide = parts[1].trim();

                    // Simple parser for [Col] operators
                    const colNames = rule.value.match(/\[(.*?)\]/g)?.map(m => m.replace(/[\[\]]/g, '')) || [];

                    // Find column IDs for all referenced columns in this table
                    const tableCols = await prisma.column.findMany({
                        where: { tableName: col.tableName, projectId }
                    });

                    const nameToId: Record<string, string> = {};
                    tableCols.forEach(tc => nameToId[tc.columnName] = tc.id);

                    // We need at least the columns mentioned
                    const missing = colNames.filter(n => !nameToId[n]);
                    if (missing.length === 0) {
                        // Construct complex SQL to join values for the same row
                        // Dynamically build a row-joiner
                        let query = `SELECT cv0."rowIndex", `;
                        colNames.forEach((n, idx) => {
                            query += `CAST(NULLIF(cv${idx}.value, '') AS DOUBLE PRECISION) as val${idx}${idx < colNames.length - 1 ? ', ' : ''} `;
                        });
                        query += ` FROM "column_values" cv0 `;
                        for (let i = 1; i < colNames.length; i++) {
                            query += `JOIN "column_values" cv${i} ON cv0."rowIndex" = cv${i}."rowIndex" AND cv${i}."columnId" = '${nameToId[colNames[i]]}' `;
                        }
                        if (filter) {
                            query += `JOIN "column_values" f ON f."rowIndex" = cv0."rowIndex" AND f."columnId" = '${filter.colId}' `;
                        }
                        query += `WHERE cv0."columnId" = '${nameToId[colNames[0]]}' `;
                        if (filter) {
                            query += `AND EXISTS(SELECT 1 FROM "column_values" rv WHERE rv."columnId" = '${filter.rangeColId}' AND rv.value = f.value) `;
                        }

                        // Parse expression into SQL
                        // replace [Name] with valX
                        let sqlExpr = rule.value.replace(/\[(.*?)\]/g, (match, name) => {
                            const idx = colNames.indexOf(name);
                            return `val${idx} `;
                        }).replace('=', '!='); // We want to FIND mismatches

                        // Handle potential division by zero or large precision errors
                        const mismatches = await prisma.$queryRawUnsafe<any[]>(`
                            WITH rows AS(${query})
SELECT * FROM rows 
                            WHERE ABS((${sqlExpr.split('!=')[0]}) - (${sqlExpr.split('!=')[1]})) > 0.1
                            LIMIT 10
    `);

                        if (mismatches.length > 0) {
                            const countRes = await prisma.$queryRawUnsafe<any[]>(`
                                WITH rows AS(${query})
                                SELECT COUNT(*) as count FROM rows 
                                WHERE ABS((${sqlExpr.split('!=')[0]}) - (${sqlExpr.split('!=')[1]})) > 0.1
    `);
                            failedCount = Number(countRes[0].count);
                            sampleFailures = mismatches.map(m => `Řádek ${m.rowIndex + 1}: Nesedí výpočet`);
                        }
                    }
                } catch (err) {
                    console.error('Math validation failed:', err);
                }
            }

            if (failedCount > 0) {
                ruleErrors.push({
                    ruleId: rule.id,
                    table: col.tableName,
                    column: col.columnName,
                    ruleType: rule.type,
                    ruleValue: rule.value,
                    description: rule.description,
                    failedCount,
                    samples: sampleFailures
                });

                allChecks.push({
                    type: 'PRAVIDLO',
                    label: `${col.tableName}.${col.columnName} (${rule.type})`,
                    status: 'ERROR',
                    checked: col.rowCount || 0,
                    failed: failedCount
                });
            } else {
                allChecks.push({
                    type: 'PRAVIDLO',
                    label: `${col.tableName}.${col.columnName} (${rule.type})`,
                    status: 'OK',
                    checked: col.rowCount || 0,
                    failed: 0
                });
            }
        }


        // 5. FINANCIAL ANALYSIS (Auto-detect amounts and currencies)
        const financialSummary: any[] = [];
        const potentialAmountCols = fkColumns.filter((c: any) =>
            c.columnName.toLowerCase().includes('amount') ||
            c.columnName.toLowerCase().includes('čás') ||
            c.columnName.toLowerCase().includes('debit') ||
            c.columnName.toLowerCase().includes('credit') ||
            c.columnName.toLowerCase().includes('total')
        );

        for (const fcol of potentialAmountCols) {
            try {
                const sumRes = await prisma.$queryRawUnsafe<Array<{ total: number }>>(`
                    SELECT SUM(CASE WHEN value ~ '^-?\\d*\\.?\\d+$' THEN CAST(value AS DOUBLE PRECISION) ELSE 0 END) as total 
                    FROM "column_values" 
                    WHERE "columnId" = '${fcol.id}'
    `);
                financialSummary.push({
                    table: fcol.tableName,
                    column: fcol.columnName,
                    total: sumRes[0].total || 0
                });
            } catch (e) { /* ignore cast errors */ }
        }

        // Generate Protocol
        const totalFailed = allChecks.filter(c => c.status === 'ERROR').length;
        const totalPassed = allChecks.length - totalFailed;

        let protocol = `PROTOKOL O VALIDACI\n =====================\nDatum: ${new Date().toLocaleString('cs-CZ')} \nStav: ${totalFailed > 0 ? 'S CHYBAMI' : 'ÚSPĚŠNÉ'} \n${'-'.repeat(40)} \nCelkem kontrol: ${allChecks.length} \nÚspěšné: ${totalPassed} \nSelhalo: ${totalFailed} \n\n`;

        if (financialSummary.length > 0) {
            protocol += `[FINANČNÍ PŘEHLED(SUMARIZACE)]\n`;
            financialSummary.forEach(f => {
                protocol += `- ${f.table}.${f.column}: ${f.total.toLocaleString('cs-CZ', { minimumFractionDigits: 2 })} \n`;
            });
            protocol += `\n`;
        }

        protocol += `[METODIKA VALIDACE]\n1.INTEGRITA(Foreign Key) \n - Metoda: SQL NOT EXISTS query.\n - Princip: Hledá hodnoty ve zdrojovém sloupci, které nemají odpovídající záznam v cílové(master) tabulce.\n2.REKONSILIACE(Shoda dat) \n - Metoda: SQL Comparison přes JOIN klíč.\n - Princip: Spojí řádky tabulek přes definovaný klíč a porovnává hodnoty v ostatních sloupcích.\n3.ZAKÁZANÉ HODNOTY(Blacklist) \n - Metoda: SQL INTERSECT(case -insensitive).\n4.PRAVIDLA(AI / Custom / FINANCIAL) \n - Regex, NotNull, Unique, Math Equations.\n\n`;

        protocol += `[DETAILNÍ VÝPIS VŠECH KONTROL]\n`;
        allChecks.forEach(check => {
            protocol += `[${check.status === 'OK' ? 'OK' : 'CHYBA'}] ${check.type}: ${check.label} (Zkontrolováno: ${check.checked}, Vadných: ${check.failed}) \n`;
        });

        if (totalFailed > 0) {
            protocol += `\n[DETAIL CHYB]\n`;

            if (forbiddenErrors.length > 0) {
                protocol += `-- - ZAKÁZANÉ HODNOTY-- -\n`;
                forbiddenErrors.forEach(e => {
                    protocol += `- ${e.targetTable}.${e.column} obsahuje ${e.count} zakázaných hodnot(např.${e.foundValues.join(', ')}) \n`;
                });
            }
            if (integrityErrors.length > 0) {
                protocol += `-- - INTEGRITA(SIROTCI)-- -\n`;
                integrityErrors.forEach(e => {
                    protocol += `- ${e.fkTable}.${e.fkColumn} -> ${e.pkTable}: ${e.missingCount} chybějících\n`;
                });
            }
            if (reconciliationErrors.length > 0) {
                protocol += `-- - REKONSILIACE(NESHODY)-- -\n`;
                reconciliationErrors.forEach(e => {
                    protocol += `- ${e.sourceTable}.${e.sourceColumn} vs ${e.targetTable}.${e.targetColumn}: ${e.failureCount} neshod\n`;
                });
            }
            if (ruleErrors.length > 0) {
                protocol += `-- - PRAVIDLA(AI)-- -\n`;
                ruleErrors.forEach(e => {
                    protocol += `- ${e.table}.${e.column} (${e.ruleType}): ${e.failedCount} chyb.${e.description || ''} \n`;
                });
            }

            protocol += `\n[ZÁVĚR]\nValidace nalezla ${totalFailed} chyb.Nutná oprava dat.\n`;
        } else {
            protocol += `\n[ZÁVĚR]\nVšechna data jsou konzistentní.Nebyly nalezeny žádné chyby.\n`;
        }

        res.json({
            success: true,
            errors: integrityErrors,
            reconciliation: reconciliationErrors,
            forbidden: forbiddenErrors,
            validationRules: ruleErrors,
            protocol: protocol,
            summary: {
                totalChecks: allChecks.length,
                passed: totalPassed,
                failed: totalFailed
            }
        });

    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// ============================================
// SERVE STATIC FRONTEND (Production)
// ============================================
// ============================================
// SERVE STATIC FRONTEND
// ============================================
const distPath = path.join(__dirname, '../../dist');
const indexPath = path.join(distPath, 'index.html');

console.log('📂 Static files path:', distPath);
console.log('📄 Index file path:', indexPath);

if (fs.existsSync(distPath)) {
    console.log('✅ Dist folder found. Serving static files...');
    app.use(express.static(distPath));

    // SPA Fallback - Catch all requests that didn't match an API route
    app.use((req, res) => {
        if (fs.existsSync(indexPath)) {
            res.sendFile(indexPath);
        } else {
            console.error('❌ Index.html not found at:', indexPath);
            res.status(404).send('Valibook Frontend not found (build missing?)');
        }
    });
} else {
    console.warn('⚠️ Dist folder NOT found at:', distPath);
    console.warn('Current directory:', __dirname);
}

// ============================================
// START SERVER
// ============================================
const server = app.listen(PORT, () => {
    console.log(`🚀 Valibook server running on http://localhost:${PORT}`);
    console.log(`📊 Database: PostgreSQL on Railway`);
});

// Set timeout to 5 minutes to allow large file processing
server.setTimeout(300000);
