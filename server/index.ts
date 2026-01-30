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
    const { isPrimaryKey, isRequired, linkedToColumnId } = req.body;

    try {
        const column = await prisma.column.update({
            where: { id },
            data: {
                ...(isPrimaryKey !== undefined && { isPrimaryKey }),
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
// AI SUGGEST RULES (OpenAI Integration)
// ============================================
app.post('/api/projects/:projectId/ai-suggest-rules', async (req, res) => {
    const { projectId } = req.params;
    const { apiKey, password } = req.body;

    // 1. Simple Security Gate
    if (password !== 'Heslo123') {
        return res.status(403).json({ error: 'Neplatné heslo pro AI funkce.' });
    }
    if (!apiKey) {
        return res.status(400).json({ error: 'Chybí OpenAI API Key.' });
    }

    try {
        // 2. Generate Profile (Reuse logic or call internal function)
        const columns = await prisma.column.findMany({
            where: { projectId },
            orderBy: [{ tableName: 'asc' }, { columnIndex: 'asc' }]
        });

        // Simplified profile for AI token limits
        const tablesProfile: Record<string, any[]> = {};
        const columnIdMap: Record<string, string> = {}; // Name -> ID mapping

        for (const col of columns) {
            if (!tablesProfile[col.tableName]) tablesProfile[col.tableName] = [];

            // Store ID for later mapping
            const key = `${col.tableName}.${col.columnName}`;
            columnIdMap[key] = col.id;

            tablesProfile[col.tableName].push({
                column: col.columnName,
                // Only send relevant stats to save likely tokens
                type: col.dataType || 'STRING',
                sample: col.sampleValues ? (col.sampleValues as string[]).slice(0, 3) : [], // reduced samples for AI context
                stats: {
                    unique: col.uniqueCount,
                    nulls: col.nullCount,
                    pattern: col.pattern // e.g. Regex if detected
                }
            });
        }

        // 3. Construct Prompt
        const prompt = `
            You are a Data Quality Engineer. Analyze the database schema below and suggest validation rules.
            
            SCHEMA:
            ${JSON.stringify(tablesProfile, null, 2)}

            INSTRUCTIONS:
            - Suggest rules to ensure data quality (Integrity, Format, Completeness).
            - Focus on: VAT numbers, Emails, Phone numbers, IDs, Required fields.
            - If a column has 'nulls: 0', suggest a NOT_NULL rule.
            - If a column has 'unique' close to row count, suggest UNIQUE rule.
            - Provide a valid Regex for standard formats (VAT, IBAN, etc).
            - Output ONLY valid JSON array.

            OUTPUT FORMAT:
            [
                {
                    "table": "TableName",
                    "column": "ColumnName",
                    "type": "REGEX" | "NOT_NULL" | "UNIQUE",
                    "value": "^[A-Z]{2}\\d+$" (for regex only),
                    "description": "VAT number must be valid format",
                    "severity": "ERROR" | "WARNING"
                }
            ]
        `;

        // 4. Call OpenAI
        const aiRes = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${apiKey}`
            },
            body: JSON.stringify({
                model: "gpt-4o-mini", // Cost effective
                messages: [
                    { role: "system", content: "You are a pragmatic Data Quality assistant. Output JSON only." },
                    { role: "user", content: prompt }
                ],
                temperature: 0.2
            })
        });

        if (!aiRes.ok) {
            const errText = await aiRes.text();
            throw new Error(`OpenAI API Error: ${errText}`);
        }

        const aiData = await aiRes.json();
        let content = aiData.choices[0].message.content;

        // Clean markdown code blocks if present
        content = content.replace(/```json/g, '').replace(/```/g, '').trim();

        const suggestedRules = JSON.parse(content);

        // 5. Save Rules to DB
        const savedRules = [];

        // Clear existing rules for this project (optional, or append?) 
        // Let's APPEND/UPDATE but for now we just create new ones.
        // Actually, user might want to clear old ones first. Let's keep it simple: create.

        for (const rule of suggestedRules) {
            const key = `${rule.table}.${rule.column}`;
            const columnId = columnIdMap[key];

            if (columnId) {
                const saved = await prisma.validationRule.create({
                    data: {
                        columnId,
                        type: rule.type,
                        value: rule.value || null,
                        severity: rule.severity || 'ERROR',
                        description: rule.description
                    }
                });
                savedRules.push(saved);
            }
        }

        res.json({ success: true, rules: savedRules });

    } catch (error) {
        console.error(error);
        res.status(500).json({ error: (error as Error).message });
    }
});

// ============================================
// AUTO-DETECT LINKS - Find columns with overlapping values
// ============================================
app.post('/api/projects/:projectId/detect-links', async (req, res) => {
    const { projectId } = req.params;

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
        const columnAllValuesMap = new Map<string, Set<string>>();

        for (const col of columns) {
            // Get 10 random samples using subquery approach
            const samples = await prisma.$queryRaw<Array<{ value: string }>>`
                SELECT value FROM (
                    SELECT DISTINCT value 
                    FROM "column_values" 
                    WHERE "columnId" = ${col.id} 
                      AND value != '' 
                      AND value IS NOT NULL
                ) AS distinct_vals
                ORDER BY random() 
                LIMIT 100
            `;
            columnSamplesMap.set(col.id, samples.map(s => s.value));

            // Get all unique values for this column (for lookup)
            const allValues = await prisma.columnValue.findMany({
                where: { columnId: col.id, value: { not: '' } },
                select: { value: true },
                distinct: ['value']
            });
            columnAllValuesMap.set(col.id, new Set(allValues.map(v => v.value)));
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

        // Track seen pairs to avoid duplicates (A->B and B->A)
        const seenPairs = new Set<string>();

        for (const colA of columns) {
            const samplesA = columnSamplesMap.get(colA.id);
            if (!samplesA || samplesA.length === 0) continue;

            for (const colB of columns) {
                if (colA.id === colB.id) continue;
                if (colA.tableName === colB.tableName) continue; // Same table

                // Skip if we've already seen this pair (in either direction)
                const pairKey = [colA.id, colB.id].sort().join('|');
                if (seenPairs.has(pairKey)) continue;

                const valuesB = columnAllValuesMap.get(colB.id);
                if (!valuesB || valuesB.size === 0) continue;

                // Count how many samples from A exist in B
                let matchCount = 0;
                for (const sample of samplesA) {
                    if (valuesB.has(sample)) matchCount++;
                }

                // Require 80% match AND minimum 5 samples (more strict with larger sample size)
                const matchPercentage = Math.round((matchCount / samplesA.length) * 100);
                if (matchPercentage >= 80 && matchCount >= 5) {
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
                                matchPercentage,
                                commonValues: matchCount,
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
                                matchPercentage,
                                commonValues: matchCount,
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
        const fkColumns = await prisma.column.findMany({
            where: {
                projectId,
                linkedToColumnId: { not: null }
            },
            include: {
                linkedToColumn: true
            }
        });

        // 1. INTEGRITY CHECKS (Orphans) - SQL Optimized
        const integrityErrors = [];
        const reconciliationErrors = [];
        const forbiddenErrors: any[] = [];
        const allChecks: Array<{ type: string; label: string; status: 'OK' | 'ERROR'; checked: number; failed: number; }> = [];

        for (const fkCol of fkColumns) {
            if (!fkCol.linkedToColumn) continue;

            // Get total checked count (non-empty values)
            const checkedCount = await prisma.columnValue.count({
                where: { columnId: fkCol.id, value: { not: '' } }
            });

            const orphans = await prisma.$queryRaw<Array<{ value: string }>>`
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
                // Get approx count
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

                integrityErrors.push({
                    fkTable: fkCol.tableName,
                    fkColumn: fkCol.columnName,
                    pkTable: fkCol.linkedToColumn.tableName,
                    pkColumn: fkCol.linkedToColumn.columnName,
                    missingValues: orphans.slice(0, 10).map(o => o.value),
                    missingCount: Number(countRes[0].count),
                    totalFkValues: checkedCount
                });

                allChecks.push({
                    type: 'INTEGRITA',
                    label: `${fkCol.tableName}.${fkCol.columnName} -> ${fkCol.linkedToColumn.tableName}`,
                    status: 'ERROR',
                    checked: checkedCount,
                    failed: Number(countRes[0].count)
                });
            } else {
                allChecks.push({
                    type: 'INTEGRITA',
                    label: `${fkCol.tableName}.${fkCol.columnName} -> ${fkCol.linkedToColumn.tableName}`,
                    status: 'OK',
                    checked: checkedCount,
                    failed: 0
                });
            }
        }

        // 2. RECONCILIATION CHECKS (Value Mismatches)
        // Group by Table Pair to identify Join Keys vs Value Checks
        const tablePairs = new Map<string, { keyLink: any, valueLinks: any[] }>();

        for (const col of fkColumns) {
            if (!col.linkedToColumn) continue;
            const pairKey = `${col.tableName}|${col.linkedToColumn.tableName}`;

            if (!tablePairs.has(pairKey)) {
                tablePairs.set(pairKey, { keyLink: null, valueLinks: [] });
            }
            const group = tablePairs.get(pairKey)!;

            // Assumption: If target is PK, it's used as JOIN KEY. Otherwise it's a value to check.
            if (col.linkedToColumn.isPrimaryKey) {
                if (!group.keyLink) group.keyLink = col;
            } else {
                group.valueLinks.push(col);
            }
        }

        for (const [pairKey, group] of tablePairs.entries()) {
            if (!group.keyLink || group.valueLinks.length === 0) continue;

            // Get Checked Count (Joined rows)
            const joinedCountRes = await prisma.$queryRaw<Array<{ count: bigint }>>`
                SELECT COUNT(*) as count
                FROM "column_values" s_key
                JOIN "column_values" t_key 
                    ON s_key.value = t_key.value 
                    AND t_key."columnId" = ${group.keyLink.linkedToColumnId}
                WHERE s_key."columnId" = ${group.keyLink.id}
                  AND s_key.value != ''
            `;
            const checkedCount = Number(joinedCountRes[0].count);

            for (const valCol of group.valueLinks) {
                const mismatches = await prisma.$queryRaw<Array<{ key: string, source: string, target: string }>>`
                    SELECT 
                        s_key.value as key,
                        s_val.value as source,
                        t_val.value as target
                    FROM "column_values" s_key
                    JOIN "column_values" t_key 
                        ON s_key.value = t_key.value 
                        AND t_key."columnId" = ${group.keyLink.linkedToColumnId}
                    JOIN "column_values" s_val
                        ON s_val."rowIndex" = s_key."rowIndex"
                        AND s_val."columnId" = ${valCol.id}
                    JOIN "column_values" t_val
                        ON t_val."rowIndex" = t_key."rowIndex"
                        AND t_val."columnId" = ${valCol.linkedToColumnId}
                    WHERE s_key."columnId" = ${group.keyLink.id}
                      AND s_val.value != t_val.value
                    LIMIT 11
                `;

                if (mismatches.length > 0) {
                    const mismatchCountRes = await prisma.$queryRaw<Array<{ count: bigint }>>`
                        SELECT COUNT(*) as count
                        FROM "column_values" s_key
                        JOIN "column_values" t_key ON s_key.value = t_key.value AND t_key."columnId" = ${group.keyLink.linkedToColumnId}
                        JOIN "column_values" s_val ON s_val."rowIndex" = s_key."rowIndex" AND s_val."columnId" = ${valCol.id}
                        JOIN "column_values" t_val ON t_val."rowIndex" = t_key."rowIndex" AND t_val."columnId" = ${valCol.linkedToColumnId}
                        WHERE s_key."columnId" = ${group.keyLink.id}
                          AND s_val.value != t_val.value
                     `;
                    const failedCount = Number(mismatchCountRes[0].count);

                    reconciliationErrors.push({
                        sourceTable: valCol.tableName,
                        sourceColumn: valCol.columnName,
                        targetTable: valCol.linkedToColumn?.tableName,
                        targetColumn: valCol.linkedToColumn?.columnName,
                        joinKey: group.keyLink.columnName,
                        mismatches: mismatches.slice(0, 10),
                        count: failedCount
                    });

                    allChecks.push({
                        type: 'REKONSILIACE',
                        label: `${valCol.tableName}.${valCol.columnName} vs ${valCol.linkedToColumn?.tableName}.${valCol.linkedToColumn?.columnName}`,
                        status: 'ERROR',
                        checked: checkedCount,
                        failed: failedCount
                    });
                } else {
                    allChecks.push({
                        type: 'REKONSILIACE',
                        label: `${valCol.tableName}.${valCol.columnName} vs ${valCol.linkedToColumn?.tableName}.${valCol.linkedToColumn?.columnName}`,
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
            // Get ALL candidate columns (SOURCE and TARGET) - no name filtering
            const candidateColumns = await prisma.column.findMany({
                where: {
                    projectId,
                    tableType: { in: ['SOURCE', 'TARGET'] }
                }
            });

            // Process each forbidden column one by one to prevent timeout
            for (const fCol of forbiddenColumns) {
                // Get count of forbidden values for logging
                const forbiddenValueCount = await prisma.columnValue.count({
                    where: { columnId: fCol.id, value: { not: '' } }
                });
                console.log(`[Forbidden Check] Processing: ${fCol.tableName}.${fCol.columnName} (${forbiddenValueCount} values)`);

                // For each forbidden column, check against ALL candidate columns
                for (const tCol of candidateColumns) {
                    // Skip if same table
                    if (fCol.tableName === tCol.tableName) continue;

                    // Find intersection with normalization (LOWER + TRIM)
                    const intersection = await prisma.$queryRaw<Array<{
                        match_count: bigint;
                        sample_values: string;
                    }>>`
                        WITH matched AS (
                            SELECT f_raw.value as val
                            FROM (
                                SELECT DISTINCT LOWER(TRIM(f.value)) as norm_val, f.value
                                FROM "column_values" f
                                WHERE f."columnId" = ${fCol.id}
                                  AND f.value != ''
                            ) f_raw
                            JOIN (
                                SELECT DISTINCT LOWER(TRIM(t.value)) as norm_val
                                FROM "column_values" t
                                WHERE t."columnId" = ${tCol.id}
                                  AND t.value != ''
                            ) t_raw ON f_raw.norm_val = t_raw.norm_val
                        )
                        SELECT 
                            (SELECT COUNT(*) FROM matched) as match_count,
                            (SELECT STRING_AGG(val, ', ') FROM (SELECT val FROM matched ORDER BY val LIMIT 10) sub) as sample_values
                    `;

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
                            label: `${tCol.tableName}.${tCol.columnName} obsahuje ${failedCount}x hodnot z ${fCol.tableName}.${fCol.columnName}`,
                            status: 'ERROR',
                            checked: 0,
                            failed: failedCount
                        });

                        console.log(`  ❌ Found ${failedCount} forbidden values in ${tCol.tableName}.${tCol.columnName}`);
                    }
                }
            }

            console.log(`[Forbidden Check] Complete. Found ${forbiddenErrors.length} issues.`);
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

            if (rule.type === 'REGEX' && rule.value) {
                // Check regex against all values (PostgreSQL Syntax ~)
                // Use NOT ~ to find mismatches
                // Filter out empty strings if NOT_NULL is not enforced here (usually format applies to non-empty)
                try {
                    const failures = await prisma.$queryRaw<Array<{ value: string }>>`
                        SELECT value FROM "column_values"
                        WHERE "columnId" = ${col.id}
                          AND value != ''
                          AND NOT (value ~ ${rule.value})
                        LIMIT 10
                    `;

                    if (failures.length > 0) {
                        const countRes = await prisma.$queryRaw<Array<{ count: bigint }>>`
                            SELECT COUNT(*) as count FROM "column_values"
                            WHERE "columnId" = ${col.id}
                              AND value != ''
                              AND NOT (value ~ ${rule.value})
                        `;
                        failedCount = Number(countRes[0].count);
                        sampleFailures = failures.map(f => f.value);
                    }
                } catch (err) {
                    console.error(`Regex check failed for ${rule.value}:`, err);
                    // Invalid regex provided by AI?
                }

            } else if (rule.type === 'NOT_NULL') {
                failedCount = col.nullCount || 0;
                if (failedCount > 0) {
                    sampleFailures = ['(Empty values)'];
                }
            } else if (rule.type === 'UNIQUE') {
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
                    failedCount = col.rowCount! - col.uniqueCount!; // Rough estimate of issues
                    sampleFailures = duplicates.map(d => `${d.value} (${d.count}x)`);
                }
            }

            if (failedCount > 0) {
                ruleErrors.push({
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


        // Generate Protocol
        const totalFailed = allChecks.filter(c => c.status === 'ERROR').length;
        const totalPassed = allChecks.filter(c => c.status === 'OK').length;

        let protocol = `PROTOKOL O VALIDACI\n=====================\nDatum: ${new Date().toLocaleString('cs-CZ')}\nStav: ${totalFailed > 0 ? 'S CHYBAMI' : 'ÚSPĚŠNÉ'}\n${'-'.repeat(40)}\nCelkem kontrol: ${allChecks.length}\nÚspěšné: ${totalPassed}\nSelhalo: ${totalFailed}\n\n`;

        protocol += `[METODIKA VALIDACE]\n1. INTEGRITA (Foreign Key)\n   - Metoda: SQL NOT EXISTS query.\n   - Princip: Hledá hodnoty ve zdrojovém sloupci, které nemají odpovídající záznam v cílové (master) tabulce.\n2. REKONSILIACE (Shoda dat)\n   - Metoda: SQL Comparison přes JOIN klíč.\n   - Princip: Spojí řádky tabulek přes definovaný klíč a porovnává hodnoty v ostatních sloupcích.\n3. ZAKÁZANÉ HODNOTY (Blacklist)\n   - Metoda: SQL INTERSECT (case-insensitive).\n4. PRAVIDLA (AI/Custom)\n   - Regex, NotNull, Unique kontroly.\n\n`;

        protocol += `[DETAILNÍ VÝPIS VŠECH KONTROL]\n`;
        allChecks.forEach(check => {
            protocol += `[${check.status === 'OK' ? 'OK' : 'CHYBA'}] ${check.type}: ${check.label} (Zkontrolováno: ${check.checked}, Vadných: ${check.failed})\n`;
        });

        if (totalFailed > 0) {
            protocol += `\n[DETAIL CHYB]\n`;

            if (forbiddenErrors.length > 0) {
                protocol += `--- ZAKÁZANÉ HODNOTY ---\n`;
                forbiddenErrors.forEach(e => {
                    protocol += `- ${e.targetTable}.${e.column} obsahuje ${e.count} zakázaných hodnot (např. ${e.foundValues.join(', ')})\n`;
                });
            }
            if (integrityErrors.length > 0) {
                protocol += `--- INTEGRITA (SIROTCI) ---\n`;
                integrityErrors.forEach(e => {
                    protocol += `- ${e.fkTable}.${e.fkColumn} -> ${e.pkTable}: ${e.missingCount} chybějících\n`;
                });
            }
            if (reconciliationErrors.length > 0) {
                protocol += `--- REKONSILIACE (NESHODY) ---\n`;
                reconciliationErrors.forEach(e => {
                    protocol += `- ${e.sourceTable} vs ${e.targetTable} (${e.joinKey}): ${e.count} neshod\n`;
                });
            }
            if (ruleErrors.length > 0) {
                protocol += `--- PRAVIDLA (AI) ---\n`;
                ruleErrors.forEach(e => {
                    protocol += `- ${e.table}.${e.column} (${e.ruleType}): ${e.failedCount} chyb. ${e.description || ''}\n`;
                });
            }

            protocol += `\n[ZÁVĚR]\nValidace nalezla ${totalFailed} chyb. Nutná oprava dat.\n`;
        } else {
            protocol += `\n[ZÁVĚR]\nVšechna data jsou konzistentní. Nebyly nalezeny žádné chyby.\n`;
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
