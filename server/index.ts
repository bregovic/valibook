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

            // Get 10 random sample values
            const shuffled = [...uniqueValues].sort(() => 0.5 - Math.random());
            const sampleValues = shuffled.slice(0, 10);

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
                    sampleValues
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
                LIMIT 10
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

                // Require 80% match AND minimum 3 samples
                const matchPercentage = Math.round((matchCount / samplesA.length) * 100);
                if (matchPercentage >= 80 && matchCount >= 3) {
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

        // 3. FORBIDDEN VALUES CHECK
        const forbiddenColumns = await prisma.column.findMany({
            where: { projectId, tableType: 'FORBIDDEN' }
        });

        if (forbiddenColumns.length > 0) {
            // Find target columns with same name in SOURCE/TARGET tables
            const candidateColumns = await prisma.column.findMany({
                where: {
                    projectId,
                    tableType: { in: ['SOURCE', 'TARGET'] }
                }
            });

            for (const fCol of forbiddenColumns) {
                // Only check columns with SAME NAME (case insensitive) for performance
                const matchingColumns = candidateColumns.filter(
                    c => c.columnName.toLowerCase() === fCol.columnName.toLowerCase()
                );

                for (const tCol of matchingColumns) {
                    // Skip if same table
                    if (tCol.tableName === fCol.tableName) continue;

                    // Check intersection
                    const intersection = await prisma.$queryRaw<Array<{ value: string }>>`
                        SELECT v1.value 
                        FROM "column_values" v1
                        JOIN "column_values" v2 ON v1.value = v2.value
                        WHERE v1."columnId" = ${tCol.id}
                          AND v2."columnId" = ${fCol.id}
                        LIMIT 11
                     `;

                    if (intersection.length > 0) {
                        const countRes = await prisma.$queryRaw<Array<{ count: bigint }>>`
                            SELECT COUNT(*) as count
                            FROM "column_values" v1
                            JOIN "column_values" v2 ON v1.value = v2.value
                            WHERE v1."columnId" = ${tCol.id}
                              AND v2."columnId" = ${fCol.id}
                        `;
                        const failedCount = Number(countRes[0].count);

                        forbiddenErrors.push({
                            forbiddenTable: fCol.tableName,
                            forbiddenColumn: fCol.columnName,
                            targetTable: tCol.tableName,
                            column: tCol.columnName,
                            foundValues: intersection.slice(0, 10).map(v => v.value),
                            count: failedCount
                        });

                        allChecks.push({
                            type: 'ZAKÁZANÉ',
                            label: `${tCol.tableName}.${tCol.columnName} obsahuje hodnoty z ${fCol.tableName}.${fCol.columnName}`,
                            status: 'ERROR',
                            checked: 0,
                            failed: failedCount
                        });
                    }
                    // Note: We don't add OK checks for every column combination - too many
                }
            }
        }

        // Generate Protocol
        const totalFailed = allChecks.filter(c => c.status === 'ERROR').length;
        const totalPassed = allChecks.filter(c => c.status === 'OK').length;
        const status = totalFailed === 0 ? 'ÚSPĚŠNÉ' : 'S CHYBAMI';
        const timestamp = new Date().toLocaleString('cs-CZ');

        let protocol = `PROTOKOL O VALIDACI\n`;
        protocol += `=====================\n`;
        protocol += `Datum: ${timestamp}\n`;
        protocol += `Stav: ${status}\n`;
        protocol += `----------------------------------------\n`;
        protocol += `Celkem kontrol: ${allChecks.length}\n`;
        protocol += `Úspěšné: ${totalPassed}\n`;
        protocol += `Selhalo: ${totalFailed}\n\n`;

        protocol += `[METODIKA VALIDACE]\n`;
        protocol += `1. INTEGRITA (Foreign Key)\n`;
        protocol += `   - Metoda: SQL NOT EXISTS query.\n`;
        protocol += `   - Princip: Hledá hodnoty ve zdrojovém sloupci, které nemají odpovídající záznam v cílové (master) tabulce.\n`;
        protocol += `2. REKONSILIACE (Shoda dat)\n`;
        protocol += `   - Metoda: SQL Comparison přes JOIN klíč.\n`;
        protocol += `   - Princip: Spojí řádky tabulek přes definovaný klíč a porovnává hodnoty v ostatních sloupcích.\n`;
        protocol += `3. ZAKÁZANÉ HODNOTY (Blacklist)\n`;
        protocol += `   - Metoda: SQL INTERSECT.\n`;
        protocol += `   - Princip: Pro každou tabulku typu 'FORBIDDEN' hledá průnik hodnot s tabulkami stejného názvu sloupce.\n\n`;

        protocol += `[DETAILNÍ VÝPIS VŠECH KONTROL]\n`;
        allChecks.forEach((check, index) => {
            const icon = check.status === 'OK' ? '✅' : '❌';
            const records = check.checked > 0 ? `${check.checked} záznamů` : (check.type === 'ZAKÁZANÉ' ? 'Analýza průniku' : 'N/A');
            protocol += `${index + 1}. [${check.type}] ${check.label}\n`;
            protocol += `   Stav: ${icon} ${check.status}\n`;
            protocol += `   Rozsah: ${records}\n`;
            if (check.status === 'ERROR') {
                protocol += `   Chyb: ${check.failed}\n`;
            }
            protocol += `\n`;
        });

        if (totalFailed > 0) {
            protocol += `[DETAIL CHYB]\n`;
            if (forbiddenErrors.length > 0) {
                protocol += `--- ZAKÁZANÉ HODNOTY ---\n`;
                forbiddenErrors.forEach(e => {
                    protocol += `- ${e.targetTable}.${e.column} obsahuje ${e.count} zakázaných hodnot z ${e.forbiddenTable}.${e.forbiddenColumn}: ${e.foundValues.join(', ')}...\n`;
                });
                protocol += `\n`;
            }

            if (integrityErrors.length > 0) {
                protocol += `--- INTEGRITA (SIROTCI) ---\n`;
                integrityErrors.forEach(e => {
                    protocol += `- ${e.fkTable}.${e.fkColumn} -> ${e.pkTable}: ${e.missingCount} chybějících\n`;
                });
                protocol += `\n`;
            }

            if (reconciliationErrors.length > 0) {
                protocol += `--- REKONSILIACE (NESHODY) ---\n`;
                reconciliationErrors.forEach(e => {
                    protocol += `- ${e.sourceTable} vs ${e.targetTable} (${e.joinKey}): ${e.count} neshod\n`;
                });
                protocol += `\n`;
            }
        } else {
            protocol += `[ZÁVĚR]\n`;
            protocol += `Všechna data jsou konzistentní. Nebyly nalezeny žádné chyby.\n`;
        }

        res.json({
            success: true,
            errors: integrityErrors,
            reconciliation: reconciliationErrors,
            forbidden: forbiddenErrors,
            protocol,
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
