import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import multer from 'multer';
import * as XLSX from 'xlsx';
import fs from 'fs';
import prisma from './database.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());
app.use(express.json());

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
    try {
        await prisma.project.delete({
            where: { id: req.params.id }
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
    const tableType = req.body.tableType as 'SOURCE' | 'TARGET' | 'CODEBOOK';

    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    if (!['SOURCE', 'TARGET', 'CODEBOOK'].includes(tableType)) {
        return res.status(400).json({ error: 'Invalid table type. Must be SOURCE, TARGET, or CODEBOOK' });
    }

    try {
        const filePath = req.file.path;
        const tableName = req.file.originalname.replace(/\.[^/.]+$/, ''); // Remove extension

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
// SERVE STATIC FRONTEND (Production)
// ============================================
if (process.env.NODE_ENV === 'production') {
    const distPath = path.join(__dirname, '../dist');
    app.use(express.static(distPath));

    app.get('*', (req, res) => {
        res.sendFile(path.join(distPath, 'index.html'));
    });
}

// ============================================
// START SERVER
// ============================================
app.listen(PORT, () => {
    console.log(`🚀 Valibook server running on http://localhost:${PORT}`);
    console.log(`📊 Database: PostgreSQL on Railway`);
});
