import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Allow overriding DB path via environment variable (useful for Railway Volumes)
const DATA_DIR = process.env.DATA_DIR || process.cwd();
const DB_PATH = path.join(DATA_DIR, 'validator.db');
// Ensure directory exists if using custom path
if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
}

// Read schema from root directory
const schema = fs.readFileSync(path.join(process.cwd(), 'schema.sql'), 'utf-8');

const db = new Database(DB_PATH);

export function initDatabase() {
    console.log('Initializing database...');
    try {
        db.exec(schema);
        console.log('Database initialized successfully.');
    } catch (error) {
        console.error('Failed to initialize database:', error);
    }
}

export default db;
