import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DB_PATH = path.join(__dirname, '../validator.db');
const SCHEMA_PATH = path.join(__dirname, '../schema.sql');

const db = new Database(DB_PATH);

export function initDatabase() {
    console.log('Initializing database...');
    try {
        const schema = fs.readFileSync(SCHEMA_PATH, 'utf-8');
        db.exec(schema);
        console.log('Database initialized successfully.');
    } catch (error) {
        console.error('Failed to initialize database:', error);
    }
}

export default db;
