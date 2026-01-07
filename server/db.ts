import Database from 'better-sqlite3';
import pg from 'pg';
import fs from 'fs';
import path from 'path';

// Common interface for DB operations
interface IDatabase {
    query(sql: string, params?: any[]): Promise<any[]>;
    run(sql: string, params?: any[]): Promise<{ id?: number | string }>;
    get(sql: string, params?: any[]): Promise<any>;
    init(): Promise<void>;
}

// SQLite Implementation
class SQLiteDB implements IDatabase {
    private db: Database.Database;

    constructor() {
        const DATA_DIR = process.env.DATA_DIR || process.cwd();
        const DB_PATH = path.join(DATA_DIR, 'validator.db');
        if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
        this.db = new Database(DB_PATH);
        console.log('Using SQLite Database');
    }

    async init() {
        const schema = fs.readFileSync(path.join(process.cwd(), 'schema.sql'), 'utf-8');
        this.db.exec(schema);
    }

    async query(sql: string, params: any[] = []) {
        return this.db.prepare(sql).all(...params);
    }

    async run(sql: string, params: any[] = []) {
        const result = this.db.prepare(sql).run(...params);
        return { id: Number(result.lastInsertRowid) };
    }

    async get(sql: string, params: any[] = []) {
        return this.db.prepare(sql).get(...params);
    }
}

// Postgres Implementation
class PostgresDB implements IDatabase {
    private pool: pg.Pool;

    constructor(connectionString: string) {
        this.pool = new pg.Pool({ connectionString });
        console.log('Using PostgreSQL Database');
    }

    async init() {
        // Simple schema migration for PG
        // Warning: schema.sql syntax must be compatible!
        // We might need to adjust schema.sql for types (INTEGER PRIMARY KEY AUTOINCREMENT vs SERIAL)

        const client = await this.pool.connect();
        try {
            await client.query(`
                CREATE TABLE IF NOT EXISTS validation_projects (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS imported_files (
                    id SERIAL PRIMARY KEY,
                    project_id INTEGER REFERENCES validation_projects(id) ON DELETE CASCADE,
                    original_filename TEXT NOT NULL,
                    stored_filename TEXT NOT NULL,
                    file_type TEXT NOT NULL,
                    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS file_columns (
                    id SERIAL PRIMARY KEY,
                    file_id INTEGER REFERENCES imported_files(id) ON DELETE CASCADE,
                    column_name TEXT NOT NULL,
                    column_index INTEGER NOT NULL,
                    sample_value TEXT
                );
                
                CREATE TABLE IF NOT EXISTS column_mappings (
                    id SERIAL PRIMARY KEY,
                    project_id INTEGER REFERENCES validation_projects(id) ON DELETE CASCADE,
                    source_column_id INTEGER REFERENCES file_columns(id),
                    target_column_id INTEGER REFERENCES file_columns(id),
                    mapping_note TEXT
                );

                CREATE TABLE IF NOT EXISTS validation_results (
                    id SERIAL PRIMARY KEY,
                    project_id INTEGER REFERENCES validation_projects(id) ON DELETE CASCADE,
                    column_mapping_id INTEGER REFERENCES column_mappings(id),
                    error_message TEXT,
                    actual_value TEXT,
                    expected_value TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            `);
        } finally {
            client.release();
        }
    }

    // Convert SQL ? placeholder to $1, $2, ...
    private normalizeSql(sql: string): string {
        let i = 1;
        return sql.replace(/\?/g, () => `$${i++}`);
    }

    async query(sql: string, params: any[] = []) {
        const res = await this.pool.query(this.normalizeSql(sql), params);
        return res.rows;
    }

    async run(sql: string, params: any[] = []) {
        // Hack: Append RETURNING id to get last inserted ID in PG
        let nSql = this.normalizeSql(sql);
        if (nSql.trim().toUpperCase().startsWith('INSERT')) {
            nSql += ' RETURNING id';
        }

        const res = await this.pool.query(nSql, params);
        if (res.rows.length > 0 && res.rows[0].id) {
            return { id: res.rows[0].id };
        }
        return { id: 0 };
    }

    async get(sql: string, params: any[] = []) {
        const res = await this.pool.query(this.normalizeSql(sql), params);
        return res.rows[0];
    }
}

// Factory
let dbInstance: IDatabase;

if (process.env.DATABASE_URL) {
    dbInstance = new PostgresDB(process.env.DATABASE_URL);
} else {
    dbInstance = new SQLiteDB();
}

export default dbInstance;
