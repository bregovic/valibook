
// Mock DB classes to simulate the environment
class MockDB {
    async query(sql: string, params: any[]) {
        console.log(`[DB] ${sql}`, params);
        if (sql.includes('FROM imported_files')) {
            return [
                { id: 1, project_id: 1, original_filename: 'Target.xlsx', file_type: 'target' },
                { id: 2, project_id: 1, original_filename: 'Source.xlsx', file_type: 'source' }
            ];
        }
        if (sql.includes('FROM file_columns') && params.includes(1)) {
            // Target columns
            return [
                { id: 10, file_id: 1, column_name: 'AccountNum', column_index: 0 },
                { id: 11, file_id: 1, column_name: 'Amount', column_index: 1 }
            ];
        }
        if (sql.includes('FROM file_columns') && params.includes(2)) {
            // Source columns - Simulate EMPTY
            return [
                { id: 20, file_id: 2, column_name: null, column_index: 0 }, // Null name?
                { id: 21, file_id: 2, column_name: 'Value', column_index: 1 }
            ];
        }
        return [];
    }
    async run(sql: string, params: any[]) {
        console.log(`[DB RUN] ${sql}`, params);
    }
}

const db = new MockDB();

async function runAutoMap() {
    const projectId = 1;
    const log = console.log;

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

            for (const sFile of potentialSources) {
                const sourceCols = await db.query('SELECT * FROM file_columns WHERE file_id = ?', [sFile.id]);

                let fileScore = 0;
                let fileMappings = [];

                for (const tCol of targetCols) {
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
                // Guess Primary Key (simply first ID-like column or first mapped column)
                let keyCandidate = bestMappings.find(m => /id|key|kod|code/i.test(m.sourceColName));
                if (!keyCandidate) keyCandidate = bestMappings[0];

                newMappings.push(...bestMappings.map(m => ({
                    sourceColumnId: m.sourceColumnId,
                    targetColumnId: m.targetColumnId,
                    note: JSON.stringify({
                        isKey: keyCandidate && m.sourceColumnId === keyCandidate.sourceColumnId,
                        codebookFileId: m.codebookFileId,
                        autoDiscovered: true,
                        strategy: 'metadata_name_match'
                    })
                })));
            }
        }

        console.log("Finished successfully");

    } catch (e) {
        console.error("CRITICAL ERROR:", e);
    }
}

runAutoMap();
