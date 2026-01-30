
import * as XLSX from 'xlsx';
import fs from 'fs';
import path from 'path';

const testDataDir = "c:\\Users\\Wendulka\\Documents\\Webhry\\valibook\\test data";

function inspectFile(filePath) {
    const fileName = path.basename(filePath);
    console.log(`\n==================================================`);
    console.log(`FILE: ${fileName}`);
    console.log(`==================================================`);

    try {
        if (!fs.existsSync(filePath)) {
            console.log("File not found!");
            return;
        }

        const workbook = XLSX.readFile(filePath);
        const sheetName = workbook.SheetNames[0];
        const sheet = workbook.Sheets[sheetName];

        // Read raw data
        const data = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });

        if (data.length === 0) {
            console.log("Empty file.");
            return;
        }

        console.log("COLUMNS:", data[0]);
        console.log("ROW COUNT:", data.length - 1);

        console.log("\nSAMPLE DATA (First 3 rows):");
        for (let i = 1; i < Math.min(data.length, 4); i++) {
            console.log(`Row ${i}:`, data[i]);
        }

    } catch (err) {
        console.error("Error reading file:", err.message);
    }
}

const files = [
    'source.xlsx',
    'source customers.xlsx',
    'Control (1).xlsx',
    'Control (2).xlsx',
    'Values Forbident.xlsx'
];

files.forEach(file => {
    inspectFile(path.join(testDataDir, file));
});
