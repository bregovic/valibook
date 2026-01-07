import * as XLSX from 'xlsx';
import fs from 'fs';

const file1 = "C:\\Users\\Wendulka\\Documents\\Webhry\\Export_validator\\data_validate\\Export\\DirPartyContactV3Entity_E02934_1.xlsx";
const file2 = "C:\\Users\\Wendulka\\Documents\\Webhry\\Export_validator\\data_validate\\Source\\Logistic adress.xlsx";

function inspectFile(path, label) {
    console.log(`\n--- INSPECTING: ${label} ---`);
    if (!fs.existsSync(path)) {
        console.log("File not found:", path);
        return;
    }
    const workbook = XLSX.readFile(path);
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    // Get headers and first 3 rows
    const data = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });

    if (data.length > 0) {
        console.log("HEADERS:", JSON.stringify(data[0]));
    }
    if (data.length > 1) {
        console.log("ROW 1:", JSON.stringify(data[1]));
    }
    if (data.length > 2) {
        console.log("ROW 2:", JSON.stringify(data[2]));
    }
}

inspectFile(file1, "TARGET (Export)");
inspectFile(file2, "SOURCE");
