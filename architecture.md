# Export Validator Architecture

## Overview
Webová aplikace pro porovnávání a validaci Excel exportů. Umožňuje nahrát dva sady souborů (Source vs Target), dynamicky mapovat sloupce a provádět validace dat a struktur.

## Tech Stack
- **Frontend**: React (Vite) - pro dynamické UI, mapování sloupců.
- **Backend**: Node.js (Express) - pro API a zpracování souborů.
- **Database**: SQLite - pro ukládání definic sloupců, mapování a výsledků validace.
- **File Processing**: `xlsx` (SheetJS) pro čtení Excelů.

## Workflow
1. **Založení projektu**: Uživatel vytvoří nový validační projekt.
2. **Upload souborů**:
   - Nahrání "Vzoru" (očekávaný stav/zdroj).
   - Nahrání "Exportu" (testovaný soubor).
3. **Analýza struktury**:
   - Systém přečte hlavičky (sloupce).
   - Uloží definice do `file_columns`.
4. **Mapování**:
   - UI nabídne automatické spárování podle názvů.
   - Uživatel může ručně přemapovat.
   - Definice vazeb se uloží do `column_mappings`.
5. **Validace**:
   - Kontrola datových typů.
   - Kontrola existence v číselníku.
   - Porovnání hodnot (Source vs Target).
6. **Report**:
   - Zobrazení chybujících řádků.
