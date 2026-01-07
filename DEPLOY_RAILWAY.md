# Export Validator

Aplikace pro validaci a porovnávání Excel exportů.

## Nasazení na Railway (Doporučeno)

1.  **Vytvořte Git repozitář**:
    *   Tento projekt je již inicializován jako git repozitář.
    *   Vytvořte si nový repozitář na GitHubu (např. `export-validator`).
    *   Propojte ho:
        ```bash
        git remote add origin https://github.com/VASE-UZIVATELSKE-JMENO/export-validator.git
        git push -u origin master
        ```

2.  **Railway**:
    *   Jděte na railway.app a přihlaste se (ideálně přes GitHub).
    *   Klikněte na "New Project" -> "Deploy from GitHub repo".
    *   Vyberte tento repozitář.
    *   Railway automaticky detekuje `package.json` a `npm start` příkaz.

3.  **Nastavení (Variables)**:
    *   Standardně není potřeba nic měnit.
    *   Port se nastaví automaticky (obvykle Railway používá proměnnou `PORT`, kterou naše aplikace čte).
    *   Aplikace používá `SQLite` (soubor `validator.db`). Na Railway je souborový systém efemérní (po restartu se smaže).
        *   **Pro trvalá data** by bylo lepší přejít např. na PostgreSQL plugin, který Railway nabízí.
        *   **Pro jednoduché použití** (validace teď a tady) to nevadí, ale nahrané soubory a historie validace zmizí při každém novém nasazení (deploy).

    *   **Tip pro trvalá data (Volume)**:
        *   Na Railway můžete přidat "Volume" a namapovat ho do složky aplikace (např. `/app/data`).
        *   Pak bychom museli jen upravit cestu k DB, aby se ukládala tam.

## Spuštění lokálně

```bash
npm install
npm run dev
```
