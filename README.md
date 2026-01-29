# Valibook - Excel Validation Tool

Nástroj pro validaci a rekonsolidaci dat z Excel souborů.

## 🎯 Funkce

- **Upload Excel souborů** - zdrojové, kontrolované a číselníky
- **Evidence sloupců** - automatická detekce hlaviček a vzorových dat
- **Primární klíče** - označení klíčových sloupců
- **Vazby** - definice vztahů mezi sloupci (FK → PK)
- **Validace** - kontrola dat proti pravidlům a číselníkům

## 🛠️ Tech Stack

- **Frontend**: React + Vite + TypeScript
- **Backend**: Express.js + TypeScript
- **Database**: PostgreSQL (Railway)
- **ORM**: Prisma
- **Excel**: xlsx (SheetJS)

## 🚀 Spuštění

### Lokální vývoj

1. Nastavte environment variables:
```bash
cp .env.example .env
# Nastavte DATABASE_URL pro Railway PostgreSQL
```

2. Nainstalujte závislosti:
```bash
npm install
```

3. Inicializujte databázi:
```bash
npm run db:push
npm run db:generate
```

4. Spusťte dev server:
```bash
npm run dev
```

### Railway Deployment

Projekt je napojen na Railway s PostgreSQL databází.

## 📊 Databázové schéma

```
projects        ──┬── imported_files ──┬── columns
                  │                    │
                  │                    ├── isPrimaryKey
                  │                    ├── sampleValues (JSON)
                  │                    └── linkedToColumnId (FK)
```

## 📝 Workflow

1. **Vytvoř projekt** - pojmenuj validační úlohu
2. **Nahraj soubory** - source, target, codebooks
3. **Označ strukturu** - primární klíče, vazby
4. **Spusť validaci** - kontrola dat
5. **Export reportu** - přehled chyb
