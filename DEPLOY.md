# Jak nasadit aplikaci (Deployment)

Tato aplikace běží na **Node.js**. Není možné ji nahrát na běžný webhosting, který podporuje pouze PHP/HTML. Potřebujete hosting s podporou Node.js (např. VPS, DigitalOcean, Heroku, Railway, nebo webhosting s Node.js selectorem).

## Postup pro nasazení

1.  **Vytvoření produkčního sestavení (Build)**
    Spusťte v terminálu tento příkaz, který vytvoří složku `dist` (frontend) a `dist-server` (backend):
    ```bash
    npm run build:all
    ```

2.  **Co nahrát na server?**
    Nahrajte tyto soubory/složky do kořenového adresáře na serveru:
    *   `dist/` (Složka s frontendem)
    *   `dist-server/` (Složka s backendem)
    *   `package.json`
    *   `schema.sql` (Pro inicializaci databáze)
    *   `uploads/` (Vytvořte prázdnou složku pro nahrávání souborů)

3.  **Instalace závislostí a spuštění**
    Připojte se na server (SSH) a v adresáři spusťte:
    ```bash
    npm install --production
    npm start
    ```
    
    Aplikace poběží na portu 3001 (nebo jiném, pokud nastavíte proměnnou prostředí `PORT`).

4.  **Databáze**
    Soubor `validator.db` se vytvoří automaticky při prvním spuštění. Ujistěte se, že aplikace má práva zápisu do složky.

## Alternativa: Docker (Doporučeno)
Pokud máte možnost, použijte Docker. Vytvořte `Dockerfile`:

```dockerfile
FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build:all
CMD ["npm", "start"]
EXPOSE 3001
```
