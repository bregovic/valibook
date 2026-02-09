#!/bin/sh
echo ">>> CONTAINER STARTUP SCRIPT"
echo ">>> Current Directory: $(pwd)"
echo ">>> Node Version: $(node -v)"
echo ">>> Listing dist-server directory:"
ls -R dist-server || echo "dist-server not found!"

echo ">>> STARTING PRISMA PUSH..."
npx prisma db push --accept-data-loss || echo ">>> PRISMA PUSH FAILED (Ignored)"

echo ">>> STARTING NODE SERVER..."
exec node dist-server/server/index.js
