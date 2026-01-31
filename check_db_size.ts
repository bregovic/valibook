import { PrismaClient } from '@prisma/client';
const prisma = new PrismaClient();

async function check() {
    const p = await prisma.project.count();
    const c = await prisma.column.count();
    const v = await prisma.columnValue.count();

    const sizes = await prisma.$queryRaw`
    SELECT relname as table_name, 
           pg_size_pretty(pg_total_relation_size(relid)) as total_size,
           pg_size_pretty(pg_relation_size(relid)) as data_size,
           pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) as index_size
    FROM pg_catalog.pg_statio_user_tables 
    ORDER BY pg_total_relation_size(relid) DESC;
  `;

    console.log('--- Database Stats ---');
    console.log('Projects:', p);
    console.log('Columns:', c);
    console.log('Values (rows):', v);
    console.table(sizes);
}

check()
    .catch(console.error)
    .finally(() => prisma.$disconnect());
