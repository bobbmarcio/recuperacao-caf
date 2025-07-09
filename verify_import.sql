-- Verificar dados importados nos schemas CAF
SELECT 
    schemaname,
    COUNT(*) as table_count
FROM pg_tables 
WHERE schemaname LIKE 'caf_2025%' 
GROUP BY schemaname 
ORDER BY schemaname;

-- Verificar tamanho dos schemas
SELECT 
    n.nspname as schema_name,
    ROUND(SUM(pg_total_relation_size(c.oid))/1024/1024) as size_mb,
    COUNT(c.oid) as objects_count
FROM pg_class c 
JOIN pg_namespace n ON n.oid = c.relnamespace 
WHERE n.nspname LIKE 'caf_2025%' 
GROUP BY n.nspname 
ORDER BY n.nspname;
