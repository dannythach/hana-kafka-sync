SELECT * 
FROM pg_extension
WHERE extname = 'postgres_fdw';

SELECT srvname, srvoptions
FROM pg_foreign_server;

SELECT *
FROM pg_user_mappings;

SELECT *
FROM information_schema.foreign_tables;

SELECT
    e.extname,
    s.srvname,
    ft.foreign_table_name
FROM pg_extension e
LEFT JOIN pg_foreign_server s ON TRUE
LEFT JOIN information_schema.foreign_tables ft ON TRUE
WHERE e.extname = 'postgres_fdw';


