-- kiểm tra extension
SELECT *
FROM pg_extension
WHERE extname = 'postgres_fdw';

-- kiểm tra foreign server
SELECT 
    s.oid,
    s.srvname,
    f.fdwname,
    s.srvoptions
FROM pg_foreign_server s
JOIN pg_foreign_data_wrapper f 
    ON s.srvfdw = f.oid;

DROP SERVER datalake_srv;
DROP SERVER datalake_server;

-- kiểm tra user mapping
SELECT 
    s.srvname AS server_name,
    um.usename AS local_user,
    um.umoptions AS options
FROM pg_user_mappings um
JOIN pg_foreign_server s 
    ON um.srvid = s.oid
ORDER BY s.srvname, um.usename;

DROP USER MAPPING FOR metabase SERVER datalake_srv;
DROP USER MAPPING FOR metabase SERVER datalake_server;

-- kiểm tra FOREIGN TABLE
SELECT 
    ft.ftrelid::regclass AS foreign_table,
    s.srvname           AS server_name,
    fdw.fdwname         AS fdw_type
FROM pg_foreign_table ft
JOIN pg_foreign_server s 
    ON ft.ftserver = s.oid
JOIN pg_foreign_data_wrapper fdw
    ON s.srvfdw = fdw.oid
ORDER BY foreign_table;

DROP FOREIGN TABLE fdw.oitm;
DROP FOREIGN TABLE oitm;
DROP FOREIGN TABLE ouom;

-- Phân biệt FOREIGN TABLE và TABLE thường
-- relkind = 'f' → foreign table
-- relkind = 'r' → table thường
SELECT 
    relname,
    relkind
FROM pg_class

-- kiểm tra cron 
SELECT jobid, jobname, schedule, command
FROM cron.job;

SELECT jobid,
       status,
       return_message,
       start_time,
       end_time
FROM cron.job_run_details
ORDER BY start_time DESC
LIMIT 5;

SHOW cron.database_name;
SHOW cron.host;
SELECT jobid, username FROM cron.job;

SELECT rolname, rolcanlogin, rolpassword
FROM pg_authid
WHERE rolname = 'metabase';

SHOW listen_addresses;
SHOW port;
SHOW cron.host;
SHOW cron.use_background_workers;
SHOW shared_preload_libraries;

-- xóa job trong cron
SELECT cron.unschedule(jobid)
FROM cron.job
WHERE jobname = 'sync_base_daily_production_plan_every_minute';
-- kiểm tra kiểu dữ liệu của bảng foreign table
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'fdw'
  AND table_name = 'oitm';


select * from fdw.b1cs_daily_prod

