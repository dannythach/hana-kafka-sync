-- Phần 1 — Cấu hình FDW trong database comi-odoo
-- Bước 1 — Tạo extension FDW
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
-- Bước 2 — Tạo foreign server trỏ sang Datalake
CREATE SERVER db_b_srv
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'localhost',
    dbname 'Datalake',
    port '5432'
);
-- Bước 3 — Tạo user mapping
CREATE USER MAPPING FOR CURRENT_USER
SERVER db_b_srv
OPTIONS (
    user 'metabase',
    password 'secret123'
);
-- Bước 4 — Tạo schema chứa foreign table
CREATE SCHEMA IF NOT EXISTS fdw;
-- Bước 5 — Import bảng oitm từ Datalake
IMPORT FOREIGN SCHEMA public
LIMIT TO (oitm)
FROM SERVER db_b_srv
INTO fdw;
________________________________________
-- PHẦN 2 — Tạo function sync
-- Bước 9 — Tạo function đồng bộ
CREATE OR REPLACE FUNCTION sync_oitm()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public.gmp_oitm (
        itemcode,
        itemname,
        frgnname,
        itmsgrpcod,
		createdate,
        updatedate,
		updatets
    )
    SELECT
        itemcode,
        itemname,
        frgnname,
        itmsgrpcod,
		createdate,
        updatedate,
		updatets
    FROM fdw.oitm
    ON CONFLICT (itemcode)
    DO UPDATE SET
        itemname   = EXCLUDED.itemname,
        frgnname   = EXCLUDED.frgnname,
        itmsgrpcod = EXCLUDED.itmsgrpcod,
		createdate = EXCLUDED.createdate,
        updatedate = EXCLUDED.updatedate,
		updatets   = EXCLUDED.updatets
	WHERE
        (public.gmp_oitm.updatedate, public.gmp_oitm.updatets)
        IS DISTINCT FROM
        (EXCLUDED.updatedate, EXCLUDED.updatets);

END;
$$;
-- PHẦN 3 — Test thủ công
--Chạy:
SELECT sync_oitm();
--Kiểm tra:
SELECT count(*) FROM public.gmp_oitm;
________________________________________
-- PHẦN 4 — Schedule bằng pg_cron
-- Bước 10 — Tạo job chạy mỗi phút
SELECT cron.schedule(
    'sync_oitm_every_minute',
    '* * * * *',
    $$SELECT sync_oitm();$$
);

select * from fdw.oitm where itemcode = '24010002'
select * from public.gmp_oitm where itemcode = '24010002'











