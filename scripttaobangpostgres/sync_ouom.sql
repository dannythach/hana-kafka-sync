-- Phần 1 — Cấu hình FDW trong database comi-odoo
-- Bước 5 — Import bảng ouom từ Datalake
IMPORT FOREIGN SCHEMA public
LIMIT TO (ouom)
FROM SERVER db_b_srv
INTO fdw;
________________________________________
-- PHẦN 2 — Tạo function sync
-- Bước 9 — Tạo function đồng bộ
CREATE OR REPLACE FUNCTION sync_ouom()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public.gmp_ouom (
        uomcode,
	    uomname,
	    locked,
	    createdate,
	    updatedate,
	    u_uomcode
    )
    SELECT
        uomcode,
	    uomname,
	    locked,
	    createdate,
	    updatedate,
	    u_uomcode
    FROM fdw.ouom
    ON CONFLICT (uomcode)
    DO UPDATE SET
        uomname   = EXCLUDED.uomname,
        locked   = EXCLUDED.locked,
		createdate = EXCLUDED.createdate,
        updatedate = EXCLUDED.updatedate,
		u_uomcode   = EXCLUDED.u_uomcode
	WHERE
    public.gmp_ouom.updatedate IS DISTINCT FROM EXCLUDED.updatedate;

END;
$$;
-- PHẦN 3 — Test thủ công
--Chạy:
SELECT sync_ouom();
--Kiểm tra:
SELECT count(*) FROM public.gmp_ouom;
________________________________________
-- PHẦN 4 — Schedule bằng pg_cron
-- Bước 10 — Tạo job chạy mỗi phút
SELECT cron.schedule(
    'sync_ouom_every_minute',
    '* * * * *',
    $$SELECT sync_ouom();$$
);

select * from fdw.ouom where uomcode = 'zzz'
select * from public.gmp_ouom where uomcode = 'zzz'











