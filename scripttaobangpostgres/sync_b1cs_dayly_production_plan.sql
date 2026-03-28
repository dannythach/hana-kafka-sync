select * from base_daily_production_plan where docentry ='4511'
select * from fdw.b1cs_daily_prod where docentry ='4511'

SELECT sync_b1cs_daily_prod();

CREATE OR REPLACE FUNCTION sync_b1cs_daily_prod()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public.base_daily_production_plan (
        docentry,
        docnum,
        canceled,
        status,
		createdate,
		createtime,
        updatedate,
		updatetime,
		remark,
		u_docdate,
		u_factory,
		u_fromwhs,
		u_towhs
    )
    SELECT
        docentry,
        docnum,
        canceled,
        status,
		createdate,
		createtime,
        updatedate,
		updatetime,
		remark,
		u_docdate,
		u_factory,
		u_fromwhs,
		u_towhs
    FROM fdw.b1cs_daily_prod
    ON CONFLICT (docentry)
    DO UPDATE SET
        docnum     = EXCLUDED.docnum,
        canceled   = EXCLUDED.canceled,
        status     = EXCLUDED.status,		
		createdate = EXCLUDED.createdate,
		createtime = EXCLUDED.createtime,
        updatedate = EXCLUDED.updatedate,
		updatetime = EXCLUDED.updatetime,
		remark	   = EXCLUDED.remark,
		u_docdate  = EXCLUDED.u_docdate,
		u_factory  = EXCLUDED.u_factory,
		u_fromwhs  = EXCLUDED.u_fromwhs,
		u_towhs	   = EXCLUDED.u_towhs
	WHERE
        (public.base_daily_production_plan.updatedate)
        IS DISTINCT FROM
        (EXCLUDED.updatedate);

END;
$$;

SELECT cron.schedule(
    'sync_base_daily_production_plan_every_minute',
    '* * * * *',
    $$SELECT sync_b1cs_daily_prod();$$
);



