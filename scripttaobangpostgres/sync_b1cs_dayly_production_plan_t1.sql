select u_remarks,* from base_daily_production_plan_detail where docentry ='4511' and proplanentry = '4511-1'
select u_remarks,* from fdw.b1cs_daily_prod_t1 where docentry ='4511' and proplanentry = '4511-1'

SELECT sync_b1cs_daily_prod_t1();

CREATE OR REPLACE FUNCTION sync_b1cs_daily_prod_t1()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public.base_daily_production_plan_detail (
        proplanentry,
        docentry,
        lineid,
        u_itemcode,
		u_dscription,
		u_quantity,
        u_uom,
		u_factor,
		u_quantitypcs,
		u_line,
		u_remarks,
		u_shift,
		u_oriline,
		u_btp,
		u_productiontype,
		u_machines,
		updatedate
    )
    SELECT
        proplanentry,
        docentry,
        lineid,
        u_itemcode,
		u_dscription,
		u_quantity,
        u_uom,
		u_factor,
		u_quantitypc,
		u_line,
		u_remarks,
		u_shift,
		u_oriline,
		u_btp,
		u_productiontype,
		u_machines,
		updatedate
    FROM fdw.b1cs_daily_prod_t1
    ON CONFLICT (proplanentry)
    DO UPDATE SET
        docentry         = EXCLUDED.docentry,
        lineid           = EXCLUDED.lineid,
        u_itemcode       = EXCLUDED.u_itemcode,		
		u_dscription     = EXCLUDED.u_dscription,
		u_quantity       = EXCLUDED.u_quantity,
        u_uom            = EXCLUDED.u_uom,
		u_factor         = EXCLUDED.u_factor,
		u_quantitypcs    = EXCLUDED.u_quantitypcs,
		u_line           = EXCLUDED.u_line,
		u_remarks        = EXCLUDED.u_remarks,
		u_shift          = EXCLUDED.u_shift,
		u_oriline	     = EXCLUDED.u_oriline,
		u_btp            = EXCLUDED.u_btp,
		u_productiontype = EXCLUDED.u_productiontype,
		u_machines       = EXCLUDED.u_machines,
		updatedate       = EXCLUDED.updatedate
	WHERE
        (public.base_daily_production_plan_detail.updatedate)
        IS DISTINCT FROM
        (EXCLUDED.updatedate);

END;
$$;

SELECT cron.schedule(
    'sync_base_daily_production_plan_detail_every_minute',
    '* * * * *',
    $$SELECT sync_b1cs_daily_prod_t1();$$
);



