select * from base_material_detail 
select * from fdw.material_detail 

SELECT sync_material_detail();

CREATE OR REPLACE FUNCTION sync_material_detail()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public.base_material_detail (
        docentry,
        docnum,
        groupno,
        itmsgrpnam,
		materialcode,
		materialname,
        onhand,
		materialqty,
		actualqty,
		remark,
		uomentry,
		uomname,
		fromwhs,
		towhs,
		createdate,
		createts,
		fromts,
		tots
    )
    SELECT
        docentry,
        docnum,
        groupno,
        itmsgrpnam,
		materialcode,
		materialname,
        onhand,
		materialqty,
		actualqty,
		remark,
		uomentry,
		uomname,
		fromwhs,
		towhs,
		createdate,
		createts,
		fromts,
		tots
    FROM fdw.material_detail
    ON CONFLICT (docentry)
    DO UPDATE SET
        docnum       = EXCLUDED.docnum,
        groupno      = EXCLUDED.groupno,
        itmsgrpnam   = EXCLUDED.itmsgrpnam,		
		materialcode = EXCLUDED.materialcode,
		materialname = EXCLUDED.materialname,
        onhand       = EXCLUDED.onhand,
		materialqty  = EXCLUDED.materialqty,
		actualqty    = EXCLUDED.actualqty,
		remark       = EXCLUDED.remark,
		uomentry     = EXCLUDED.uomentry,
		uomname      = EXCLUDED.uomname,
		fromwhs	     = EXCLUDED.fromwhs,
		towhs        = EXCLUDED.towhs,
		createdate   = EXCLUDED.createdate,
		createts     = EXCLUDED.createts,
		fromts       = EXCLUDED.fromts,
		tots = EXCLUDED.fromts
	WHERE
        (public.base_material_detail.createdate)
        IS DISTINCT FROM
        (EXCLUDED.createdate);

END;
$$;

SELECT cron.schedule(
    'sync_base_material_detail_every_minute',
    '* * * * *',
    $$SELECT sync_material_detail();$$
);



