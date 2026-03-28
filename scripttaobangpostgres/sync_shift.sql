select * from base_shift
select * from fdw.shift 

SELECT sync_shift();

CREATE OR REPLACE FUNCTION sync_shift()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public.base_shift (
        code,
		name
    )
    SELECT
        code,
		name
    FROM fdw.shift
    ON CONFLICT (code)
    DO UPDATE SET
        name         = EXCLUDED.name
	WHERE
        (public.base_shift.code)
        IS DISTINCT FROM
        (EXCLUDED.code);

END;
$$;

SELECT cron.schedule(
    'sync_shift_every_minute',
    '* * * * *',
    $$SELECT sync_shift();$$
);












