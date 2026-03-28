select * from base_line
select * from fdw.line 

SELECT sync_line();

CREATE OR REPLACE FUNCTION sync_line()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public.base_line (
        code,
		name
    )
    SELECT
        code,
		name
    FROM fdw.line
    ON CONFLICT (code)
    DO UPDATE SET
        name         = EXCLUDED.name
	WHERE
        (public.base_line.code)
        IS DISTINCT FROM
        (EXCLUDED.code);

END;
$$;

SELECT cron.schedule(
    'sync_line_every_minute',
    '* * * * *',
    $$SELECT sync_line();$$
);



