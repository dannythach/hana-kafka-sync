-- Table: public.ouom

--DROP TABLE IF EXISTS public.ouom;

CREATE TABLE IF NOT EXISTS public.ouom
(
    uomcode text NOT NULL,
    uomname text,
    locked character(1),

    createdate timestamptz,
    updatedate timestamptz NOT NULL,

    u_uomcode text,

    CONSTRAINT ouom_pkey PRIMARY KEY (uomcode)
);

ALTER TABLE IF EXISTS public.ouom
    OWNER TO metabase;

CREATE INDEX IF NOT EXISTS idx_ouom_updatedate
    ON public.ouom (updatedate);