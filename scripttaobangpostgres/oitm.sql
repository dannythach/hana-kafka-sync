-- Table: public.oitm

-- DROP TABLE IF EXISTS public.oitm;

CREATE TABLE IF NOT EXISTS public.oitm
(
    id integer NOT NULL,
    itemcode text NOT NULL,
    itemname text,
    frgnname text,
    itmsgrpcod integer,

    createdate timestamptz,
    updatedate timestamptz NOT NULL,

    updatets integer NOT NULL,

    CONSTRAINT oitm_pkey PRIMARY KEY (itemcode)
);

ALTER TABLE IF EXISTS public.oitm
    OWNER TO metabase;

CREATE INDEX IF NOT EXISTS idx_oitm_updatedate
    ON public.oitm (updatedate);