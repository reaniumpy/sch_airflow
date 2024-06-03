CREATE TABLE sch.dim_product_scd2 (
    id string,
    tiki_pid BIGINT,
    name STRING ,
    brand_name STRING,
    origin STRING,
    ingestion_dt_unix BIGINT,
    valid_from BIGINT,
    valid_to BIGINT,
    is_current BOOLEAN DEFAULT true
);