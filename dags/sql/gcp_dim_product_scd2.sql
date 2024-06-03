MERGE INTO `sch.dim_product_scd2` AS target
USING (
    -- Subquery to identify changes or new rows
    SELECT
        source.tiki_pid,
        source.name,
        source.brand_name,
        source.origin,
        source.ingestion_dt_unix,
        target.id AS existing_id,
        target.valid_from AS existing_valid_from,
        target.valid_to AS existing_valid_to,
        target.is_current AS existing_is_current
    FROM `sch.dim_product` AS source
    LEFT JOIN `sch.dim_product_scd2` AS target
    ON source.tiki_pid = target.tiki_pid
    AND target.is_current = TRUE
) AS changes
ON changes.tiki_pid = target.tiki_pid AND target.is_current = TRUE
WHEN MATCHED AND (
    changes.name <> target.name
    OR changes.origin <> target.origin
    OR changes.brand_name <> target.brand_name
) THEN
    -- Update the existing row to set valid_to and is_current
    UPDATE SET
        target.valid_to = changes.ingestion_dt_unix,
        target.is_current = FALSE
WHEN NOT MATCHED BY TARGET THEN
    -- Insert a new row for new records or changed records
    INSERT (id, tiki_pid, name, brand_name, origin, ingestion_dt_unix, valid_from, valid_to, is_current)
    VALUES (GENERATE_UUID(), changes.tiki_pid, changes.name, changes.brand_name, changes.origin, changes.ingestion_dt_unix, changes.ingestion_dt_unix, NULL, TRUE);