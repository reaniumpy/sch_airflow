

BEGIN TRANSACTION;

 -- Step 1: Insert New Records
  INSERT INTO `sch.dim_product_scd2` (id, tiki_pid, name, brand_name, origin, ingestion_dt_unix, valid_from, valid_to, is_current)
  SELECT
      GENERATE_UUID(),
      dim_product.tiki_pid,
      dim_product.name,
      dim_product.brand_name,
      dim_product.origin,
      dim_product.ingestion_dt_unix,
      dim_product.ingestion_dt_unix,
      NULL,
      TRUE
  FROM
      `sch.dim_product` AS dim_product
  LEFT JOIN
      `sch.dim_product_scd2` AS dim_product_scd2
  ON
      dim_product.tiki_pid = dim_product_scd2.tiki_pid
  WHERE
      dim_product_scd2.tiki_pid IS NULL;

  -- Step 2: Insert Changed Records
  INSERT INTO `sch.dim_product_scd2` (id, tiki_pid, name, brand_name, origin, ingestion_dt_unix, valid_from, valid_to, is_current)
  SELECT
      GENERATE_UUID(),
      dim_product.tiki_pid,
      dim_product.name,
      dim_product.brand_name,
      dim_product.origin,
      dim_product.ingestion_dt_unix,
      dim_product.ingestion_dt_unix,
      NULL,
      TRUE
  FROM
      `sch.dim_product` AS dim_product
  JOIN
      `sch.dim_product_scd2` AS dim_product_scd2
  ON
      dim_product.tiki_pid = dim_product_scd2.tiki_pid
  WHERE
      (
          (dim_product.name IS DISTINCT FROM dim_product_scd2.name) OR 
          (dim_product.origin IS DISTINCT FROM dim_product_scd2.origin) OR 
          (dim_product.brand_name IS DISTINCT FROM dim_product_scd2.brand_name)
      )
      AND dim_product_scd2.is_current = TRUE;

  -- Step 3: Update Existing Records
  UPDATE
      `sch.dim_product_scd2`
  SET
      valid_to = sub.ingestion_dt_unix,
      is_current = FALSE
  FROM (
      SELECT
          dim_product.tiki_pid,
          dim_product_scd2.id,
          dim_product.ingestion_dt_unix
      FROM
          `sch.dim_product` AS dim_product
      JOIN
          `sch.dim_product_scd2` AS dim_product_scd2
      ON
          dim_product.tiki_pid = dim_product_scd2.tiki_pid
      WHERE
          (
              (dim_product.name IS DISTINCT FROM dim_product_scd2.name) OR 
              (dim_product.origin IS DISTINCT FROM dim_product_scd2.origin) OR 
              (dim_product.brand_name IS DISTINCT FROM dim_product_scd2.brand_name)
          )
          AND dim_product_scd2.valid_to IS NULL
          AND dim_product_scd2.is_current = TRUE
  ) AS sub
  WHERE
      `sch.dim_product_scd2`.id = sub.id;
COMMIT TRANSACTION;