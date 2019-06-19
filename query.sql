CREATE TEMP FUNCTION
  commitmentSKUToNegationSKU(sku_desc STRING)
  RETURNS STRING AS ( IF(REGEXP_CONTAINS(sku_desc, r"Commitment v[0-9]: [a-zA-Z]+ in [a-zA-Z0-9\\-]+ for [0-9]+ [_a-zA-Z]+"),
      CONCAT(
        --prefix
        "Reattribution_Negation_CUD_",
        --number
        REGEXP_EXTRACT(sku_desc, r"Commitment v[0-9]: [a-zA-Z]+ in [a-zA-Z0-9\\-]+ for ([0-9]+) [_a-zA-Z]+"),
        --timeframe
        REGEXP_EXTRACT(sku_desc, r"Commitment v[0-9]: [a-zA-Z]+ in [a-zA-Z0-9\\-]+ for [0-9]+ ([_a-zA-Z]+)"), "_",
        --UPPER(type)
        UPPER(REGEXP_EXTRACT(sku_desc, r"Commitment v[0-9]: ([a-zA-Z]+) in [a-zA-Z0-9\\-]+ for [0-9]+ [_a-zA-Z]+")), "_COST_",
        --region
        REGEXP_EXTRACT(sku_desc, r"Commitment v[0-9]: [a-zA-Z]+ in ([a-zA-Z0-9\\-]+) for [0-9]+ [_a-zA-Z]+") ),
      NULL));
CREATE TEMP FUNCTION
  regionMapping(gcp_region STRING)
  RETURNS STRING AS (
    CASE
      WHEN gcp_region IS NULL THEN NULL
      WHEN gcp_region LIKE "us-%"
    OR gcp_region LIKE "northamerica%"
    OR gcp_region LIKE "southamerica%" THEN "Americas"
      WHEN gcp_region LIKE "europe-%" THEN "EMEA"
      WHEN gcp_region LIKE "australia-%"
    OR gcp_region LIKE "asia-%" THEN"APAC" END);
CREATE TEMP FUNCTION
   ratio(numerator float64, denominator float64)
  as (IF(denominator = 0,
        0,
        numerator / denominator));
(
  WITH
    billing_export_table AS (
        SELECT
         *
        FROM
         `{export_table}`
    ),
    billing_id_table AS (
    SELECT
      billing_account_id
    FROM
      billing_export_table
    GROUP BY
      billing_account_id
    LIMIT 1 ),
    usage_data AS (
    SELECT
      CAST(DATETIME(usage_start_time, "America/Los_Angeles") AS DATE) as usage_date,
      invoice.month AS invoice_month,
      sku.id AS sku_id,
      sku.description AS sku_description,
      -- Only include region if we are looking at data from 9/20/2018 and onwards
      location.region AS region,
      service.id AS service_id,
      service.description AS service_description,
      project.id AS project_id,
      usage.unit AS unit,
      cost,
      usage.amount AS usage_amount,
      credits,
      cost_type
    FROM
      billing_export_table
    WHERE
      service.description = "Compute Engine"
      AND (
       FALSE OR (LOWER(sku.description) LIKE "%instance%"
          OR LOWER(sku.description) LIKE "% intel %")
        OR LOWER(sku.description) LIKE "%memory optimized core%"
        OR LOWER(sku.description) LIKE "%memory optimized ram%"
        OR LOWER(sku.description) LIKE "%commitment%")
     -- Filter out Sole Tenancy skus that do not represent billable compute instance usage
    AND NOT
    (
      -- the VMs that run on sole tenancy nodes are not actually billed. Just the sole tenant node is
      LOWER(sku.description) LIKE "%hosted on sole tenancy%"
      -- sole tenancy premium charge is not eligible instance usage
      OR LOWER(sku.description) LIKE "sole tenancy premium%"
    )
    -- Filter to time range when necessary columns (region) were released into Billing BQ Export
    AND CAST(DATETIME(usage_start_time, "America/Los_Angeles") AS DATE) >= "2018-09-20"),
    -- Create temporary table prices, in order to calculate unit price per (date, sku, region) tuple.
    -- Export table only includes the credit dollar amount in the credit.amount field. We can get the credit
    -- usage amount (e.g. core hours) by dividing credit.amount by unit price for that sku.
    -- This assumes that the unit price for the usage is equal to the unit price for the associated
    -- CUD credit. This should be correct, except in rare cases where unit price for that sku changed
    -- during the day (i.e. a price drop, change in spending-based discount %)
    -- It is necessary to do this in a separate table and join back into the main data set vs.
    -- separately on each usage line because some line items have CUD credit but no associated
    -- usage. We would not otherwise be able to calculate a unit price for these line items.
    prices AS (
    SELECT
      usage_date,
      sku_id,
      -- Only include region if we are looking at data from 9/20/2018 and onwards
      region,
      -- calculate unit price per sku for each day. Catch line items with 0 usage to avoid divide by zero.
      -- using 1 assumes that there are no relevant (CUD related) skus with cost but 0 usage,
      -- which is correct for current billing data
      ratio(sum(cost), sum(usage_amount)) as unit_price

    FROM
      usage_data,
      UNNEST(credits) AS cred
    WHERE
      cred.name LIKE "%Committed%"
    GROUP BY 1, 2, 3),
    -- sku_metadata temporary table captures information about skus, such as CUD eligibility,
    -- whether the sku is vCPU or RAM, etc.
    sku_metadata AS (
    SELECT
      sku_id,
      -- parse sku_description to identify whether usage is CUD eligible, or if the
      -- line item is for a commitment charge
      CASE
        WHEN LOWER(sku_description) LIKE "%commitment%" THEN "CUD Commitment"
        WHEN ( LOWER(sku_description) LIKE "%preemptible%"
        OR LOWER(sku_description) LIKE "%micro%"
        OR LOWER(sku_description) LIKE "%small%"
        OR LOWER(sku_description) LIKE "%extended%" ) THEN "Ineligible Usage"
        WHEN ( (LOWER(sku_description) LIKE "%instance%" OR LOWER(sku_description) LIKE "% intel %") OR LOWER(sku_description) LIKE "%core%" OR LOWER(sku_description) LIKE "%ram%" ) THEN "Eligible Usage"
        ELSE NULL
      END AS usage_type,
      CASE
        WHEN ( LOWER(sku_description) LIKE "%megamem%" OR LOWER(sku_description) LIKE "%ultramem%" OR LOWER(sku_description) LIKE "%memory optimized%" ) THEN "Memory Optimized Usage"
        ELSE "Regular Usage"
      END AS cud_type,
      CASE
        WHEN LOWER(sku_description) LIKE "%americas%" THEN "AMERICAS"
        WHEN LOWER(sku_description) LIKE "%emea%" THEN "EMEA"
        WHEN LOWER(sku_description) LIKE "%apac%" THEN "APAC"
        ELSE NULL
      END AS geo,
      -- for VM skus and commitments, "seconds" unit uniquely identifies vCPU usage
      -- and "byte-seconds" unit uniquely identifies RAM
      CASE
        WHEN LOWER(unit) LIKE "seconds" THEN "vCPU"
        WHEN LOWER(unit) LIKE "byte-seconds" THEN "RAM"
        ELSE NULL
      END AS unit_type,
      CASE
        WHEN LOWER(unit) LIKE "seconds" THEN "Avg. Concurrent vCPU"
        WHEN LOWER(unit) LIKE "byte-seconds" THEN "Avg. Concurrent RAM GB"
        ELSE NULL
      END AS display_unit
    FROM
      usage_data
    GROUP BY 1, 2, 3, 4, 5, 6),
    -- create temporary usage_credit_data table to separate out credits from usage into their own line items
    -- and associate necessary sku metadata with usage, commitment, and credit line items
    usage_credit_data AS ( (
        -- First usage query pulls out amount and dollar cost of Eligible Usage and Commitment charges
      SELECT
        usage_date,
        service_id,
        service_description,
        region,
        usage_type,
        cud_type,
        unit_type,
        unit,
        project_id,
        cost_type,
        SUM(usage_amount) AS usage_amount,
        SUM(cost) AS cost
      FROM
        usage_data AS u
      JOIN
        sku_metadata
      ON
        u.sku_id = sku_metadata.sku_id
      WHERE
        usage_type IS NOT NULL
      GROUP BY
        1,2, 3, 4, 5, 6, 7, 8, 9, 10)
    UNION ALL (
        -- Second query pulls out CUD and SUD Credit usage and cost. This is done in a separate
        -- SELECT and unioned because if we unnest the repeated field for credit types, we can
        -- no longer correctly sum the usage in the first query.
      SELECT
        usage_date,
        service_id,
        service_description,
        region,
        usage_type,
        cud_type,
        unit_type,
        unit,
        project_id,
        cost_type,
        SUM(usage_amount) AS usage_amount,
        SUM(cost) AS cost
      FROM (
        SELECT
          u.usage_date,
          service_id,
          service_description,
          u.region,
          CASE
            WHEN LOWER(cred.name) LIKE "%committed%" THEN "CUD Credit"
            WHEN LOWER(cred.name) LIKE "%sustained%" THEN "SUD Credit"
            ELSE NULL
          END AS usage_type,
          cud_type,
          unit_type,
          unit,
          project_id,
          unit_price,
          cost_type,
          IF ( prices.unit_price = 0,
            0,
            CASE
            -- Divide by # seconds in a day to get to core*days == avg daily concurrent usage
              WHEN LOWER(unit_type) LIKE "vcpu" THEN -1*SUM(cred.amount)/prices.unit_price -- / 86400
            -- Divide by # seconds in a day and # bytes in a GB to get to
            -- GB*days == avg daily concurrent RAM GB
              WHEN LOWER(unit_type) = "ram" THEN -1*SUM(cred.amount)/prices.unit_price -- / (86400 * 1073741824)
              ELSE NULL
            END ) AS usage_amount,
          SUM(cred.amount) AS cost
        FROM
          usage_data AS u,
          UNNEST(credits) AS cred
        JOIN
          sku_metadata
        ON
          u.sku_id = sku_metadata.sku_id
        JOIN
          prices
        ON
              u.sku_id = prices.sku_id
          AND u.usage_date = prices.usage_date
          AND u.region = prices.region
        WHERE
          LOWER(cred.name) LIKE "%committed%"
          OR LOWER(cred.name) LIKE "%sustained%"
        GROUP BY
          1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
      GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ),
    -- project_credit_breakout sums usage amount and cost
    -- across the cost organization schema of interest: labels within projects
    project_label_credit_breakout AS (
    SELECT
      usage_date,
      service_id,
      service_description,
      region,
      project_id,
      cud_type,
      unit_type,
      cost_type,
      SUM(IF(usage_type LIKE "CUD Commitment",
          usage_amount,
          0)) AS commitment_usage_amount,
      SUM(IF(usage_type LIKE "CUD Commitment",
          cost,
          0)) AS commitment_cost,
      SUM(IF(usage_type LIKE "CUD Credit",
          usage_amount,
          0)) AS cud_credit_usage_amount,
      SUM(IF(usage_type LIKE "CUD Credit",
          cost,
          0)) AS cud_credit_cost,
      SUM(IF(usage_type LIKE "SUD Credit",
          usage_amount,
          0)) AS sud_credit_usage_amount,
      SUM(IF(usage_type LIKE "SUD Credit",
          cost,
          0)) AS sud_credit_cost,
      SUM(IF(usage_type LIKE "Eligible Usage",
          usage_amount,
          0)) AS usage_amount,
      SUM(IF(usage_type LIKE "Eligible Usage",
          cost,
          0)) AS cost
    FROM
      usage_credit_data
    GROUP BY
      1, 2, 3, 4, 5, 6, 7, 8),



    PG_purchased_commitments AS (
    SELECT
      pc.id as pg_id,
      usage_date,
      p.region as region,
      p.cud_type as cud_type,
      p.unit_type as unit_type,
      ANY_VALUE(project_ids) as project_ids,
      LEAST(SUM(c.amount), SUM(p.usage_amount)) as PG_purchased_committments_usage,
      SUM(p.usage_amount) as PG_all_eligible_usage
    FROM
        project_label_credit_breakout p
      JOIN `{commitment_table}` pc
      ON p.project_id in unnest(pc.project_ids)
      JOIN UNNEST(pc.commitments) as c
      ON p.region = c.region
      AND p.cud_type = c.cud_type
      AND p.unit_type = c.unit_type
      group by 1,2,3,4,5
    ),

    BA_credit_breakout AS (
    SELECT
      usage_date,
      region,
      cud_type,
      unit_type,
      SUM(usage_amount) AS BA_usage_amount,
      sum(cud_credit_usage_amount) as BA_cud_credit_usage,
      SUM(commitment_cost) AS BA_commitment_cost,
      SUM(cud_credit_cost) AS BA_cud_credit_cost,
      SUM(sud_credit_cost) AS BA_sud_credit_cost
    FROM
      project_label_credit_breakout
    GROUP BY 1, 2, 3, 4 ),


    BA_purchased_credit_breakout AS (
    SELECT
      p.usage_date as usage_date,
      p.region as region,
      p.cud_type as cud_type,
      p.unit_type as unit_type,
      sum(PG_purchased_committments_usage) as BA_usage_amount,
      -- this should be BA_purchased_commitment_cost
      (sum(b.BA_commitment_cost) * sum(PG_purchased_committments_usage) / SUM(b.BA_cud_credit_usage)) AS BA_commitment_cost,
      -- this should be BA_purchased_cud_credit_cost
      (sum(b.BA_cud_credit_cost) * sum(PG_purchased_committments_usage) / SUM(b.BA_cud_credit_usage)) AS BA_cud_credit_cost,
      -- this is very wrong
      (sum(b.BA_sud_credit_cost) * sum(PG_purchased_committments_usage) / SUM(b.BA_cud_credit_usage)) AS BA_sud_credit_cost
    FROM
      BA_credit_breakout b
      left join PG_purchased_commitments p
      on p.usage_date = b.usage_date
      AND p.region = b.region
      AND p.cud_type = b.cud_type
      AND p.unit_type = b.unit_type
    GROUP BY 1, 2, 3, 4),

    BA_unpurchased_credit_breakout AS (
    SELECT
      b.usage_date as usage_date,
      b.region as region,
      b.cud_type as cud_type,
      b.unit_type as unit_type,
      sum(usage_amount) as BA_all_usage,
      sum(usage_amount) - any_value(if(pcb.BA_usage_amount is null, 0, pcb.BA_usage_amount))  as BA_unpurchased_usage,
      sum(cud_credit_usage_amount) - any_value(if(pcb.BA_usage_amount is null, 0, pcb.BA_usage_amount)) as BA_usage_amount,

      SUM(commitment_cost) - any_value(if(pcb.BA_commitment_cost is null, 0, pcb.BA_commitment_cost)) AS BA_commitment_cost,
      SUM(cud_credit_cost) - any_value(if(pcb.BA_cud_credit_cost is null, 0, pcb.BA_cud_credit_cost)) AS BA_cud_credit_cost,
      SUM(sud_credit_cost) - any_value(if(pcb.BA_sud_credit_cost is null, 0, pcb.BA_sud_credit_cost)) AS BA_sud_credit_cost
    FROM
      project_label_credit_breakout b
      left join
      BA_purchased_credit_breakout pcb
      on pcb.usage_date = b.usage_date
      AND pcb.region = b.region
      AND pcb.cud_type = b.cud_type
      AND pcb.unit_type = b.unit_type
      group by 1,2,3,4),

    PG_purchased_credit_breakout AS (
    SELECT
      pg_id,
      pg.usage_date as usage_date,
      pg.region as region,
      pg.cud_type as cud_type,
      pg.unit_type as unit_type,
      pg.project_ids as project_ids,
      pg.PG_purchased_committments_usage as PG_purchased_committments_usage,
      pg.PG_all_eligible_usage as PG_all_eligible_usage,
      IF(b.BA_usage_amount=0,
        0,
       b.BA_usage_amount * (PG_purchased_committments_usage / b.BA_usage_amount))  as PG_usage_amount,
      IF(b.BA_usage_amount=0,
        0,
      b.BA_commitment_cost * (PG_purchased_committments_usage / b.BA_usage_amount)) as PG_commitment_cost,
      IF(b.BA_usage_amount=0,
        0,
      b.BA_cud_credit_cost * (PG_purchased_committments_usage / b.BA_usage_amount)) as PG_cud_credit_cost,
      IF(b.BA_usage_amount=0,
        0,
      (PG_purchased_committments_usage / b.BA_usage_amount) * b.BA_sud_credit_cost) as PG_sud_credit_cost
    FROM
      PG_purchased_commitments AS pg
    LEFT JOIN
      BA_purchased_credit_breakout AS b
    ON
          pg.usage_date = b.usage_date
      AND pg.region = b.region
      AND pg.cud_type = b.cud_type
      AND pg.unit_type = b.unit_type),


    -- distribute pg_purchseed_credit_breakout to projects.
    final_data_purchased as(
    SELECT
      p.usage_date as usage_date,
      p.service_id as service_id,
      p.cost_type as cost_type,
      p.service_description as service_description,
      p.region as region,
      p.unit_type as unit_type,
      p.cud_type as cud_type,
      p.project_id as project_id,
      IF(PG_purchased_committments_usage=0,
        0,
       (p.usage_amount / PG_all_eligible_usage) * PG_purchased_committments_usage) as P_aloc_usage,

      IF(PG_usage_amount=0,
        0,
        (p.usage_amount / PG_all_eligible_usage) * PG_cud_credit_cost) AS P_alloc_cud_credit_cost,
      IF(PG_usage_amount=0,
        0,
        (p.usage_amount / PG_all_eligible_usage) * PG_sud_credit_cost) AS P_alloc_sud_credit_cost,
      IF(PG_usage_amount=0,
        0,
        (p.usage_amount / PG_all_eligible_usage) * PG_commitment_cost) AS P_method_2_commitment_cost
    FROM
      project_label_credit_breakout AS p
    JOIN
      PG_purchased_credit_breakout AS b
    ON
      p.usage_date = b.usage_date
      AND p.region = b.region
      AND p.cud_type = b.cud_type
      AND p.unit_type = b.unit_type
      AND p.project_id in unnest(b.project_ids)
    ),

    final_data_unpurchased AS (
    SELECT
      p.usage_date as usage_date,
      p.service_id as service_id,
      p.cost_type as cost_type,
      p.service_description as service_description,
      p.region as region,
      p.unit_type as unit_type,
      p.cud_type as cud_type,
      p.project_id as project_id,
      0 as P_aloc_usage,
      IF(BA_unpurchased_usage=0,
        0,
        ((p.usage_amount - IF(fdp.P_aloc_usage is NULL, 0, fdp.P_aloc_usage)) / BA_unpurchased_usage) * BA_cud_credit_cost) AS P_alloc_cud_credit_cost,
      IF(BA_unpurchased_usage=0,
        0,
        ((p.usage_amount - IF(fdp.P_aloc_usage is NULL, 0, fdp.P_aloc_usage)) / BA_unpurchased_usage) * BA_sud_credit_cost) AS P_alloc_sud_credit_cost,
      IF(BA_unpurchased_usage=0,
        0,
        ((p.usage_amount - IF(fdp.P_aloc_usage is NULL, 0, fdp.P_aloc_usage)) / BA_unpurchased_usage) * BA_commitment_cost) AS P_method_2_commitment_cost
    FROM
      project_label_credit_breakout AS p
    JOIN
      BA_unpurchased_credit_breakout AS b
    ON
      p.usage_date = b.usage_date
      AND p.region = b.region
      AND p.cud_type = b.cud_type
      AND p.unit_type = b.unit_type
    left join
      final_data_purchased fdp
    ON
      fdp.project_id = p.project_id),
    correct_cud_costs AS (
    SELECT
      b.billing_account_id AS billing_account_id,
      STRUCT ( service_id AS id,
        service_description AS description) AS service,
      STRUCT (CONCAT("Reattribution_Addition_CUD_", IF(LOWER(unit_type) LIKE "ram",
            "RAM_COST",
            "CORE_COST"), "_", regionMapping(region)) AS id,
        CONCAT("Reattribution_Addition_CUD_", IF(LOWER(unit_type) LIKE "ram",
            "RAM_COST",
            "CORE_COST"), "_", regionMapping(region)) AS description) AS sku,
      TIMESTAMP(usage_date) AS usage_start_time,
      TIMESTAMP_ADD(TIMESTAMP(usage_date), INTERVAL ((3600*23)+3599) SECOND) AS usage_end_time,
      STRUCT ( project_id AS id,
        "" AS name,
        ARRAY<STRUCT<key STRING,
        value STRING>> [] AS labels,
        "" AS ancestry_numbers) AS project,
      ARRAY<STRUCT<key STRING,
      value STRING>> [] AS labels,
      ARRAY<STRUCT<key STRING,
      value STRING>> [] AS system_labels,
      STRUCT ( "" AS location,
        "" AS country,
        region AS region,
        "" AS zone ) AS location,
      CURRENT_TIMESTAMP() AS export_time,
      P_method_2_commitment_cost AS cost,
      "USD" AS currency,
      1.0 AS currency_conversion_rate,
      STRUCT ( 0.0 AS amount,
        "TODO" AS unit,
        0.0 AS amount_in_pricing_units,
        "TODO" AS pricing_unit ) AS usage,
      ARRAY<STRUCT<name STRING,
      amount FLOAT64>> [] AS credits,
      STRUCT ( FORMAT_DATE("%Y%m", usage_date) AS month) AS invoice,
      cost_type
    FROM
      (select * from final_data_purchased union all select * from final_data_unpurchased),
      billing_id_table AS b
    WHERE
      P_method_2_commitment_cost <> 0),
    correct_cud_credits AS (
    SELECT
      b.billing_account_id AS billing_account_id,
      STRUCT ( service_id AS id,
        service_description AS description) AS service,
      STRUCT ( CONCAT("Reattribution_Addition_CUD_", IF(LOWER(unit_type) LIKE "ram",
            "RAM",
            "CORE"), "_CREDIT_", regionMapping(region)) AS id,
        CONCAT("Reattribution_Addition_CUD_", IF(LOWER(unit_type) LIKE "ram",
            "RAM",
            "CORE"), "_CREDIT_", regionMapping(region)) AS description) AS sku,
      TIMESTAMP(usage_date) AS usage_start_time,
      TIMESTAMP_ADD(TIMESTAMP(usage_date), INTERVAL ((3600*23)+3599) SECOND) AS usage_end_time,
      STRUCT ( project_id AS id,
        "" AS name,
        ARRAY<STRUCT<key STRING,
        value STRING>> [] AS labels,
        "" AS ancestry_numbers) AS project,
      ARRAY<STRUCT<key STRING,
      value STRING>> [] AS labels,
      ARRAY<STRUCT<key STRING,
      value STRING>> [] AS system_labels,
      STRUCT ( "" AS location,
        "" AS country,
        region AS region,
        "" AS zone ) AS location,
      CURRENT_TIMESTAMP() AS export_time,
      0.0 AS cost,
      "USD" AS currency,
      1.0 AS currency_conversion_rate,
      STRUCT ( 0.0 AS amount,
        "TODO" AS unit,
        0.0 AS amount_in_pricing_units,
        "TODO" AS pricing_unit ) AS usage,
      ARRAY<STRUCT<name STRING,
      amount FLOAT64>> [(IF(LOWER(unit_type) LIKE "ram",
          "Committed Usage Discount: RAM",
          "Committed Usage Discount: CPU"),
        P_alloc_cud_credit_cost)] AS credits,
      STRUCT ( FORMAT_DATE("%Y%m", usage_date) AS month) AS invoice,
      cost_type
    FROM
      (select * from final_data_purchased union all select * from final_data_unpurchased),
      billing_id_table AS b
    WHERE
      P_alloc_cud_credit_cost <> 0)

  SELECT
    *
  FROM
    correct_cud_credits
)
