/****************************************************************************************************************************
****************************************************************************************************************************/
WITH projected_skips AS (
    SELECT
      entity_id,
      IFF(DATE_TRUNC('
    'month' ', CURRENT_DATE) = ' '2022-12-01' ', 1, 0) AS is_dec,
      IFF(DATE_TRUNC(' 'month'
    ', CURRENT_DATE) = ' '2023-01-01'
    ', 1, 0) AS is_jan,
      COALESCE(LEAST(1, (31 - DAY(CURRENT_DATE)) / 16), 0) AS current_month_skip_inclusion,
      CASE
        WHEN is_dec = 1 THEN current_month_skip_inclusion * dec_skips + jan_skips
        WHEN is_jan = 1 THEN current_month_skip_inclusion * jan_skips
        ELSE 0
      END AS proj_skips
    FROM ANALYTICS.source.static_jan_property_budgets
  )
SELECT
MD5(ep.property_id) AS property_key,
budg.*,
ps.proj_skips
FROM ANALYTICS.source.static_jan_property_budgets budg
LEFT JOIN projected_skips ps ON budg.entity_id = ps.entity_id
LEFT JOIN ANALYTICS.source.ent_properties p ON p.lookup_code = budg.entity_id
LEFT JOIN ANALYTICS.source.si_entrata_properties ep ON ep.entrata_id = p.id

/****************************************************************************************************************************
****************************************************************************************************************************/
CREATE OR REPLACE VIEW ANALYTICS.transform.bolc_bridge_ongoing_leasing_contracts
AS (
  WITH
    fall_leasing_eff_rates AS (
      SELECT
        contract_key,
        effective_rate
      FROM ANALYTICS.public.fact_contract_revenues
      WHERE property_academic_year = 2022
    ),
    scheduled_billing_eff_rates AS (
      SELECT
        fsc.contract_key,
        AVG(fsc.effective_rate) AS effective_rate
      FROM ANALYTICS.transform.fcr_contract_revenue_monthly fsc
      WHERE fsc.property_academic_year = 2022
      GROUP BY 1
    ),
    oct_occ AS (
      SELECT contract_key, customer_key
      FROM ANALYTICS.public.contracts
      WHERE ''2022-10-01'' BETWEEN occupancy_start_date AND occupancy_end_date
        AND unit_id != -1
    ),
    current_occ AS (
      SELECT contract_key, customer_key
      FROM ANALYTICS.public.contracts
      WHERE lease_status IN (''Current'', ''Notice'')
        AND unit_id != -1
    ),
    current_m2m AS (
      SELECT contract_key, customer_key
      FROM ANALYTICS.public.contracts
      WHERE lease_status IN (''Current'', ''Notice'')
        AND unit_id != -1
        AND lease_interval_type = ''Month To Month''
    ),
    feb_occ AS (
      SELECT contract_key, customer_key
      FROM ANALYTICS.public.contracts
      WHERE ''2023-02-01'' BETWEEN lease_start_date AND lease_end_date
        AND (
          (
            lease_status IN (''Current'', ''Notice'')
            AND unit_id != -1
            AND move_out_date NOT BETWEEN ''2022-10-01'' AND ''2023-01-31''
          )
          OR (
            lease_status IN (''Future'', ''Applicant'')
            AND lease_start_date > ''2022-10-01''
            AND prelease_start_date != ''1900-01-01''
            AND prelease_end_date = ''2100-01-01''
            AND move_in_date <= ''2023-01-31''
          )
        )
    ),
    oct_to_current_moveins AS (
      SELECT contract_key, customer_key
      FROM ANALYTICS.public.contracts
      WHERE lease_status IN (''Current'', ''Notice'', ''Past'')
        AND unit_id != -1
        AND occupancy_start_date BETWEEN ''2022-10-02'' AND current_date
        AND is_property_tenure_start = 1
    ),
    oct_to_current_moveouts AS (
      SELECT contract_key, customer_key
      FROM ANALYTICS.public.contracts
      WHERE lease_status IN (''Past'')
        AND unit_id != -1
        AND occupancy_end_date BETWEEN ''2022-10-02'' AND current_date
        AND is_property_tenure_end = 1
    ),
    current_thru_jan_moveouts AS (
      SELECT contract_key, customer_key
      FROM ANALYTICS.public.contracts
      WHERE lease_status IN (''Current'', ''Notice'')
        AND unit_id != -1
        AND (
          NULLIF(move_out_date, ''1900-01-01'') <= ''2023-01-31''
          OR
          NULLIF(lease_end_date, ''1900-01-01'') <= ''2023-01-31''
        )
        AND is_property_tenure_end = 1
    ),
    current_thru_jan_moveins AS (
      SELECT contract_key, customer_key
      FROM ANALYTICS.public.contracts
      WHERE TRUE
        AND is_property_tenure_start = 1
        AND current_date BETWEEN prelease_start_date AND prelease_end_date
        AND lease_status IN (''Future'', ''Applicant'')
        AND ''2023-02-01'' BETWEEN lease_start_date AND lease_end_date
        AND lease_start_date > ''2022-10-01''
        AND move_in_date <= ''2023-01-31''
    ),
    final AS (
      SELECT
        c.contract_key,
        c.property_key,
        c.floorplan_id,
        c.beds_leased,
        COALESCE(fler.effective_rate, sber.effective_rate) AS _effective_rate,
        IFF(_effective_rate < 300, NULL, _effective_rate) AS effective_rate,
        IFF(oct_occ.contract_key IS NOT NULL, 1, 0) AS is_oct_occ,
        IFF(current_occ.contract_key IS NOT NULL, 1, 0) AS is_current_occ,
        IFF(current_m2m.contract_key IS NOT NULL, 1, 0) AS is_current_m2m,
        IFF(feb_occ.contract_key IS NOT NULL, 1, 0) AS is_feb_occ,
        IFF(oct_to_current_moveins.contract_key IS NOT NULL, 1, 0) AS is_oct_to_current_movein,
        IFF(oct_to_current_moveouts.contract_key IS NOT NULL, 1, 0) AS is_oct_to_current_moveout,
        IFF(current_thru_jan_moveouts.contract_key IS NOT NULL, 1, 0) AS is_current_thru_jan_moveout,
        IFF(current_thru_jan_moveins.contract_key IS NOT NULL, 1, 0) AS is_current_thru_jan_movein,
        IFF(is_feb_occ = 1 AND c.lease_start_date > ''2022-10-01'' AND is_property_tenure_start = 1, 1, 0) AS is_backfill
      FROM ANALYTICS.public.contracts c
      LEFT JOIN oct_occ ON oct_occ.contract_key = c.contract_key
      LEFT JOIN current_occ ON current_occ.contract_key = c.contract_key
      LEFT JOIN feb_occ ON feb_occ.contract_key = c.contract_key
      LEFT JOIN oct_to_current_moveins ON oct_to_current_moveins.contract_key = c.contract_key
      LEFT JOIN oct_to_current_moveouts ON oct_to_current_moveouts.contract_key = c.contract_key
      LEFT JOIN current_thru_jan_moveouts ON current_thru_jan_moveouts.contract_key = c.contract_key
      LEFT JOIN current_thru_jan_moveins ON current_thru_jan_moveins.contract_key = c.contract_key
      LEFT JOIN current_m2m ON current_m2m.contract_key = c.contract_key
      LEFT JOIN fall_leasing_eff_rates fler ON fler.contract_key = c.contract_key
      LEFT JOIN scheduled_billing_eff_rates sber ON sber.contract_key = c.contract_key
      WHERE (
        is_oct_occ
        + is_current_occ
        + is_feb_occ
        + is_oct_to_current_movein
        + is_oct_to_current_moveout
        + is_current_thru_jan_moveout
        + is_current_thru_jan_movein) > 0
    )
  SELECT *
  FROM final
)
/****************************************************************************************************************************
****************************************************************************************************************************/

-- Complex Snowflake query with PIVOT, LATERAL FLATTEN, and dynamic columns
WITH sales_data AS (
    SELECT
        d.value:customer_id::VARCHAR AS customer_id,
        d.value:product_name::VARCHAR AS product_name,
        d.value:region::VARCHAR AS region,
        d.value:amount::DECIMAL(10,2) AS amount,
        TO_TIMESTAMP_LTZ(d.value:sale_date::VARCHAR) AS sale_date
    FROM my_db.my_schema.raw_sales,
    LATERAL FLATTEN(input => json_data) d
    WHERE d.value:status = 'completed'
),
regional_sales AS (
    SELECT
        customer_id,
        product_name,
        region,
        SUM(amount) as total_amount,
        COUNT(*) as transaction_count,
        LISTAGG(DISTINCT sale_date::DATE, ', ') WITHIN GROUP (ORDER BY sale_date) as sale_dates
    FROM sales_data
    WHERE sale_date >= DATEADD(month, -6, CURRENT_DATE())
    GROUP BY customer_id, product_name, region
)
SELECT * FROM regional_sales
PIVOT (
    SUM(total_amount) as total,
    AVG(total_amount) as avg,
    MAX(transaction_count) as max_txn
    FOR region IN ('North America', 'Europe', 'Asia Pacific', 'Latin America')
)
ORDER BY customer_id;

/****************************************************************************************************************************
****************************************************************************************************************************/

-- /Workspace/Users/mohana.basak@databricks.com/useful/migration/databricks_migrator/src/notebooks/batch_converter_notebook
-- /Volumes/users/mohana_basak/mohana_test_vol/converter_input/input_dir_suraj/
-- /Volumes/users/mohana_basak/mohana_test_vol/converter_output/test_1/
-- /Workspace/Users/mohana.basak@databricks.com/useful/migration/databricks_migrator/src/notebooks/test_1/
-- users.mohana_basak.dbx_converter_results
