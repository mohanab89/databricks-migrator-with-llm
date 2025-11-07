# Databricks notebook source
# MAGIC %md
# MAGIC ##MD5 Table row by row comparison

# COMMAND ----------

dbutils.widgets.text("strSRCDB", "migration.td_schema", "Source Catalog.Schema")
dbutils.widgets.text("strTGTDB", "migration.dbx_schema", "Target Catalog.Schema")
dbutils.widgets.text("MODEL_NAME", "", "Select Model Name")
dbutils.widgets.text("RECONCILE_RESULTS_TABLE_NAME", "users.mohana_basak.reconcile_results", "Reconcile Results Table Name")

# COMMAND ----------

from pyspark.sql.functions import concat_ws, col, lit
from functools import reduce
from src.utils import common_helper

# COMMAND ----------

# Get the source and target database names from widgets
src_db = dbutils.widgets.get("strSRCDB")
tgt_db = dbutils.widgets.get("strTGTDB")
MODEL_NAME = dbutils.widgets.get("MODEL_NAME")
RESULTS_TABLE_NAME = dbutils.widgets.get("RECONCILE_RESULTS_TABLE_NAME")

# Create backticked version for tables with special characters in catalog/schema names
table_parts = RESULTS_TABLE_NAME.split('.')
RESULTS_TABLE_NAME_QUOTED = f"`{table_parts[0]}`.`{table_parts[1]}`.`{table_parts[2]}`"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prerequisites Check and Validation
# MAGIC **IMPORTANT:** This cell validates that either:
# MAGIC 1. The results table already exists and is writable, OR
# MAGIC 2. The service principal has permission to create the table
# MAGIC 
# MAGIC The job will fail fast with a clear error if prerequisites are not met.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table creation

# COMMAND ----------

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {RESULTS_TABLE_NAME_QUOTED} (    
              query_id BIGINT GENERATED ALWAYS AS IDENTITY,
              table_Name STRING,
              status STRING,
              source_row_count INTEGER,
              target_row_count INTEGER,
              validation_result_analysis VARIANT,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          ) TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');
          """)

# COMMAND ----------

# Query to get the mismatch SQLs
mismatch_sqls_df = spark.sql(f"""
SELECT 
  src.table_catalog AS src_catalog,
  src.table_schema AS src_schema,
  src.table_name AS src_table,
  tgt.table_catalog AS tgt_catalog,
  tgt.table_schema AS tgt_schema,
  tgt.table_name AS tgt_table,
  CONCAT(
         'WITH src AS (SELECT concat_ws(",", *) AS src_concatenated_columns, md5(concat_ws(",", *)) AS src_concatenated_md5 FROM ', src.table_catalog, '.', src.table_schema, '.', src.table_name, '), ',
         'tgt AS (SELECT concat_ws(",", *) AS tgt_concatenated_columns, md5(concat_ws(",", *)) AS tgt_concatenated_md5 FROM ', tgt.table_catalog, '.', tgt.table_schema, '.', tgt.table_name, ') ',
         'SELECT "', src.table_catalog, '.', src.table_schema, '.', src.table_name, '" AS src_table_name, ',
         '"', tgt.table_catalog, '.', tgt.table_schema, '.', tgt.table_name, '" AS tgt_table_name, ',
         '(SELECT COUNT(*) FROM ', src.table_catalog, '.', src.table_schema, '.', src.table_name, ') AS src_count, ',
         '(SELECT COUNT(*) FROM ', tgt.table_catalog, '.', tgt.table_schema, '.', tgt.table_name, ') AS tgt_count, ',
         's.src_concatenated_columns, s.src_concatenated_md5, ',
         't.tgt_concatenated_columns, t.tgt_concatenated_md5, ',
         'CASE WHEN s.src_concatenated_md5 IS NULL OR t.tgt_concatenated_md5 IS NULL OR s.src_concatenated_md5 != t.tgt_concatenated_md5 THEN "MISMATCH" ELSE "MATCH" END AS status ',
         'FROM src s FULL OUTER JOIN tgt t ON s.src_concatenated_md5 = t.tgt_concatenated_md5 ',
         'WHERE s.src_concatenated_md5 IS NULL OR t.tgt_concatenated_md5 IS NULL OR s.src_concatenated_md5 != t.tgt_concatenated_md5 ',
         'UNION ALL ',
         'SELECT "', src.table_catalog, '.', src.table_schema, '.', src.table_name, '" AS src_table_name, ',
         '"', tgt.table_catalog, '.', tgt.table_schema, '.', tgt.table_name, '" AS tgt_table_name, ',
         '(SELECT COUNT(*) FROM ', src.table_catalog, '.', src.table_schema, '.', src.table_name, ') AS src_count, ',
         '(SELECT COUNT(*) FROM ', tgt.table_catalog, '.', tgt.table_schema, '.', tgt.table_name, ') AS tgt_count, ',
         'NULL AS src_concatenated_columns, NULL AS src_concatenated_md5, ',
         'NULL AS tgt_concatenated_columns, NULL AS tgt_concatenated_md5, ',
         '"MATCH" AS status ',
         'WHERE NOT EXISTS (SELECT 1 FROM src s FULL OUTER JOIN tgt t ON s.src_concatenated_md5 = t.tgt_concatenated_md5 WHERE s.src_concatenated_md5 IS NULL OR t.tgt_concatenated_md5 IS NULL OR s.src_concatenated_md5 != t.tgt_concatenated_md5)'
       ) AS mismatch_sql
FROM system.information_schema.tables src
INNER JOIN system.information_schema.tables tgt
  ON src.table_name = tgt.table_name
WHERE src.table_catalog = split_part('{src_db}', '.', 1)
  AND src.table_schema = split_part('{src_db}', '.', 2)
  AND tgt.table_catalog = split_part('{tgt_db}', '.', 1)
  AND tgt.table_schema = split_part('{tgt_db}', '.', 2)
""")

# Collect the mismatch SQLs and table info
mismatch_sqls_info = mismatch_sqls_df.collect()

dfs = []
for row in mismatch_sqls_info:
    src_table_full = f"{row['src_catalog']}.{row['src_schema']}.{row['src_table']}"
    tgt_table_full = f"{row['tgt_catalog']}.{row['tgt_schema']}.{row['tgt_table']}"
    mismatch_sql = row['mismatch_sql']

    # Get columns for src and tgt tables
    src_columns = [r['col_name'] for r in spark.sql(f"DESCRIBE TABLE {src_table_full}").select(col("col_name")).filter(col("col_name") != '').collect()]
    tgt_columns = [r['col_name'] for r in spark.sql(f"DESCRIBE TABLE {tgt_table_full}").select(col("col_name")).filter(col("col_name") != '').collect()]

    # Run the mismatch SQL
    df = spark.sql(mismatch_sql)

    # Add columns as first row to concatenated columns
    if df.columns and "src_concatenated_columns" in df.columns and "tgt_concatenated_columns" in df.columns:
        src_header = ",".join(src_columns)
        tgt_header = ",".join(tgt_columns)
        df = df.withColumn("src_concatenated_columns",
                           concat_ws('\n', lit(src_header), col("src_concatenated_columns")))
        df = df.withColumn("tgt_concatenated_columns",
                           concat_ws('\n', lit(tgt_header), col("tgt_concatenated_columns")))
    dfs.append(df)

mismatch_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs) if dfs else spark.createDataFrame([], schema=None)

mismatch_df.createOrReplaceTempView("mismatch_df")
# display(mismatch_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use AI to build the report and detailed differences

# COMMAND ----------

my_ai_validation_df = spark.sql(f"""
          SELECT src_table_name as table_name,
       status,
       src_count,
       tgt_count,
    ai_query(
        '{MODEL_NAME}',
        'You are a data migration validation assistant. This output has both datasets comparsion representing legacy and new system outputs.

        Output a JSON summary for each table with:
        - table_name: ' || table_name ||
        '- src_row_count: ' || src_count ||
        '- tgt_row_count: ' || tgt_count ||
        '- discrepancy_count: show the counts of each differences between the two datasets ' ||
        '- status: '|| status ||
        '- validation_result: If ' || status || ' equals MISMATCH show a detail descriptive difference of report summary from comparing' || src_concatenated_columns || ' and ' || tgt_concatenated_columns || ' describe which records or columns were different otherwise show NA. Make sure to always show the JSON output summary as JSON.'
        ,
        modelParameters => named_struct(
            {common_helper.get_model_params(MODEL_NAME)}
        )
    ) AS validation_result
FROM mismatch_df
""")
my_ai_validation_df.createOrReplaceTempView("my_ai_validation_df")
# display(my_ai_validation_df)

# COMMAND ----------

ai_validation_report_df = spark.sql(f"""
INSERT INTO {RESULTS_TABLE_NAME_QUOTED} (table_Name, status, source_row_count, target_row_count, validation_result_analysis)
SELECT table_Name,
    status,
    src_count,
    tgt_count,
    PARSE_JSON(regexp_replace(validation_result, '```json|```', '')) as validation_result_analysis
FROM my_ai_validation_df""")
# display(ai_validation_report_df)

# COMMAND ----------

written_df = spark.sql(f"""
SELECT table_Name,source_row_count, target_row_count, variant_get(validation_result_analysis, '$.validation_result', 'string') as validation_report, created_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY Table_Name ORDER BY query_id DESC) AS rn
    FROM {RESULTS_TABLE_NAME_QUOTED}
) subquery
WHERE rn = 1
""")
display(written_df)

# COMMAND ----------

written_json = written_df.select('table_name', 'source_row_count', 'target_row_count', 'validation_report', 'created_at').toPandas().to_json(orient='records')
dbutils.notebook.exit(written_json)
