# Databricks notebook source
# MAGIC %md
# MAGIC # Legacy system to Databricks SQL Migration Utility
# MAGIC This notebook assists in converting SQL to Databricks SQL using LLMs, and optionally validates the results.
# MAGIC
# MAGIC ## Pre-requisites:
# MAGIC - This script must be run in a Databricks notebook.
# MAGIC - The input SQL files should be accessible from your Databricks workspace.
# MAGIC
# MAGIC ## Configure Parameters:
# MAGIC - Use the widgets above to set up your migration parameters, including model selection, input/output folders, and validation options.
# MAGIC
# MAGIC ## Limitation
# MAGIC - For big datasets you will encounter REQUEST_LIMIT_EXCEEDED error with claude model. Use models (like llama) optimized for AI functions for big datasets. More info [here](https://docs.databricks.com/aws/en/machine-learning/model-serving/foundation-model-overview).

# COMMAND ----------

# MAGIC %pip install PyYAML
# MAGIC %pip install databricks-sdk==0.61.0
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("SOURCE_DIALECT", "", "Select Source Dialect")
dbutils.widgets.text("MODEL_NAME", "", "Select Model Name")
dbutils.widgets.text("INPUT_FOLDER", "/Volumes/users/mohana_basak/mohana_test_vol/converter_input/", "Input Folder")
dbutils.widgets.text("DATABRICKS_OUTPUT_FOLDER", "/Volumes/users/mohana_basak/mohana_test_vol/converter_output/", "Databricks Notebook Output Folder")
dbutils.widgets.text("RESULTS_TABLE_NAME", "users.mohana_basak.dbx_converter_results", "Results Table Name")
dbutils.widgets.text("ADDITIONAL_PROMPTS", "", "Any additional prompts to be fed to the LLM")
dbutils.widgets.text("VALIDATE_RESULTS", "Yes", "Validate Results")
dbutils.widgets.text("MAX_RERUN_COUNT", "1", "Number of times to rerun in case of error")
dbutils.widgets.text("OUTPUT_LANG", "sql", "The notebook language for output: sql or python")
dbutils.widgets.text("OUTPUT_MODE", "notebook", "The output mode: notebook/workflow/file")

# COMMAND ----------

# IMPORTANT: Update these params according to your requirement.

# The folder containing your source files.
INPUT_FOLDER = dbutils.widgets.get("INPUT_FOLDER")

# The folder where the converted Databricks notebooks will be saved.
DATABRICKS_OUTPUT_FOLDER = dbutils.widgets.get("DATABRICKS_OUTPUT_FOLDER")

# The folder where the conversion results will be saved.
RESULTS_TABLE_NAME = dbutils.widgets.get("RESULTS_TABLE_NAME")

# Create backticked version for tables with special characters in catalog/schema names
table_parts = RESULTS_TABLE_NAME.split('.')
RESULTS_TABLE_NAME_QUOTED = f"`{table_parts[0]}`.`{table_parts[1]}`.`{table_parts[2]}`"

# The source input dialect.
SOURCE_DIALECT = dbutils.widgets.get("SOURCE_DIALECT")

# The Databricks Foundational Model to use for the tasks.
MODEL_NAME = dbutils.widgets.get("MODEL_NAME")

# Whether the converted SQL needs to be validated.
VALIDATE_RESULTS = dbutils.widgets.get("VALIDATE_RESULTS")

# Maximum number of times to retry.
MAX_RERUN_COUNT = int(dbutils.widgets.get("MAX_RERUN_COUNT"))

# Additional prompts for the LLM.
ADDITIONAL_PROMPTS = dbutils.widgets.get("ADDITIONAL_PROMPTS")

# The language of output notebook: sql or python.
OUTPUT_LANG = dbutils.widgets.get("OUTPUT_LANG")

# The mode of output: notebook, workflow, or file.
OUTPUT_MODE = dbutils.widgets.get("OUTPUT_MODE")

# COMMAND ----------

import os
import re
import pandas as pd
from pyspark.sql.functions import collect_list, array_join, col, lit, current_timestamp, split, regexp_replace, posexplode, expr, from_json, array
from datetime import datetime
from src.utils import common_helper, batch_helper
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from databricks.sdk import WorkspaceClient
from pyspark.sql import Row
from urllib.parse import quote

# COMMAND ----------

current_timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
run_id = f"{context.jobRunId().get()}_{current_timestamp_str}" if context.jobRunId().isDefined() else current_timestamp_str
print(run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prerequisites Check and Validation
# MAGIC **IMPORTANT:** This cell validates that either:
# MAGIC 1. The results table already exists and is writable, OR
# MAGIC 2. The service principal has permission to create the table
# MAGIC 
# MAGIC The job will fail fast with a clear error if prerequisites are not met.

# COMMAND ----------

print("--- Starting Legacy SQL to Databricks SQL Conversion Process ---")

# Check if the input directory exists
input_dir = os.path.dirname(INPUT_FOLDER) if os.path.isfile(INPUT_FOLDER) or '.' in os.path.basename(INPUT_FOLDER) else INPUT_FOLDER

if not os.path.exists(input_dir) or not os.listdir(input_dir):
    print(f"\n[FATAL ERROR] Input directory '{input_dir}' is empty or does not exist.")
    print("Please create it and add your .sql files.")
    raise Exception('Input folder not found')

if OUTPUT_MODE.lower() == 'workflow' and not DATABRICKS_OUTPUT_FOLDER.lower().startswith('/workspace/'):
    output_mode_folder_msg = 'Output folder must be a workspace path for workflows.'
    print(output_mode_folder_msg)
    raise Exception(output_mode_folder_msg)

print(f"\nReading files from: {input_dir}")


@udf(returnType=StringType())
def determine_row_type(value):
    return common_helper.classify_sql(value, SOURCE_DIALECT, OUTPUT_MODE)


# COMMAND ----------

if INPUT_FOLDER.startswith('/Workspace/'):
    records = batch_helper.read_files_to_df(spark, INPUT_FOLDER)
    orig_df = spark.createDataFrame([Row(**r) for r in records]).selectExpr('path as input_file', 'content as input_sql')
else:
    input_folder_mod = batch_helper.get_input_path_with_subdirs(INPUT_FOLDER)
    df = spark.read.text(input_folder_mod).selectExpr("value", "_metadata.file_path as input_file")
    orig_df = df.groupBy("input_file").agg(array_join(collect_list("value"), "\n").alias("input_sql"))

# Get the row_type
row_type_df = orig_df.withColumn("row_type", determine_row_type(col("input_sql")))

if 'procedure' in OUTPUT_MODE.lower():
    exploded_df = row_type_df.filter(col("row_type") == "sql_script").withColumn("position", lit(0)).distinct()
else:
    exploded_df = row_type_df.filter(col("row_type") == "sql_script").withColumn("parts", split(col("input_sql"), ";", -1)) \
        .select("input_file", "row_type", posexplode("parts").alias("position", "sql_statement")) \
        .withColumn("cleaned_col", regexp_replace(col("sql_statement"), r"^[^a-zA-Z0-9]+", "")) \
        .filter("cleaned_col != ''").drop("input_sql").withColumnRenamed("cleaned_col", "input_sql").selectExpr(
        "input_file", "input_sql", "row_type", "position").distinct()

# Filter rows where row_type is not 'sql_script'
non_exploded_df = row_type_df.filter(col("row_type") != "sql_script").withColumn("position", lit(0)).distinct()

# Union the two DataFrames
input_df = exploded_df.union(non_exploded_df)

attempt = 0
retried_validated_df = None
validated_df = None
is_rerun = False
input_df_count = input_df.count()

while attempt <= MAX_RERUN_COUNT and input_df_count > 0:
    if attempt > 0:
        print(f"[WARNING] Attempt #{attempt} failed. Retrying {input_df_count} rows...")
        is_rerun = True
    else:
        print(f"Starting conversion of {input_df_count} rows...")
    attempt += 1
    input_df = input_df.repartition(common_helper.get_dynamic_partitions(input_df_count))

    # Convert using LLM
    converted_sql_df = input_df.filter(col("row_type") == "sql_script").selectExpr("input_file", "input_sql", "row_type", "position", f"ai_query('{MODEL_NAME}', {batch_helper.prompt_to_convert_sql_with_ai_batch(SOURCE_DIALECT, ADDITIONAL_PROMPTS, is_rerun, OUTPUT_MODE, OUTPUT_LANG, 'sql_script')}, modelParameters => named_struct({common_helper.get_model_params(MODEL_NAME)})) as databricks_sql")
    converted_nonsql_df = input_df.filter(col("row_type") != "sql_script").selectExpr("input_file", "input_sql", "row_type", "position", f"ai_query('{MODEL_NAME}', {batch_helper.prompt_to_convert_sql_with_ai_batch(SOURCE_DIALECT, ADDITIONAL_PROMPTS, is_rerun, OUTPUT_MODE, OUTPUT_LANG, 'procedure')}, modelParameters => named_struct({common_helper.get_model_params(MODEL_NAME)})) as databricks_sql")
    converted_df = converted_sql_df.union(converted_nonsql_df)
    if OUTPUT_LANG.lower() == 'sql':
        converted_df = (
            converted_df.withColumn("databricks_sql", regexp_replace(col("databricks_sql"), common_helper.IDENT_SUFFIX_FIX_SPARK, common_helper.IDENT_SUFFIX_REPLACEMENT))
            .withColumn("databricks_sql", regexp_replace(col("databricks_sql"), common_helper.VIEW_TO_TABLE_REGEX, common_helper.VIEW_TO_TABLE_REPLACEMENT))
        )

    if OUTPUT_MODE.lower() == 'workflow':
        converted_df = (
            converted_df
            .withColumn("parsed", from_json("databricks_sql", common_helper.workflow_schema))
            .withColumn("databricks_sql_content", expr("concat_ws('\n\n', transform(parsed.tasks, x -> x.content))"))
            .selectExpr("input_file", "input_sql", "row_type", "position", "databricks_sql as json_content", "databricks_sql_content as databricks_sql")
        )

    if VALIDATE_RESULTS == 'Yes':
        collected_rows = converted_df.collect()
        validation_results = []
        for row in collected_rows:
            databricks_sql = row["databricks_sql"]
            databricks_sql = re.sub(r"(?s)^.*?(-- Databricks notebook source|# Databricks notebook source)", r"\1", databricks_sql)
            # Keep only relevant lines
            if OUTPUT_LANG.lower() == 'sql':
                cleaned = "\n".join(
                    line for line in databricks_sql.splitlines()
                    if not line.strip().startswith("CREATE WIDGET") and not line.strip().startswith("--")
                )
                if 'procedure' in OUTPUT_MODE.lower():
                    databricks_sqls = [re.sub(r"^USE IDENTIFIER.*?;\s*", "", cleaned, flags=re.MULTILINE)]
                else:
                    databricks_sqls = cleaned.split(';')
            else:
                databricks_sqls = batch_helper.extract_sql_from_python_str(databricks_sql)
            errors = []
            for each_sql in databricks_sqls:
                if each_sql.strip():
                    try:
                        spark.sql(f"EXPLAIN {each_sql}")
                    except Exception as ex:
                        error_message = str(ex)
                        jvm_index = error_message.find("JVM stacktrace:")
                        if jvm_index != -1:
                            error_message = error_message[:jvm_index].strip()
                        errors.append(error_message[0:1000])

            if errors:
                validation_result = 'FAILURE'
            else:
                validation_result = 'SUCCESS'
            if OUTPUT_MODE.lower() == 'workflow':
                databricks_sql = row["json_content"]
            validation_results.append((row["input_file"], row["row_type"], row["position"], row["input_sql"], databricks_sql, validation_result, errors))

        schema = StructType([
            StructField("input_file", StringType(), True),
            StructField("row_type", StringType(), True),
            StructField("position", IntegerType(), True),
            StructField("input_sql", StringType(), True),
            StructField("databricks_sql", StringType(), True),
            StructField("validation_result", StringType(), True),
            StructField("validation_error", ArrayType(StringType()), True)
        ])
        validated_df = spark.createDataFrame(validation_results, schema)
    else:
        # When validation is skipped, add validation_result and validation_error columns
        validated_df = converted_df.withColumn("validation_result", lit("SKIPPED")) \
                                    .withColumn("validation_error", array().cast("array<string>"))

    if retried_validated_df:
        retried_validated_df = retried_validated_df.union(validated_df.where("validation_result IN ('SUCCESS', 'SKIPPED')"))
    else:
        retried_validated_df = validated_df.where("validation_result IN ('SUCCESS', 'SKIPPED')")
    # Only retry FAILURE rows (not SUCCESS or SKIPPED)
    input_df = validated_df.where("validation_result = 'FAILURE'")
    input_df_count = input_df.count()

# After retries exhausted, add any remaining FAILURE rows to the final result
if retried_validated_df:
    retried_validated_df = retried_validated_df.union(validated_df.where("validation_result = 'FAILURE'"))
else:
    retried_validated_df = validated_df.where("validation_result = 'FAILURE'")

# Prepare final DataFrame with metadata and upsert into results table, marking previous versions as not latest
final_df = retried_validated_df.withColumn("converted_at", current_timestamp()) \
    .withColumn("version_id", lit(run_id))

# display(final_df)
# Append the final output into the results table
final_df.write.mode('append').saveAsTable(RESULTS_TABLE_NAME_QUOTED)

# COMMAND ----------

written_df = spark.sql(f"select * from {RESULTS_TABLE_NAME_QUOTED} where version_id = '{run_id}'")
display(written_df)

# COMMAND ----------

# Write converted Databricks SQL queries to output files, preserving folder structure.
# If DATABRICKS_SQL_OUTPUT_FOLDER is not set, skip file writing.
if not DATABRICKS_OUTPUT_FOLDER or DATABRICKS_OUTPUT_FOLDER.strip() == "":
    print("DATABRICKS_SQL_OUTPUT_FOLDER is empty or null. Skipping writing output to files.")
else:
    # Rebuild complete files and write the content.
    df_to_write = written_df
    if OUTPUT_MODE.lower() == 'notebook':
        # For notebook mode, build proper notebook structure with headers and cell separators
        df_to_write = (df_to_write.groupby("input_file")
                       .applyInPandas(batch_helper.build_file_content, schema="input_file string, content_to_write string"))
    elif OUTPUT_MODE.lower() == 'file':
        # For file mode, concatenate raw SQL/Python without notebook formatting
        df_to_write = (df_to_write.groupby("input_file")
                       .applyInPandas(lambda pdf: pd.DataFrame([[
                           pdf["input_file"].iloc[0],
                           "\n".join(pdf.sort_values("position")["databricks_sql"].tolist())
                       ]], columns=["input_file", "content_to_write"]), 
                       schema="input_file string, content_to_write string"))

    DATABRICKS_OUTPUT_FOLDER = DATABRICKS_OUTPUT_FOLDER if DATABRICKS_OUTPUT_FOLDER.endswith('/') else f'{DATABRICKS_OUTPUT_FOLDER}/'
    batch_helper.mirror_structure(INPUT_FOLDER, DATABRICKS_OUTPUT_FOLDER)
    w = WorkspaceClient()

    for row in df_to_write.collect():
        input_path = f"{row['input_file']}"

        if OUTPUT_MODE.lower() == 'workflow':
            try:
                job_id = batch_helper.create_workflow(row["databricks_sql"], input_path, INPUT_FOLDER, DATABRICKS_OUTPUT_FOLDER, OUTPUT_LANG, OUTPUT_MODE, w)
                job_url = f"{context.apiUrl().get()}/jobs/{job_id}"
                displayHTML(f'<p><strong>Job created for {input_path}: </strong><a href="{job_url}" target="_blank">{job_url}</a></p>')
            except Exception as exc:
                print(f"Failed to create workflow for {input_path}: {exc}")
        else:
            try:
                batch_helper.write_output_files(input_path, INPUT_FOLDER, DATABRICKS_OUTPUT_FOLDER, OUTPUT_LANG, OUTPUT_MODE, row['content_to_write'], w)
            except Exception as exc:
                print(f"Failed to write {input_path}: {exc}")

    if DATABRICKS_OUTPUT_FOLDER.lower().startswith('/volumes/'):
        parts = [p for p in DATABRICKS_OUTPUT_FOLDER.split("/") if p]
        prefix = "/" + "/".join(parts[:4])
        encoded = quote(DATABRICKS_OUTPUT_FOLDER, safe="")
        full_url = f"{context.apiUrl().get()}/explore/data{prefix}?volumePath={encoded}"
        displayHTML(f'<p><strong>Output Directory URL: </strong><a href="{full_url}" target="_blank">{full_url}</a></p>')
    elif DATABRICKS_OUTPUT_FOLDER.lower().startswith('/workspace/'):
        full_url = f"{context.apiUrl().get()}#workspace{DATABRICKS_OUTPUT_FOLDER}"
        displayHTML(f'<p><strong>Output Directory URL: </strong><a href="{full_url}" target="_blank">{full_url}</a></p>')

# COMMAND ----------

written_df_out = spark.sql(f"""
SELECT
  input_file,
  FIRST(row_type, TRUE) AS row_type,
  FIRST(position, TRUE)      AS position,
  FIRST(converted_at, TRUE)  AS converted_at,
  FIRST(version_id, TRUE)    AS version_id,
  FIRST(input_sql, TRUE)     AS input_sql,
  ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(validation_error))) AS validation_error,
  CASE 
    WHEN LENGTH(CONCAT_WS('\n\n', TRANSFORM(SORT_ARRAY(COLLECT_LIST(NAMED_STRUCT('p', position, 's', databricks_sql))), x -> x.s))) > 200
    THEN CONCAT(SUBSTR(CONCAT_WS('\n\n', TRANSFORM(SORT_ARRAY(COLLECT_LIST(NAMED_STRUCT('p', position, 's', databricks_sql))), x -> x.s)), 1, 200), '...[TRUNCATED]')
    ELSE CONCAT_WS('\n\n', TRANSFORM(SORT_ARRAY(COLLECT_LIST(NAMED_STRUCT('p', position, 's', databricks_sql))), x -> x.s))
  END AS databricks_sql,
  CASE
    WHEN SUM(CASE WHEN validation_result = 'FAILURE' THEN 1 ELSE 0 END) > 0
    THEN 'FAILURE' ELSE 'SUCCESS'
  END AS validation_result
FROM {RESULTS_TABLE_NAME_QUOTED}
where
    version_id = '{run_id}'
GROUP BY input_file;
""")
written_json = written_df_out.select('input_file', 'databricks_sql', 'validation_result').limit(20).toPandas().to_json(orient='records')
dbutils.notebook.exit(written_json)
