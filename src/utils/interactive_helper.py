import re
import pandas as pd
from databricks import sql
from . import common_helper
from . import prompt_helper


def prompt_to_convert_sql_with_ai_interactive(src_dialect, input_sql, additional_prompts, validation_comments):
    prompt = prompt_helper.prompt_to_convert_sql_with_ai(src_dialect, additional_prompts, 'interactive', 'sql', 'sql_script')
    prompt = prompt.replace('%%##input_sql##%%', input_sql)
    if validation_comments:
        prompt = prompt + f"' Received this error with last attempt. Please try to fix -- {validation_comments}'"
    return prompt


def prompt_to_generate_ddls_with_ai_interactive(databricks_sql, extracted_table_names, error_hint):
    prompt = prompt_helper.prompt_to_generate_ddls_with_ai()
    prompt = prompt.replace('%%##temp_type##%%', 'TEMPORARY TABLE').replace('%%##databricks_sql##%%', databricks_sql).replace('%%##extracted_table_names##%%', extracted_table_names).replace('%%##create_or_replace##%%', 'CREATE')
    if error_hint:
        prompt = prompt + f"' \n Received this error with last attempt. Please try to fix -- {error_hint}'"
    return prompt


def execute_sql(cfg, query: str, warehouse_id: str) -> pd.DataFrame:
    with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{warehouse_id}",
            credentials_provider=lambda: cfg.authenticate,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()


def execute_sqls(cfg, multi_query: str, warehouse_id: str) -> pd.DataFrame:
    queries = [q.strip() for q in multi_query.split(";") if q.strip()]
    with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{warehouse_id}",
            credentials_provider=lambda: cfg.authenticate,
    ) as connection:
        with connection.cursor() as cursor:
            for q in queries:
                cursor.execute(q)
            return cursor.fetchall_arrow().to_pandas()


def split_sql_statements(s: str, keep_semicolon: bool = False):
    parts = re.split(r';(?!\s*$)', s, flags=0)  # split on ';' except the very final trailing one
    out = []
    for i, chunk in enumerate(parts):
        chunk = chunk.strip()
        if not chunk:
            continue
        if keep_semicolon and i < len(parts) - 1:
            out.append(chunk + ';')
        else:
            if chunk.endswith(';'):
                chunk = chunk[:-1]
            out.append(chunk)
    return out


def validate_query(databricks_sql, llm_model_interactive, cfg, warehouse_id):
    databricks_sqls = split_sql_statements(databricks_sql, False)
    errors = []
    for each_sql in databricks_sqls:
        if each_sql.strip():
            if not each_sql.strip().startswith("CREATE WIDGET TEXT") and not each_sql.strip().startswith("--"):
                try:
                    execute_sql(cfg, f"EXPLAIN {each_sql}", warehouse_id)
                except Exception as ex:
                    error_message = str(ex)
                    jvm_index = error_message.find("JVM stacktrace:")
                    if jvm_index != -1:
                        error_message = error_message[:jvm_index].strip()
                    errors.append(error_message[0:1000])

    if errors:
        err_str = ";\n".join(errors)
        return {"valid": False, "reason": f"Validation Result: [FAILURE] Explanation: {err_str}"}
    else:
        return {"valid": True, "reason": "Validation Result: [SUCCESS] Explanation: EXPLAIN ran successfully."}


def regenerate_with_err_context(validation_result, llm_model_interactive, dialect_interactive, llm_prompts_interactive, cfg, warehouse_id, databricks_sql):
    err = validation_result['reason'].replace("'", "''")
    esc = (databricks_sql or "").replace("'", "''")
    model_full = common_helper.get_model_full_name(llm_model_interactive)
    q = f"""
        SELECT ai_query('{model_full}', {prompt_to_convert_sql_with_ai_interactive(dialect_interactive, esc, llm_prompts_interactive, err)},
         modelParameters => named_struct(
            {common_helper.get_model_params(model_full)}
            )) AS databricks_sql
        """
    df = execute_sql(cfg, q, warehouse_id)
    return df
