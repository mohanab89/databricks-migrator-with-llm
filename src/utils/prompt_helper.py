from . import common_helper


def prompt_to_convert_sql_with_ai(src_dialect, additional_prompts, output_mode, output_lang, row_type):
    prompt = f"'{common_helper.common_prompts[output_mode][output_lang][row_type].replace('%%##src_dialect##%%', src_dialect).replace('%%##conversion_prompts##%%', common_helper.conversion_prompts[src_dialect].strip()).replace('%%##additional_prompts##%%', additional_prompts).strip()}'"
    return prompt


def prompt_to_generate_ddls_with_ai():
    prompt = f"""
    'You are a Databricks schema architect. Generate **self-contained** DDLs.

    TASK:
    - Given a Databricks SQL query, generate a **%%##create_or_replace##%% %%##temp_type##%%** for **each** table in this list (comma-separated): %%##extracted_table_names##%%

    STRICT OUTPUT RULES:
      - The table/view definition must be **self-contained**:
      - Use **only** `AS SELECT <literal> AS <col>, ...` with **exactly 1 row**.
      - **Do NOT** include any of: FROM, JOIN, USING, WITH, LATERAL, TABLE, VALUES, UNNEST.
      - No references to any table/view other than the one being created.
      - Infer column names and types **from how they are used** in the provided query:
          - SUM/AVG/arith → numeric (e.g., INT, DECIMAL(38,12))
          - date/time functions → DATE or TIMESTAMP
          - else → STRING
      - If a name contains dots, do not split into parts, do not change the name, do not wrap the name inside backticks.
      - Return **only** raw `%%##create_or_replace##%% %%##temp_type##%% ...;` statements, one per table, each ending with a semicolon. **No code fences, no commentary.**
      - If you’re unsure of a column, make your best guess and keep the set minimal.
      - When generating sample values, always place the literal first, and if you need to apply a datatype, use CAST(literal AS TYPE).
      - Do not write DECIMAL(10,2) ''100.00''. Instead, write either 100.00 (for a numeric) or CAST(''100.00'' AS DECIMAL(10,2)).
      - For strings, use single quotes ''value''.
      - For timestamps, use functions like CURRENT_TIMESTAMP() or CAST(''2024-01-01'' AS TIMESTAMP).
      - Ensure the generated DDL is valid Databricks SQL.

      INPUT QUERY:
        --- START ---
        %%##databricks_sql##%%
        --- END ---'
      """
    return prompt


def prompt_to_validate_converted_sql_with_ai():
    prompt = f"""
      'You are a Databricks SQL syntax validator. Analyze the following SQL query.
      Your task is to determine if it is valid and compatible with Databricks.

      1.  Start your analysis with a one-line summary: "Validation Result: [SUCCESS]" or "Validation Result: [FAILURE]".
      2.  Then, provide an explanation similar to what an `EXPLAIN` command would output.
      3.  Describe the logical plan.
      4.  Explicitly point out any potential syntax errors, incorrect function usage, or compatibility issues with Databricks SQL.
      5.  If the query is valid, state that clearly.
      6.  Sample output format: "Validation Result: [SUCCESS] Explanation: ..."

      --- START OF DATABRICKS SQL TO VALIDATE ---'
      || databricks_sql ||
      '--- END OF DATABRICKS SQL TO VALIDATE ---'
      """
    return prompt
