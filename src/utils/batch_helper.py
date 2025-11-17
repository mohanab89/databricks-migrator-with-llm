import base64
import json
import os
import re
from pathlib import Path
from typing import Iterable, Optional, List, Dict
import pandas as pd
from . import prompt_helper
from . import common_helper
from databricks.sdk.service.jobs import (
    Task,
    NotebookTask,
    JobAccessControlRequest,
    JobPermissionLevel,
    PerformanceTarget,
    TaskDependency
)
from databricks.sdk.service import workspace


def prompt_to_convert_sql_with_ai_batch(src_dialect, additional_prompts, is_rerun, output_mode, output_lang, row_type):
    prompt = prompt_helper.prompt_to_convert_sql_with_ai(src_dialect, additional_prompts, output_mode, output_lang, row_type)
    prompt = prompt.replace('%%##input_sql##%%', "' || input_sql || '")
    if is_rerun:
        prompt = prompt + """||
        
        'Last result generate by LLM that has errors:
        --- START OF PREVIOUSLY GENERATED OUTPUT --- '
        || databricks_sql ||
        '--- END OF SQL PREVIOUSLY GENERATED OUTPUT ---
        
        Received this error with last attempt. Please try to fix -- ' || concat_ws(';\n', validation_error)"""
    return prompt


def prompt_to_generate_ddls_with_ai_batch():
    prompt = prompt_helper.prompt_to_generate_ddls_with_ai()
    prompt = prompt.replace('%%##temp_type##%%', 'TEMP VIEW').replace('%%##databricks_sql##%%',
                                                                      "' || databricks_sql || '").replace(
        '%%##extracted_table_names##%%', "' || extracted_table_names || '").replace('%%##create_or_replace##%%',
                                                                                    'CREATE OR REPLACE')
    return prompt


def trigger_job(dialect_batch, input_folder, output_folder, llm_model_batch, validation_strategy_batch, results_table,
                rerun_failures_batch, llm_prompts_batch, w, job_name, nb_path_batch, output_language, output_mode):
    batch_params = {
        "SOURCE_DIALECT": dialect_batch,
        "INPUT_FOLDER": input_folder,
        "DATABRICKS_OUTPUT_FOLDER": output_folder,
        "MODEL_NAME": llm_model_batch,
        "VALIDATE_RESULTS": "No" if validation_strategy_batch == "No Validation" else "Yes",
        "RESULTS_TABLE_NAME": results_table,
        "MAX_RERUN_COUNT": rerun_failures_batch,
        "ADDITIONAL_PROMPTS": llm_prompts_batch,
        "OUTPUT_LANG": output_language,
        "OUTPUT_MODE": output_mode
    }

    # Upsert or create job by name
    jobs = list(w.jobs.list(name=job_name))
    job = next((j for j in jobs if j.settings.name == job_name), None)
    if not job:
        default_params = {
            "SOURCE_DIALECT": "",
            "INPUT_FOLDER": "/Volumes/users/user_name/volume_name/converter_input/",
            "DATABRICKS_OUTPUT_FOLDER": "/Workspaces/Users/user_name/databricks-migrator-with-llm/converter_output/",
            "MODEL_NAME": llm_model_batch,
            "VALIDATE_RESULTS": "Yes",
            "RESULTS_TABLE_NAME": "main.default.dbx_converter_results",
            "RERUN_FOR_FAILURES": "1",
            "ADDITIONAL_PROMPTS": "",
            "OUTPUT_LANG": "python",
            "OUTPUT_MODE": "notebook"
        }
        job = w.jobs.create(
            name=job_name,
            performance_target=PerformanceTarget.PERFORMANCE_OPTIMIZED,
            tasks=[
                Task(
                    notebook_task=NotebookTask(
                        notebook_path=nb_path_batch, base_parameters=default_params
                    ),
                    task_key="sql-conversion",
                    disable_auto_optimization=True,
                )
            ],
        )
        w.jobs.update_permissions(
            str(job.job_id),
            access_control_list=[
                JobAccessControlRequest(
                    group_name="users", permission_level=JobPermissionLevel.CAN_MANAGE
                )
            ],
        )

    run = w.jobs.run_now(job_id=job.job_id, job_parameters=batch_params)
    return job.job_id, run.run_id


def trigger_reconcile_job(llm_model_batch, results_table, src_schema, tgt_schema, w, job_name, nb_path_batch):
    batch_params = {
        "RECONCILE_RESULTS_TABLE_NAME": results_table,
        "MODEL_NAME": llm_model_batch,
        "strSRCDB": src_schema,
        "strTGTDB": tgt_schema
    }

    # Upsert or create job by name
    jobs = list(w.jobs.list(name=job_name))
    job = next((j for j in jobs if j.settings.name == job_name), None)
    if not job:
        default_params = {
            "RECONCILE_RESULTS_TABLE_NAME": "main.default.reconcile_results",
            "MODEL_NAME": llm_model_batch,
            "strSRCDB": "src.default",
            "strTGTDB": "tgt.default"
        }
        job = w.jobs.create(
            name=job_name,
            performance_target=PerformanceTarget.PERFORMANCE_OPTIMIZED,
            tasks=[
                Task(
                    notebook_task=NotebookTask(
                        notebook_path=nb_path_batch, base_parameters=default_params
                    ),
                    task_key="reconcile",
                    disable_auto_optimization=True,
                )
            ],
        )
        w.jobs.update_permissions(
            str(job.job_id),
            access_control_list=[
                JobAccessControlRequest(
                    group_name="users", permission_level=JobPermissionLevel.CAN_MANAGE
                )
            ],
        )

    run = w.jobs.run_now(job_id=job.job_id, job_parameters=batch_params)
    return job.job_id, run.run_id


def run_explain_on_query(spark, databricks_sql):
    explain_df = spark.sql("EXPLAIN " + databricks_sql)
    explain_lines = [r[0] for r in explain_df.collect()]

    # Get the first 2 lines (or fewer if not available), then join and truncate
    first_two_lines = " ".join(explain_lines[:2]).replace("\n", " ").strip()

    if first_two_lines.lower().startswith("error"):
        validation_result = f"Validation Result: [FAILURE] Explanation: {first_two_lines[:200]}"
    else:
        validation_result = "Validation Result: [SUCCESS] Explanation: EXPLAIN ran successfully on converted query with temporary datasets."
    return validation_result


def get_input_path_with_subdirs(input_folder):
    if os.path.isdir(input_folder):
        if input_folder.endswith('/'):
            return input_folder + "**"
        else:
            return input_folder + "/**"
    else:
        return input_folder


def mirror_structure(input_path: str, output_path: str):
    if os.path.isfile(input_path):
        # input_path is a single file → just replicate its parent folder structure
        rel_dir = os.path.dirname(input_path)
        rel_dir = os.path.relpath(rel_dir, start=os.path.commonpath([input_path]))
        target_dir = os.path.join(output_path, rel_dir)
        os.makedirs(target_dir, exist_ok=True)
        print(f"Created {target_dir} for file {os.path.basename(input_path)}")
    else:
        # input_path is a directory → walk through it
        for root, dirs, files in os.walk(input_path):
            rel_path = os.path.relpath(root, input_path)
            target_dir = os.path.join(output_path, rel_path)
            os.makedirs(target_dir, exist_ok=True)
            print(f"Created {target_dir}")


def write_output_files(input_path, input_folder, databricks_output_folder, output_type, output_mode, content_to_write, w):
    if input_path.startswith("dbfs:/Volumes/"):
        input_path = input_path.replace("dbfs:/", "/")
    elif input_path.startswith("file:"):
        input_path = input_path.replace("file:", "")
    input_direc = os.path.dirname(input_folder) if os.path.isfile(input_folder) or '.' in os.path.basename(
        input_folder) else input_folder
    file_path = input_path.replace(input_direc, databricks_output_folder)
    file_path_without_ext = os.path.splitext(file_path)[0]
    if file_path_without_ext.startswith('/Workspace/'):
        if output_mode.lower() == 'file':
            # For file mode, create plain .sql or .py files (not notebooks)
            if output_type.lower() == 'sql':
                file_path_without_ext = f'{file_path_without_ext}.sql'
            else:
                file_path_without_ext = f'{file_path_without_ext}.py'
            
            w.workspace.import_(
                content=base64.b64encode(content_to_write.encode()).decode(),
                format=workspace.ImportFormat.AUTO,  # AUTO format creates plain files
                overwrite=True,
                path=file_path_without_ext,
            )
        else:
            # For notebook/workflow mode, create notebook files
            if output_type.lower() == 'sql':
                language = workspace.Language.SQL
            else:
                language = workspace.Language.PYTHON
            
            w.workspace.import_(
                content=base64.b64encode(content_to_write.encode()).decode(),
                format=workspace.ImportFormat.SOURCE,
                language=language,
                overwrite=True,
                path=file_path_without_ext,
            )
    else:
        if output_type.lower() == 'sql':
            file_path = f'{file_path_without_ext}.sql'
        else:
            file_path = f'{file_path_without_ext}.py'
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content_to_write)


def extract_sql_from_python_str(input_str: str) -> list[str]:
    # This regex captures both triple-quoted and single-quoted strings
    # after spark.sql(
    text = re.sub(r"{.*?}", "PARAM", input_str)
    pattern = re.compile(r"""spark\.sql\(\s*(?:f|r|fr|rf)?(?P<quote>['"]{1,3})(.*?)(?P=quote)\s*\)""", re.S | re.I | re.X, )
    sql_blocks = [m.group(2).strip() for m in pattern.finditer(text)]
    return sql_blocks


def read_files_to_df(
        spark,
        input_path: str,
        include_ext: Optional[Iterable[str]] = None,  # e.g. [".sql", ".ddl", ".txt"]
        max_bytes_per_file: Optional[int] = 5_000_000,  # skip files larger than this; None = no limit
        encoding: str = "utf-8",
        errors: str = "replace",  # don't crash on bad bytes
):
    root = Path(input_path)
    if not root.exists():
        raise FileNotFoundError(f"Path not found: {root}")

    # Normalize extensions for comparison
    include_ext_norm = None
    if include_ext:
        include_ext_norm = {e.lower() if e.startswith(".") else f".{e.lower()}" for e in include_ext}

    records: List[Dict[str, str]] = []

    if root.is_file():
        candidates = [root]
        base = root.parent
    else:
        candidates = []
        base = root
        for dirpath, _dirs, files in os.walk(root):
            for fn in files:
                candidates.append(Path(dirpath) / fn)

    for f in candidates:
        ext = f.suffix.lower()
        if include_ext_norm and ext not in include_ext_norm:
            continue

        try:
            if max_bytes_per_file is not None and f.stat().st_size > max_bytes_per_file:
                # Skip very large files for safety
                continue

            # Read as text; errors="replace" prevents Unicode decode failures
            with open(f, "r", encoding=encoding, errors=errors) as fh:
                content = fh.read()

            rel_path = str(f.relative_to(base)) if f.is_absolute() else str(f)
            records.append({
                "path": str(f),
                "rel_path": rel_path.replace("\\", "/"),
                "name": f.name,
                "ext": ext,
                "content": content,
            })
        except Exception as e:
            print(f"Skipping {f}: {e}")
            pass
    return records


HEADERS = {"-- Databricks notebook source", "# Databricks notebook source"}
SEPS = {"-- COMMAND ----------", "# COMMAND ----------"}
HDR_DEF = "-- Databricks notebook source"
SEP_DEF = "-- COMMAND ----------"
GET_CALL_RE = re.compile(r'dbutils\.widgets\.get\(\s*["\']([^"\']+)["\']\s*\)', re.IGNORECASE)
TEXT_CALL_RE = re.compile(r'dbutils\.widgets\.text\(\s*["\']([^"\']+)["\']\s*,', re.IGNORECASE)


def build_file_content(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values("position", kind="mergesort")
    input_file = pdf["input_file"].iloc[0] if not pdf.empty else None

    seen_hdr, seen_sep = None, None
    wdef_seen, wget_seen = set(), set()
    wdef, wget, body = [], [], []

    def norm_cell(text: str) -> str:
        nonlocal seen_hdr, seen_sep
        out, last_sep = [], False
        for ln in text.splitlines():
            s = ln.strip()
            if not s:
                out.append(ln); continue
            if s in HEADERS:
                if seen_hdr is None: seen_hdr = s
                continue
            if s in SEPS:
                if seen_sep is None: seen_sep = s
                if last_sep:  # collapse consecutive seps
                    continue
                out.append(s); last_sep = True; continue
            # widget defs (CREATE WIDGET... or dbutils.widgets.text(...))
            m_txt = TEXT_CALL_RE.search(s)
            if s.upper().startswith("CREATE WIDGET ") or m_txt:
                key = (m_txt.group(1).casefold() if m_txt else s.lstrip().casefold())
                if key not in wdef_seen:
                    wdef_seen.add(key); wdef.append(ln)
                continue
            # widget gets anywhere in line: var = dbutils.widgets.get("name")
            m_get = GET_CALL_RE.search(s)
            if m_get:
                key = m_get.group(1).casefold()
                if key not in wget_seen:
                    wget_seen.add(key); wget.append(ln)
                continue
            out.append(ln); last_sep = False

        # strip leading/trailing blanks & seps
        while out and not out[0].strip(): out.pop(0)
        while out and out[0].strip() in SEPS: out.pop(0)
        while out and not out[-1].strip(): out.pop()
        if out and out[-1].strip() in SEPS: out.pop()
        return "\n".join(out).strip()

    for _, r in pdf.iterrows():
        txt = norm_cell((r["databricks_sql"] or ""))
        if txt: body.append(txt)

    header = seen_hdr or HDR_DEF
    sep    = seen_sep or SEP_DEF

    blocks = []
    if wdef: blocks.append("\n".join(wdef))     # cell 1: widget defs
    if wget: blocks.append("\n".join(wget))     # cell 2: widget gets
    blocks.extend(body)                         # rest of notebook

    def starts_with_sep(t: str) -> bool:
        for ln in t.splitlines():
            s = ln.strip()
            if not s: continue
            return s in SEPS
        return False

    def ends_with_sep(t: str) -> bool:
        for ln in reversed(t.rstrip().splitlines()):
            s = ln.strip()
            if not s: continue
            return s in SEPS
        return False

    def strip_leading_seps(text: str) -> str:
        lines = text.splitlines(); i = 0
        while i < len(lines) and not lines[i].strip(): i += 1
        while i < len(lines) and lines[i].strip() in SEPS: i += 1
        return "\n".join(lines[i:])

    out = header
    first_block = True
    for blk in blocks:
        if not blk: continue
        if first_block:
            # 🔑 Never allow a COMMAND cell immediately after the header
            blk = strip_leading_seps(blk)
            if not blk: continue
            out = f"{out}\n{blk.lstrip()}"
            first_block = False
            continue

        if ends_with_sep(out) and starts_with_sep(blk):
            blk = strip_leading_seps(blk)
            if not blk: continue
            out = f"{out.rstrip()}\n{blk.lstrip()}"
        elif ends_with_sep(out) or starts_with_sep(blk):
            out = f"{out.rstrip()}\n{blk.lstrip()}"
        else:
            out = f"{out.rstrip()}\n\n{sep}\n{blk.lstrip()}"

    return pd.DataFrame([[input_file, out.rstrip()]], columns=["input_file", "content_to_write"])


def create_folder_path_for_workflow(input_path, input_folder, databricks_output_folder):
    if input_path.startswith("dbfs:/Volumes/"):
        input_path = input_path.replace("dbfs:/", "/")
    elif input_path.startswith("file:"):
        input_path = input_path.replace("file:", "")
    file_path_without_ext = os.path.splitext(input_path)[0]
    input_direc = os.path.dirname(input_folder) if os.path.isfile(input_folder) or '.' in os.path.basename(
        input_folder) else input_folder
    file_path = file_path_without_ext.replace(input_direc, databricks_output_folder)
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    return file_path, file_path_without_ext


def create_workflow(json_content, input_path, input_folder, databricks_output_folder, output_lang, output_mode, w):
    workflow_json = json.loads(json_content)
    file_path, file_path_without_ext = create_folder_path_for_workflow(input_path, input_folder, databricks_output_folder)
    all_tasks = []
    for tasks in workflow_json["tasks"]:
        task_name = tasks["task_name"]
        task_dependencies = []
        if len(tasks['depends_on']) > 0:
            for each_depend in tasks['depends_on']:
                task_dependencies.append(TaskDependency(task_key=each_depend))

        file_name = f"{file_path_without_ext}{tasks['task_name']}" if file_path_without_ext.endswith('/') else f"{file_path_without_ext}/{tasks['task_name']}"
        pandas_df = pd.DataFrame(
            [[input_path, 0, tasks["content"]]],
            columns=["input_file", "position", "databricks_sql"]
        )
        out_pandas_df = build_file_content(pandas_df)
        out = str(out_pandas_df.loc[0, "content_to_write"])

        write_output_files(file_name, input_folder, databricks_output_folder, output_lang, output_mode, out, w)
        task = Task(task_key=task_name, notebook_task=NotebookTask(notebook_path=f"{file_path}/{task_name}", base_parameters=tasks['parameters']), depends_on=task_dependencies)
        all_tasks.append(task)

    workflow_name = common_helper.workflow_name_prefix + workflow_json["workflow_name"]
    job = w.jobs.create(
        name=workflow_name,
        tasks=all_tasks,
    )

    w.jobs.update_permissions(
        str(job.job_id),
        access_control_list=[
            JobAccessControlRequest(
                group_name="account users", permission_level=JobPermissionLevel.CAN_MANAGE
            )
        ],
    )
    print(f"Job created. ID: {job.job_id}; Name: {workflow_name}; For file: {input_path}")
    return job.job_id
