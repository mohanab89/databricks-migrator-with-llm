import json
import os
import traceback
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from urllib.parse import quote

# Databricks SDK
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service.jobs import (
    RunLifeCycleState,
    RunResultState,
)

from src.utils import common_helper, interactive_helper, batch_helper

# ---------------------------------
# Page / Theming
# ---------------------------------
st.set_page_config(
    page_title="BrickMod ➜ Databricks AI Migrate to Modernize",
    page_icon="images/brickmod.png",
    layout="wide"
)

# ---------------------------------
# Logo + Title side by side
# ---------------------------------
c1, c2 = st.columns([15, 85], vertical_alignment="center")
with c1:
    st.image("images/brickmod.png", width=215)
with c2:
    st.markdown("## **BrickMod ➜ Databricks AI: Migrate & Modernize**")
    st.caption("Accelerate SQL & Stored Procedure Migration with AI")

# ---------------------------------
# Databricks clients
# ---------------------------------
w = WorkspaceClient()
cfg = Config()


@st.cache_data(ttl=900, show_spinner=True)
def get_serving_endpoints():
    return common_helper.get_serving_endpoints(w)


@st.cache_data(ttl=300, show_spinner=True)
def get_warehouses():
    """
    Get SQL warehouses with a 5-minute cache.
    Cache can be cleared manually via the refresh button.
    """
    return common_helper.get_sql_warehouses(w)


@st.cache_data(ttl=3600, show_spinner=False)
def get_notebook_path(notebook_name: str):
    """
    Get notebook path with 1-hour cache to avoid repeated API calls.
    """
    return common_helper.get_notebook_path(w, 'databricks-migrator', notebook_name)


@st.cache_data(ttl=900, show_spinner=False)
def get_sorted_models():
    """
    Get sorted model list with 15-minute cache.
    """
    return common_helper.get_sorted_models(w)


# ---------------------------------
# Global session state defaults
# ---------------------------------
ss = st.session_state

# Interactive defaults
ss.setdefault("databricks_sql", "")
ss.setdefault("validation_result", None)

# Batch defaults
ss.setdefault("run_id", None)
ss.setdefault("job_id", None)
ss.setdefault("job_name", None)
ss.setdefault("run_page_url", None)
ss.setdefault("job_status", "Not Started")
ss.setdefault("final_results_df", None)
ss.setdefault("results_written_path", None)
ss.setdefault("job_error_message", None)
ss.setdefault("nb_path_batch", get_notebook_path('batch_converter_notebook'))

# Reconcile tab state
ss.setdefault("recon_nb_path", get_notebook_path('schema_reconciler_notebook'))
ss.setdefault("recon_run_id", None)
ss.setdefault("recon_job_id", None)
ss.setdefault("recon_job_name", None)
ss.setdefault("recon_run_page_url", None)
ss.setdefault("recon_job_status", "Not Started")
ss.setdefault("recon_results_df", None)
ss.setdefault("recon_error", None)


# ---------------------------------
# Tabs
# ---------------------------------
st.info("👇 Use the tabs below to switch between **Interactive**, **Batch**, and **Reconciliation** modes.")
interactive_tab, batch_tab, recon_tab = st.tabs(["🧪 Interactive", "📦 Batch Jobs", "🔍 Reconcile Tables"])

# =============================================================
# 🧪 INTERACTIVE TAB
# =============================================================
with interactive_tab:
    st.subheader("Interactive Conversion")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.selectbox(
            "LLM Model",
            get_sorted_models(),
            index=0,
            key="llm_model_interactive",
            help="Choose the language model to use for the conversion. Claude and GPT models are recommended for code migration."
        )
    with col2:
        # Warehouse selection with refresh capability
        wh_col, refresh_col = st.columns([4, 1])
        
        with wh_col:
            try:
                warehouses = get_warehouses()
            except Exception as e:
                st.error(f"Failed to fetch SQL warehouses: {e}")
                warehouses = {}
            
            if not warehouses:
                st.warning("⚠️ No warehouses accessible. Grant the service principal access, then click refresh.")
            
            wh_name = st.selectbox(
                "SQL Warehouse",
                options=list(warehouses.keys()) or [""],
                key="warehouse_interactive",
                help="Warehouse that runs conversion/validation queries."
            )
            if wh_name:
                ss.warehouse_id = warehouses.get(wh_name)
        
        with refresh_col:
            st.markdown("<div style='height: 28px;'></div>", unsafe_allow_html=True)  # Align with selectbox
            if st.button("🔄", key="refresh_warehouses", help="Refresh warehouse list"):
                get_warehouses.clear()
                st.rerun()
    with col3:
        st.selectbox(
            "Source Dialect",
            common_helper.dialect_options,
            index=0,
            key="dialect_interactive",
            help="Select the source SQL dialect of the input queries."
            )

    st.text_area(
        "Custom LLM Prompts (optional)",
        key="llm_prompts_interactive",
        placeholder="- LATERAL/FLATTEN ➝ explode()/inline()\n- ARRAY_AGG ➝ collect_list/collect_set\n- TO_TIMESTAMP_LTZ ➝ TO_TIMESTAMP",
        help="An optional space to provide specific rules to guide the LLM."
    )

    # Service Principal Access Note - Made prominent
    sp_id = os.getenv('DATABRICKS_CLIENT_ID')
    st.warning(f"⚠️ **IMPORTANT:** The service principal must have `CAN USE` permission on the selected SQL Warehouse.\n\n**Service Principal ID:** `{sp_id}`")

    st.divider()

    in_col, out_col = st.columns(2)

    with in_col:
        st.markdown("**Input SQL**")
        dialect_input = st.text_area(
            "Enter SQL",
            height=360,
            key="dialect_input_interactive",
            placeholder=(
                "SELECT\n\tuser_id,\n\tMAX(order_date) AS last_order_date\n"
                "FROM my_db.my_schema.orders\nGROUP BY 1;"
            ),
        )

        if st.button("Convert Query", type="primary", use_container_width=True, key="btn_convert_interactive"):
            if not dialect_input.strip() or not ss.warehouse_id or not ss.dialect_interactive:
                st.warning("Please enter SQL and select a warehouse and source dialect.")
            else:
                with st.spinner("Converting with AI…"):
                    try:
                        escaped_sql = dialect_input.replace("'", "''")
                        model_full = common_helper.get_model_full_name(ss.llm_model_interactive, w)
                        q = f"""
                        SELECT ai_query('{model_full}', {interactive_helper.prompt_to_convert_sql_with_ai_interactive(ss.dialect_interactive, escaped_sql, ss.llm_prompts_interactive, None)},
                             modelParameters => named_struct(
                                {common_helper.get_model_params(model_full)}
                                )) AS databricks_sql
                        """
                        df = interactive_helper.execute_sql(cfg, q, ss.warehouse_id)
                        if not df.empty:
                            ss.databricks_sql = df.iloc[0]["databricks_sql"]
                            ss.validation_result = None
                        else:
                            ss.databricks_sql = "Conversion failed: empty result."
                    except Exception:
                        st.error("Conversion failed.")
                        with st.expander("Details"):
                            st.code(traceback.format_exc())
                    # finally:
                    #     st.rerun()

    with out_col:
        st.markdown("**Databricks SQL (output)**")
        st.code(ss.databricks_sql or "", language="sql", line_numbers=True)

        if ss.databricks_sql:
            if st.button("Validate Result", use_container_width=True, key="btn_validate_interactive"):
                with st.spinner("Running EXPLAIN…"):
                    ss.validation_result = interactive_helper.validate_query(ss.databricks_sql, ss.llm_model_interactive, cfg, ss.warehouse_id)
                    st.rerun()

        if ss.validation_result:
            if ss.validation_result["valid"]:
                st.success(f"✅ {ss.validation_result['reason']}")
            else:
                st.error(f"❌ {ss.validation_result['reason']}")
                if st.button("Try to Fix", use_container_width=True, key="btn_fix_interactive"):
                    with st.spinner("Re-asking the LLM with the error context…"):
                        try:
                            df = interactive_helper.regenerate_with_err_context(ss.validation_result, ss.llm_model_interactive, ss.dialect_interactive, ss.llm_prompts_interactive, cfg, ss.warehouse_id, ss.databricks_sql, w)
                            if not df.empty:
                                ss.databricks_sql = df.iloc[0]['databricks_sql']
                                ss.validation_result = None
                                st.rerun()
                        except Exception:
                            st.error("Fix attempt failed.")
                            with st.expander("Details"):
                                st.code(traceback.format_exc())
                        # finally:
                        #     st.rerun()

# =============================================================
# 📦 BATCH TAB
# =============================================================
with batch_tab:
    st.subheader("Batch Conversion Job")

    with st.form("batch_job_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.selectbox(
                "LLM Model",
                get_sorted_models(),
                index=0,
                key="llm_model_batch",
                help="Choose the language model to use for the conversion. Claude and GPT models are recommended for code migration."
            )
            st.selectbox(
                "Source Dialect",
                common_helper.dialect_options,
                index=0,
                key="dialect_batch",
                help="Select the source SQL dialect of the input queries."
            )
            st.selectbox(
                "Validation Strategy",
                ["No Validation", "Validate by running EXPLAIN"],
                index=1,
                key="validation_strategy_batch",
                help="Select the validation strategy to run on converted queries."
            )
            st.selectbox(
                "Max Retry Count",
                list(range(0, 11)),
                index=1,
                key="rerun_failures_batch",
                help="Select the maximum number of retries by the LLM on conversions that failed the validation step."
            )
        with c2:
            input_folder = st.text_input(
                "Input Folder",
                value="/Volumes/users/user_name/volume_name/converter_input/",
                key="input_folder_batch",
                help="The path (/Workspace or /Volumes) to the folder containing the legacy files to be converted."
            )
            output_folder = st.text_input(
                "Databricks Notebook Output Folder",
                value="/Workspaces/Users/user_name/databricks-migrator-with-llm/converter_output/",
                key="output_folder_batch",
                help="An optional path to save converted queries as a (python/sql) notebook. If not provided, the conversion results will only be stored in the resultant table."
            )
            results_table = st.text_input(
                "Results Delta Table",
                value="main.default.dbx_converter_results",
                key="results_table_batch",
                help="The three-part name of the Delta table for logging all conversion results."
            )
        with c3:
            st.selectbox(
                "Output Notebook Language",
                common_helper.output_lang_options,
                index=0,
                key="output_language",
                help="Output notebook type."
            )
            st.selectbox(
                "Output Mode",
                common_helper.output_options,
                index=0,
                key="output_mode",
                help="The intended output type."
            )
            with st.expander("Advanced Settings", expanded=True):
                st.text_input(
                    "Notebook Path",
                    key="nb_path_batch",
                    help="The full path to the conversion notebook."
                )

        st.text_area(
            "Custom LLM Prompts (optional)",
            key="llm_prompts_batch",
            placeholder="- LATERAL/FLATTEN ➝ explode()/inline()\n- ARRAY_AGG ➝ collect_list/collect_set\n- TO_TIMESTAMP_LTZ ➝ TO_TIMESTAMP",
            help="An optional space to provide specific rules to guide the LLM."
        )

        st.warning(f"⚠️ **IMPORTANT Prerequisites:**\n"
                   f"1. **Pre-create** the catalog and schema for the results table (e.g., `main.default`)\n"
                   f"2. Grant the service principal:\n"
                   f"   - `READ` permission on input folders\n"
                   f"   - Output folder permissions:\n"
                   f"     • **Workspace folder**: `MANAGE` permission (to create directory structures)\n"
                   f"     • **Volume folder**: `WRITE VOLUME` or higher permission\n"
                   f"   - `CREATE TABLE` permission on the results table schema  (auto-created on first run)\n\n"
                   f"**Service Principal ID:** `{sp_id}`")
        submitted = st.form_submit_button("Start Batch Conversion Job", type="primary", use_container_width=True)

    if submitted:
        input_folder = input_folder.strip()
        output_folder = output_folder.strip()
        if not all([ss.nb_path_batch, ss.llm_model_batch, input_folder, results_table, ss.dialect_batch]):
            st.warning("Please fill in all required configuration fields.")
        else:
            with st.spinner("Submitting job…"):
                try:
                    ss.update({"final_results_df": None, "results_written_path": None, "job_status": "SUBMITTING"})
                    ss.job_name = "Databricks Migrator Batch Conversion"
                    job_id, run_id = batch_helper.trigger_job(ss.dialect_batch, input_folder, output_folder, common_helper.get_model_full_name(ss.llm_model_batch, w), ss.validation_strategy_batch, results_table, ss.rerun_failures_batch, ss.llm_prompts_batch, w, ss.job_name, ss.nb_path_batch, ss.output_language, ss.output_mode)
                    ss.run_id = run_id
                    ss.job_id = job_id
                    st.rerun()
                except Exception:
                    st.error("Failed to submit job.")
                    with st.expander("Error Details"):
                        st.code(traceback.format_exc())

    st.markdown("---")
    st.header("Batch SQL Conversion Tracker")

    # Single tracker section only
    if ss.run_id:
        st_autorefresh(interval=15 * 1000, key="job_status_refresh")
        try:
            run_info = w.jobs.get_run(ss.run_id)
            ss.job_status = run_info.state.life_cycle_state
            ss.run_page_url = run_info.run_page_url

            with st.container(border=True):
                st.markdown(f"**Job Name:** `{ss.job_name}`")
                st.markdown(f"**Job ID:** `{ss.job_id}`")
                st.markdown(f"**Run ID:** `{ss.run_id}`")
                if ss.run_page_url:
                    st.markdown(f"**Job Run URL:** [Open in Databricks]({ss.run_page_url})")
                st.info(f"**Current Status:** {getattr(ss.job_status, 'value', ss.job_status)}")

            if ss.job_status == RunLifeCycleState.TERMINATED and run_info.state.result_state == RunResultState.SUCCESS:
                result_json = w.jobs.get_run_output(run_id=run_info.tasks[0].run_id)
                result_data = json.loads(result_json.notebook_output.result)
                results_df = pd.DataFrame(result_data)
                if output_folder.lower().startswith('/volumes/'):
                    parts = [p for p in output_folder.split("/") if p]
                    prefix = "/" + "/".join(parts[:4])
                    encoded = quote(output_folder, safe="")
                    results_written_path_url = f"https://{os.environ.get('DATABRICKS_HOST')}/explore/data{prefix}?volumePath={encoded}"
                elif output_folder.lower().startswith('/workspace/'):
                    results_written_path_url = f"https://{os.environ.get('DATABRICKS_HOST')}#workspace{quote(output_folder, safe='/:')}"
                else:
                    results_written_path_url = None

                ss.update({
                    "job_error_message": None,
                    "run_id": None,
                    "final_results_df": results_df[["input_file", "databricks_sql", "validation_result"]],
                    "results_written_path": results_written_path_url,
                    "completed_job_url": ss.run_page_url,  # Keep the job URL
                    "completed_job_id": ss.job_id,
                    "completed_run_id": run_info.run_id
                })
                st.rerun()
            elif ss.job_status == RunLifeCycleState.TERMINATED:
                ss.update({
                    "job_error_message": f"Job terminated: {run_info.state.result_state.value}. Reason: {run_info.state.state_message}",
                    "run_id": None,
                    "final_results_df": None,
                    "results_written_path": None,
                    "completed_job_url": ss.run_page_url,  # Keep the job URL for failed jobs
                    "completed_job_id": ss.job_id,
                    "completed_run_id": run_info.run_id
                })
                st.rerun()
            elif ss.job_status in [RunLifeCycleState.INTERNAL_ERROR, RunLifeCycleState.SKIPPED]:
                ss.update({
                    "job_error_message": f"Job failed with status: {ss.job_status.value}. Reason: {run_info.state.state_message}",
                    "run_id": None,
                    "final_results_df": None,
                    "results_written_path": None,
                    "completed_job_url": ss.run_page_url,  # Keep the job URL for failed jobs
                    "completed_job_id": ss.job_id,
                    "completed_run_id": run_info.run_id
                })
                st.rerun()
        except Exception:
            st.error("An error occurred while tracking the job.")
            with st.expander("Error Details"):
                st.code(traceback.format_exc())
            ss.run_id = None

    else:
        if ss.get("job_error_message"):
            st.header("❌ Job Failed")
            
            # Display job information even for failed jobs
            if ss.get("completed_job_url"):
                with st.container(border=True):
                    col1, col2 = st.columns(2)
                    with col1:
                        if ss.get("completed_job_id"):
                            st.markdown(f"**Job ID:** `{ss.completed_job_id}`")
                        if ss.get("completed_run_id"):
                            st.markdown(f"**Run ID:** `{ss.completed_run_id}`")
                    with col2:
                        st.markdown(f"**Job Run:** [Open in Databricks]({ss.completed_job_url}) 🔗")
            
            st.error(ss.job_error_message)
            if st.button("Start New Batch", key="btn_restart_batch"):
                ss.update({
                    "job_error_message": None, 
                    "final_results_df": None, 
                    "results_written_path": None,
                    "completed_job_url": None,
                    "completed_job_id": None,
                    "completed_run_id": None
                })
                st.rerun()
        elif ss.final_results_df is not None:
            st.header("✅ Results from Last Completed Job")
            
            # Display job information
            if ss.get("completed_job_url"):
                with st.container(border=True):
                    col1, col2 = st.columns(2)
                    with col1:
                        if ss.get("completed_job_id"):
                            st.markdown(f"**Job ID:** `{ss.completed_job_id}`")
                        if ss.get("completed_run_id"):
                            st.markdown(f"**Run ID:** `{ss.completed_run_id}`")
                    with col2:
                        st.markdown(f"**Job Run:** [Open in Databricks]({ss.completed_job_url}) 🔗")
            
            st.dataframe(ss.final_results_df, use_container_width=True)
            if ss.results_written_path is not None:
                st.markdown(
                    f"📂 Output has been written to: [**{output_folder}**]({ss.get('results_written_path')})",
                    unsafe_allow_html=True,
                )

            if st.button("Start New Batch", key="btn_new_batch"):
                ss.update({
                    "job_error_message": None, 
                    "final_results_df": None, 
                    "results_written_path": None,
                    "completed_job_url": None,
                    "completed_job_id": None,
                    "completed_run_id": None
                })
                st.rerun()
        else:
            st.info("ℹ️ Configure and start a new job in the Batch tab above.")

# =============================================================
# 🔍 Reconcile Tables TAB
# =============================================================
with recon_tab:
    st.subheader("Reconcile Tables")

    with st.form("reconcile_form", clear_on_submit=False):
        st.markdown("Provide **source** and **target** schemas in `catalog.schema` format.")
        c1, c2 = st.columns(2)
        with c1:
            st.selectbox(
                "LLM Model",
                get_sorted_models(),
                index=0,
                key="reconcile_llm_model",
                help="Choose the language model to use for reconciliation. Claude and GPT models are recommended."
            )
            recon_source_schema = st.text_input(
                "Source schema (catalog.schema)",
                value="src.default",
                key="recon_source_schema_input",
                help="The catalog.schema containing the source tables."
            )
            recon_target_schema = st.text_input(
                "Target schema (catalog.schema)",
                value="tgt.default",
                key="recon_target_schema_input",
                help="The catalog.schema containing the target tables."
            )
        with c2:
            recon_results_table = st.text_input(
                "Results Delta Table",
                value="main.default.reconcile_results",
                key="recon_results_table",
                help="The three-part name of the Delta table for logging all reconciliation results."
            )
            with st.expander("Advanced Settings", expanded=True):
                st.text_input(
                    "Notebook Path",
                    key="recon_nb_path",
                    help="The full path to the reconciliation notebook."
                )

        st.warning(f"⚠️ **IMPORTANT Prerequisites:**\n"
                   f"1. **Pre-create** the catalog and schema for the results table (e.g., `main.default`)\n"
                   f"2. Grant the service principal:\n"
                   f"   - `SELECT` permission on source and target schemas\n"
                   f"   - `CREATE TABLE` permission on the results table schema  (auto-created on first run)\n\n"
                   f"**Service Principal ID:** `{sp_id}`")
        reconcile_submitted = st.form_submit_button("Start Reconciliation Job", type="primary", use_container_width=True)

    if reconcile_submitted:
        if not all([ss.recon_nb_path, ss.reconcile_llm_model, recon_source_schema, recon_target_schema, recon_results_table]):
            st.warning("Please fill in all required configuration fields.")
        else:
            with st.spinner("Submitting job…"):
                try:
                    ss.update({"recon_results_df": None, "recon_job_status": "SUBMITTING"})
                    ss.recon_job_name = "Databricks Migrator Batch Reconciliation"
                    recon_job_id, recon_run_id = batch_helper.trigger_reconcile_job(common_helper.get_model_full_name(ss.reconcile_llm_model, w), recon_results_table, recon_source_schema, recon_target_schema, w, ss.recon_job_name, ss.recon_nb_path)
                    ss.recon_run_id = recon_run_id
                    ss.recon_job_id = recon_job_id
                    st.rerun()
                except Exception:
                    st.error("Failed to submit job.")
                    with st.expander("Error Details"):
                        st.code(traceback.format_exc())

    st.markdown("---")
    st.header("Batch Reconciliation Tracker")

    # Single tracker section only
    if ss.recon_run_id:
        st_autorefresh(interval=15 * 1000, key="recon_job_status_refresh")
        try:
            recon_run_info = w.jobs.get_run(ss.recon_run_id)
            ss.recon_job_status = recon_run_info.state.life_cycle_state
            ss.recon_run_page_url = recon_run_info.run_page_url

            with st.container(border=True):
                st.markdown(f"**Job Name:** `{ss.recon_job_name}`")
                st.markdown(f"**Job ID:** `{ss.recon_job_id}`")
                st.markdown(f"**Run ID:** `{ss.recon_run_id}`")
                if ss.recon_run_page_url:
                    st.markdown(f"**Job Run URL:** [Open in Databricks]({ss.recon_run_page_url})")
                st.info(f"**Current Status:** {getattr(ss.recon_job_status, 'value', ss.recon_job_status)}")

            if ss.recon_job_status == RunLifeCycleState.TERMINATED and recon_run_info.state.result_state == RunResultState.SUCCESS:
                recon_result_json = w.jobs.get_run_output(run_id=recon_run_info.tasks[0].run_id)
                recon_result_data = json.loads(recon_result_json.notebook_output.result)
                recon_results_df = pd.DataFrame(recon_result_data)
                ss.update({
                    "recon_error": None,
                    "recon_run_id": None,
                    "recon_results_df": recon_results_df[["table_name", "source_row_count", "target_row_count", "validation_report"]],
                    "recon_completed_job_url": ss.recon_run_page_url,  # Keep the job URL
                    "recon_completed_job_id": ss.recon_job_id,
                    "recon_completed_run_id": recon_run_info.run_id
                })
                st.rerun()
            elif ss.recon_job_status == RunLifeCycleState.TERMINATED:
                ss.update({
                    "recon_error": f"Job terminated: {recon_run_info.state.result_state.value}. Reason: {recon_run_info.state.state_message}",
                    "recon_run_id": None,
                    "recon_results_df": None,
                    "recon_completed_job_url": ss.recon_run_page_url,  # Keep the job URL for failed jobs
                    "recon_completed_job_id": ss.recon_job_id,
                    "recon_completed_run_id": recon_run_info.run_id
                })
                st.rerun()
            elif ss.recon_job_status in [RunLifeCycleState.INTERNAL_ERROR, RunLifeCycleState.SKIPPED]:
                ss.update({
                    "recon_error": f"Job failed with status: {ss.recon_job_status.value}. Reason: {recon_run_info.state.state_message}",
                    "recon_run_id": None,
                    "recon_results_df": None,
                    "recon_completed_job_url": ss.recon_run_page_url,  # Keep the job URL for failed jobs
                    "recon_completed_job_id": ss.recon_job_id,
                    "recon_completed_run_id": recon_run_info.run_id
                })
                st.rerun()
        except Exception:
            st.error("An error occurred while tracking the job.")
            with st.expander("Error Details"):
                st.code(traceback.format_exc())
            ss.recon_run_id = None

    else:
        if ss.get("recon_error"):
            st.header("❌ Job Failed")
            
            # Display job information even for failed jobs
            if ss.get("recon_completed_job_url"):
                with st.container(border=True):
                    col1, col2 = st.columns(2)
                    with col1:
                        if ss.get("recon_completed_job_id"):
                            st.markdown(f"**Job ID:** `{ss.recon_completed_job_id}`")
                        if ss.get("recon_completed_run_id"):
                            st.markdown(f"**Run ID:** `{ss.recon_completed_run_id}`")
                    with col2:
                        st.markdown(f"**Job Run:** [Open in Databricks]({ss.recon_completed_job_url}) 🔗")
            
            st.error(ss.recon_error)
            if st.button("Start New Batch", key="recon_btn_restart_batch"):
                ss.update({
                    "recon_error": None, 
                    "recon_results_df": None,
                    "recon_completed_job_url": None,
                    "recon_completed_job_id": None,
                    "recon_completed_run_id": None
                })
                st.rerun()
        elif ss.recon_results_df is not None:
            st.header("✅ Results from Last Completed Job")
            
            # Display job information
            if ss.get("recon_completed_job_url"):
                with st.container(border=True):
                    col1, col2 = st.columns(2)
                    with col1:
                        if ss.get("recon_completed_job_id"):
                            st.markdown(f"**Job ID:** `{ss.recon_completed_job_id}`")
                        if ss.get("recon_completed_run_id"):
                            st.markdown(f"**Run ID:** `{ss.recon_completed_run_id}`")
                    with col2:
                        st.markdown(f"**Job Run:** [Open in Databricks]({ss.recon_completed_job_url}) 🔗")
            
            st.dataframe(ss.recon_results_df, use_container_width=True)
            if st.button("Start New Batch", key="recon_btn_new_batch"):
                ss.update({
                    "recon_error": None, 
                    "recon_results_df": None,
                    "recon_completed_job_url": None,
                    "recon_completed_job_id": None,
                    "recon_completed_run_id": None
                })
                st.rerun()
        else:
            st.info("ℹ️ Configure and start a new job in the Reconcile tab above.")
