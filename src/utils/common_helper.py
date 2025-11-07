from pathlib import Path
import yaml
import re
from typing import Literal

SqlKind = Literal["sql_script", "procedure", "json", "xml", "workflow"]
_COMMENT_BLOCK = re.compile(r"/\*.*?\*/", re.S)
_COMMENT_LINE = re.compile(r"(--|#)[^\n]*")

current_dir = Path(__file__).resolve().parent
yaml_path = current_dir.parent / "resources" / "conversion_prompts.yaml"
with open(yaml_path, "r") as f:
    conversion_prompts = yaml.safe_load(f)
yaml_path = current_dir.parent / "resources" / "common_prompts.yaml"
with open(yaml_path, "r") as f:
    common_prompts = yaml.safe_load(f)

dialect_options = list(conversion_prompts.keys())
output_options = list(common_prompts.keys())[1:]  # Removing interactive output type
output_lang = []
for key, value in common_prompts.items():
    if isinstance(value, dict):
        output_lang.extend(value.keys())
output_lang_options = sorted(list(set(output_lang)), key=len, reverse=True)

# model_dict = {
#     "Claude 3.7": "databricks-claude-3-7-sonnet",
#     "Claude 4": "databricks-claude-sonnet-4",
#     "Claude 4.5": "databricks-claude-sonnet-4-5",
#     "Llama": "databricks-llama-4-maverick",
#     "Gemma": "databricks-gemma-3-12b",
#     "GPT": "databricks-gpt-oss-120b"}
model_params = {
    "databricks-claude-3-7-sonnet": "'temperature', 0.0, 'max_tokens', 131072",
    "databricks-claude-sonnet-4": "'temperature', 0.0",
    "databricks-claude-sonnet-4-5": "'temperature', 0.0",
    "databricks-llama-4-maverick": "'temperature', 0.0",
    "databricks-gemma-3-12b": "'temperature', 0.0",
    "databricks-gpt-oss-120b": "'temperature', 0.0",
    "databricks-gpt-5": "'temperature', 1"
}

workflow_schema = "STRUCT<workflow_name: STRING, tasks: ARRAY<STRUCT<task_name: STRING, filename: STRING, depends_on: ARRAY<STRING>, notebook_language: STRING, parameters: ARRAY<STRUCT<name: STRING, default: STRING>>, content: STRING>>>"
workflow_name_prefix = 'auto_converter_'

IDENT_SUFFIX_FIX_SPARK = (
    r"(?i)(\bIDENTIFIER\s*\(\s*:[A-Za-z_]\w*\s*\|\|\s*)(?:\r?\n\s*)*\.(\w+)(\s*\))"
)
IDENT_SUFFIX_REPLACEMENT = r"$1'.$2'$3"

VIEW_TO_TABLE_REGEX = (
    r"(?i)\b(CREATE(?:\s+OR\s+REPLACE)?)\s+(?:TEMP|TEMPORARY)?\s*(VIEW|TABLE)\b"
)
VIEW_TO_TABLE_REPLACEMENT = r"$1 TABLE"


def get_serving_endpoints(w):
    model_dict = {}
    serving_endponts = w.serving_endpoints.list()
    for serving_endpont in serving_endponts:
        if serving_endpont.config:
            for served_entity in serving_endpont.config.served_entities:
                if served_entity.foundation_model is not None:
                    details = w.serving_endpoints.get(served_entity.name)
                    if details.task.endswith('chat'):
                        model_dict[served_entity.foundation_model.display_name] = served_entity.name
    return model_dict


def get_model_full_name(model_name, w):
    """
    Get the full model endpoint name from the display name.
    
    Args:
        model_name: The display name of the model
        w: WorkspaceClient instance
    
    Returns:
        The full endpoint name for the model
    """
    model_dict = get_serving_endpoints(w)
    return model_dict[model_name]


def get_sorted_models(w):
    """
    Get LLM models sorted by preference for code migration tasks.
    Claude and GPT models are shown first (best for code migration),
    followed by other models alphabetically.
    
    Args:
        w: WorkspaceClient instance
    
    Returns:
        List of model names sorted by preference
    """
    models = list(get_serving_endpoints(w).keys())
    
    # Define preference order for code migration
    preferred_models = []
    other_models = []
    
    for model in models:
        model_lower = model.lower()
        # Prioritize Claude and GPT models
        if 'claude' in model_lower or 'gpt' in model_lower:
            preferred_models.append(model)
        else:
            other_models.append(model)
    
    # Sort each group alphabetically
    preferred_models.sort()
    other_models.sort()
    
    # Return preferred models first, then others
    return preferred_models + other_models


def get_dynamic_partitions(row_count: int) -> int:
    if row_count == 0:
        return 1
    if row_count <= 10:
        return row_count
    elif row_count <= 50:
        return 20
    elif row_count <= 100:
        return 35
    elif row_count <= 1000:
        return 150
    else:
        base_partitions = 150
        # Add 10 partitions for every additional 1000 rows, but cap at 400 total
        additional_partitions = int((row_count - 1000) / 100) * 1
        return min(base_partitions + additional_partitions, 400)


def get_model_params(model_name):
    model_param = model_params[model_name] if model_name in model_params else ''
    return model_param


def get_sql_warehouses(w):
    """
    Get all SQL warehouses, with serverless warehouses sorted first and labeled.
    Returns a dict with formatted warehouse names as keys and IDs as values.
    """
    warehouses = w.warehouses.list()
    
    # Separate serverless and classic warehouses
    serverless = []
    classic = []
    
    for wh in warehouses:
        # Check if warehouse is serverless
        is_serverless = getattr(wh, "enable_serverless_compute", False) or \
                       getattr(wh, "warehouse_type", None) == "SERVERLESS"
        
        if is_serverless:
            serverless.append((f"⚡ {wh.name} (Serverless)", wh.id))
        else:
            classic.append((wh.name, wh.id))
    
    # Sort each list alphabetically by name
    serverless.sort(key=lambda x: x[0].lower())
    classic.sort(key=lambda x: x[0].lower())
    
    # Combine with serverless first
    all_warehouses = serverless + classic
    
    return {name: wh_id for name, wh_id in all_warehouses}


def get_notebook_path(w, app_name, notebook_name):
    try:
        app_details = w.apps.get(app_name)
        path = f"{app_details.default_source_code_path}/src/notebooks/{notebook_name}"
        if not path:
            raise Exception('Path not found')
    except Exception:
        print('Path not found, defaulting to fixed path.')
        path = f'/Workspace/Users/you@databricks.com/path/to/{notebook_name}'
    return path


IDENT_SUFFIX_FIX = re.compile(
    r"(\bIDENTIFIER\s*\(\s*:[a-zA-Z_]\w*\s*\|\|\s*)(?:\r?\n\s*)*\.(\w+)(\s*\))",
    re.IGNORECASE,
)


def fix_identifier_suffix_quotes(sql_text: str) -> str:
    return IDENT_SUFFIX_FIX.sub(r"\1'.\2'\3", sql_text)


def _strip_comments(s: str) -> str:
    s = _COMMENT_BLOCK.sub("", s)
    return _COMMENT_LINE.sub("", s)


# Dialect-agnostic starters (case-insensitive)
RE_PROCEDURE = re.compile(
    r"""
    \b(create|alter)\s+(or\s+replace\s+)?          # CREATE/ALTER [OR REPLACE]
    (procedure|proc)\b                               # procedure/proc
    """, re.I | re.X
)
RE_FUNCTION = re.compile(
    r"""\b(create|alter)\s+(or\s+replace\s+)?function\b""", re.I
)
RE_PACKAGE = re.compile(
    r"""\b(create|alter)\s+(or\s+replace\s+)?package\b""", re.I
)
RE_JSON = re.compile(
    r'^\s*(\{.*}|\[.*])\s*$', re.DOTALL
)
RE_XML = re.compile(
    r'^\s*<\?*xml?.*?>?.*<.+>.*$', re.DOTALL | re.IGNORECASE
)

# Hints that a body follows (various dialects)
RE_BODY_HINTS = re.compile(
    r"""
    \bbegin\b.*\bend\b|                # BEGIN ... END
    \bas\s*\$\$.*\$\$\s*|              # AS $$ ... $$ (Postgres/Redshift)
    \blanguage\s+(sql|plpgsql|java|javascript|python)\b| # language clause
    \bdelimiter\b|                     # MySQL delimiter
    \bgo\b                             # T-SQL batch
    """, re.I | re.S | re.X
)

# Anonymous procedural blocks (Oracle/PL/pgSQL)
RE_ANON_BLOCK = re.compile(r"\b(declare\s+.+\bbegin\b|begin\b).*\bend\s*;?", re.I | re.S)


def classify_sql(text: str, src_dialect: str, output_mode: str) -> SqlKind:
    core = _strip_comments(text).strip()

    # Look only at the first ~10KB for speed
    head = core[:10_000]

    # Normalize whitespace for simpler extraction
    head_single = re.sub(r"\s+", " ", head)

    # Stored procedure/function/package?
    for kind, pattern in (("procedure", RE_PROCEDURE),
                          ("function",  RE_FUNCTION),
                          ("package",   RE_PACKAGE)):
        m = pattern.search(head_single)
        if m:
            return "procedure"

    # Anonymous block?
    if RE_ANON_BLOCK.search(head):
        return "procedure"

    # Heuristic: if body hints appear early AND we see DECLARE/CURSOR/etc., treat as procedural anyway
    if RE_BODY_HINTS.search(head) and re.search(r"\b(declare|cursor|exception|handler)\b", head, re.I):
        return "procedure"

    if RE_JSON.search(head) or src_dialect.lower() == 'adf':
        return "json"

    if RE_XML.search(head):
        return "xml"

    if output_mode.lower() == 'workflow':
        return "workflow"

    # Otherwise: plain SQL script
    return "sql_script"
