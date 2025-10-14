# Databricks notebook source
# MAGIC %pip install databricks-sdk==0.61.0
# MAGIC %restart_python

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import App, AppDeployment
from databricks.sdk.service.workspace import WorkspaceObjectAccessControlRequest, WorkspaceObjectPermissionLevel

w = WorkspaceClient()

# COMMAND ----------

name = 'databricks-migrator'
description = 'An application for converting legacy SQL to Databricks SQL using LLM.'
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
default_source_code_path = '/Workspace' + notebook_path.rsplit('/', 1)[0] if notebook_path else None

app = w.apps.create_and_wait(
    app=App(
        name=name,
        description=description,
        default_source_code_path=default_source_code_path,
    )
)
print(app)

# COMMAND ----------

app_deployment = w.apps.deploy_and_wait(
    app_name=name,
    app_deployment=AppDeployment(
        source_code_path=default_source_code_path
    )
)
print(app_deployment)

# COMMAND ----------

obj_info = w.workspace.get_status(default_source_code_path)

update_permissions = w.workspace.update_permissions(
    workspace_object_type='directories',
    workspace_object_id=obj_info.object_id,
    access_control_list=[
        WorkspaceObjectAccessControlRequest(
            service_principal_name=app.service_principal_client_id,
            permission_level=WorkspaceObjectPermissionLevel.CAN_READ
        )
    ]
)
print(update_permissions)

# COMMAND ----------

print(f'Application deployed successfully. URL: {app.url}')
print(f'Service principal of the app: Name - {app.service_principal_name}, ID - {app.service_principal_client_id}')
