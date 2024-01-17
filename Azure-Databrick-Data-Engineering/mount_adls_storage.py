# Databricks notebook source
storage_account_name = "formula001datalake"
client_id            = dbutils.secrets.get(scope='formula0001', key='formula0001-app-client-id')
tenant_id            = dbutils.secrets.get(scope='formula0001', key='formula0001-app-tenant-id')
client_secret        = dbutils.secrets.get(scope='formula0001', key='formula0001-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount the containers using the `mount_adls`

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

# Mount 'raw' container
mount_adls("raw")

# COMMAND ----------

# Mount 'processed' container
mount_adls("processed")

# COMMAND ----------

# MAGIC %md
# MAGIC check the containers

# COMMAND ----------

dbutils.fs.mounts()
