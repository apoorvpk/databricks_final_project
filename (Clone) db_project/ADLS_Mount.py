# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "24c8d57d-7d35-48e0-8cd2-38312cc43367",
          "fs.azure.account.oauth2.client.secret": "Hoa8Q~lHHxG_tRQ6~PUM7ALjoR9g4lHc~f.J6a.5",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/06b70e43-66d7-413a-9cff-16b738f3edd9/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@adlsdevkadevraw.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/bronze"
