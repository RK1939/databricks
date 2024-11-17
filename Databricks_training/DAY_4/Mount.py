# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://raw@rkadlsstorage.blob.core.windows.net",
  mount_point = "/mnt/rkadlsstorage/raw",
  extra_configs = {"fs.azure.account.key.rkadlsstorage.blob.core.windows.net": "key"}
)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/rkadlsstorage/raw/project/
