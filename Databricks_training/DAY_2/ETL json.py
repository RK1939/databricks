# Databricks notebook source
# MAGIC %run /Workspace/Databricks_training/includes

# COMMAND ----------

df=spark.read.json("/Volumes/rk_databricks/michelin/raw/constructors.json")

# COMMAND ----------

df1=add_ingestion_col(df)

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("constructor")
