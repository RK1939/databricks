# Databricks notebook source
# MAGIC %run /Workspace/Databricks_training/DAY_4/include

# COMMAND ----------

dbutils.widgets.text("source","")
source_file_name=dbutils.widgets.get("source")

# COMMAND ----------

dbutils.widgets.text("catalog","")
catalog=dbutils.widgets.get("catalog")

# COMMAND ----------

dbutils.widgets.text("schema","")
schema=dbutils.widgets.get("schema")

# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}{source_file_name}",header=True,inferSchema=True)
df_sales_final=add_ingestion_col(df_sales)
df_sales_final.write.mode("overwrite").saveAsTable(f"{schema}.{source_file_name}")
