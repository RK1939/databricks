# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

input_path= "dbfs:/mnt/rkadlsstorage/raw/project/"


# COMMAND ----------

def add_ingestion_col(df):
  df_final=df.withColumn("ingestion_date",current_timestamp())
  df_final2=df_final.withColumn("source_path",input_file_name())
  return df_final2

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog rk_databricks;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists  rk_databricks.bronze;
# MAGIC create schema if not exists  rk_databricks.silver;
# MAGIC create schema if not exists  rk_databricks.gold;
