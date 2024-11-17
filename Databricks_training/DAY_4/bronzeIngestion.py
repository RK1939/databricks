# Databricks notebook source
# MAGIC %run /Workspace/Databricks_training/DAY_4/include

# COMMAND ----------

input_path

# COMMAND ----------

df_demo=spark.read.csv(f"{input_path}sales(in).csv",header=True,inferSchema=True)

# COMMAND ----------

df_demo_final=add_ingestion_col(df_demo)

# COMMAND ----------

dbutils.widgets.text("source","")
source_file_name=dbutils.widgets.get("source")

# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}{source_file_name}",header=True,inferSchema=True)
df_sales_final=add_ingestion_col(df_sales)
df_sales_final.write.mode("overwrite").saveAsTable("bronze.customers")
 

# COMMAND ----------

df_sales_final.display()

# COMMAND ----------

df=spark.table("rk_databricks.bronze.sales")
df1=df.dropDuplicates().dropna()
df1.write.mode("overwrite").saveAsTable("rk_databricks.silver.sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rk_databricks.silver.sales order by order_id 
