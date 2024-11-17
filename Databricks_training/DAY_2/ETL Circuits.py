# Databricks notebook source


# COMMAND ----------

df=spark.read.csv("/Volumes/rk_databricks/michelin/raw/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.select ("*").display()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col("circuitId").alias("circuit_id"),"name").display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").display()

# COMMAND ----------

df.withColumnsRenamed({"circuitId":"circuit_id","circuitRef":"circuit_ref"}).display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.withColumn("ingestion_date",current_date()).display()
 
df.withColumn("country",upper("country")).display()
 

# COMMAND ----------

df1=df\
.withColumnsRenamed({"circuitId":"circuit_id","circuitRef":"circuit_ref","lat":"latitude","lng":"longitude","alt":"altitude"})\
.withColumn("ingestion_date",current_timestamp())\
. drop("url")


# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("michelin.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rk_databricks.michelin.circuits
