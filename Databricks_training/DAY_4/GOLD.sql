-- Databricks notebook source
create or replace view rk_databricks.gold.customer_total_sales as (select customer_id,customer_name, round(sum(total_amount)) as total_amount from rk_databricks.silver.sales_customer group by all )

-- COMMAND ----------

create or replace view rk_databricks.gold.total_sale as (select round(sum(total_amount)) as total_sales from rk_databricks.silver.sales)

-- COMMAND ----------

create or replace view rk_databricks.gold.total_sale as (select round(sum(total_amount)) as total_sales from rk_databricks.silver.sales)

-- COMMAND ----------

select * from rk_databricks.gold.customer_total_sales
