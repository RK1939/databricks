-- Databricks notebook source
create view michelin.circuits_vw as (select country , count(country) as count  from rk_databricks.michelin.circuits group by country order by count );

-- COMMAND ----------

create or replace temp view country_temp as (select country ,location, count(country) as count  from rk_databricks.michelin.circuits group by country,location  order by count );

-- COMMAND ----------

show views;

-- COMMAND ----------

create or replace global temp view country_temp as (select country ,name,location, count(country) as count  from rk_databricks.michelin.circuits group by country,location,name  order by count );

-- COMMAND ----------

show views in global_temp;
