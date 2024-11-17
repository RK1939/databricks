# Databricks notebook source
# MAGIC %sql
# MAGIC create table michelin.emp(id int, name string, age int);
# MAGIC insert into table michelin.emp values (1,'a',30);
# MAGIC insert into table michelin.emp values (2,'b',29),(3,'c',16),(4,'d',11);
