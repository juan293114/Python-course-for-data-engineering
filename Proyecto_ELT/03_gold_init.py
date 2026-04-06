# Databricks notebook source
# MAGIC %sql
# MAGIC -- Eliminar schema Gold completo con todas sus tablas
# MAGIC DROP SCHEMA IF EXISTS workspace.gold CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC   COMMENT 'Capa Gold: modelo dimensional con dimensiones y tabla de hechos';