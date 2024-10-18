# Databricks notebook source
# MAGIC %sql
# MAGIC USE SCHEMA industrial_analytics

# COMMAND ----------

# MAGIC %run ./f_delivery_amount

# COMMAND ----------

# MAGIC %run ./f_estoques

# COMMAND ----------

# MAGIC %run ./f_stock_adjusts

# COMMAND ----------

# MAGIC %run ./f_production_orders_components

# COMMAND ----------

# MAGIC %run ./f_production_orders_header

# COMMAND ----------

# MAGIC %run ./f_scheduling_agreement_schedule_lines

# COMMAND ----------

# MAGIC %run ./f_production_orders_operations

# COMMAND ----------

# MAGIC %run ./f_bloqueios

# COMMAND ----------

# MAGIC %run ./d_calendario

# COMMAND ----------

# MAGIC %run ./d_fornecedor

# COMMAND ----------

# MAGIC %run ./d_materiais
