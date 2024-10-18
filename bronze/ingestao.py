# Databricks notebook source
# DBTITLE 1,SCHEMA
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS industrial_analytics;
# MAGIC USE industrial_analytics;

# COMMAND ----------

# DBTITLE 1,SETUP
#import delta
#import sys
#Limitação do Databricks Community, sem Unity Catalog
database = 'industrial_analytics'
tables = ['bronze_d_calendario', 'bronze_d_fornecedor','bronze_d_materiais', 'bronze_f_bloqueios', 'bronze_f_delivery_amount', 'bronze_f_estoques', 'bronze_f_production_orders_components', 'bronze_f_production_orders_header', 'bronze_f_production_orders_operations', 'bronze_f_scheduling_agreement_schedule_lines', 'bronze_f_stock_adjusts']

# COMMAND ----------

# DBTITLE 1,EXECUÇÃO
for table in tables:
    if not table_exists(database=database, table=table, spark=spark):
        print(f"Criando a tabela {table}")
        ingestao = IngestaoFullBronze(table=table, spark=spark)
        ingestao.auto()
        print("ok")
