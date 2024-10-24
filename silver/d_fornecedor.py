# Databricks notebook source
# MAGIC %sql
# MAGIC USE SCHEMA industrial_analytics

# COMMAND ----------

df = spark.sql(
    f""" --Utilizar 3 aspas duplas para não dar problema com quebras de linha
    SELECT
        *
    FROM industrial_analytics.bronze_d_fornecedor
    """
)

df.write.mode("overwrite").format("delta").saveAsTable("silver_d_fornecedor")
