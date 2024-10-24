# Databricks notebook source
# MAGIC %sql
# MAGIC USE SCHEMA industrial_analytics

# COMMAND ----------

df = spark.sql(
    """ --Utilizar 3 aspas duplas para não dar problema com quebras de linha
    SELECT
        cod_doc_compras
        ,TRIM(LEADING '0' FROM cod_item_doc_compras) AS cod_item_doc_compras
        ,cod_doc_referencia
        ,cod_material
        ,cod_centro
        ,cod_fornecedor
        ,cod_incoterm
        ,ds_status_recebimento
        ,cod_recebimento
        ,CAST(REPLACE(nr_quantidade_remessa, ',', '.') AS FLOAT) AS nr_quantidade_remessa
        ,TO_DATE(dt_criacao, 'dd/MM/yyyy') AS dt_criacao
    FROM industrial_analytics.bronze_f_delivery_amount
    """
)

df.write.mode("overwrite").format("delta").saveAsTable("silver_f_delivery_amount")
