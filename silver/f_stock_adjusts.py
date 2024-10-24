# Databricks notebook source
# MAGIC %sql
# MAGIC USE SCHEMA industrial_analytics

# COMMAND ----------

df = spark.sql(
    """ --Utilizar 3 aspas duplas para não dar problema com quebras de linha
    SELECT
        cod_documento_material
        ,cod_item_documento_material
        ,cod_ano_documento_material
        ,cod_material
        ,cod_centro
        ,cod_item_ordem_cliente_estoque_avaliado_cliente
        ,cod_ordem_cliente_estoque_avaliado_cliente
        ,cod_deposito
        ,cod_tipo_movimento
        ,ds_ajuste
        ,CAST(REPLACE(nr_quantidade, ',', '.') AS FLOAT) AS nr_quantidade
        ,CAST(REPLACE(vl_montante_moeda_interna, ',', '.') AS FLOAT) AS vl_montante_moeda_interna
        ,CAST(REPLACE(nr_preco_medio_unitario, ',', '.') AS FLOAT) AS nr_preco_medio_unitario
        ,TO_DATE(dt_lancamento, 'dd/MM/yyyy') AS dt_lancamento
    FROM industrial_analytics.bronze_f_stock_adjusts
    """
)

df.write.mode("overwrite").format("delta").saveAsTable("silver_f_stock_adjusts")
