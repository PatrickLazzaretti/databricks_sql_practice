# Databricks notebook source
# MAGIC %sql
# MAGIC USE SCHEMA industrial_analytics

# COMMAND ----------

df = spark.sql(
    """ --Utilizar 3 aspas duplas para não dar problema com quebras de linha
    SELECT
        cod_documento_material
        ,cod_item_documento_material
        ,cod_deposito
        ,cod_material
        ,cod_centro
        ,cod_tipo_movimento
        ,ds_bloqueio_qualidade
        ,ds_texto_cabecalho_documento
        ,nm_usuario
        ,TO_DATE(dt_lancamento_documento, 'dd/MM/yyyy') AS dt_lancamento
        ,CAST(REPLACE(nr_quantidade, ',', '.') AS FLOAT) AS nr_quantidade
        ,CAST(REPLACE(nr_preco_medio_unitario, ',', '.') AS FLOAT) AS nr_preco_medio_unitario
        ,CAST(REPLACE(vl_montante_moeda_interna, ',', '.') AS FLOAT) AS vl_montante_moeda_interna
    FROM industrial_analytics.bronze_f_bloqueios
    """
)

df.write.mode("overwrite").format("delta").saveAsTable("silver_f_bloqueios")
