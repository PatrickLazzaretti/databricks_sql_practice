# Databricks notebook source
# MAGIC %sql
# MAGIC USE SCHEMA industrial_analytics

# COMMAND ----------

df = spark.sql(
    """ --Utilizar 3 aspas duplas para não dar problema com quebras de linha
    SELECT
        cod_ordem
        ,cod_centro
        ,cod_reserva
        ,cod_item_reserva
        ,cod_tipo_ordem
        ,cod_material
        ,cod_origem
        ,cod_tipo_movimento
        ,cod_operacao
        ,cod_deposito
        ,cod_baixa_explosao
        ,bool_item_dummy
        ,bool_permitido_movimento
        ,bool_registro_final
        ,bool_item_eliminado
        ,bool_material_granel
        ,TO_DATE(dt_ultima_data_necessidade, 'dd/MM/yyyy') AS dt_ultima_data_necessidade
        ,TO_DATE(dt_necessidade, 'dd/MM/yyyy') AS dt_necessidade
        ,CAST(REPLACE(nr_quantidade_necessaria, ',', '.') AS FLOAT) AS nr_quantidade_necessaria
        ,CAST(REPLACE(nr_quantidade_retirada, ',', '.') AS FLOAT) AS nr_quantidade_retirada
        ,CAST(REPLACE(nr_qtd_pendente, ',', '.') AS FLOAT) AS nr_qtd_pendente
        ,CAST(REPLACE(nr_quantidade_falta, ',', '.') AS FLOAT) AS nr_quantidade_falta
        ,CAST(REPLACE(nr_quantidade_verific, ',', '.') AS FLOAT) AS nr_quantidade_verific
    FROM bronze_f_production_orders_components
    """
)

df.write.mode("overwrite").format("delta").saveAsTable("silver_f_production_orders_components")
