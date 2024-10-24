# Databricks notebook source
# MAGIC %sql
# MAGIC USE SCHEMA industrial_analytics

# COMMAND ----------

dt_referencia = '2024/08/08'
df = spark.sql(
    f""" --Utilizar 3 aspas para não dar problema com quebras de linha
    SELECT
        cod_ordem
        ,cod_centro
        ,cod_ordem_cliente
        ,cod_item_ordem_cliente
        ,cod_tipo_ordem
        ,cod_serie
        ,cod_responsavel_controle_producao
        ,cod_material
        ,cod_cliente
        ,cod_numero_objeto
        ,cod_ordem_principal
        ,CASE 
            WHEN TO_DATE(dt_data_fim_programado_ordem, 'dd/MM/yyyy') < TO_DATE('{dt_referencia}', 'yyyy/MM/dd')
                THEN 'Atraso' 
            ELSE 'Pendente'
        END AS ds_status_finalizacao_ordem
        ,TO_DATE(dt_data_inicio_programado_ordem, 'dd/MM/yyyy') AS dt_data_inicio_programado_ordem
        ,TO_DATE(dt_data_inicio_real_ordem, 'dd/MM/yyyy') AS dt_data_inicio_real_ordem
        ,TO_DATE(dt_data_fim_programado_ordem, 'dd/MM/yyyy') AS dt_data_fim_programado_ordem
        ,TO_DATE(dt_data_fim_real_ordem, 'dd/MM/yyyy') AS dt_data_fim_real_ordem
        ,TO_DATE(dt_data_fim_real_ordem_confirmacao, 'dd/MM/yyyy') AS dt_data_fim_real_ordem_confirmacao
        ,TO_DATE(dt_data_entrada_ordem, 'dd/MM/yyyy') AS dt_data_entrada_ordem
        ,TO_DATE(dt_data_modificacao_ordem, 'dd/MM/yyyy') AS dt_data_modificacao_ordem
        ,TO_TIMESTAMP(dt_fim_real_ordem, 'dd/MM/yyyy HH:mm') AS dt_fim_real_ordem
        ,TO_TIMESTAMP(dt_inicio_programado_ordem, 'dd/MM/yyyy HH:mm') AS dt_inicio_programado_ordem
        ,CAST(REPLACE(nr_qtd_teorica, ',', '.') AS FLOAT) AS nr_quantidade_teorica
        ,CAST(REPLACE(nr_quantidade_confirmada, ',', '.') AS FLOAT) AS nr_quantidade_confirmada
    FROM industrial_analytics.bronze_f_production_orders_header
    """
)
df.write.mode("overwrite").format("delta").saveAsTable("silver_f_production_orders_header")
