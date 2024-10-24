# Databricks notebook source
df_classificacao_operacao = spark.sql(
    f"""
    WITH cod_operacao_em_processo AS (
    SELECT
        cod_ordem
        ,MIN(CAST(cod_operacao AS INT)) AS cod_operacao
    FROM industrial_analytics.bronze_f_production_orders_operations
    WHERE 
        ds_status_operacao NOT LIKE '%ELIM%' 
        AND dt_data_fim_real_operacao IS NULL
    GROUP BY cod_ordem
    )
    SELECT
    poo.cod_ordem
    ,poo.cod_operacao
    ,poo.ds_status_operacao
    ,poo.dt_data_fim_real_operacao
    ,CASE
        WHEN poo.ds_status_operacao LIKE '%ELIM%'
            THEN 'Encerrado'
        WHEN poo.dt_data_fim_real_operacao IS NOT NULL
            THEN 'Finalizado'
        WHEN coep.cod_operacao IS NOT NULL
            THEN 'Em Processo'
        ELSE 'Não Iniciado'
    END AS ds_classificacao_operacao
    FROM industrial_analytics.bronze_f_production_orders_operations poo
    LEFT JOIN cod_operacao_em_processo coep
    ON poo.cod_ordem = coep.cod_ordem
    AND poo.cod_operacao = coep.cod_operacao
    """
)

df_classificacao_operacao.createOrReplaceTempView('ds_classificacao_operacao')

# COMMAND ----------

dt_referencia = '2024/08/08'
df = spark.sql(
    f""" --Utilizar 3 aspas duplas para não dar problema com quebras de linha
    SELECT
        poo.cod_ordem
        ,poo.cod_centro
        ,poo.cod_operacao
        ,poo.cod_centro_trabalho
        ,poo.cod_chave_controle
        ,poo.ds_operacao
        ,poo.ds_proc_group
        ,poo.ds_status_operacao
        ,CASE
           WHEN TO_DATE(dt_ultima_data_fim_operacao, 'dd/MM/yyyy') < TO_DATE('{dt_referencia}', 'yyyy/MM/dd')
                THEN 'Atraso'
            ELSE 'Pendente' 
        END AS ds_status_finalizacao_operacao
        ,co.ds_classificacao_operacao
        ,CAST(REPLACE(poo.nr_empregados, ',', '.') AS FLOAT) AS nr_empregados
        ,CAST(REPLACE(poo.vl_standard1, ',', '.') AS FLOAT) AS vl_standard1
        ,CAST(REPLACE(poo.vl_standard2, ',', '.') AS FLOAT) AS vl_standard2
        ,poo.cod_unidade_medida_valor_standard_01
        ,poo.cod_unidade_medida_valor_standard_02
        ,TO_DATE(poo.dt_data_inicio_programado_operacao, 'dd/MM/yyyy') AS dt_data_inicio_programado_operacao
        ,TO_DATE(poo.dt_ultima_data_fim_operacao, 'dd/MM/yyyy') AS dt_ultima_data_fim_operacao
        ,TO_DATE(poo.dt_data_fim_real_operacao, 'dd/MM/yyyy') AS dt_data_fim_real_operacao
        ,TO_TIMESTAMP(poo.dt_inicio_programado_operacao, 'dd/MM/yyyy HH:mm') AS dt_inicio_programado_operacao
        ,TO_TIMESTAMP(poo.dt_inicio_real_operacao, 'dd/MM/yyyy HH:mm') AS dt_inicio_real_operacao
    FROM industrial_analytics.bronze_f_production_orders_operations poo
    LEFT JOIN ds_classificacao_operacao co
        ON poo.cod_ordem = co.cod_ordem
        AND poo.cod_operacao = co.cod_operacao 
    """
)

df.write.mode("overwrite").format("delta").saveAsTable("silver_f_production_orders_operations")
