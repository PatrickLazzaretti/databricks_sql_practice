# Databricks notebook source
dt_referencia = '2024/08/08' #Dia de extração dos dados
df = spark.sql(
    f""" --Utilizar 3 aspas para não dar problema com quebras de linha
    WITH faltas AS ( --Tabela que mostra todos os materiais indisponíveis com necessidade atrasada ou dia do período de extração dos dados
        SELECT DISTINCT
            cod_material
            ,cod_centro
        FROM f_disponibilidade_materiais
        WHERE ds_disponibilidade = 'NOK'
        AND dt_necessidade <= TO_DATE('{dt_referencia}', 'yyyy/MM/dd') --Menor ou igual ao período da extração dos dados
    ),
    ajustes AS (
        SELECT
            cod_material
            ,cod_centro
            ,SUM(nr_quantidade) * -1 AS nr_quantidade -- multiplicar para deixar valor positivo
        FROM f_ajustes
        WHERE ds_ajuste = 'Negativo'
        AND dt_lancamento BETWEEN DATEADD(DAY, -14, TO_DATE('{dt_referencia}', 'yyyy/MM/dd')) AND TO_DATE('{dt_referencia}', 'yyyy/MM/dd') --últimos 14 dias
        GROUP BY cod_material, cod_centro
    ),
    bloqueios AS (
        SELECT
            cod_material
            ,cod_centro
            ,SUM(nr_quantidade) AS nr_quantidade --Se estiver > 0, houveram mais bloqueios que desbloqueios no período. Importante filtrar
        FROM f_bloqueios
        WHERE dt_lancamento BETWEEN DATEADD(DAY, -14, TO_DATE('{dt_referencia}', 'yyyy/MM/dd')) AND TO_DATE('{dt_referencia}', 'yyyy/MM/dd') --últimos 14 dias
        GROUP BY cod_material, cod_centro
    ),
    atraso_producao AS (
        SELECT
            cod_material
            ,cod_centro
            ,SUM(nr_quantidade_teorica) AS nr_quantidade
        FROM f_production_orders_header
        WHERE ds_status_finalizacao_ordem = 'Atraso'
        GROUP BY cod_material, cod_centro
    ),
    atraso_fornecedor AS (
        SELECT
            cod_material
            ,cod_centro
            ,SUM(nr_saldo_divisao) AS nr_quantidade
        FROM f_scheduling_Agreement_schedule_lines
        WHERE ds_status_remessa = 'Atraso'
        GROUP BY cod_material, cod_centro
    )
    SELECT
        faltas.cod_material
        ,faltas.cod_centro
        ,COALESCE(ajustes.nr_quantidade, 0) AS nr_ajustes
        ,COALESCE(bloqueios.nr_quantidade, 0) AS nr_bloqueios
        ,COALESCE(prod.nr_quantidade, 0) AS nr_atraso_producao
        ,COALESCE(forn.nr_quantidade, 0) AS nr_atraso_fornecedor
        ,COALESCE(ajustes.nr_quantidade, 0) + COALESCE(bloqueios.nr_quantidade, 0) + COALESCE(prod.nr_quantidade, 0) + COALESCE(forn.nr_quantidade, 0) AS nr_total_disturbios 
    FROM faltas
    LEFT JOIN ajustes
        ON faltas.cod_material = ajustes.cod_material
        AND faltas.cod_centro = ajustes.cod_centro
    LEFT JOIN bloqueios
        ON faltas.cod_material = bloqueios.cod_material
        AND faltas.cod_centro = bloqueios.cod_centro
        AND bloqueios.nr_quantidade > 0
    LEFT JOIN atraso_producao prod
        ON faltas.cod_material = prod.cod_material
        AND faltas.cod_centro = prod.cod_centro
    LEFT JOIN atraso_fornecedor forn
        ON faltas.cod_material = forn.cod_material
        AND faltas.cod_centro = forn.cod_centro
    """
)

# COMMAND ----------

#Registra o DataFrame da consulta SQL para ser consultado em outros Notebooks
permanent_table_name = "f_analise_faltas"
# Exclui a tabela do metastore
spark.sql(f"DROP TABLE IF EXISTS {permanent_table_name}")
# Remove o diretório da tabela no DBFS
dbutils.fs.rm(f'dbfs:/user/hive/warehouse/{permanent_table_name}', True)
# Limpa o cache
spark.catalog.clearCache()
# Salva o DataFrame como uma nova tabela
df.write.mode("overwrite").format("parquet").saveAsTable(permanent_table_name)
