# Databricks notebook source
# MAGIC %sql
# MAGIC USE SCHEMA industrial_analytics

# COMMAND ----------

df = spark.sql(
    """ --Utilizar 3 aspas para não dar problema com quebras de linha
    WITH remessas AS ( --Tabela que mostra todos os materiais indisponíveis com necessidade atrasada ou dia do período de extração dos dados
        SELECT
            cod_documento_compras
            ,cod_item_documento_compras
            ,cod_divisao_remessa
            ,dt_data_remessa
            ,nr_saldo_divisao
            ,SUM(nr_saldo_divisao) OVER (PARTITION BY cod_documento_compras, cod_item_documento_compras ORDER BY dt_data_remessa, cod_divisao_remessa ASC) AS nr_quantidade_acumulada
        FROM industrial_analytics.silver_f_scheduling_agreement_schedule_lines
    ),
    status_faturados AS (
        SELECT
            cod_doc_compras
            ,cod_item_doc_compras
            ,ds_status_recebimento
            ,SUM(nr_quantidade_remessa) AS nr_quantidade_remessa
        FROM industrial_analytics.silver_f_delivery_amount
        GROUP BY cod_doc_compras, cod_item_doc_compras, ds_status_recebimento
    )
    SELECT
        r.cod_documento_compras
        ,r.cod_item_documento_compras
        ,r.cod_divisao_remessa
        ,r.dt_data_remessa
        ,r.nr_saldo_divisao
        ,r.nr_quantidade_acumulada
        ,sf_avtran.nr_quantidade_remessa AS AVTRAN
        ,sf_avped.nr_quantidade_remessa AS AVPED
        ,CASE
            WHEN sf_avped.nr_quantidade_remessa > r.nr_quantidade_acumulada
                THEN 'AvPed'
            WHEN COALESCE(sf_avped.nr_quantidade_remessa, 0) + coalesce(sf_avtran.nr_quantidade_remessa, 0) > r.nr_quantidade_acumulada
                THEN 'AvTran'
            ELSE 'Não Faturado'
        END AS ds_status_remessa
    FROM remessas r
    LEFT JOIN status_faturados sf_avtran
        ON r.cod_documento_compras = sf_avtran.cod_doc_compras
        AND r.cod_item_documento_compras = sf_avtran.cod_item_doc_compras
        AND sf_avtran.ds_status_recebimento = 'AvTran'
    LEFT JOIN status_faturados sf_avped
        ON r.cod_documento_compras = sf_avped.cod_doc_compras
        AND r.cod_item_documento_compras = sf_avped.cod_item_doc_compras
        AND sf_avped.ds_status_recebimento = 'AvPed'
    """
)

df.write.mode("overwrite").format("delta").saveAsTable("gold_f_status_entregas")
