# Databricks notebook source
dt_referencia = '2024/08/08'
df = spark.sql(
    f""" --Utilizar 3 aspas duplas para não dar problema com quebras de linha
    SELECT
        cod_documento_compras
        ,cod_item_documento_compras
        ,cod_divisao_remessa
        ,cod_fornecedor
        ,cod_incoterm_01
        ,cod_incoterm_02
        ,cod_periodo_fixado
        ,cod_material
        ,cod_centro
        ,CASE 
            WHEN TO_DATE(dt_data_remessa, 'dd/MM/yyyy') < TO_DATE('{dt_referencia}', 'yyyy/MM/dd')
                THEN 'Atraso' 
            ELSE 'Pendente'
        END AS ds_status_remessa
        ,TO_DATE(dt_data_pedido, 'dd/MM/yyyy') AS dt_data_pedido
        ,TO_DATE(dt_data_remessa, 'dd/MM/yyyy') AS dt_data_remessa
        ,TO_TIMESTAMP(etl_date, 'dd/MM/yyyy HH:mm') AS etl_date
        ,CAST(REPLACE(nr_qtd_divisao, ',', '.') AS FLOAT) AS nr_qtd_divisao
        ,CAST(REPLACE(nr_qtd_fornecida, ',', '.') AS FLOAT) AS nr_qtd_fornecida
        ,CAST(REPLACE(nr_saldo_divisao, ',', '.') AS FLOAT) AS nr_saldo_divisao
    FROM industrial_analytics.bronze_f_scheduling_agreement_schedule_lines
    """
)

df.write.mode("overwrite").format("delta").saveAsTable("silver_f_scheduling_agreement_schedule_lines")

# COMMAND ----------

#permanent_table_name = "silver_f_scheduling_agreement_schedule_lines"
# Exclui a tabela do metastore
#spark.sql(f"DROP TABLE IF EXISTS {permanent_table_name}")
# Remove o diretório da tabela no DBFS
#dbutils.fs.rm(f'dbfs:/user/hive/warehouse/{permanent_table_name}', True)
# Limpa o cache
#spark.catalog.clearCache()
# Salva o DataFrame como uma nova tabela
#df.write.mode("overwrite").format("delta").saveAsTable(silver_f_scheduling_agreement_schedule_lines)
