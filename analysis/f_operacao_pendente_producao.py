# Databricks notebook source
cod_operacao_pendente = spark.sql(
"""
  WITH cod_operacao_pendente AS (
    SELECT
    cod_ordem
    ,MIN(CAST(cod_operacao AS INT)) AS cod_operacao_pendente
  FROM f_production_orders_operations
  WHERE ds_classificacao_operacao IN ('Não Iniciado', 'Pendente')
  GROUP BY cod_ordem
)
SELECT
  cop.cod_ordem
  ,cop.cod_operacao_pendente
  ,poo.cod_centro_trabalho
  ,poo.ds_operacao
  ,poo.ds_proc_group
  ,poo.ds_status_finalizacao_operacao
FROM f_production_orders_operations poo
INNER JOIN cod_operacao_pendente cop
  ON poo.cod_ordem = cop.cod_ordem
  AND poo.cod_operacao = cop.cod_operacao_pendente
"""
)
cod_operacao_pendente.createOrReplaceTempView('temp_production_orders_operations_pendente')

# COMMAND ----------

#Cria um novo DataFrame baseado na consulta SQL do arquivo CSV
df = spark.sql(
    """ --Utilizar 3 aspas para não dar problema com quebras de linha
    SELECT  
        poh.cod_ordem
        ,poh.cod_material
        ,poh.dt_data_fim_programado_ordem
        ,poh.nr_quantidade_teorica
        ,poop.cod_operacao_pendente
        ,poop.cod_centro_trabalho
        ,poop.ds_operacao
        ,poop.ds_proc_group
        ,poh.ds_status_finalizacao_ordem
    FROM f_production_orders_header poh
    INNER JOIN temp_production_orders_operations_pendente poop ON poh.cod_ordem = poop.cod_ordem
    """
)

# COMMAND ----------

#Registra o DataFrame da consulta SQL para ser consultado em outros Notebooks
permanent_table_name = "f_operacao_pendente_producao"
# Exclui a tabela do metastore
spark.sql(f"DROP TABLE IF EXISTS {permanent_table_name}")
# Remove o diretório da tabela no DBFS
dbutils.fs.rm(f'dbfs:/user/hive/warehouse/{permanent_table_name}', True)
# Limpa o cache
spark.catalog.clearCache()
# Salva o DataFrame como uma nova tabela
df.write.mode("overwrite").format("parquet").saveAsTable(permanent_table_name)
