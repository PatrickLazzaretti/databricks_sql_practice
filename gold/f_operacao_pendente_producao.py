# Databricks notebook source
# MAGIC %sql
# MAGIC USE SCHEMA industrial_analytics

# COMMAND ----------

#Utilizando Spark

from pyspark.sql.functions import col, min
df_operacao = spark.sql("SELECT * FROM industrial_analytics.silver_f_production_orders_operations")
df_cabecalho = spark.sql("SELECT * FROM industrial_analytics.silver_f_production_orders_header")

df_operacao_pendente = (
df_operacao
    .withColumn("cod_operacao", col("cod_operacao").cast("int"))
    .where(col("ds_classificacao_operacao").isin('Em Processo', "Não Iniciado"))
    .groupBy("cod_ordem")
    .agg(min(col("cod_operacao")).alias("cod_operacao_pendente"))
    .select(col("cod_ordem").alias("cod_ordem_op"), col("cod_operacao_pendente").alias("cod_operacao_op"))
)

df_operacao_pendente = (
    df_operacao_pendente.join(
        df_operacao, 
        (df_operacao_pendente["cod_ordem_op"] == df_operacao["cod_ordem"]) & 
        (df_operacao_pendente["cod_operacao_op"] == df_operacao["cod_operacao"]), 
        "inner"
    )
    .select("cod_ordem_op", "cod_operacao", "cod_centro_trabalho", "ds_operacao", "ds_proc_group", "ds_status_finalizacao_operacao")
)

df_operacao_pendente = (
    df_operacao_pendente
    .join(df_cabecalho, df_operacao_pendente["cod_ordem_op"] == df_cabecalho["cod_ordem"], "inner")
    .select("cod_ordem", "cod_operacao", "cod_centro_trabalho", "ds_operacao", "ds_proc_group", "ds_status_finalizacao_operacao", "cod_material", "dt_data_fim_programado_ordem", "nr_quantidade_teorica", "ds_status_finalizacao_ordem")
)

# COMMAND ----------

#Utilizando SQL
cod_operacao_pendente = spark.sql(
"""
  WITH cod_operacao_pendente AS (
    SELECT
    cod_ordem
    ,MIN(CAST(cod_operacao AS INT)) AS cod_operacao_pendente
  FROM industrial_analytics.silver_f_production_orders_operations
  WHERE ds_classificacao_operacao IN ('Não Iniciado', 'Em Processo')
  GROUP BY cod_ordem
)
SELECT
  cop.cod_ordem
  ,cop.cod_operacao_pendente
  ,poo.cod_centro_trabalho
  ,poo.ds_operacao
  ,poo.ds_proc_group
  ,poo.ds_status_finalizacao_operacao
FROM industrial_analytics.silver_f_production_orders_operations poo
INNER JOIN cod_operacao_pendente cop
  ON poo.cod_ordem = cop.cod_ordem
  AND poo.cod_operacao = cop.cod_operacao_pendente
"""
)
cod_operacao_pendente.createOrReplaceTempView('temp_production_orders_operations_pendente')

#Cria um novo DataFrame baseado na consulta SQL
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
    FROM industrial_analytics.silver_f_production_orders_header poh
    INNER JOIN temp_production_orders_operations_pendente poop ON poh.cod_ordem = poop.cod_ordem
    """
)

df.write.mode("overwrite").format("delta").saveAsTable("gold_f_operacao_pendente_producao")
