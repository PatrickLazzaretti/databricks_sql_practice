# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

#Caminho dos Arquivos CSV
file_location = "/FileStore/tables/f_production_orders_operations.csv"
file_type = "csv"

#Configurações do CSV
infer_schema = "false"
first_row_is_header = "true"
delimiter = ";"

#Crição do DataFrame com o CSV
df_csv = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------

#Criar tabela temporária do DataFrame

temp_table_name = "f_production_orders_operations_csv"
df_csv.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

df_classificacao_operacao = spark.sql(
    f"""
    WITH cod_operacao_em_progresso AS (
    SELECT
        cod_ordem
        ,MIN(CAST(cod_operacao AS INT)) AS cod_operacao
    FROM f_production_orders_operations_csv
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
            THEN 'Em Progresso'
        ELSE 'Não Iniciado'
    END AS ds_classificacao_operacao
    FROM f_production_orders_operations_csv poo
    LEFT JOIN cod_operacao_em_progresso coep
    ON poo.cod_ordem = coep.cod_ordem
    AND poo.cod_operacao = coep.cod_operacao
    """
)

df_classificacao_operacao.createOrReplaceTempView('ds_classificacao_operacao')

# COMMAND ----------

#Cria um novo DataFrame baseado na consulta SQL do arquivo CSV
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
    FROM `f_production_orders_operations_csv` poo
    LEFT JOIN ds_classificacao_operacao co
        ON poo.cod_ordem = co.cod_ordem
        AND poo.cod_operacao = co.cod_operacao 
    """
)

# COMMAND ----------

#Registra o DataFrame da consulta SQL para ser consultado em outros Notebooks
permanent_table_name = "f_production_orders_operations"
# Exclui a tabela do metastore
spark.sql(f"DROP TABLE IF EXISTS {permanent_table_name}")
# Remove o diretório da tabela no DBFS
dbutils.fs.rm(f'dbfs:/user/hive/warehouse/{permanent_table_name}', True)
# Limpa o cache
spark.catalog.clearCache()
# Salva o DataFrame como uma nova tabela
df.write.mode("overwrite").format("parquet").saveAsTable(permanent_table_name)
