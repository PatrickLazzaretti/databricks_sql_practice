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
file_location = "/FileStore/tables/f_bloqueios.csv"
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

temp_table_name = "f_bloqueios_csv"
df_csv.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

#Cria um novo DataFrame baseado na consulta SQL do arquivo CSV

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
    FROM `f_bloqueios_csv`
    """
)

# COMMAND ----------

#Registra o DataFrame da consulta SQL para ser consultado em outros Notebooks
permanent_table_name = "f_bloqueios"
# Exclui a tabela do metastore
spark.sql(f"DROP TABLE IF EXISTS {permanent_table_name}")
# Remove o diretório da tabela no DBFS
dbutils.fs.rm(f'dbfs:/user/hive/warehouse/{permanent_table_name}', True)
# Limpa o cache
spark.catalog.clearCache()
# Salva o DataFrame como uma nova tabela
df.write.mode("overwrite").format("parquet").saveAsTable(permanent_table_name)
