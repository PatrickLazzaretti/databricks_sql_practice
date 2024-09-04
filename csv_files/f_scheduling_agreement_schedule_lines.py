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
file_location = "/FileStore/tables/f_scheduling_agreement_schedule_lines.csv"
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

temp_table_name = "f_scheduling_agreement_schedule_lines_csv"

df_csv.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

#Cria um novo DataFrame baseado na consulta SQL do arquivo CSV
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
    FROM `f_scheduling_agreement_schedule_lines_csv`
    """
)

# COMMAND ----------

#Registra o DataFrame da consulta SQL para ser consultado em outros Notebooks
permanent_table_name = "f_scheduling_agreement_schedule_lines"
# Exclui a tabela do metastore
spark.sql(f"DROP TABLE IF EXISTS {permanent_table_name}")
# Remove o diretório da tabela no DBFS
dbutils.fs.rm(f'dbfs:/user/hive/warehouse/{permanent_table_name}', True)
# Limpa o cache
spark.catalog.clearCache()
# Salva o DataFrame como uma nova tabela
df.write.mode("overwrite").format("parquet").saveAsTable(permanent_table_name)
