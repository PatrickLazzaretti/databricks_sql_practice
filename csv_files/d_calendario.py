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
file_location = "/FileStore/tables/d_calendario.csv"
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

df_csv.display()

# COMMAND ----------

#Criar tabela temporária do DataFrame

temp_table_name = "d_calendario_csv"

df_csv.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

#Cria um novo DataFrame baseado na consulta SQL do arquivo CSV

df = spark.sql(
    """ --Utilizar 3 aspas duplas para não dar problema com quebras de linha
    SELECT
        TO_DATE(dt_data, 'dd/MM/yyyy') AS dt_data
        ,TO_DATE(dt_mes_referencia, 'dd/MM/yyyy') AS dt_inicio_mes
        ,TO_DATE(dt_fim_mes, 'dd/MM/yyyy') AS dt_fim_mes
        ,nm_mes_abr
        ,nm_mes_ano_abreviado
        ,nm_mes
        ,nm_dia_da_semana_abr
        ,nm_dia_da_semana
        ,nm_trimestre
        ,nm_mes_ano
        ,nm_mes_ano_referencia
        ,CAST(nr_trimestre AS INT) AS nr_trimestre
        ,CAST(nr_mes_ano AS INT) AS nr_mes_ano
        ,CAST(nr_ocorrencia_dia_da_semana_no_mes AS INT) AS nr_ocorrencia_dia_da_semana_no_mes
        ,CAST(nr_ano AS INT) AS nr_ano
        ,CAST(nr_mes AS INT) AS nr_mes
        ,CAST(nr_dia AS INT) AS nr_dia
        ,CAST(nr_dia_da_semana AS INT) AS nr_dia_da_semana
        ,CAST(nr_dia_do_ano AS INT) AS nr_dia_do_ano
        ,CAST(nr_semana_do_ano AS INT) AS nr_semana_do_ano
        ,CAST(DIF_DIAS_UTEIS AS INT) AS nr_dif_dias_uteis
        ,CAST(DIF_DIAS_UTEIS_GERAL AS INT) AS nr_dif_dias_uteis_geral
        ,bool_dia_util
        ,bool_data_passada
        ,bool_feriado_eua
        ,bool_feriado_br
        ,ds_feriado_eua
        ,ds_feriado_br
    FROM `d_calendario_csv`
    """
)

# COMMAND ----------

#Registra o DataFrame da consulta SQL para ser consultado em outros Notebooks


permanent_table_name = "d_calendario"

df.write.format("parquet").saveAsTable(permanent_table_name)
