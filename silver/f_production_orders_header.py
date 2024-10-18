# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Explicação
# MAGIC
# MAGIC Esse notebook tem o intuito de mostrar o código por trás da criação da tabela f_production_orders_header, de origem CSV.

# COMMAND ----------

#Caminho dos Arquivos CSV
file_location = "/FileStore/tables/f_production_orders_header.csv"
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

temp_table_name = "f_production_orders_header_csv"

df_csv.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

#Cria um novo DataFrame baseado na consulta SQL do arquivo CSV
dt_referencia = '2024/08/08'
df = spark.sql(
    f""" --Utilizar 3 aspas para não dar problema com quebras de linha
    SELECT
        cod_ordem
        ,cod_centro
        ,cod_ordem_cliente
        ,cod_item_ordem_cliente
        ,cod_tipo_ordem
        ,cod_serie
        ,cod_responsavel_controle_producao
        ,cod_material
        ,cod_cliente
        ,cod_numero_objeto
        ,cod_ordem_principal
        ,CASE 
            WHEN TO_DATE(dt_data_fim_programado_ordem, 'dd/MM/yyyy') < TO_DATE('{dt_referencia}', 'yyyy/MM/dd')
                THEN 'Atraso' 
            ELSE 'Pendente'
        END AS ds_status_finalizacao_ordem
        ,TO_DATE(dt_data_inicio_programado_ordem, 'dd/MM/yyyy') AS dt_data_inicio_programado_ordem
        ,TO_DATE(dt_data_inicio_real_ordem, 'dd/MM/yyyy') AS dt_data_inicio_real_ordem
        ,TO_DATE(dt_data_fim_programado_ordem, 'dd/MM/yyyy') AS dt_data_fim_programado_ordem
        ,TO_DATE(dt_data_fim_real_ordem, 'dd/MM/yyyy') AS dt_data_fim_real_ordem
        ,TO_DATE(dt_data_fim_real_ordem_confirmacao, 'dd/MM/yyyy') AS dt_data_fim_real_ordem_confirmacao
        ,TO_DATE(dt_data_entrada_ordem, 'dd/MM/yyyy') AS dt_data_entrada_ordem
        ,TO_DATE(dt_data_modificacao_ordem, 'dd/MM/yyyy') AS dt_data_modificacao_ordem
        ,TO_TIMESTAMP(dt_fim_real_ordem, 'dd/MM/yyyy HH:mm') AS dt_fim_real_ordem
        ,TO_TIMESTAMP(dt_inicio_programado_ordem, 'dd/MM/yyyy HH:mm') AS dt_inicio_programado_ordem
        ,CAST(REPLACE(nr_qtd_teorica, ',', '.') AS FLOAT) AS nr_quantidade_teorica
        ,CAST(REPLACE(nr_quantidade_confirmada, ',', '.') AS FLOAT) AS nr_quantidade_confirmada
    FROM `f_production_orders_header_csv`
    """
)

# COMMAND ----------

#Registra o DataFrame da consulta SQL para ser consultado em outros Notebooks
permanent_table_name = "f_production_orders_header"
# Exclui a tabela do metastore
spark.sql(f"DROP TABLE IF EXISTS {permanent_table_name}")
# Remove o diretório da tabela no DBFS
dbutils.fs.rm(f'dbfs:/user/hive/warehouse/{permanent_table_name}', True)
# Limpa o cache
spark.catalog.clearCache()
# Salva o DataFrame como uma nova tabela
df.write.mode("overwrite").format("parquet").saveAsTable(permanent_table_name)
