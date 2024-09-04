# Databricks notebook source
from pyspark.sql.functions import col

#Utilizando SQL + temp views
#Agrupar o estoque por material e centro
df_estoques = spark.sql("SELECT * FROM f_estoques")
df_estoques = (df_estoques
    .where(~col("cod_deposito").isin('203', '230'))\
    .select("cod_material", "cod_centro")\
    .groupBy("cod_material", "cod_centro")\
    .agg(sum("nr_qtd_estoque").alias("nr_estoque"))
)


df_estoques.display()

# COMMAND ----------

#Utilizando SQL + temp views
#Agrupar o estoque por material e centro
df_estoque = spark.sql(
    """
    SELECT
        cod_material
        ,cod_centro
        ,SUM(nr_qtd_estoque) AS nr_estoque
    FROM f_estoques
    WHERE ds_classe_estoque LIKE '%Livre%'
        AND cod_deposito <> ''
        AND cod_deposito NOT IN('203', '230')
    GROUP BY cod_material, cod_centro
    """
)

df_estoque.createOrReplaceTempView("estoque")

#Agrupar a demanda por material, centro e data
df_demanda = spark.sql(
    """
    SELECT
        cod_material
        ,cod_centro
        ,dt_necessidade
        ,SUM(nr_qtd_pendente) AS nr_demanda
    FROM f_production_orders_components
    GROUP BY cod_material, cod_centro, dt_necessidade
    """
)

df_demanda.createOrReplaceTempView("demanda")

#Verifica a disponibilidade baseada no material e centro, avaliando em que data irá faltar
df_disponibilidade = spark.sql(
    """
    SELECT
        d.*
        ,SUM(d.nr_demanda) OVER (PARTITION BY d.cod_material, d.cod_centro ORDER BY d.dt_necessidade ASC ) AS nr_demanda_acumulada --Acumula a quantidade pela data
        ,e.nr_estoque
        ,CASE
            WHEN e.nr_estoque >= nr_demanda_acumulada
            THEN 'OK'
            ELSE 'NOK'
        END AS ds_disponibilidade
    FROM demanda d
    LEFT JOIN estoque e 
        ON d.cod_centro = e.cod_centro
        AND d.cod_material = e.cod_material
    """
)

# COMMAND ----------

#Utilizando SQL + CTEs
df = spark.sql(
"""
WITH estoque AS (
    SELECT
        cod_material
        ,cod_centro
        ,SUM(nr_qtd_estoque) AS nr_estoque
    FROM f_estoques
    WHERE ds_classe_estoque LIKE '%Livre%'
        AND cod_deposito <> ''
        AND cod_deposito NOT IN('203', '230')
    GROUP BY cod_material, cod_centro
  ),
demanda AS (
  SELECT
    cod_material
    ,cod_centro
    ,dt_necessidade
    ,SUM(nr_qtd_pendente) AS nr_demanda
  FROM f_production_orders_components
  GROUP BY cod_material, cod_centro, dt_necessidade
  )
SELECT
  d.*
  ,SUM(d.nr_demanda) OVER (PARTITION BY d.cod_material, d.cod_centro ORDER BY d.dt_necessidade ASC ) AS nr_demanda_acumulada --Acumula a quantidade pela data
  ,e.nr_estoque
  ,CASE
    WHEN e.nr_estoque >= nr_demanda_acumulada
      THEN 'OK'
    ELSE 'NOK'
  END AS ds_disponibilidade
  FROM demanda d
  LEFT JOIN estoque e 
    ON d.cod_centro = e.cod_centro
    AND d.cod_material = e.cod_material
"""
)

# COMMAND ----------

#Registra o DataFrame da consulta SQL para ser consultado em outros Notebooks
permanent_table_name = "f_disponibilidade_materiais"
# Exclui a tabela do metastore
spark.sql(f"DROP TABLE IF EXISTS {permanent_table_name}")
# Remove o diretório da tabela no DBFS
dbutils.fs.rm(f'dbfs:/user/hive/warehouse/{permanent_table_name}', True)
# Limpa o cache
spark.catalog.clearCache()
# Salva o DataFrame como uma nova tabela
df.write.mode("overwrite").format("parquet").saveAsTable(permanent_table_name)
