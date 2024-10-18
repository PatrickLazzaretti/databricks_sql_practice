# Databricks notebook source
# MAGIC %sql
# MAGIC USE SCHEMA industrial_analytics

# COMMAND ----------

from pyspark.sql.functions import col, sum, when
from pyspark.sql.window import Window

#Utilizando Spark

df_estoques = spark.sql("SELECT * FROM industrial_analytics.silver_f_estoques")
df_estoques = (
df_estoques
    .where(~col("cod_deposito").isin('203', '230'))\
    .groupBy("cod_material", "cod_centro")\
    .agg(sum(col("nr_qtd_estoque")).alias('nr_estoque'))\
    .select("cod_material", "cod_centro", "nr_estoque")\
)

df_demanda = spark.sql("SELECT * FROM industrial_analytics.silver_f_production_orders_components")
df_demanda = (
df_demanda
    .groupBy("cod_material", "cod_centro", "dt_necessidade")
    .agg(sum(col("nr_qtd_pendente")).alias("nr_demanda"))
    .select("cod_material", "cod_centro", "dt_necessidade", "nr_demanda")
    .orderBy(col("dt_necessidade"))
)

windowSpec = Window.partitionBy("cod_centro", "cod_material").orderBy("dt_necessidade")


df_disponibilidade = (
df_demanda
    .withColumn("nr_demanda_acumulada", sum(col("nr_demanda")).over(windowSpec))
)
df_disponibilidade = df_disponibilidade.join(df_estoques, on=["cod_material", "cod_centro"])
df_disponibilidade = (
df_disponibilidade.withColumn(
    "ds_disponibilidade", when(col("nr_estoque") >=  col("nr_demanda_acumulada"), "OK")
    .otherwise("NOK")
    )
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
    FROM industrial_analytics.silver_f_estoques
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
  FROM industrial_analytics.silver_f_production_orders_components
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

df.write.mode("overwrite").format("delta").saveAsTable("gold_f_disponibilidade_materiais")
