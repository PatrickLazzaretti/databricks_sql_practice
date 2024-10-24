# Databricks notebook source
# MAGIC %sql
# MAGIC USE SCHEMA industrial_analytics

# COMMAND ----------

df = spark.sql(
    """ --Utilizar 3 aspas duplas para não dar problema com quebras de linha
    SELECT
        cod_material
        ,cod_centro
        ,cod_deposito
        ,ds_deposito
        ,cod_estoque_especial
        ,cod_lote
        ,cod_documento_vendas
        ,cod_item_documento_vendas
        ,cod_cliente
        ,cod_fornecedor
        ,cod_elemento_pep
        ,ds_classe_estoque
        ,ds_tipo_estoque
        ,ds_tipo_estoque_resumo
        ,cod_tipo_documento_vendas
        ,ds_classificacao_mercado_venda
        ,vl_grau_baixo_giro
        ,CAST(REPLACE(nr_qtd_estoque, ',', '.') AS FLOAT) AS nr_qtd_estoque
        ,CAST(REPLACE(vl_valor_final, ',', '.') AS FLOAT) AS vl_valor_final
        ,CAST(REPLACE(vl_valor_estoque_preco_medio_movel, ',', '.') AS FLOAT) AS vl_valor_estoque_preco_medio_movel
        ,CAST(REPLACE(vl_valor_estoque_preco_standard, ',', '.') AS FLOAT) AS vl_valor_estoque_preco_standard
        ,CAST(REPLACE(vl_valor_provisao, ',', '.') AS FLOAT) AS vl_valor_provisao
        ,TO_DATE(dt_movimento, 'dd/MM/yyyy') AS dt_movimento
        ,CAST(nr_dias_sem_consumo AS INT) AS nr_dias_sem_consumo
    FROM industrial_analytics.bronze_f_estoques
    """
)

df.write.mode("overwrite").format("delta").saveAsTable("silver_f_estoques")
