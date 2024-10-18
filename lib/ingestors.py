# Databricks notebook source
# DBTITLE 1,Executa Notebook com Funções
# MAGIC %run ./db

# COMMAND ----------

# DBTITLE 1,Setup
import delta

class IngestaoFullBronze:
    def __init__(self, table, spark):
        self.table = table
        self.table_format = '_'.join(self.table.split('_')[1:])
        self.spark = spark
        self.path = f"dbfs:/FileStore/industrial_analytics/raw/{self.table_format}.csv"
        self.tablename = f'industrial_analytics_{self.table}'
                        
    def read(self):
        df = (spark.read.format("csv") \
            .option("inferSchema", "false") \
            .option("header", "true") \
            .option("sep", ";") \
            .load(f"dbfs:/FileStore/industrial_analytics/raw/{self.table_format}.csv"))
        return df


    def save(self, df):
        (df.write
           .mode("overwrite")
           .format("delta")
           .option("overwriteSchema", "true")
           .saveAsTable(self.table))

    def auto(self):
        df = self.read()
        self.save(df)

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/industrial_analytics/raw/")

# COMMAND ----------

dbutils.fs.mv("dbfs:/FileStore/industrial_analytics/raw/stock_adjusts.csv", "dbfs:/FileStore/industrial_analytics/raw/f_ajustes.csv")
