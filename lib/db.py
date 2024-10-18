# Databricks notebook source
def table_exists(database, table, spark):
    table_format = '_'.join(table.split('_')[1:])
    count = (spark.sql(f"SHOW TABLES FROM {database}")
                  .filter(f"database = '{database}'")
                  .filter(f"tableName = '{table_format}'")
                  .count())
    return count == 1
