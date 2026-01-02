# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze Ingestion Dynamic Notebook

# COMMAND ----------

source_list = ['customers', 'drivers', 'trips', 'payments', 'locations', 'vehicles']

# COMMAND ----------


for source in source_list:
  source_data = spark.readStream.format('cloudFiles')\
                .option('cloudFiles.format', 'csv')\
                .option('cloudFiles.schemaLocation', f'/Volumes/pyspark_dbt/bronze/checkpoints/{source}/')\
                .option('cloudFiles.schemaEvolutionMode', 'rescue')\
                .load(f'/Volumes/pyspark_dbt/source/source_data/{source}')

  source_data.writeStream.format('delta')\
    .option('outputMode', 'append')\
    .option('checkpointLocation', f'/Volumes/pyspark_dbt/bronze/checkpoints/{source}/')\
    .trigger(once=True)\
    .toTable(f'pyspark_dbt.bronze.bronze_{source}')

# COMMAND ----------

