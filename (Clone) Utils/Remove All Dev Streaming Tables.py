# Databricks notebook source
# MAGIC %md
# MAGIC # Removes all streaming tables with prefix "streamingmoy" and folders in datalake

# COMMAND ----------

tables = spark.sql('SHOW TABLES IN operations.iot').collect()

# COMMAND ----------

drop_tables = [table.tableName for table in tables if table.tableName.find('silver') > -1]

# COMMAND ----------

drop_tables

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG operations

# COMMAND ----------

for table in drop_tables:
    print(f'operations.iot.{table}')
    spark.sql(f'DROP TABLE iot.{table}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Folders
# MAGIC Bronze Folder Location: abfss://eventhub-telemetry@iotctsistorage.dfs.core.windows.net/bronze_dev_stream
# MAGIC Bronze Stream External Location: abfss://eventhub-telemetry@iotctsistorage.dfs.core.windows.net/bronze_dev_stream/bronze  
# MAGIC Bronze Checkpoint External Location: abfss://eventhub-telemetry@iotctsistorage.dfs.core.windows.net/bronze_dev_stream/checkpoint  
# MAGIC
# MAGIC # Silver Folders
# MAGIC Silver Folder Location: abfss://eventhub-telemetry@iotctsistorage.dfs.core.windows.net/silver_dev_stream  
# MAGIC Silver Stream External Location: abfss://eventhub-telemetry@iotctsistorage.dfs.core.windows.net/silver_dev_stream/silver  
# MAGIC Silver Checkpoint External Location: abfss://eventhub-telemetry@iotctsistorage.dfs.core.windows.net/silver_dev_stream/checkpoint  

# COMMAND ----------

# path_list = [
#   'abfss://eventhub-telemetry@iotctsistorage.dfs.core.windows.net/silver_dev_stream'
#   ,'abfss://eventhub-telemetry@iotctsistorage.dfs.core.windows.net/bronze_dev_stream'
# ]

# COMMAND ----------

path_list = [
  'abfss://eventhub-telemetry@iotctsistorage.dfs.core.windows.net/silver'
  ,'abfss://eventhub-telemetry@iotctsistorage.dfs.core.windows.net/checkpoints/silver'
]

# COMMAND ----------

# Remove all temp parquet files created
for path in path_list:
  print(f'Deleting folder from Storage Account: {path}')
  dbutils.fs.rm(path, recurse=True)
