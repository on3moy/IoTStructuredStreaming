# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Notebook 🥉
# MAGIC This notebook will read Event Hub telemetry data and store it in [iotctsistorage](https://portal.azure.com/?pwa=1#@CLNE.onmicrosoft.com/asset/Microsoft_Azure_Storage/StorageAccount/subscriptions/3467f76c-b48c-40f0-90c9-c3b6429c0415/resourceGroups/IOTC-TSI/providers/Microsoft.Storage/storageAccounts/iotctsistorage) for a historical table.
# MAGIC ### Columns added to raw data
# MAGIC 1. **rowhash**: For deduplication when appending to storage account
# MAGIC 2. **loaddatetime**: To record when batch was ran.  
# MAGIC 3. **Year and Month**: To partition files in data lake.
# MAGIC ### Tables Used
# MAGIC 1. **Operations.IoT.Bronze**: Stores all historical telemetry data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Requirements
# MAGIC **Library**  
# MAGIC com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22

# COMMAND ----------

# MAGIC                                                                                                                                                                                                             %sh pip install --upgrade pip azure-cli-eventhubs

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports 🪄

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql.functions import sha2, concat_ws, current_timestamp, col, from_json, year, month
from pyspark.sql.types import *
import json

# COMMAND ----------

# MAGIC %md
# MAGIC # Widgets 🏷️

# COMMAND ----------

# Time to wait for stream to process information, example 1 min means stream will run for 1 min to gather all data points then execute a write.
dbutils.widgets.text('Trigger', '1 minute')
triggerTime = dbutils.widgets.get("Trigger")
if triggerTime in ['', '0']:
    triggerTime = '0 seconds'
else:
    triggerTime = f'{triggerTime} minutes'
print(f'Trigger Time: {triggerTime}')

# COMMAND ----------

# Time to wait before ending stream. 0 means stream will not end
dbutils.widgets.text('JobProcessingTime', '15')
JobProcessingTime = int(dbutils.widgets.get("JobProcessingTime"))
print(f'Job Time in Minutes: {JobProcessingTime}')

# COMMAND ----------

dbutils.widgets.text('EventHubMinutes', '60')
minutes = int(dbutils.widgets.get("EventHubMinutes"))
print(f'EventHub Minutes: {minutes}')

# COMMAND ----------

StartTime_dt = datetime.now() - timedelta(minutes=minutes)
StartTime_str = StartTime_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
print(StartTime_str)

# COMMAND ----------

# Set table name
dbutils.widgets.text(
    "Bronze Table"
    ,"operations.IoT.Bronze")
BRONZE_TABLE_NAME = dbutils.widgets.get('Bronze Table')
print(f'Table Path: {BRONZE_TABLE_NAME}')

# COMMAND ----------

# Set event hub minute interval
StartTime_dt = datetime.now() - timedelta(minutes=minutes)
StartTime_str = StartTime_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

START_TIME = StartTime_str
print(f'EventHub StartTime: {START_TIME}')

# COMMAND ----------

# Set managed bronze external location
dbutils.widgets.text('External Location', 'abfss://eventhub-telemetry@iotctsistorage.dfs.core.windows.net/')
BRONZE_STREAM_FODER_LOC = dbutils.widgets.get("External Location")
BRONZE_STREAM = BRONZE_STREAM_FODER_LOC + 'bronze/'
BRONZE_CHECKPOINT = BRONZE_STREAM_FODER_LOC + 'checkpoints/bronze/'
print(f'Stream External Location: {BRONZE_STREAM_FODER_LOC}')
print(f'Bronze Stream External Location: {BRONZE_STREAM}')
print(f'Bronze Checkpoint External Location: {BRONZE_CHECKPOINT}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Eventhub Configuration ⚙️

# COMMAND ----------

# Event Hub is not reading encrypted connection string from Azure Key Vaults properly
connectionString = dbutils.secrets.get('sgdataanalyticsdev-key-vault-scope','EventHub-Endpoint')

# COMMAND ----------

ehConf = {}
ehConf['eventhubs.connectionString'] = connectionString
ehConf['eventhubs.consumerGroup'] = "$Default"
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

# COMMAND ----------

# 1 hour 60
# 1 day 1,440
# 2 days 2,880
# 3 days 4,320
# 7 days 10,080
# StartTime_dt = datetime.now() - timedelta(minutes=60)
# StartTime_str = StartTime_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
StartTime_str = START_TIME

EndTime_dt = datetime.now() - timedelta(minutes=0)
EndTime_str = EndTime_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

StartingEventPosition = {
  "offset": None,
  "seqNo": -1,            
  "enqueuedTime": StartTime_str,
  "isInclusive": True
}
EndingEventPosition = {
  "offset": None,           
  "seqNo": -1,
  "enqueuedTime": EndTime_str,
  "isInclusive": True
}

ehConf["eventhubs.startingPosition"] = json.dumps(StartingEventPosition)
ehConf["eventhubs.endingPosition"] = json.dumps(EndingEventPosition)

print(f'Start Time: {StartTime_str}\nEnd Time: {EndTime_str}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Stream Read / Write ETL 📥

# COMMAND ----------

# Read events from the Event Hub  specifically for template information
stream = (spark
    .readStream
    .format("eventhubs")
    .options(**ehConf).load()
    .withWatermark('enqueuedTime','1 minute')
)

# COMMAND ----------

stream.isStreaming

# COMMAND ----------

# Add a rowhash to create a composite key for the merge process and a loaddatetime stamp
bronze_schema = StructType([
    StructField('enqueuedTime', TimestampType(), True)
    ,StructField('deviceId', StringType(),True)
    ,StructField('templateName', StringType(), True)
    ,StructField('templateId', StringType(), True)
])

# COMMAND ----------

stream = (stream
    .withColumn('body', col('body').cast('string'))
    .withColumn('templateId', 
        from_json(col('body')
            ,schema=bronze_schema
        )['templateId']
    )
    .withColumn('templateName', 
        from_json(col('body')
            ,schema=bronze_schema
        )['templateName']
    )
    .withColumn('deviceId', 
        from_json(col('body')
            ,schema=bronze_schema
        )['deviceId']
    )
    .withColumn('deviceEnqueueTtime', 
        from_json(col('body')
            ,schema=bronze_schema
        )['enqueuedTime']
    )
    .withColumn('yearDevice', year(col('deviceEnqueueTtime')))
    .withColumn('monthDevice', month(col('deviceEnqueueTtime')))
    .withColumn('rowhash', sha2(col('body'), 256))
    .withColumn('loadDatetime', current_timestamp())
)

# COMMAND ----------

print(f'Cluster Name: {spark.conf.get("spark.databricks.clusterUsageTags.clusterName")}')
print(f'Cluster NodeType: {spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType")}')
print(f'Autotermination (mins): {spark.conf.get("spark.databricks.clusterUsageTags.autoTerminationMinutes")}')
print(f'Cluster Workers: {spark.conf.get("spark.databricks.clusterUsageTags.clusterWorkers")}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Table 📤

# COMMAND ----------

# MAGIC %run "./Utils/0. function_write_stream"

# COMMAND ----------

print(f'Path: {BRONZE_STREAM}')
print(f'Checkpoint Location: {BRONZE_CHECKPOINT}')
print(f'Target Merge Table: {BRONZE_TABLE_NAME}')
print(f'Stream Trigger Time: {triggerTime} (0 means process batches as soon as they come)')
print(f'Stream Job Time: {JobProcessingTime} mins (0 means indefinitely)')

param = {
    'path': BRONZE_STREAM
    ,'checkpointlocation': BRONZE_CHECKPOINT
}
query = (stream
    .writeStream
    .foreachBatch(process_each_batch(BRONZE_TABLE_NAME))
    .options(**param)
    .trigger(processingTime=triggerTime)
    .start()
)
print(f'Active Write Stream Id: {query.id}')

# COMMAND ----------

import time

# Time the Job Processing Time
start = time.time()

# Wait for Job to complete
query.awaitTermination(termination_time(JobProcessingTime))
query.stop()

print(f'Total Job Duration : {(time.time() - start) // 60} minutes')

# COMMAND ----------

# MAGIC %md
# MAGIC # Ensure deduplication
# MAGIC The delta table must have a certain property enabled in order to be accurate on a transactional level.  
# MAGIC [Databricks Serializable Documentation](https://docs.databricks.com/en/optimizations/isolation-level.html)   
# MAGIC
# MAGIC `ALTER TABLE operations.iot.bronze SET TBLPROPERTIES ('delta.isolationLevel' = 'Serializable')`
