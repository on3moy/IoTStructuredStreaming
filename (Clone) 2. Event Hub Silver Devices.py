# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Notebook 🥈
# MAGIC Reads Bronze data as a stream and processes into Silver Table Structure.  
# MAGIC **Storage Account**: iotctsistorage  
# MAGIC **Container**: IOTeventhubtelemetry

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports 🪄

# COMMAND ----------

# Imports
from datetime import datetime, timedelta
from pyspark.sql.functions import schema_of_json, lit, from_json, to_date, to_timestamp, max, min, when, isnull, isnotnull, col, split, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, FloatType, IntegerType
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

# Set table name
dbutils.widgets.text(
    "Bronze"
    ,"operations.IoT.Bronze")
BRONZE_TABLE = dbutils.widgets.get('Bronze')
print(f'Bronze Table Name: {BRONZE_TABLE}')

# COMMAND ----------

# Set table name
dbutils.widgets.text("Silver Table","operations.IoT.Silver")
SILVER_TABLE = dbutils.widgets.get('Silver Table')
print(f'Silver Table Name: {SILVER_TABLE}')

# COMMAND ----------

# Set managed bronze external location
dbutils.widgets.text('External Location', 'abfss://eventhub-telemetry@iotctsistorage.dfs.core.windows.net/')
SILVER_STREAM_FOLDER_LOC =  dbutils.widgets.get("External Location")
SILVER_STREAM = SILVER_STREAM_FOLDER_LOC + 'silver/'
SILVER_CHECKPOINT = SILVER_STREAM_FOLDER_LOC + 'checkpoints/silver/'
print(f'Silver Folder Location: {SILVER_STREAM_FOLDER_LOC}')
print(f'Silver Stream External Location: {SILVER_STREAM}')
print(f'Silver Checkpoint External Location: {SILVER_CHECKPOINT}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Device Template JSON Parsing 🛠️

# COMMAND ----------

# MAGIC %run "./Device Templates/DeviceTemplateDictionary"

# COMMAND ----------

print(f'Device Template: {TEMPLATE_JSON.keys()}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Bronze Table as Stream ⬇️

# COMMAND ----------

silver_df = spark.readStream.format('delta').table(BRONZE_TABLE)

# COMMAND ----------

# Adjust template json file location
TEMPLATE_LOCATION = '../IOT Compressor Telemetry Batch/Device Templates/Devices/'

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Telemetry Template Files 🦖
# MAGIC Read JSON files and create a dictionary of assets and their schemas

# COMMAND ----------

# IOT Central Exported Compressor Device Template
PYSPARK_DATATYPES = {
    'boolean': BooleanType()
    ,'float': FloatType()
    ,'double': DoubleType()
    ,'integer': IntegerType()
}
asset_schemas = {}
for asset, location in TEMPLATE_JSON.items():
    print(f'Getting {asset} json')
    with open(TEMPLATE_LOCATION + location) as f:
        data = json.load(f)
        template_telemetry = {}
        for x in data:
            for key, value in x.items():
                for y in x['contents']:
                    template_telemetry[y['name']] = y['schema']
        mappings = {}
        for key, value in template_telemetry.items():
            mappings[key] = PYSPARK_DATATYPES[value]
            structfields = [StructField(header, formats, True) for header, formats in list(sorted(mappings.items()))]
            asset_schemas[asset] = structfields
    print(f'Completed {asset} json')


# COMMAND ----------

# MAGIC %md
# MAGIC # Write to Unity Catalog ⬆️

# COMMAND ----------

# MAGIC %run "./Utils/0. function_write_stream"

# COMMAND ----------

for asset in TEMPLATE_JSON.keys():
    try:
        # Dynamically create asset table names, folder paths, and checkpoint paths
        assetname = asset.replace(' ','_')
        print(asset)
        asset_table =  SILVER_TABLE + '_' +  assetname
        asset_checkpoint =  SILVER_CHECKPOINT + assetname
        asset_folder = SILVER_STREAM + assetname + '/'
        print(asset_folder)
        print(asset_table)
        print('\n')
        
        stream = silver_df.filter(col('TemplateName') == asset)

        # This is the main schema for all IOT Devices, what differs are the telemetry values
        template_struct = StructType([
            StructField('applicationId', StringType(), True)
            ,StructField('component', StringType(), True)
            ,StructField('enrichments', StringType(), True)
            ,StructField('messageProperties'
                ,StructType([StructField('iothub-creation-time-utc', StringType(), True)]), True)
            ,StructField('messageSource', StringType(), True)
            ,StructField('module', StringType(), True)
            ,StructField('schema', StringType(), True)
            ,StructField('templateId', StringType(), True)
            ,StructField('enqueuedTime', StringType(), True)
            ,StructField('deviceId', StringType(),True)
            ,StructField('templateName', StringType(), True)
            ,StructField('telemetry'
                ,StructType(
                    asset_schemas[asset]
                ), True)
        ])

        stream = (stream
        .withColumn('JSONTemplate', from_json(stream['body'], schema=template_struct))
        )

        stream = stream.withColumn('flagged'
            ,when(
                (isnull(stream["JSONTemplate"]) & isnotnull(stream["body"])) | isnull((stream['JSONTemplate']['telemetry']))
                , 1
                )
            .otherwise(0)
        )

        stream = stream.filter(col('flagged') == 0)
        
        stream = (
            stream
            .withColumn('Eventhub_enqueuedTime', to_timestamp(col('enqueuedTime')))
            .withColumn('Device_EnqueuedTime', to_timestamp(col('JSONTemplate.enqueuedTime')))
            .withColumn('Device_EnqueuedTime_PST', from_utc_timestamp(col('JSONTemplate.enqueuedTime'), 'PST'))
            .withColumn('Device_Date', to_date(col('JSONTemplate.enqueuedTime')))
            .withColumn('stationid', split(col('deviceId'),'-')[0].cast(IntegerType()))
            .select('Eventhub_enqueuedTime','Device_EnqueuedTime','Device_EnqueuedTime_PST', 'Device_Date', 'yearDevice', 'monthDevice', 'body', 'stationid', 'deviceId', 'templateName', 'JSONTemplate.telemetry.*','loaddatetime', 'rowhash')   
        )
        # Writing parameters
        if spark.catalog.tableExists(f'{asset_table}'):
            print('Merging in progress')
            print('-' * 20 + '\n')
            param = {
                'path': asset_folder
                ,'checkpointlocation': asset_checkpoint
                ,'mergeSchema': "true"
            }

            query = (stream
                .writeStream
                .foreachBatch(process_each_batch(asset_table))
                .options(**param)
                .trigger(processingTime=triggerTime)
                .start()
            )
        else:
            print('Table Creation to repopulate from historical data')
            query = (stream
                .writeStream
                .format('delta')
                .outputMode('append')
                .option('mergeSchema', 'true')
                .option('path', asset_folder)
                .option('checkpointlocation', asset_checkpoint)
                .trigger(once=True)
                .partitionBy('yearDevice', 'monthDevice')
                .toTable(asset_table)
            )
    except Exception as e:
        print(e)

# COMMAND ----------

query.awaitTermination(termination_time(JobProcessingTime))

# COMMAND ----------

for q in spark.streams.active:
    q.stop()
