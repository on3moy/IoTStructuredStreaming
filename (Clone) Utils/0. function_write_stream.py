# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Stream Function
# MAGIC **termination_time**: Used to set a trigger if neccessary  
# MAGIC **process_each_batch**: Merge new data only to target table
# MAGIC ```
# MAGIC termination_time(time)
# MAGIC process_each_batch(delta_table_name):
# MAGIC ```
# MAGIC

# COMMAND ----------

# def write_stream(df, **kwargs):
#     '''
#     df = spark dataframe
#     mergeSchema = False or True (Default set to True)
#     outputmode = 'append', 'complete' (Default set to append)
#         - append - add rows as they come in
#         - complete - replace table once processed, mostly for aggregations
#     partitionBy = create folders in storage account for faster querying
#     toTable = name of table in metastore
#     path = path to storeage account to store table files
#     checkpointlocation = path to storeaccount to store checkpoint files
#     trigger = time to process data in micro-batch mode, 0 defaults to microbatch mode which processes as batches come (no trigger)
#     '''
#     trigger = kwargs.get('trigger', '0 seconds')
#     # Print Processing Time if any
#     if trigger == '0 seconds':
#         print('Defaulting to micro-batch mode, where batches are generated as soon as previous batch is completed processing.')
#     else:
#         print(f'processingTime is set to: {trigger}')

#     return (
#         df
#             .writeStream
#             .format('delta')
#             .outputMode(kwargs.get('outputMode', 'append'))
#             .option('mergeSchema', kwargs.get('mergeSchema', 'true'))
#             .option('path', kwargs.get('path', None))
#             .option('checkpointlocation', kwargs.get('checkpointlocation', None))
#             .trigger(processingTime=trigger)
#             .partitionBy(kwargs.get('partitionBy', None))
#             .toTable(kwargs.get('toTable', None))    
#     )

# COMMAND ----------

def termination_time(time):
    try:
        if int(time) == 0:
            return None
        else:
            return int(time) * 60
    except Exception as e:
        print(e)
        return None

# COMMAND ----------

from delta.tables import DeltaTable

def process_each_batch(delta_table_name):
    '''Function merges the incoming stream to delta_table_name.
        user inputs delta table name only
        df = Current Batch DF
        epoch_id = Current Batch Id
    '''

    def each_batch(df, epoch_id):
        # Instantiate DeltaTable Object
        delta_table_target = DeltaTable.forName(spark, delta_table_name)

        # Start the merge process using source column
        delta_table_target.alias('target').merge(
            df.alias('source')
            ,'target.rowhash = source.rowhash'
        ).whenNotMatchedInsertAll().execute()

        try:
            print(f'MERGE completed for Table: {delta_table_name}\t\tbatchId: {epoch_id}')
            last_operation = delta_table_target.history(1).collect()[0]
            rows_inserted = last_operation['operationMetrics']['numTargetRowsInserted']
            print(f'Batch Row Count: {df.count()}\t\t\tTotal Rows Inserted: {rows_inserted}')
            print('-' * 100)
            print('\n')
        except Exception as e:
            print(e)
    return each_batch
