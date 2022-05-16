# Databricks notebook source
# MAGIC %md
# MAGIC # Import Libraries

# COMMAND ----------

#import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from functools import reduce

# COMMAND ----------

# MAGIC %md # Define target schema

# COMMAND ----------

# Sample test data to defien the schema
data = [("free_bike_status","data_bikes_bike_id",1,"BIKE_ID","/mnt/test_container/source/free-bike-status-1.json"),
         ("free_bike_status","data_bikes_name",2,"BIKE_NAME","/mnt/test_container/source/free-bike-status-1.json"),
         ("free_bike_status","data_bikes_is_disabled",3,"IS_DISABLED","/mnt/test_container/source/free-bike-status-1.json"),
         ("free_bike_status","data_bikes_is_reserved",4,"IS_RESERVED","/mnt/test_container/source/free-bike-status-1.json"),
         ("free_bike_status","data_bikes_lat",5,"BIKE_LAT","/mnt/test_container/source/free-bike-status-1.json"),
         ("free_bike_status","data_bikes_lon",6,"BIKE_LON","/mnt/test_container/source/free-bike-status-1.json"),
         ("station_status","data_stations_station_id",1,"STATION_ID","/mnt/test_container/source/station-status-8.json"),
         ("station_status","data_stations_num_docks_available",2,"NUM_OF_DOCKS_AVAILABLE","/mnt/test_container/source/station-status-8.json"),
         ("station_status","data_stations_num_bikes_available",3,"NUM_OF_BIKES_AVAILABLE","/mnt/test_container/source/station-status-8.json"),
         ("station_status","data_stations_num_bikes_disabled",4,"NUM_OF_BIKES_DISABLED","/mnt/test_container/source/station-status-8.json"),
         ("station_status","data_stations_is_installed",5,"IS_INSTALLED","/mnt/test_container/source/station-status-8.json"),
         ("station_status","data_stations_is_renting",6,"IS_RENTING","/mnt/test_container/source/station-status-8.json"),
         ("station_status","data_stations_last_reported",7,"LAST_REPORTED","/mnt/test_container/source/station-status-8.json"),
         ("station_status","data_stations_is_returning",8,"IS_RETURNING","/mnt/test_container/source/station-status-8.json"),
        ("station_information","data_stations_station_id",8,"STATION_ID","/mnt/test_container/source/station-information-9.json"),
        ("station_information","data_stations_name",8,"STATION_NAME","/mnt/test_container/source/station-information-9.json"),
        ("station_information","data_stations_region_id",8,"REGION_ID","/mnt/test_container/source/station-information-9.json"),
  ]

schema = StructType([ \
                     StructField("DATASET_NAME",StringType(),True), \
                     StructField("RAW_COLUMN_NAME",StringType(),True), \
                     StructField("POSITION",IntegerType(),True), \
                     StructField("TARGET_COLUMN_NAME", StringType(), True),\
                     StructField("RAW_FILE_PATH", StringType(), True)\
  ])
 
df_schema = spark.createDataFrame(data=data,schema=schema)
display(df_schema)

# COMMAND ----------

# MAGIC %md # Utility Functions

# COMMAND ----------

# Rename the columns
def rename_cols(dataset_name,df_schema,df_raw_data):
    df_schema = df_schema.filter(col('DATASET_NAME') == dataset_name)
    new_schema = df_schema.rdd.map(lambda x : x.TARGET_COLUMN_NAME).collect()
    raw_schema = df_schema.rdd.map(lambda x : x.RAW_COLUMN_NAME).collect()
    df_renamed_cols = (reduce(lambda df_raw_data, i: df_raw_data.withColumnRenamed(raw_schema[i], 
                                                                               new_schema[i]), range(len(raw_schema)),df_raw_data)
              )
    df_renamed_cols = df_renamed_cols.select(new_schema)
    return df_renamed_cols

# COMMAND ----------

#Flatten array of structs and structs
def flatten(df: DataFrame):
   # compute Complex Fields (Lists and Structs) in Schema   
   complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
      # if StructType then convert all sub element to columns.
      # i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType):
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
         df=df.select("*", *expanded).drop(col_name)
    
      # if ArrayType then add the Array Elements as Rows using the explode function
      # i.e. explode Arrays
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,explode_outer(col_name))
    
      # recompute remaining Complex Fields in Schema       
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df

# COMMAND ----------

# MAGIC %md # Clean RAW data

# COMMAND ----------

def load_to_clean_zone(dataset_name,source_file_path):
    # Create delta view of the raw data 
    @dlt.view(
        comment="Source bike status data ingested to RAW",
        name = 'source_' + dataset_name
    )
    def read_raw_data():
        df_1 = spark.read.format('json').load(source_file_path)
        df_flattened = flatten(df_1)
        return df_flattened
    
    # Clean the raw data and write it to CLEAN zone
    @dlt.table(
        comment="Clean bikes status data",
        name = 'clean_' + dataset_name
    )
    def clean_raw_data():
        df_raw = dlt.read('source_' + dataset_name)
        df_renamed = rename_cols(dataset_name,df_schema,df_raw)
        return df_renamed

# COMMAND ----------

# MAGIC %md # Load the datasets to clean zone in loop

# COMMAND ----------

dataset_name = df_schema.select('DATASET_NAME').distinct()
dataset_list = dataset_name.rdd.map(lambda x : x.DATASET_NAME).collect()

for dataset in dataset_list:
    source_file_path = df_schema.select('RAW_FILE_PATH').filter(col('DATASET_NAME') == dataset).distinct()
    source_file_list = source_file_path.rdd.map(lambda x : x.RAW_FILE_PATH).collect()
    for source_file in source_file_list:
        load_to_clean_zone(dataset,source_file)
