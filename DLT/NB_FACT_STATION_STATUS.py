# Databricks notebook source
# MAGIC %md
# MAGIC # Import Libraries

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from functools import reduce

# COMMAND ----------

# MAGIC %md
# MAGIC # Read clean delta tables

# COMMAND ----------

# Read from clean zone
@dlt.table(
    comment="Read Station information",
    name = 'fact_station_status'
)
def fact_station_status():
    df_station_status = dlt.read('clean_station_status')
    df_station_infrmation = dlt.read('clean_station_information')
    df_station_status = df_station_status.join(df_station_infrmation,['STATION_ID'],'left')
    return df_station_status

