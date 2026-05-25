# Databricks notebook source
# import sys
# import os
# # Go two levels up to reach the project root
# project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

# if project_root not in sys.path:
#     sys.path.append(project_root)

# from modules.utils.date_utils import get_month_start_n_months_ago
from pyspark.sql.functions import date_format, col

# COMMAND ----------

# Read the 'yellow_trips_enriched' table from the 'nyctaxi.02_silver' schema
# and filter to only include trips with a pickup datetime
# later than the start date from two months ago
process_date = dbutils.widgets.get("process_date") + '-01'
taxi_type = dbutils.widgets.get("taxi_type")

df = spark.read.table(f"nyctaxi.02_silver.{taxi_type}_trips_enriched")\
    .where(col('pickup_datetime') >= process_date)

# COMMAND ----------

# Add a year_month column, formated as yyyy-MM

df = df.withColumn("year_month", date_format("pickup_datetime", "yyyy-MM"))

# COMMAND ----------

# Write the yellow_trips data in JSON format to the External Table "yellow_trips_export"
account_name = "nyctaxiexportstorage"
container_name = "nyctaxi-export"
file_path = f'{taxi_type}-export'
path = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/{file_path}"

df.write.\
    option("path", path).\
    format("json").\
    mode("append").\
    partitionBy("vendor", "year_month").\
    saveAsTable(f"nyctaxi.04_export.{taxi_type}_trips_export")
