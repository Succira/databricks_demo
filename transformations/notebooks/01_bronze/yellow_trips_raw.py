# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from dateutil.relativedelta import relativedelta

# COMMAND ----------


taxi_type = dbutils.widgets.get("taxi_type")
# two_months_ago = date.today() - relativedelta(months=2)
# formatted_date = two_months_ago.strftime("%Y-%m")
formatted_date = dbutils.widgets.get("process_date")

# Read all Parquet files from the landing directory into a DataFrame
df = spark.read.format("parquet").load(f"/Volumes/nyctaxi/00_landing/data_sources/{taxi_type}_tripdata/{taxi_type}_tripdata_{formatted_date}.parquet")


# COMMAND ----------

# Add a column to capture when the data was processed
df = df.withColumn("processed_timestamp", current_timestamp())

# COMMAND ----------

# Write the DataFrame to a Unity Catalog managed Delta table in the bronze schema, overwriting any existing data
df.write.mode("append").saveAsTable(f"nyctaxi.01_bronze.{taxi_type}_trips_raw")
