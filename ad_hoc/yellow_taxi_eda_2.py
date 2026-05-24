# Databricks notebook source
from pyspark.sql.functions import date_format, count, sum

# COMMAND ----------

taxi_type = 'green'

spark.read.table(f"nyctaxi.`01_bronze`.{taxi_type}_trips_raw").\
    groupBy(date_format("lpep_pickup_datetime", "yyyy-MM").alias("year_month")).\
    agg(count("*").alias("total_records")).\
    orderBy("year_month").display()

# COMMAND ----------

spark.read.table(f"nyctaxi.`02_silver`.{taxi_type}_trips_cleansed").\
    groupBy(date_format("pickup_datetime", "yyyy-MM").alias("year_month")).\
    agg(count("*").alias("total_records")).\
    orderBy("year_month").display()

# COMMAND ----------

spark.read.table(f"nyctaxi.`02_silver`.{taxi_type}_trips_enriched").\
    groupBy(date_format("pickup_datetime", "yyyy-MM").alias("year_month")).\
    agg(count("*").alias("total_records")).\
    orderBy("year_month").display()

# COMMAND ----------

spark.read.table("nyctaxi.`03_gold`.daily_trip_summary").\
    groupBy(date_format("pickup_date", "yyyy-MM").alias("year_month")).\
    agg(sum("total_trips").alias("total_records")).\
    orderBy("year_month").display()

# COMMAND ----------

df = spark.read.table("nyctaxi.`02_silver`.taxi_zone_lookup")
df.where('location_id IN (1, 999)').display()
df.count()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `nyctaxi`; select * from `02_silver`.`taxi_zone_lookup` limit 300;
