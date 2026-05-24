# Databricks notebook source
from pyspark.sql.functions import count, max, min, avg, sum, round

# COMMAND ----------

# Load the enriched trip dataset
two_month_ago_start = dbutils.widgets.get("process_date") + '-01'
two_month_ago_end = dbutils.widgets.get("process_date_end") + '-01'


taxi_type = dbutils.widgets.get("taxi_type")

df = spark.read.table(f"nyctaxi.02_silver.{taxi_type}_trips_enriched")
df = df.where(df.pickup_datetime >= two_month_ago_start)


# COMMAND ----------

# Aggregate trip data by pickup date with key metrics

df = df.groupBy(df.pickup_datetime.cast("date").alias("pickup_date") ).\
        agg(
            count("*").alias("total_trips"),                             # total number of trips per day
            round(avg("passenger_count"), 1).alias("average_passengers"), # average passengers per trip
            round(avg("trip_distance"), 1).alias("average_distance"),     # average trip distance (miles)
            round(avg("fare_amount"), 2).alias("average_fare_per_trip"),   # average fare per trip ($)
            max("fare_amount").alias("max_fare"),                         # highest single-trip fare
            min("fare_amount").alias("min_fare"),                         # lowest single-trip fare
            round(sum("total_amount"), 2).alias("total_revenue")          # total revenue for the day ($)
        )

# COMMAND ----------

# Write the daily summary to a Unity Catalog managed Delta table in the gold schema
df.write.mode("append").saveAsTable("nyctaxi.03_gold.daily_trip_summary")

