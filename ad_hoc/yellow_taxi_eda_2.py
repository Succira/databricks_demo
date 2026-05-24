# Databricks notebook source
from pyspark.sql.functions import date_format, count, sum, desc

# COMMAND ----------

df = spark.read.table('nyctaxi.`02_silver`.taxi_zone_lookup')


import pyspark
def gcount(self, *col):
    return self.groupBy(*col).count()

def gsum(self, *col, measure):
    return self.groupBy(*col).sum(measure)

pyspark.sql.connect.dataframe.DataFrame.gcount = gcount
pyspark.sql.connect.dataframe.DataFrame.gsum = gsum

# df.display()
# df.gcount('borough',).display()
# df.gsum('borough', measure='location_id').display()


def gcount1(self, col):
    return self.groupBy(date_format(col, "yyyy-MM").alias("year_month")).\
                agg(count("*").alias("total_records")).\
                orderBy(desc("year_month")).display()
pyspark.sql.connect.dataframe.DataFrame.gcount1 = gcount1


# COMMAND ----------

taxi_type = 'green'

spark.read.table(f"nyctaxi.`01_bronze`.{taxi_type}_trips_raw").gcount1('lpep_pickup_datetime')

# COMMAND ----------

spark.read.table(f"nyctaxi.`02_silver`.{taxi_type}_trips_cleansed").gcount1('pickup_datetime')

# COMMAND ----------

spark.read.table(f"nyctaxi.`02_silver`.{taxi_type}_trips_enriched").gcount1('pickup_datetime')

# COMMAND ----------

spark.read.table("nyctaxi.`03_gold`.daily_trip_summary").\
    groupBy(date_format("pickup_date", "yyyy-MM").alias("year_month")).\
    agg(sum("total_trips").alias("total_records")).\
    orderBy(desc("year_month")).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog nyctaxi; select * from `nyctaxi`.`03_gold`.`daily_trip_summary` limit 300;
