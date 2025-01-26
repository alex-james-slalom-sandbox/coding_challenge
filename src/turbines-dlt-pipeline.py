# Databricks notebook source
import dlt
from pyspark.sql.functions import *

complete_records = {"complete_data": "wind_speed is not null and wind_direction is not null and power_output is not null"}

@dlt.table(comment="complete raw turbines data")
@dlt.expect_all_or_drop(complete_records)
def turbines_cleaned_raw_records():
  return (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", True)
    .load("/Volumes/main/bronze/turbines_raw/")
  )

@dlt.table(comment="summary stats view to identity and remove outliers")
@dlt.expect_or_drop("outliers", "outlier_flag = false")
def turbines_cleaned_aggregations():
    return (
        dlt.read("turbines_cleaned_raw_records")
        .groupBy("turbine_id", date_format(col("timestamp"), "yyyy-MM-dd").alias("day_loaded"))
        .agg(min("power_output").alias("min_output"),
             max("power_output").alias("max_output"),
             avg("power_output").alias("avg_output"),
            stddev("power_output").alias("stddev_output"))
        .withColumn("upper_bound", col("avg_output") + 2 * col("stddev_output"))
        .withColumn("lower_bound", col("avg_output") - 2 * col("stddev_output"))
        .withColumn("outlier_flag", (col("min_output") < col("lower_bound")) | (col("max_output") > col("upper_bound")))
        
    )

