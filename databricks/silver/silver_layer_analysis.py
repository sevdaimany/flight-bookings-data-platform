# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")

display(df)

# COMMAND ----------

df = df.withColumn("amount", df.amount.cast("double"))\
        .drop("_rescued_data")\
        .withColumn("modifiedDate", current_timestamp())\
        .withColumn("booking_date", to_date(col("booking_date")))
display(df)   

# COMMAND ----------

df = spark.read.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")

display(df)

# COMMAND ----------

df = spark.read.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")

display(df)

# COMMAND ----------

df = spark.read.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")

display(df)