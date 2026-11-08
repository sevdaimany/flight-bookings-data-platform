# Databricks notebook source
# MAGIC %md
# MAGIC ### Incremental Data Injestion

# COMMAND ----------

dbutils.widgets.text("src", "")

# COMMAND ----------

src_value = dbutils.widgets.get("src")

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("cloudFiles.schemaLocation", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint")\
    .option("cloudFiles.schemaEvolutionMode", "rescue")\
    .load(f"/Volumes/workspace/raw/rawvolume/rawdata/{src_value}/")

# COMMAND ----------

query = df.writeStream.format("delta")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("checkpointLocation", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint")\
    .option("path", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/data")\
    .start()
query.awaitTermination()

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE delta.`/Volumes/workspace/bronze/bronzevolume/{src_value}/data/`
    SET TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.enableRowTracking' = 'true'
    )
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT *
# MAGIC -- FROM delta.`/Volumes/workspace/bronze/bronzevolume/bookings/data`

# COMMAND ----------

# volume_path = "/Volumes/workspace/bronze/bronzevolume/customers/data"

# df = spark.read.format("delta") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .load(volume_path)\
#     .filter("passenger_id = 'P0049'")
# display(df)

# COMMAND ----------

