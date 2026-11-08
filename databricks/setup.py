# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw.rawvolume

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/bookings")
dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/flights")
dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/customers")
dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/airports")



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA workspace.bronze;
# MAGIC CREATE SCHEMA workspace.silver;
# MAGIC CREATE SCHEMA workspace.gold;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME workspace.bronze.bronzevolume;
# MAGIC CREATE VOLUME workspace.silver.silvervolume;
# MAGIC CREATE VOLUME workspace.gold.goldvolume;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES delta.`/Volumes/workspace/bronze/bronzevolume/flights/data/`;