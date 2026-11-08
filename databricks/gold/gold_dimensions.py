# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Gold layer dimention tables:
# MAGIC * Get the silver objects, add surrogate_key, create_date and update_date
# MAGIC * process only incrementally loaded data
# MAGIC * Insert new records and update existing ones
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM workspace.silver.silver_passengers where passenger_id = "P0049";
# MAGIC SELECT * FROM workspace.gold.dim_passengers where passenger_id = "P0049";
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters

# COMMAND ----------

catalog = "workspace"
source_schema = "silver"
source_object = "silver_flights"
target_schema = "gold"
targer_object = "dim_flights"
keycols = "['flight_id']"
surrogate_key = "dim_flights_key"
cdc_col = "modified_date"
backdated_refresh = ""


keycols = eval(keycols)
target = f"{catalog}.{target_schema}.{targer_object}"
source = f"{catalog}.{source_schema}.{source_object}"
exists = spark.catalog.tableExists(target)
print(f"Target exists: {exists}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1) last load date

# COMMAND ----------

if backdated_refresh:
    last_load = backdated_refresh # forced load
elif exists:
    last_load = spark.sql( f"""
    SELECT COALESCE(
        MAX({cdc_col}),
        TIMESTAMP('1900-01-01 00:00:00')
    )
    FROM {target}
    """).collect()[0][0]
else:
    last_load = "1900-01-01 00:00:00"    
print(f"last load : {last_load}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2) read only new or changed rows

# COMMAND ----------

df_src = spark.sql(f"SELECT * FROM {source} WHERE {cdc_col} > '{last_load}' ")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3) target dataframe

# COMMAND ----------

if exists:
    df_trg = spark.sql(f"SELECT {", ".join(keycols)}, {surrogate_key}, create_date, update_date FROM {target}")

else: # we need only schema
    df_trg = df_src.select(*keycols).limit(0)\
                .withColumn(surrogate_key, lit(None).cast("int"))\
                .withColumn("create_date", lit(None).cast("timestamp"))\
                .withColumn("update_date", lit(None).cast("timestamp"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4) Join src and trg and split processed and new rows

# COMMAND ----------

join_cond = " AND ".join(f"src.{i} = trg.{i}" for i in keycols)

# create temprory views from spark dataframes
df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")

df_join = spark.sql(f"SELECT src.*, trg.{surrogate_key}, trg.create_date, trg.update_date\
                    FROM src\
                    LEFT JOIN trg\
                    ON {join_cond}\
                    ")

df_old = df_join.filter(col(surrogate_key).isNotNull())
df_new = df_join.filter(col(surrogate_key).isNull())


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5) enrich new and updated data frames differently

# COMMAND ----------

# df_old : just need to update update_date col
df_old = df_old.withColumn("update_date", current_timestamp())

# df_new: add surrogate keys, create_date and update_date
max_sk = spark.sql(f"SELECT COALESCE(max({surrogate_key}), 0) FROM {target}").collect()[0][0] if exists else 0

df_new = df_new.withColumn(surrogate_key, lit(max_sk + 1) + monotonically_increasing_id())\
    .withColumn("create_date", current_timestamp())\
    .withColumn("update_date", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6) Union and Write

# COMMAND ----------

df_union = df_old.unionByName(df_new)      # matches by column name, not position

if exists:
    # merge: upsert happens in the following order:
    # 1 - matched rows where src.{cdc_col} >= trg.{cdc_col} are updated
    # 2 - unmatched rows are inserted
    DeltaTable.forName(spark, target).alias("trg")\
        .merge(df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}")\
        .whenMatchedUpdateAll(condition=f"src.{cdc_col} >= trg.{cdc_col}")\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_union.write.format("delta")\
        .mode("append")\
        .saveAsTable(target)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from workspace.gold.dim_flights;