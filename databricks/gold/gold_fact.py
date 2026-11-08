# Databricks notebook source
catalog = "workspace"
source_schema = "silver"
source_object = "silver_bookings"
target_schema = "gold"
targer_object = "fact_bookings"

cdc_col = "modified_date"
backdated_refresh = ""

fact_table = f"{catalog}.{target_schema}.{targer_object}"
source = f"{catalog}.{source_schema}.{source_object}"
exists = spark.catalog.tableExists(fact_table)
print(f"Target exists: {exists}")

# COMMAND ----------

dimensions = [
    {
        "table": "workspace.gold.dim_passengers",
        "alias": "dim_passengers",
        "sk": "dim_passengers_key",
        "join_keys": [("passenger_id", "passenger_id")] # (fact_col, dim_col)
    },

    {
        "table": "workspace.gold.dim_flights",
        "alias": "dim_flights",
        "sk": "dim_flights_key",
        "join_keys": [("flight_id", "flight_id")] # (fact_col, dim_col)
    },
    {
        "table": "workspace.gold.dim_airports",
        "alias": "dim_airports",
        "sk": "dim_airports_key",
        "join_keys": [("airport_id", "airport_id")] # (fact_col, dim_col)
    }
]

fact_columns = ["amount", "booking_date", "modified_date"]

# COMMAND ----------

## Last load date

if backdated_refresh:
    last_load = backdated_refresh # forced load
elif exists:
    last_load = spark.sql( f"""
    SELECT COALESCE(
        MAX({cdc_col}),
        TIMESTAMP('1900-01-01 00:00:00')
    )
    FROM {fact_table}
    """).collect()[0][0]
else:
    last_load = "1900-01-01 00:00:00"    
print(f"last load : {last_load}")

# COMMAND ----------

def generate_fact_query(fact_table, dimensions, fact_columns, cdc_col, last_date):

    select_fact = ", ".join(f"f.{c}" for c in fact_columns)
    select_dims = ", ".join(f"{d['alias']}.{d['sk']}" for d in dimensions)
    joins = ""
    for d in dimensions:
        join = f" LEFT JOIN {d['table']} {d['alias']} ON "
        for fc, dc in d["join_keys"]:
            join += f"f.{fc} = {d['alias']}.{dc} AND"
        join = join[:-4] + "\n" # remove last AND
        joins += join 
        
    return f"""
        SELECT {select_fact}, {select_dims}
        FROM {fact_table} f
        {joins}
        WHERE f.{cdc_col} > '{last_load}'
    """

query = generate_fact_query(source, dimensions, fact_columns, cdc_col, last_load)
print(query)
df_fact = spark.sql(query)

# COMMAND ----------

# display(df_fact)

# COMMAND ----------

## incremental load: upsert

from delta.tables import DeltaTable

fact_key_columns = ["dim_passengers_key", "dim_flights_key", "dim_airports_key", "booking_date"]
merge_conditions = " AND ".join(f"trg.{col} = src.{col}" for col in fact_key_columns)

if exists:
    DeltaTable.forName(spark, fact_table).alias('trg')\
            .merge(df_fact.alias('src'), merge_conditions)\
            .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
            .whenNotMatchedInsertAll()\
            .execute()

else:
    df_fact.write.format("delta").mode("append").saveAsTable(fact_table)


# COMMAND ----------

df = spark.sql(f"SELECT * from {fact_table}").groupby(fact_key_columns).count().filter("count > 1")
display(df)