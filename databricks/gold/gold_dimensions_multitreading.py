from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.functions import *
from delta.tables import DeltaTable

def build_dim(cfg):
    # parameters
    catalog = "workspace"
    source_schema = "silver"
    source_object = cfg["source_object"]
    target_schema = "gold"
    target_object = cfg["target_object"]
    keycols = cfg["key_columns"]
    surrogate_key = cfg["surrogate_key"]
    cdc_col = "modified_date"
    backdated_refresh = ""


    # keycols = eval(keycols)
    target = f"{catalog}.{target_schema}.{target_object}"
    source = f"{catalog}.{source_schema}.{source_object}"
    exists = spark.catalog.tableExists(target)
    print(f"Target exists: {exists}")
    print(f"[INFO] Processing {source} -> {target}")

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

    df_src = spark.sql(f"SELECT * FROM {source} WHERE {cdc_col} > '{last_load}' ")

    if exists:
        df_trg = spark.sql(f"SELECT {", ".join(keycols)}, {surrogate_key}, create_date, update_date FROM {target}")

    else: # we need only schema
        df_trg = df_src.select(*keycols).limit(0)\
                    .withColumn(surrogate_key, lit(None).cast("int"))\
                    .withColumn("create_date", lit(None).cast("timestamp"))\
                    .withColumn("update_date", lit(None).cast("timestamp"))
    
    join_cond = " AND ".join(f"src.{i} = trg.{i}" for i in keycols)

    # create temprory views from spark dataframes
    # views are session-scoped and threads run concurrently, so each call
    # needs a unique view name to avoid overwriting another thread's views
    src_view = f"src_{source_object}"
    trg_view = f"trg_{target_object}"
    df_src.createOrReplaceTempView(src_view)
    df_trg.createOrReplaceTempView(trg_view)

    df_join = spark.sql(f"SELECT src.*, trg.{surrogate_key}, trg.create_date, trg.update_date\
                        FROM {src_view} src\
                        LEFT JOIN {trg_view} trg\
                        ON {join_cond}\
                        ")

    df_old = df_join.filter(col(surrogate_key).isNotNull())
    df_new = df_join.filter(col(surrogate_key).isNull())

    # df_old : just need to update update_date col
    df_old = df_old.withColumn("update_date", current_timestamp())

    # df_new: add surrogate keys, create_date and update_date
    max_sk = spark.sql(f"SELECT COALESCE(max({surrogate_key}), 0) FROM {target}").collect()[0][0] if exists else 0

    df_new = df_new.withColumn(surrogate_key, lit(max_sk + 1) + monotonically_increasing_id())\
        .withColumn("create_date", current_timestamp())\
        .withColumn("update_date", current_timestamp())
    
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



configs = [
    {"source_object": "silver_flights",    "target_object": "dim_flights",
     "key_columns": ["flight_id"],       "surrogate_key": "dim_flights_key"},
    {"source_object": "silver_airports",   "target_object": "dim_airports",
     "key_columns": ["airport_id"],      "surrogate_key": "dim_airports_key"},
    {"source_object": "silver_passengers", "target_object": "dim_passengers",
     "key_columns": ["passenger_id"],    "surrogate_key": "dim_passengers_key"},
]

with ThreadPoolExecutor(max_workers=3) as pool:
    list(pool.map(build_dim, configs))