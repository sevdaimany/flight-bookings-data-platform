from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_date,
    expr
)

# =========================================================================
#  BOOKINGS TRANSFORMATIONS

@dp.temporary_view(name = "trans_bookings")
def trans_bookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")\
        .withColumn("amount",col("amount").cast("double"))\
        .drop("_rescued_data")\
        .withColumn("modified_date", current_timestamp())\
        .withColumn("booking_date", to_date(col("booking_date")))

    return df

rules_bookings = {"rule1": "booking_id IS NOT NULL",
                  "rule2": "passenger_id IS NOT NULL"} 
@dp.table(name = "silver_bookings")
@dp.expect_all_or_drop(rules_bookings)
def silver_bookings():
    return spark.readStream.table("trans_bookings")


# FLIGHTS TRANSFORMATIONS

@dp.temporary_view(name = "trans_flights")
def trans_flights():
    df = spark.readStream.format("delta")\
        .option("readChangeFeed", "true")\
        .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")\
        .drop("_rescued_data")\
        .withColumn("modified_date", current_timestamp())\
        .withColumn("flight_date", to_date(col("flight_date")))\
        .withColumnRenamed("_change_type", "source_change_type")\
        .withColumnRenamed("_commit_version", "source_commit_version")\
        .withColumnRenamed("_commit_timestamp", "source_commit_timestamp")\
        .filter(col("source_change_type") != "update_preimage")


    return df

# AUTO CDC
dp.create_streaming_table("silver_flights")
dp.create_auto_cdc_flow(
    target = "silver_flights",
    source = "trans_flights",
    keys=["flight_id"],                                # upsert key
    sequence_by=col("source_commit_version"),                    # which version of a row wins
    apply_as_deletes=expr("source_change_type = 'delete'"),
    except_column_list=[
        "source_change_type",
        "source_commit_version",
        "source_commit_timestamp"
    ],
    stored_as_scd_type=1,                              # overwrite, no history
)


# ===========================================================================
# PASSENGER TRANSFORMATIONS

@dp.temporary_view(name = "trans_passengers")
def trans_passengers():
    df = spark.readStream.format("delta")\
        .option("readChangeFeed", "true") \
        .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")\
        .drop("_rescued_data")\
        .withColumn("modified_date", current_timestamp())\
        .withColumnRenamed("_change_type", "source_change_type")\
        .withColumnRenamed("_commit_version", "source_commit_version")\
        .withColumnRenamed("_commit_timestamp", "source_commit_timestamp")\
        .filter(col("source_change_type") != "update_preimage")

    return df

# AUTO CDC
dp.create_streaming_table("silver_passengers")
dp.create_auto_cdc_flow(
    target = "silver_passengers",
    source = "trans_passengers",
    keys=["passenger_id"],                             # upsert key
    sequence_by=col("source_commit_version"),                    # which version of a row wins
    apply_as_deletes=expr("source_change_type = 'delete'"),
    except_column_list=[
        "source_change_type",
        "source_commit_version",
        "source_commit_timestamp"
    ],
    stored_as_scd_type=1,                              # overwrite, no history
)

# ===========================================================================
# AIRPORTS TRANSFORMATIONS

@dp.temporary_view(name = "trans_airports")
def trans_airports():
    df = spark.readStream.format("delta")\
        .option("readChangeFeed", "true") \
        .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")\
        .drop("_rescued_data")\
        .withColumn("modified_date", current_timestamp())\
        .withColumnRenamed("_change_type", "source_change_type")\
        .withColumnRenamed("_commit_version", "source_commit_version")\
        .withColumnRenamed("_commit_timestamp", "source_commit_timestamp")\
        .filter(col("source_change_type") != "update_preimage")

    return df

# AUTO CDC
dp.create_streaming_table("silver_airports")
dp.create_auto_cdc_flow(
    target = "silver_airports",
    source = "trans_airports",
    keys=["airport_id"],                               # upsert key
    sequence_by=col("source_commit_version"),                    # which version of a row wins
    apply_as_deletes=expr("source_change_type = 'delete'"),
    except_column_list=[
        "source_change_type",
        "source_commit_version",
        "source_commit_timestamp"
    ],
    stored_as_scd_type=1,                              # overwrite, no history
)

# ===========================================================================
# SILVER BUISINESS
@dp.materialized_view(name="silver_business")
def silver_business():
    bookings = spark.read.table("silver_bookings")
    airports = spark.read.table("silver_airports").drop("modified_date")
    flights = spark.read.table("silver_flights").drop("modified_date")
    passengers = spark.read.table("silver_passengers").drop("modified_date")
    
    return bookings.join(flights, "flight_id", "left")\
        .join(passengers, "passenger_id", "left")\
        .join(airports, "airport_id", "left")





