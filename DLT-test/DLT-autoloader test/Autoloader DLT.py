# Databricks notebook source
import dlt
from pyspark.sql.functions import *

spark.conf.set("spark.databricks.cloudFiles.recordEventChanges", "true")
spark.conf.set("spark.databricks.cloudFiles.optimizedEventSerialization.enabled", "true")


# The output will be ST

# Define configuration paths (source path, checkpoint, and destination)
source_path = "s3://one-env-uc-external-location/am-auto-loader-test/data"
# checkpoint_path = "s3://one-env-uc-external-location/am-auto-loader-test/checkpoints"

# Step 1: Declare the input data table using Auto Loader
@dlt.view(
    name="raw_v",
    comment="Ingest raw data incrementally from the source directory using Auto Loader."
)

def raw_v():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")  # Auto Loader format (CSV in this case)
            .option("header", "true")
            # .option("checkpointLocation", checkpoint_path)  # Set the custom checkpoint location
            .load(source_path).withColumn("processed_timestamp", current_timestamp())
    )

# Step 2: Transform the data (if needed) - Define another DLT table to manage transformations
dlt.create_streaming_table(
  name = "transformed_data",
  comment="Cleaned and transformed data.",
  table_properties = {"delta.enableChangeDataFeed": "true"}
)
dlt.apply_changes(
  flow_name="transformed_data_flow",
  target = "transformed_data",
  source = "raw_v",
  keys = ["id"],
  sequence_by = col("processed_timestamp"),
  except_column_list = [],
  stored_as_scd_type = 2
)


# COMMAND ----------

# import dlt
# from pyspark.sql.functions import *

# spark.conf.set("spark.databricks.cloudFiles.recordEventChanges", "true")
# spark.conf.set("spark.databricks.cloudFiles.optimizedEventSerialization.enabled", "true")

# # The output will be MV

# # Define configuration paths (source path, checkpoint, and destination)
# source_path = "s3://one-env-uc-external-location/am-auto-loader-test/data"
# checkpoint_path = "s3://one-env-uc-external-location/am-auto-loader-test/checkpoints"

# # Step 1: Declare the input data table using Auto Loader
# @dlt.table(
#   comment="Ingest raw data incrementally from the source directory using Auto Loader."
# )
# def raw_data():
#     return (
#         spark.readStream.format("cloudFiles")
#             .option("cloudFiles.format", "csv")  # Auto Loader format (CSV in this case)
#             .option("header", "true")
#             # .option("checkpointLocation", checkpoint_path)  # Set the custom checkpoint location
#             .load(source_path)
#     )

# # Step 2: Transform the data (if needed) - Define another DLT table to manage transformations
# @dlt.table(
#   comment="Cleaned and transformed data."
# )
# def transformed_data():
#     return (
#         dlt.read("raw_data")  # Reading from the 'raw_data' table
#             .withColumn("processed_timestamp", current_timestamp())
#               # Add a timestamp column
#     )

