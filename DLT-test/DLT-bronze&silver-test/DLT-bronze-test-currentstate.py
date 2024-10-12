# Databricks notebook source
# MAGIC %md
# MAGIC ## One flow

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Table

# COMMAND ----------

# import dlt
# from pyspark.sql.functions import col
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType
# spark.conf.set("spark.databricks.sql.streamingTable.cdf.applyChanges.returnPhysicalCdf", True)

# @dlt.view(
#     name="v_contract",
#     comment="This is a view created from a CDF of contract"
# )
# def v_contract():
#     # Your query to read from the CDF
#     return spark.readStream.format("delta") \
#         .option("readChangeFeed", "true") \
#         .option("startingVersion", 0) \
#         .table("main.nab_staging_test_am.contract")


# schema = StructType([
#     StructField("CONTRACT_ID", IntegerType(), nullable=True),
#     StructField("CNTRCT_SHRT_NME", StringType(), nullable=True),
#     StructField("CONTRACT_BRAND_CDE", StringType(), nullable=True),
#     StructField("CONTRACT_TYPE_CDE", StringType(), nullable=True),
#     StructField("OPEN_DTE", DateType(), nullable=True),
#     StructField("CLOSE_DTE", DateType(), nullable=True),
#     StructField("EXTRACT_DTTM", TimestampType(), nullable=True),
#     StructField("EXTRACT_DTE", DateType(), nullable=True),
#     StructField("__START_AT", TimestampType(), nullable=True),
#     StructField("__END_AT", TimestampType(), nullable=True),
# ])


# dlt.create_streaming_table(
#   name = "contract_test",
#   schema = schema,
#   table_properties = {"delta.enableChangeDataFeed": "true"}
#   )

# dlt.apply_changes(
#   flow_name="contract_test_flow1",
#   target = "contract_test",
#   source = "v_contract",
#   keys = ["CONTRACT_ID"],
#   sequence_by = col("EXTRACT_DTTM"),
#   except_column_list = ["_change_type","_commit_version","_commit_timestamp"],
#   stored_as_scd_type = 2
# )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialised View

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType
spark.conf.set("spark.databricks.sql.streamingTable.cdf.applyChanges.returnPhysicalCdf", True)

@dlt.table(
    name="v_contract",
    comment="This is a view created from a CDF of contract"
)
def v_contract():
    # Your query to read from the CDF
    return spark.readStream.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", 0) \
        .table("main.nab_staging_test_am.contract")


schema = StructType([
    StructField("CONTRACT_ID", IntegerType(), nullable=True),
    StructField("CNTRCT_SHRT_NME", StringType(), nullable=True),
    StructField("CONTRACT_BRAND_CDE", StringType(), nullable=True),
    StructField("CONTRACT_TYPE_CDE", StringType(), nullable=True),
    StructField("OPEN_DTE", DateType(), nullable=True),
    StructField("CLOSE_DTE", DateType(), nullable=True),
    StructField("EXTRACT_DTTM", TimestampType(), nullable=True),
    StructField("EXTRACT_DTE", DateType(), nullable=True),
    StructField("__START_AT", TimestampType(), nullable=True),
    StructField("__END_AT", TimestampType(), nullable=True),
])


@dlt.table(
  comment="Cleaned and transformed data."
)
def transformed_data():
    return (
        dlt.read("v_contract")  # Reading from the 'raw_data' table
            .withColumn("processed_timestamp", current_timestamp())
              # Add a timestamp column
    )

