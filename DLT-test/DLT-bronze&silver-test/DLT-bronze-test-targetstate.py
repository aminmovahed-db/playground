# Databricks notebook source
# MAGIC %md
# MAGIC ## One flow
# MAGIC Object oriented

# COMMAND ----------

import dlt
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType
spark.conf.set("spark.databricks.sql.streamingTable.cdf.applyChanges.returnPhysicalCdf", True)

class DltTest:
  def __init__(self, spark):
    self.spark = spark
    dlt.view(
    self._temp,
    name="v_contract",
    comment="This is a view created from a CDF of contract"
    )
    self._setup()

  def _temp(self):
    return self.spark.readStream.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", 0) \
            .table("main.nab_staging_test_am.contract")

  def _schema(self):
    return StructType([
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

  def _setup(self):
    dlt.create_streaming_table(
    name = "contract_test",
    schema = self._schema(),
    table_properties = {"delta.enableChangeDataFeed": "true"}
    )
    dlt.apply_changes(
      target = "contract_test",
      source = "v_contract",
      keys = ["CONTRACT_ID"],
      sequence_by = col("EXTRACT_DTTM"),
      except_column_list = ["_change_type","_commit_version","_commit_timestamp"],
      stored_as_scd_type = 2
    )

DltTest(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Two flows
# MAGIC Simple

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

# @dlt.view(
#     name="v2_contract",
#     comment="This is a view created from a CDF of contract"
# )
# def v2_contract():
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

# dlt.apply_changes(
#   flow_name="contract_test_flow2",
#   target = "contract_test",
#   source = "v2_contract",
#   once=True,
#   keys = ["CONTRACT_ID"],
#   sequence_by = col("EXTRACT_DTTM"),
#   except_column_list = ["_change_type","_commit_version","_commit_timestamp"],
#   stored_as_scd_type = 2
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## One flow
# MAGIC Simple

# COMMAND ----------

# import dlt
# from pyspark.sql.functions import col
# spark.conf.set("spark.databricks.sql.streamingTable.cdf.applyChanges.returnPhysicalCdf", True)

# @dlt.view(
#     name="v_loan",
#     comment="This is a view created from a CDF of contract"
# )
# def v_loan():
#     # Your query to read from the CDF
#     return spark.readStream.format("delta") \
#         .option("readChangeFeed", "true") \
#         .option("startingVersion", 0) \
#         .table("main.nab_staging_test_am.loan")

#  from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, DecimalType

# schema = StructType([
#     StructField("LOAN_ID", IntegerType(), nullable=True),
#     StructField("CONTRACT_ID", IntegerType(), nullable=True),
#     StructField("LOAN_TYPE_CDE", StringType(), nullable=True),
#     StructField("LOAN_STATUS_CDE", StringType(), nullable=True),
#     StructField("VALUE", DecimalType(22, 6), nullable=True),
#     StructField("EXTRACT_DTTM", TimestampType(), nullable=True),
#     StructField("EXTRACT_DTE", DateType(), nullable=True),
#     StructField("__START_AT", TimestampType(), nullable=True),
#     StructField("__END_AT", TimestampType(), nullable=True),
# ])

# dlt.create_streaming_table(
#   name = "loan_test",
#   schema = schema,
#   table_properties = {"delta.enableChangeDataFeed": "true"}
#   )

# dlt.apply_changes(
#   target = "loan_test",
#   source = "v_loan",
#   keys = ["LOAN_ID"],
#   sequence_by = col("EXTRACT_DTTM"),
#   except_column_list = ["_change_type","_commit_version","_commit_timestamp"],
#   stored_as_scd_type = 2
# )
