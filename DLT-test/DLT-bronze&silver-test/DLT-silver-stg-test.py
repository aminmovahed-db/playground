# Databricks notebook source
import dlt
from pyspark.sql.functions import col
spark.conf.set("spark.databricks.sql.streamingTable.cdf.applyChanges.returnPhysicalCdf", True)

@dlt.view(
    name="v_brz_contract",
    comment="This is a view created from a CDF of contract"
)
def v_brz_contract():
    # Your query to read from the CDF
    return spark.readStream.format("delta") \
        .option("readChangeFeed", "true") \
        .table("main.nab_bronze_test_4.contract")

@dlt.view(
    name="v_brz_loan",
    comment="This is a view created from a CDF of contract"
)
def v_brz_loan():
    # Your query to read from the CDF
    return spark.readStream.format("delta") \
        .option("readChangeFeed", "true") \
        .table("main.nab_bronze_test_4.loan")

dlt.create_streaming_table( name = "staging_table_apnd_test")

@dlt.append_flow(name = "flow_1", target = "staging_table_apnd_test")
def flow_1():
    df = spark.readStream.table("live.v_brz_contract").drop("_change_type","_commit_version","_commit_timestamp")
    return df

@dlt.append_flow(name = "flow_2", target = "staging_table_apnd_test")
def flow_2():
    df = spark.readStream.table("live.v_brz_loan").drop("_change_type","_commit_version","_commit_timestamp")
    return df


dlt.create_streaming_table( name = "staging_table_mrg_test", table_properties = {"delta.enableChangeDataFeed": "true"} )

dlt.apply_changes(
    target = "staging_table_mrg_test",
    source = "staging_table_apnd_test",
    keys = ["CONTRACT_ID"],
    sequence_by = col("EXTRACT_DTTM"),
    except_column_list = ["__START_AT", "__END_AT"],
    stored_as_scd_type = 2
)



