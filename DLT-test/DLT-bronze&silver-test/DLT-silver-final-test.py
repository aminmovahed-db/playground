# Databricks notebook source
import dlt
from pyspark.sql.functions import col
spark.conf.set("spark.databricks.sql.streamingTable.cdf.applyChanges.returnPhysicalCdf", True)

@dlt.view(
    name="v_final_cdf_feed",
    comment="This is a view created from a CDF of staging_table_mrg_test"
)
def v_final_cdf_feed():
    # Your query to read from the CDF
    return spark.readStream.format("delta") \
        .option("readChangeFeed", "true") \
        .table("main.nab_silver_test_4.staging_table_mrg_test")

@dlt.view(
    name="v_final_transform",
    comment="This is a view created from a CDF of v_final_cdf_feed"
)
def v_final_transform():
    # Your query to read from the CDF
    return spark.sql("""
                     SELECT
                        MD5(CONCAT_WS('', 'AFS', 'AF', LPAD(CONTRACT_ID,9,0))) AS AR_ID
                        , OPEN_DTE AS OPEN_DTE
                        , CASE
                            WHEN CLOSE_DTE IS NULL THEN CAST('2999-12-31' AS TIMESTAMP)
                            ELSE CLOSE_DTE
                            END AS AR_CLOSE_DTE
                        , COALESCE(CONTRACT_BRAND_CDE, 'UNKWN') AS BRND_CDE
                        , COALESCE(CONTRACT_TYPE_CDE, 'UNKWN') AS CONTRACT_TYPE_CDE
                        , CNTRCT_SHRT_NME AS AR_SHRT_NAME
                        , CASE 
                            WHEN CONTRACT_BRAND_CDE = 'B1' AND LOAN_STATUS_CDE = 'O1' THEN 'CATEGORY 1'
                            WHEN CONTRACT_BRAND_CDE = 'B2' AND LOAN_STATUS_CDE = 'O1' THEN 'CATEGORY 2'
                            ELSE 'CATEGORY 3'
                            END AS CATEGORY
                        , LOAN_TYPE_CDE AS TYPE_CDE
                        , LOAN_STATUS_CDE AS STATUS_CDE
                        , value AS CT_VALUE
                        , 'AF' AS DATA_SRCE_CDE
                        , 'AFS' AS DATA_SGRGTN_CDE
                        , __START_AT
                    FROM stream(live.v_final_cdf_feed)
        """
    )

dlt.create_streaming_table( name = "example_target_test", table_properties = {"delta.enableChangeDataFeed": "true"} )

dlt.apply_changes(
    target = "example_target_test",
    source = "v_final_transform",
    keys = ["AR_ID","DATA_SGRGTN_CDE"],
    sequence_by = col("__START_AT"),
    except_column_list = ["__START_AT"],
    stored_as_scd_type = 2
)



