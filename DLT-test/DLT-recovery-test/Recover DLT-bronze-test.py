# Databricks notebook source


# COMMAND ----------

# import dlt
import json
from pyspark.sql.functions import col

class RecoverDltPipeline:
  def __init__(
    self,
    spark,
    config_json
    ):
        self.spark = spark
        self.config = self._init_pipeline_config(config_json)
        self.schema_name = self.config["schema_name"]
        self.table_name = self.config["table_name"]
        self.source_schema = self.config["source_schema"]
        self.source_table = self.config["source_table"]
        # self._create_pipeline()

  def _init_pipeline_config(self, config_json):
    """
    Initializes the pipeline configuration from a JSON string.
    """
    try:
        config = json.loads(config_json)
        print("Pipeline configuration successfully loaded.")
        return config
    except json.JSONDecodeError as e:
        print(f"Error parsing config: {e}")
        return {}
          
  def get_value(self):
    return self.schema_name
    
  def _create_pipeline(self):

  # def _create_table_run_once_restore_flow(self):
  #   print("Creating the table run once restore flow...")
  #   # TODO: this should be for all the tables in a schema
  #   view_name = f"v_run_once_{self.table_name}"
  #   flow_name = f"f_run_once_{self.table_name}"

  #   self.view = dlt.view(
  #           self._get_df,
  #           name=self.view_name,
  #           comment=f"input dataset view for {self.view_name}",
  #       )
    
  #   self._call_apply_changes(
  #       target_table=self.target_table, 
  #       source_view_name=view_name,
  #       cdc_apply_changes=self.cdc_apply_changes,
  #       flow_name=flow_name,
  #       additional_except_columns=additional_except_columns,
  #       run_once=True)
    # create the view for backup table
    # create apply changes from backup tables once true
    # create the view for source
    # create apply changes from source tables


  
  # self.view = dlt.view(
  #           self._get_df,
  #           name=self.view_name,
  #           comment=f"input dataset view for {self.view_name}",
  #       )


# COMMAND ----------

config = {
    "schema_name": "schema_name_value",
    "table_name": "table_name_value",
    "source_schema": "source_schema_value",
    "source_table": "source_table_value"
}

if __name__ == '__main__':  
  DLT = RecoverDltPipeline(spark,json.dumps(config))
  print(DLT.get_value())

# COMMAND ----------

spark.conf.set("spark.databricks.sql.streamingTable.cdf.applyChanges.returnPhysicalCdf", True)

@dlt.view(
    name="v_contract",
    comment="This is a view created from a CDF of contract"
)
def v_contract():
    # Your query to read from the CDF
    return spark.readStream.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", 0) \
        .table("main.nab_staging_test_am_target.contract_backup")


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

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

# COMMAND ----------

dlt.create_streaming_table(
  name = "contract_test",
  schema = schema,
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

