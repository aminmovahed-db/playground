# Databricks notebook source
def get_table_name(schema_name, table_name):
  table_format = spark.sql(f"Describe extended main.{schema_name}.{table_name}").where("col_name == 'Type'").select("data_type").first()[0]
  return table_format
print(get_table_name("nab_bronze_test_am", "v_contract"))

# COMMAND ----------

def get_job_id(table_full_name):
  history_df = spark.sql(f"DESCRIBE HISTORY {table_full_name}")
  return history_df.collect()

display(get_job_id("main.nab_bronze_test_am.v_contract"))

# COMMAND ----------

def get_version(table_full_name):
  history_df = spark.sql(f"DESCRIBE HISTORY {table_full_name}")
  return history_df.first()["version"]

print(get_version("main.nab_bronze_test_am.v_contract"))

# COMMAND ----------

def get_table_name(schema_name, table_name):
  table_properties = spark.sql(f"Describe detail main.{schema_name}.{table_name}").select("properties").first()[0]
  return 'dlt' if any('pipeline' in key for key in table_properties) else 'delta'
display(get_table_name("default", "main_tables_info"))

# COMMAND ----------

import json
import pyspark.sql.types as T
import pyspark.sql.functions as F

# Define the schema
schema = T.StructType([
    T.StructField("id", T.StringType(), True),
    T.StructField("table_info", T.StringType(), True)
])

# Sample data for the DataFrame
data = [
    ("1", json.dumps({"type": "delta", "mode": "batch"})),
    ("2", json.dumps({"type": "dlt", "mode": "streaming"}))
]
df = spark.createDataFrame(data, schema)
print(df.select(F.get_json_object(F.col("table_info"), "$.mode")).first()[0])

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe extended main.nab_shared_mode_test.target

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.information_schema.tables where table_schema = 'nab_shared_mode_test' and table_name = 'target'
