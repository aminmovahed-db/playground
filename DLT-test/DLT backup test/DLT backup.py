# Databricks notebook source
import datetime

# COMMAND ----------

def get_version(schema_name, table_name):
  if spark.catalog.tableExists(f"main.{schema_name}.{table_name}"):
    history_df = spark.sql(f"describe history main.{schema_name}.{table_name}")
    return history_df.select("version").collect()[0]["version"]
  else:
    return None

# COMMAND ----------

def get_backup_version_df(table_path):
  if spark.catalog.tableExists(table_path):
    df = spark.table(table_path)
    return df.filter(f"backup_status == 'Success'").groupBy(df.schema_name).agg(max(df.version).alias("backup_version"))
  else:
    return None

# COMMAND ----------

def clone_dlt(schema_name, table_name, backup_version):
  current_version = get_version(schema_name, table_name)
  if backup_version < current_version:
    source_version = get_version(source_schema, source_table)
    pipeline_id = spark.sql(f"DESCRIBE detail main.{schema_name}.{table_name}").select("properties").collect()[0]["properties"]['pipelines.pipelineId'].replace('-', '_')
    spark.sql(f"CREATE TABLE IF NOT EXISTS main.{schema_name}_target.{table_name}_backup CLONE __databricks_internal.__dlt_materialization_schema_{pipeline_id}.{table_name};")
    return (schema_name, table_name, "Sucess", current_version, {"source_schema": source_schema, "source_table": source_table, "source_version": source_version}, datetime.datetime.now())

# COMMAND ----------

def clone_table(schema_name, table_name, backup_version_df):
  if backup_version_df is not None:
    backup_version_f = backup_version_df.filter(f"schema_name == '{schema_name}' and table_name == '{table_name}'")
    if not backup_version_f.isEmpty():
      backup_version = backup_version_f.first()["backup_version"]
    else:
      backup_version = -1
  else:
    backup_version = -1
  result = clone_dlt(schema_name, table_name, backup_version)
  return result

# COMMAND ----------

def summary(clone_result, audit_table):
  if clone_result:
    df = spark.createDataFrame([clone_result], schema=["schema_name", "table_name", "status", "backup_version", "source_info", "timestamp"])
    if not spark.catalog.tableExists(audit_table):
      df.write.mode("overwrite").saveAsTable(audit_table)
    else:
      df.write.mode("append").saveAsTable(audit_table)

# COMMAND ----------

if __name__ == "__main__":
  schema_name = "nab_bronze_test_am"
  table_name = "contract_test"
  source_schema = "nab_staging_test_am"
  source_table = "contract"
  audit_table = "main.nab_bronze_test_am.backup_status"
  backup_version_df = get_backup_version_df(audit_table)
  clone_result = clone_table(schema_name, table_name, backup_version_df)
  summary(clone_result, audit_table)
