# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists main.nab_staging_test_am.transformed_data

# COMMAND ----------

dbutils.fs.rm(f"s3://one-env-uc-external-location/am-auto-loader-test/data/", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists main.nab_staging_test_am;

# COMMAND ----------

csv_file = "data_0.csv"
source_path = f"file:/Workspace/Users/amin.movahed@databricks.com/NAB/playground/DLT-test/DLT-autoloader test/csv_data/{csv_file}"
s3_destination_path = f"s3://one-env-uc-external-location/am-auto-loader-test/data/{csv_file}"
dbutils.fs.cp(source_path, s3_destination_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.nab_staging_test_am.transformed_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CLOUD_FILES_STATE(TABLE(main.nab_staging_test_am.transformed_data));
