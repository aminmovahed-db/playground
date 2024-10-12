# Databricks notebook source
csv_file = "data_1.csv"
source_path = f"file:/Workspace/Users/amin.movahed@databricks.com/NAB/DLT-test/DLT-autoloader test/csv_data/{csv_file}"
s3_destination_path = f"s3://one-env-uc-external-location/am-auto-loader-test/data/{csv_file}"
dbutils.fs.cp(source_path, s3_destination_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table main.nab_staging_test_am.transformed_data

# COMMAND ----------

dbutils.fs.rm(f"s3://one-env-uc-external-location/am-auto-loader-test/data/", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists main.nab_staging_test_am;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.nab_staging_test_am.transformed_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CLOUD_FILES_STATE(TABLE(main.nab_staging_test_am.transformed_data));

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT path FROM CLOUD_FILES_STATE(TABLE(__databricks_internal.__dlt_materialization_schema_57c55cde_ca4c_44ec_925d_c591bf63645a.__materialization_mat_transformed_data_1));
