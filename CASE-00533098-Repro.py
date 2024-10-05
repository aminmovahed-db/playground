# Databricks notebook source
bronze_schema = f'main.nab_shared_mode_test'
silver_schema = f'main.nab_shared_mode_test'

spark.conf.set ('db.bronze_schema', bronze_schema)
spark.conf.set ('db.silver_schema', silver_schema)

spark.conf.set("spark.databricks.analyzer.noSyncIsStreamingToCteReference", "true")
checkpoint_path = '/tmp/nab_shared_mode_test'

# COMMAND ----------

dbutils.fs.rm(checkpoint_path, True)

# COMMAND ----------

def simulate_framework_read():
    emp_df = spark.readStream.table(f"{bronze_schema}.employee")
    emp_df.createOrReplaceGlobalTempView("employee")

    mgr_df = spark.readStream.table(f"{bronze_schema}.employee_phone")
    mgr_df.createOrReplaceGlobalTempView("employee_phone")

    return spark.sql("""
                     WITH driver as (
                       SELECT DISTINCT * FROM (
                          select emp_id FROM global_temp.employee  
                          UNION ALL
                          select emp_id FROM global_temp.employee_phone
                       ) 
                     ),
                     src_raw AS (
                      SELECT 
                      e.emp_id
                      , e.emp_name
                      , e.emp_batch_no
                      , p.phone
                      , e.extract_dttm as EXTRACT_DTTM
                      FROM driver d 
                        LEFT JOIN ${db.bronze_schema}.employee e 
                          ON d.emp_id = e.emp_id 
                        LEFT JOIN ${db.bronze_schema}.employee_phone p 
                          ON d.emp_id = p.emp_id
                     )
                     SELECT 
                      src.* 
                     FROM src_raw src
                  """)

def upsertToDelta(microBatchOutputDF, batchId):
  microBatchOutputDF.createOrReplaceTempView("updates")
  microBatchOutputDF.sparkSession.sql("""
    MERGE INTO main.nab_shared_mode_test.target t
    USING updates s
    ON s.emp_id = t.emp_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)

def simulate_framework_write(df):
    (df.writeStream
     .trigger(once=True)
     .option("checkpointLocation", checkpoint_path)
     .foreachBatch(upsertToDelta)
     .outputMode("update")
     .start()
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${db.bronze_schema};
# MAGIC CREATE SCHEMA IF NOT EXISTS ${db.silver_schema};

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ${db.bronze_schema}.employee;
# MAGIC CREATE TABLE ${db.bronze_schema}.employee (
# MAGIC   EMP_ID string,
# MAGIC   EMP_NAME string,
# MAGIC   EMP_BATCH_NO string,
# MAGIC   MANAGER_ID string,
# MAGIC   EXTRACT_DTTM timestamp)
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC DROP TABLE IF EXISTS ${db.bronze_schema}.employee_phone;
# MAGIC CREATE TABLE ${db.bronze_schema}.employee_phone (
# MAGIC   EMP_ID string,
# MAGIC   PHONE string,
# MAGIC   EXTRACT_DTTM timestamp)
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC DROP TABLE IF EXISTS ${db.silver_schema}.target;
# MAGIC CREATE TABLE ${db.silver_schema}.target (
# MAGIC   EMP_ID string,
# MAGIC   EMP_NAME string,
# MAGIC   EMP_BATCH_NO string,
# MAGIC   PHONE string,
# MAGIC   EXTRACT_DTTM timestamp)
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${db.bronze_schema}.employee (
# MAGIC   EMP_ID,
# MAGIC   EMP_NAME,
# MAGIC   EMP_BATCH_NO,
# MAGIC   MANAGER_ID,
# MAGIC   EXTRACT_DTTM)
# MAGIC VALUES
# MAGIC   ('E1', 'E1-Name', 'B1-E1-1111', 'M1', '2024-01-02 00:00:00');
# MAGIC
# MAGIC
# MAGIC INSERT INTO TABLE ${db.bronze_schema}.employee_phone (
# MAGIC   EMP_ID,
# MAGIC   PHONE,
# MAGIC   EXTRACT_DTTM)
# MAGIC VALUES
# MAGIC   ('E1', '0411111111', '2024-01-02 00:00:00');

# COMMAND ----------

# DBTITLE 1,single user mode test
df = simulate_framework_read()
df = df.distinct()
simulate_framework_write(df)

# COMMAND ----------

dbutils.fs.ls(checkpoint_path)

# COMMAND ----------

dbutils.fs.head('dbfs:/tmp/nab_shared_mode_test/metadata')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC detail ${db.bronze_schema}.employee;

# COMMAND ----------

from pyspark.sql.functions import max, col

df = (spark.read
      .json('dbfs:/tmp/nab_shared_mode_test/offsets')
      .where("reservoirId is not null")
      .selectExpr('isStartingVersion', 'reservoirId', 'reservoirVersion', 'sourceVersion', '_metadata.file_path', '_metadata.file_name'))
display(df)

group_df = df.groupBy("reservoirId").agg(max("reservoirVersion").alias("reservoirVersion")).withColumn("reservoirVersion", col("reservoirVersion")-1)
display(group_df)

# COMMAND ----------

from pyspark.sql.functions import max, col

max_commit = spark.read.json('dbfs:/tmp/nab_shared_mode_test/commits').selectExpr('_metadata.file_name').agg(max('_metadata.file_name')).collect()[0][0]

df = (spark.read
      .json('dbfs:/tmp/nab_shared_mode_test/offsets')
      .where("reservoirId is not null")
      .where(f"_metadata.file_name = '{max_commit}'")).withColumn("Version", col("reservoirVersion") + col("index")).selectExpr('reservoirId', 'Version','_metadata.file_name').withColumnRenamed('_metadata.file_name', 'commit_version')
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.information_schema.tables where table_name = 'employee'-- and storage_sub_directory like '%6f68dbae-4811-48d9-9f28-231c93ca113d%'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC history ${db.bronze_schema}.employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC detail ${db.bronze_schema}.employee_phone;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${db.bronze_schema}.employee (
# MAGIC   EMP_ID,
# MAGIC   EMP_NAME,
# MAGIC   EMP_BATCH_NO,
# MAGIC   MANAGER_ID,
# MAGIC   EXTRACT_DTTM)
# MAGIC VALUES
# MAGIC   ('E2', 'E2-Name', 'B2-E2-1111', 'M2', '2024-01-02 00:00:00');
# MAGIC
# MAGIC INSERT INTO TABLE ${db.bronze_schema}.employee_phone (
# MAGIC   EMP_ID,
# MAGIC   PHONE,
# MAGIC   EXTRACT_DTTM)
# MAGIC VALUES
# MAGIC   ('E2', '0422222222', '2024-01-02 00:00:00');

# COMMAND ----------

# DBTITLE 1,shared mode cluster
df = simulate_framework_read()
df = df.distinct()
simulate_framework_write(df)

# COMMAND ----------

# DBTITLE 1,possible fix
#tried something like this at NAB but does not work
#confirmed df column order is consistent across runs
#single user mode seems to have a different column order regardless of what gets specified.
df = simulate_framework_read()
print(df.columns)
df = df.dropDuplicates(df.columns)
simulate_framework_write(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.nab_shared_mode_test.target
# MAGIC -- DROP TABLE main.nab_shared_mode_test.target
# MAGIC -- truncate table main.nab_shared_mode_test.target
