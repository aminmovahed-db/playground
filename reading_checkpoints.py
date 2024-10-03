from pyspark.sql.functions import max, col

max_commit = spark.read.json('dbfs:/tmp/nab_shared_mode_test/commits').selectExpr('_metadata.file_name').agg(max('_metadata.file_name')).collect()[0][0]

df = (spark.read
      .json('dbfs:/tmp/nab_shared_mode_test/offsets')
      .where("reservoirId is not null")
      .where(f"_metadata.file_name = '{max_commit}'")).withColumn("Version", col("reservoirVersion") + col("index")).selectExpr('reservoirId', 'Version','_metadata.file_name').withColumnRenamed('_metadata.file_name', 'commit_version')
display(df)