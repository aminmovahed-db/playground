"""Module providing a function printing python version."""

import json
import functools
from datetime import datetime

from databricks.connect import DatabricksSession
from pyspark.sql import Row
from pyspark.sql import types as T

spark = DatabricksSession.builder.getOrCreate()


class RestoreSilver:

    def __init__(self, spark_session, json_payload, audit_table):
        self.spark = spark_session
        self.audit_df = self.spark.read.table(audit_table)
        self._read_config(json_payload)
        # self._revert_table()

    def show_config(self):
        print(f"table_catalog: {self.table_catalog}")
        print(f"table_schema: {self.table_schema}")
        print(f"table_name: {self.table_name}")
        print(f"version: {self.version}")
        print(f"use_checkpoints: {self.use_checkpoints}")

    @functools.lru_cache(maxsize=1)
    def _table_history(self):
        return self.spark.sql(
            f"DESCRIBE HISTORY {self.table_catalog}.{self.table_schema}.{self.table_name}"
        ).select("version", "timestamp")

    def _show(self, df):
        df.show()

    def _display(self, df):
        pandas_df = df.toPandas()
        print(pandas_df)

    def show_table_history(self):
        self._show(self._table_history())

    def display_table_history(self):
        self._display(self._table_history())

    def _read_config(self, json_payload):
        try:
            config = json.loads(json_payload)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        self.table_catalog = config.get("table_catalog")
        self.table_schema = config.get("table_schema")
        self.table_name = config.get("table_name")
        self.version = int(config.get("version"))
        self.use_checkpoints = config.get("use_checkpoints").lower() == "true"

    def _version_exits_in_history(self):
        df = self._table_history()
        return df.filter(df.version == self.version).count() > 0

    # def _revert_table(self):
    #     if self._version_exits_in_history():
    #         self._time_travel()
    #     else:
    #         self._restore_backup()

    def _time_travel(self):
        self.spark.sql(
            f"RESTORE TABLE {self.table_catalog}.{self.table_schema}.{self.table_name} TO VERSION {self.version}"
        )

    # def _restore_backup(self):


# Example usage
if __name__ == "__main__":
    JSON_PAYLOAD = """
    {
        "table_name": "target",
        "table_schema": "nab_shared_mode_test",
        "table_catalog": "main",
        "version": "1",
        "use_checkpoints": "false"
    }
    """
    AUDIT_TBL = "main.nab_shared_mode_test.audit"

    # for testing purposes
    schema = T.StructType(
        [
            T.StructField("layer", T.StringType(), True),
            T.StructField("table_schema", T.StringType(), True),
            T.StructField("table_name", T.StringType(), True),
            T.StructField("backup_status", T.StringType(), True),
            T.StructField("exception", T.StringType(), True),
            T.StructField("version", T.LongType(), True),
            T.StructField("source_info", T.StringType(), True),
            T.StructField("backup_timestamp", T.TimestampType(), True),
            T.StructField("job_id", T.StringType(), True),
        ]
    )

    test_data = [
        Row(
            layer="raw",
            table_schema="public",
            table_name="customers",
            backup_status="success",
            exception=None,
            version=1,
            source_info="source_A",
            backup_timestamp=datetime(2024, 10, 20, 12, 30, 45),
            job_id="job_1234",
        ),
    ]

    spark.createDataFrame(test_data, schema).write.mode("overwrite").saveAsTable(AUDIT_TBL)
    restore_silver = RestoreSilver(spark, JSON_PAYLOAD, AUDIT_TBL)
    restore_silver.show_config()
    restore_silver.display_table_history()
    print(restore_silver._version_exits_in_history())
