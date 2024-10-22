"""Module providing a function printing python version."""

import json
import functools
from datetime import datetime

from databricks.connect import DatabricksSession
from pyspark.dbutils import DBUtils

from pyspark.sql import Row
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException


spark = DatabricksSession.builder.getOrCreate()
dbutils = DBUtils(spark)


class RestoreSilver:

    def __init__(self, spark_session, json_payload, audit_table):
        self.spark = spark_session
        self._read_config(json_payload)
        self.audit_df = self._read_audit_table(audit_table)
        self.source_info = self._get_source_info()
        self.table_info = self._get_table_info()
        self.batch = self.table_info.get("batch") == "true"

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

    def _read_json(self, json_payload):
        try:
            return json.loads(json_payload)
        except json.JSONDecodeError as e:
            raise ValueError(f"Error decoding JSON: {e}") from e

    def _read_config(self, json_payload):
        config = self._read_json(json_payload)
        self.table_catalog = config.get("table_catalog")
        self.dr_table_catalog = config.get("dr_table_catalog")
        self.table_schema = config.get("table_schema")
        self.table_name = config.get("table_name")
        self.version = int(config.get("version"))
        self.use_checkpoints = config.get("use_checkpoints").lower() == "true"

    def _read_audit_table(self, audit_table):
        return self.spark.read.table(audit_table).filter(
            f"table_name = '{self.table_name}' AND table_schema = '{self.table_schema}'"
        )

    def _check_version_availability(self, table_full_name, version_number):
        try:
            self.spark.read.format("delta").option("versionAsOf", version_number).table(
                table_full_name
            ).limit(1)
            return True

        except AnalysisException as e:
            if "Cannot time travel Delta table to version" in str(e):
                return False
            raise e

    def revert_table(self):
        table_full_name = f"{self.table_catalog}.{self.table_schema}.{self.table_name}"
        backup_version = self._get_backup_version()

        if self._check_version_availability(table_full_name, self.version):
            print(f"Time travel in progress for table {table_full_name} to version {self.version}")
            self._time_travel()
        elif self._check_version_availability(
            f"{self.dr_table_catalog}.{self.table_schema}.{self.table_name}", backup_version
        ):
            print(f"Restoring table {table_full_name} from backup version {backup_version}")
            self._restore_backup(backup_version)
        else:
            raise RuntimeError(
                f"Backup version {backup_version} not available for table {table_full_name}"
            )

    def _time_travel(self):
        try:
            self.spark.sql(
                f"RESTORE TABLE {self.table_catalog}.{self.table_schema}."
                f"{self.table_name} TO VERSION AS OF {self.version}"
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to restore table {self.table_catalog}.{self.table_schema}."
                f"{self.table_name} to version {self.version}: {e}"
            ) from e

    def _restore_backup(self, backup_version):
        try:
            self.spark.sql(
                f"""
            CREATE OR REPLACE TABLE {self.table_catalog}.{self.table_schema}.{self.table_name}
            DEEP CLONE {self.dr_table_catalog}.{self.table_schema}.{self.table_name}_backup
            VERSION AS OF {backup_version};
            """
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to restore backup version {backup_version} for table "
                f"{self.table_catalog}.{self.table_schema}.{self.table_name}: {e}"
            ) from e

    def _get_backup_version(self):
        backup_version_row = (
            self.audit_df.filter(f"table_version = '{self.version}'")
            .select("backup_version")
            .sort("backup_timestamp", ascending=False)
            .first()
        )
        if backup_version_row is None:
            raise ValueError(
                f"No backup version found for table {self.table_name} with version {self.version}"
            )
        return backup_version_row[0]

    def _get_source_info(self):
        source_info = self.audit_df.select("source_info").first()[0]
        return self._read_json(source_info)

    def _get_table_info(self):
        table_info = self.audit_df.select("table_info").first()[0]
        return self._read_json(table_info)

    def revert_checkpoints(self):
        if self.batch:
            print("Skipping checkpoint restoration for batch tables")
            return
        else:
            print("Trying to revert the streaming checkpoints...")
            self._revert_checkpoints()

    def _check_commit(self, commit, cp_path):
        commits_path = f"{cp_path}/commits/{commit}"
        offsets_path = f"{cp_path}/offsets/{commit}"
        return dbutils.fs.exists(commits_path) and dbutils.fs.exists(offsets_path)

    def _commits_available(self):
        for file in self.source_info["files"]:
            if not self._check_commit(file["commit"], file["cp_path"]):
                return False
        return True

    def _backup_checkpoints(self):
        for file in self.source_info["files"]:
            dbutils.fs.cp(file["cp_path"], f"{file['cp_path']}_backup", recurse=True)

    def _delete_checkpoints(self, checkpoint_path, commit_number):
        if commit_number == -1:
            dbutils.fs.rm(checkpoint_path, recurse=True)

        for folder_path in [f"{checkpoint_path}/commits", f"{checkpoint_path}/offsets"]:
            for folder in [
                (file_info.name, file_info.path) for file_info in dbutils.fs.ls(folder_path)
            ]:
                if folder[0] > commit_number:
                    dbutils.fs.rm(folder[1], recurse=True)

    def _restore_checkpoints(self, reset=False):
        for file in self.source_info["files"]:
            if reset:
                file["commit"] = -1
            self._delete_checkpoints(file["cp_path"], file["commit"])

    def _revert_checkpoints(self):
        if self.use_checkpoints:
            if self._commits_available():
                print("Checkpoints will be restored for this table")
                self._restore_checkpoints()
            else:
                print(
                    "All checkpoints are not available for this table"
                    " - startingVersion should be used"
                )
                self.use_checkpoints = False
                self._restore_checkpoints(reset=True)
        else:
            print("startingVersion will be used for this table")
            self._restore_checkpoints(reset=True)

    def generate_output_config(self):

        def recursive_filter(d, keys):
            if isinstance(d, dict):
                return {k: recursive_filter(v, keys) for k, v in d.items() if k in keys}
            elif isinstance(d, list):
                return [recursive_filter(item, keys) for item in d]
            return d

        if self.batch:
            return json.dumps(
                {
                    "table": f"{self.table_catalog}.{self.table_schema}.{self.table_name}",
                    "source_info": "NA - run as batch table",
                }
            )
        if self.use_checkpoints:
            return json.dumps(
                {
                    "table": f"{self.table_catalog}.{self.table_schema}.{self.table_name}",
                    "source_info": recursive_filter(
                        self.source_info, ["source_type", "files", "sources", "table"]
                    ),
                }
            )
        return json.dumps(
            {
                "table": f"{self.table_catalog}.{self.table_schema}.{self.table_name}",
                "source_info": recursive_filter(
                    self.source_info, ["source_type", "files", "sources", "table", "version"]
                ),
            }
        )


def create_audit_table(audit_table):
    # for testing purposes
    schema = T.StructType(
        [
            T.StructField("layer", T.StringType(), True),
            T.StructField("table_schema", T.StringType(), True),
            T.StructField("table_name", T.StringType(), True),
            T.StructField("backup_status", T.StringType(), True),
            T.StructField("exception", T.StringType(), True),
            T.StructField("table_version", T.LongType(), True),
            T.StructField("backup_version", T.LongType(), True),
            T.StructField("source_info", T.StringType(), True),
            T.StructField("backup_timestamp", T.TimestampType(), True),
            T.StructField("job_id", T.StringType(), True),
            T.StructField("table_info", T.StringType(), True),
        ]
    )

    # source_info = json.dumps(
    #     {
    #         "source_type": "delta",
    #         "sources": [
    #             {
    #                 "table": "main.nab_shared_mode_test.employee",
    #                 "table_id": "646f1a23-4c5d-4449-8acc-959e5a525887",
    #                 "commit": "delta",
    #                 "version": "delta",
    #                 "file_name": "file_1.sql",
    #                 "cp_path": "/Volumes/main/nab_shared_mode_test/checkpoints",
    #             },
    #             {
    #                 "table": "main.nab_shared_mode_test.employee_phone",
    #                 "table_id": "ce09ac30-7dde-4dde-b114-63433a674100",
    #                 "commit": "delta",
    #                 "version": "delta",
    #                 "file_name": "file_1.sql",
    #                 "cp_path": "/Volumes/main/nab_shared_mode_test/checkpoints",
    #             },
    #         ],
    #     }
    # )

    source_info = json.dumps(
        {
            "source_type": "delta",
            "files": [
                {
                    "commit": "delta",
                    "file_name": "file_1.sql",
                    "cp_path": "/Volumes/main/nab_shared_mode_test/checkpoints",
                    "incr_type": "multi",
                    "sources": [
                        {
                            "table": "main.nab_shared_mode_test.employee",
                            "table_id": "646f1a23-4c5d-4449-8acc-959e5a525887",
                            "version": "delta",
                        },
                        {
                            "table": "main.nab_shared_mode_test.employee_phone",
                            "table_id": "ce09ac30-7dde-4dde-b114-63433a674100",
                            "version": "delta",
                        },
                    ],
                },
                {
                    "commit": "delta",
                    "file_name": "file_2.sql",
                    "cp_path": "/Volumes/main/nab_shared_mode_test/checkpoints",
                    "incr_type": "multi",
                    "sources": [
                        {
                            "table": "main.nab_shared_mode_test.employee",
                            "table_id": "646f1a23-4c5d-4449-8acc-959e5a525887",
                            "version": "delta",
                        },
                        {
                            "table": "main.nab_shared_mode_test.employee_phone",
                            "table_id": "ce09ac30-7dde-4dde-b114-63433a674100",
                            "version": "delta",
                        },
                    ],
                },
            ],
        }
    )

    table_info = json.dumps({"table_format": "delta", "batch": "false"})

    test_data = [
        Row(
            layer="silver",
            table_schema="nab_shared_mode_test",
            table_name="target",
            backup_status="success",
            exception=None,
            table_version=1,
            backup_version=1,
            source_info=source_info,
            backup_timestamp=datetime(2024, 10, 18, 12, 30, 45),
            job_id="job_1234",
            table_info=table_info,
        ),
        Row(
            layer="silver",
            table_schema="nab_shared_mode_test",
            table_name="target",
            backup_status="success",
            exception=None,
            table_version=2,
            backup_version=2,
            source_info=source_info,
            backup_timestamp=datetime(2024, 10, 19, 12, 30, 45),
            job_id="job_1234",
            table_info=table_info,
        ),
        Row(
            layer="silver",
            table_schema="nab_shared_mode_test",
            table_name="target",
            backup_status="success",
            exception=None,
            table_version=3,
            backup_version=3,
            source_info=source_info,
            backup_timestamp=datetime(2024, 10, 20, 12, 30, 45),
            job_id="job_1234",
            table_info=table_info,
        ),
        Row(
            layer="silver",
            table_schema="nab_shared_mode_test",
            table_name="target",
            backup_status="success",
            exception=None,
            table_version=4,
            backup_version=4,
            source_info=source_info,
            backup_timestamp=datetime(2024, 10, 21, 12, 30, 45),
            job_id="job_1234",
            table_info=table_info,
        ),
    ]

    spark.createDataFrame(test_data, schema).write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(audit_table)


# Example usage
if __name__ == "__main__":
    JSON_PAYLOAD = """
    {
        "table_name": "target",
        "table_schema": "nab_shared_mode_test",
        "table_catalog": "main",
        "dr_table_catalog": "main",
        "version": "1",
        "use_checkpoints": "false"
    }
    """
    AUDIT_TBL = "main.nab_shared_mode_test.audit"

    # create_audit_table(AUDIT_TBL)

    restore_silver = RestoreSilver(spark, JSON_PAYLOAD, AUDIT_TBL)
