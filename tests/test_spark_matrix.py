"""
End-to-end backup -> restore -> cleanup runs of the notebooks on local Spark + Delta Lake
(see spark_harness.py for what is and isn't simulated).

Matrix: {non-schema source, schema source} x {non-schema target, schema target} x {v1 backup, v2 backup}.
v1 backups are built here in the exact folder layout the original backup notebook wrote
(tables/<table>, tables/<table>_metadata, files/, _manifest without layout_version).

Skipped unless pyspark and delta-spark are installed. Set REQUIRE_SPARK_TESTS=1 to fail instead.
"""

import datetime
import importlib.util
import json
import os
import shutil
import tempfile
import unittest
import warnings

SPARK_AVAILABLE = bool(
    importlib.util.find_spec("pyspark") and importlib.util.find_spec("delta")
    and (shutil.which("java") or os.environ.get("JAVA_HOME"))
)
if os.environ.get("REQUIRE_SPARK_TESTS") == "1" and not SPARK_AVAILABLE:
    raise RuntimeError("REQUIRE_SPARK_TESTS=1 but pyspark, delta-spark or Java is missing")

if SPARK_AVAILABLE:
    from spark_harness import create_spark, install_fake_notebookutils, run_notebook

CUSTOMERS = [(1, "Ada", datetime.date(1500, 1, 1)), (2, "Grace", datetime.date(1906, 12, 9)), (3, "Linus", datetime.date(1969, 12, 28))]
CUSTOMERS_DDL = "id INT, name STRING, since DATE"
ORDERS = [(1, "AU", 10.0), (2, "AU", 12.5), (3, "NZ", 7.0), (4, "US", 99.9)]
MORE_ORDERS = [(5, "NZ", 1.0)]
ORDERS_DDL = "id INT, region STRING, amount DOUBLE"
AUDIT = [("login",), ("logout",)]
AUDIT_DDL = "event STRING"
FILES = {"reports/monthly.txt": "monthly report", "data/import.csv": "a,b\n1,2\n", "_hidden/skip.txt": "not backed up"}

V1_FOLDER = "flat_ws_src_backup_2025-11-10_14-30-15"
V1_NO_MANIFEST_FOLDER = "flat_ws_src_backup_2025-11-11_14-30-15"


@unittest.skipUnless(SPARK_AVAILABLE, "needs pyspark, delta-spark and Java")
class SparkMatrixTests(unittest.TestCase):

    # ------------------------------------------------------------------ setup

    @classmethod
    def setUpClass(cls):
        warnings.simplefilter("ignore", ResourceWarning)  # py4j sockets
        cls.work = tempfile.mkdtemp(prefix="fabric-lakehouse-backup-")
        cls.onelake = os.path.join(cls.work, "onelake")
        install_fake_notebookutils()
        cls.spark = create_spark(cls.work)

        cls.make_lakehouse("ws_src", "lh_flat", schema_enabled=False, tables={
            "customers": (CUSTOMERS, CUSTOMERS_DDL, None),
            "orders": (ORDERS, ORDERS_DDL, "region"),
            "audit_metadata": (AUDIT, AUDIT_DDL, None),  # ends in _metadata, but no 'audit' table next to it
        }, files=FILES)
        cls.make_lakehouse("ws_src", "lh_schema", schema_enabled=True, tables={
            "dbo/customers": (CUSTOMERS, CUSTOMERS_DDL, None),
            "sales/orders": (ORDERS, ORDERS_DDL, "region"),
            "sales/customers": (CUSTOMERS[:2], CUSTOMERS_DDL, None),  # same table name as dbo.customers
        }, files={"x/notes.txt": "schema lakehouse file"})
        cls.append(cls.table_path("ws_src", "lh_schema", "sales/orders"), MORE_ORDERS, ORDERS_DDL)  # Delta version 1
        cls.make_lakehouse("ws_src", "lh_single", schema_enabled=True, tables={
            "dbo/customers": (CUSTOMERS, CUSTOMERS_DDL, None),
            "dbo/orders": (ORDERS, ORDERS_DDL, "region"),
        })
        cls.make_lakehouse("ws_bkp", "lh_backup", schema_enabled=False)

        cls.backup_flat = cls.run_backup("lh_flat")
        cls.backup_schema = cls.run_backup("lh_schema")
        cls.backup_single = cls.run_backup("lh_single")
        cls.make_v1_backup(V1_FOLDER, with_manifest=True)
        cls.make_v1_backup(V1_NO_MANIFEST_FOLDER, with_manifest=False)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        shutil.rmtree(cls.work, ignore_errors=True)

    @classmethod
    def lakehouse(cls, workspace, lakehouse):
        return f"{cls.onelake}/{workspace}/{lakehouse}.Lakehouse"

    @classmethod
    def table_path(cls, workspace, lakehouse, relpath):
        return f"{cls.lakehouse(workspace, lakehouse)}/Tables/{relpath}"

    @classmethod
    def make_lakehouse(cls, workspace, lakehouse, schema_enabled, tables=None, files=None):
        root = cls.lakehouse(workspace, lakehouse)
        os.makedirs(f"{root}/Tables/dbo" if schema_enabled else f"{root}/Tables", exist_ok=True)
        os.makedirs(f"{root}/Files", exist_ok=True)
        for relpath, (rows, ddl, partition_by) in (tables or {}).items():
            writer = cls.spark.createDataFrame(rows, ddl).write.format("delta")
            if partition_by:
                writer = writer.partitionBy(partition_by)
            writer.save(f"{root}/Tables/{relpath}")
        for relpath, content in (files or {}).items():
            os.makedirs(os.path.dirname(f"{root}/Files/{relpath}"), exist_ok=True)
            with open(f"{root}/Files/{relpath}", "w") as f:
                f.write(content)
        return root

    @classmethod
    def append(cls, path, rows, ddl):
        cls.spark.createDataFrame(rows, ddl).write.format("delta").mode("append").save(path)

    @classmethod
    def run_nb(cls, notebook, parameters, overrides=None):
        return run_notebook(notebook, cls.spark, parameters, cls.onelake, overrides=overrides)

    @classmethod
    def run_backup(cls, lakehouse, workspace="ws_src", overrides=None, expect_ok=True):
        run = cls.run_nb("backup", {
            "source_lakehouse_name": lakehouse, "source_workspace_name": workspace,
            "backup_lakehouse_name": "lh_backup", "backup_workspace_name": "ws_bkp",
        }, overrides)
        if expect_ok and not run.ok:
            raise AssertionError(f"backup of {lakehouse} failed: {run.error}\n{run.output[-4000:]}")
        return run if not expect_ok else run.namespace["backup_folder"]

    @classmethod
    def backup_root(cls, folder):
        return f"{cls.lakehouse('ws_bkp', 'lh_backup')}/Files/{folder}"

    @classmethod
    def make_v1_backup(cls, folder, with_manifest):
        """The folder layout written by the original backup notebook, from the lh_flat source"""
        base = cls.backup_root(folder)
        tables_info = []
        for table in ("audit_metadata", "customers", "orders"):
            source = cls.table_path("ws_src", "lh_flat", table)
            df = cls.spark.read.format("delta").load(source)
            df.write.mode("overwrite").format("delta").save(f"{base}/tables/{table}")  # v1 did not keep partitioning
            sidecar = {"table_name": table, "row_count": df.count(), "columns": df.columns, "backup_method": "delta_read_write",
                       "backup_timestamp": "2025-11-10T14:30:15", "source_path": source, "backup_path": f"{base}/tables/{table}"}
            cls.spark.createDataFrame([(json.dumps(sidecar),)], "metadata_json STRING").write.format("delta").save(f"{base}/tables/{table}_metadata")
            tables_info.append({"name": table, "row_count": str(df.count())})
        source_files = f"{cls.lakehouse('ws_src', 'lh_flat')}/Files"
        shutil.copytree(source_files, f"{base}/files", ignore=shutil.ignore_patterns("_*"))
        if with_manifest:
            # Spark 3.5 inferred tables_info as array<map<string,string>>
            cls.spark.createDataFrame(
                [("10-11-15", folder, "2025-11-10_14-30-15", "complete_lakehouse_direct_copy", "lh_flat", "ws_src", 3, 3, 0, tables_info)],
                "backup_id STRING, backup_folder_name STRING, backup_timestamp STRING, backup_type STRING, source_lakehouse_name STRING, "
                "source_workspace_name STRING, tables_discovered LONG, tables_backed_up LONG, tables_failed LONG, tables_info ARRAY<MAP<STRING,STRING>>",
            ).write.format("delta").save(f"{base}/_manifest")

    def restore(self, backup_folder, target_schema_enabled, target=None, overrides=None, **parameters):
        target = target or self._testMethodName
        if not os.path.exists(self.lakehouse("ws_tgt", target)):
            self.make_lakehouse("ws_tgt", target, schema_enabled=target_schema_enabled)
        params = {
            "backup_lakehouse_name": "lh_backup", "backup_workspace_name": "ws_bkp", "backup_path": backup_folder,
            "target_lakehouse_name": target, "target_workspace_name": "ws_tgt",
        }
        params.update(parameters)
        return self.run_nb("restore", params, overrides), self.lakehouse("ws_tgt", target)

    def count(self, path):
        return self.spark.read.format("delta").load(path).count()

    def partition_columns(self, path):
        from delta.tables import DeltaTable
        return list(DeltaTable.forPath(self.spark, path).detail().collect()[0]["partitionColumns"])

    def list_tables(self, lakehouse_root):
        """Relative paths of every Delta table under Tables/"""
        found = []
        for dirpath, dirnames, _ in os.walk(f"{lakehouse_root}/Tables"):
            if "_delta_log" in dirnames:
                found.append(os.path.relpath(dirpath, lakehouse_root))
                dirnames[:] = []
        return sorted(found)

    def assert_ok(self, run):
        if not run.ok:
            self.fail(f"{type(run.error).__name__}: {run.error}\n--- notebook output (tail) ---\n{run.output[-4000:]}")

    def assert_restored(self, run, target, expected):
        """expected: {target relpath: source table path}"""
        self.assert_ok(run)
        self.assertEqual(self.list_tables(target), sorted(expected))
        for relpath, source in expected.items():
            self.assertEqual(self.count(f"{target}/{relpath}"), self.count(source), relpath)
        verification = run.namespace["verification_result"]
        self.assertTrue(verification["overall_success"])
        compared = [t for t in verification["tables"].values() if t["data_matches"] is not None]
        self.assertEqual(len(compared), len(expected), "every table compared with a recorded row count")

    def assert_nothing_written(self, run, target, error_text):
        self.assertIsInstance(run.error, ValueError)
        self.assertIn(error_text, str(run.error))
        self.assertEqual(self.list_tables(target), [])
        self.assertEqual(os.listdir(f"{target}/Files"), [])

    def flat_source(self, table):
        return self.table_path("ws_src", "lh_flat", table)

    def schema_source(self, relpath, lakehouse="lh_schema"):
        return self.table_path("ws_src", lakehouse, relpath)

    # ---------------------------------------------------------- v1 backups

    def test_v1_backup_into_non_schema_target(self):
        run, target = self.restore(V1_FOLDER, target_schema_enabled=False)
        self.assert_restored(run, target, {f"Tables/{t}": self.flat_source(t) for t in ("audit_metadata", "customers", "orders")})
        with open(f"{target}/Files/reports/monthly.txt") as f:
            self.assertEqual(f.read(), "monthly report")
        self.assertFalse(os.path.exists(f"{target}/Files/_hidden"))
        sources = {t["expected_rows_source"] for t in run.namespace["verification_result"]["tables"].values()}
        self.assertEqual(sources, {"sidecar"})

    def test_v1_backup_into_schema_target_goes_to_dbo(self):
        run, target = self.restore(V1_FOLDER, target_schema_enabled=True)
        self.assert_restored(run, target, {f"Tables/dbo/{t}": self.flat_source(t) for t in ("audit_metadata", "customers", "orders")})

    def test_v1_backup_into_schema_target_with_target_schema(self):
        run, target = self.restore(V1_FOLDER, target_schema_enabled=True, target_schema="legacy")
        self.assert_restored(run, target, {f"Tables/legacy/{t}": self.flat_source(t) for t in ("audit_metadata", "customers", "orders")})

    def test_v1_backup_without_manifest(self):
        # What the original notebook left behind on Spark 4 (its manifest write failed)
        run, target = self.restore(V1_NO_MANIFEST_FOLDER, target_schema_enabled=False)
        self.assert_restored(run, target, {f"Tables/{t}": self.flat_source(t) for t in ("audit_metadata", "customers", "orders")})

    def test_target_schema_into_non_schema_target_fails_before_writing(self):
        run, target = self.restore(V1_FOLDER, target_schema_enabled=False, target_schema="legacy")
        self.assert_nothing_written(run, target, "not schema-enabled")

    # ------------------------------------------- v2 backup, non-schema source

    def test_v2_flat_backup_into_non_schema_target(self):
        run, target = self.restore(self.backup_flat, target_schema_enabled=False)
        self.assert_restored(run, target, {f"Tables/{t}": self.flat_source(t) for t in ("audit_metadata", "customers", "orders")})
        self.assertEqual(self.partition_columns(f"{target}/Tables/orders"), ["region"])
        with open(f"{target}/Files/data/import.csv") as f:
            self.assertEqual(f.read(), "a,b\n1,2\n")

    def test_v2_flat_backup_into_schema_target_goes_to_dbo(self):
        run, target = self.restore(self.backup_flat, target_schema_enabled=True)
        self.assert_restored(run, target, {f"Tables/dbo/{t}": self.flat_source(t) for t in ("audit_metadata", "customers", "orders")})

    # ----------------------------------------------- v2 backup, schema source

    def test_v2_schema_backup_into_schema_target_keeps_schemas(self):
        run, target = self.restore(self.backup_schema, target_schema_enabled=True)
        self.assert_restored(run, target, {
            "Tables/dbo/customers": self.schema_source("dbo/customers"),
            "Tables/sales/customers": self.schema_source("sales/customers"),
            "Tables/sales/orders": self.schema_source("sales/orders"),
        })
        self.assertEqual(self.partition_columns(f"{target}/Tables/sales/orders"), ["region"])
        self.assertEqual(self.count(f"{target}/Tables/sales/orders"), len(ORDERS) + len(MORE_ORDERS))

    def test_v2_schema_backup_remap_with_collision_fails_before_writing(self):
        run, target = self.restore(self.backup_schema, target_schema_enabled=True, target_schema="restored")
        self.assert_nothing_written(run, target, "same target table")

    def test_v2_schema_backup_remap_into_one_schema(self):
        run, target = self.restore(self.backup_single, target_schema_enabled=True, target_schema="restored")
        self.assert_restored(run, target, {
            "Tables/restored/customers": self.schema_source("dbo/customers", "lh_single"),
            "Tables/restored/orders": self.schema_source("dbo/orders", "lh_single"),
        })

    def test_v2_multi_schema_backup_into_non_schema_target_fails_before_writing(self):
        run, target = self.restore(self.backup_schema, target_schema_enabled=False)
        self.assert_nothing_written(run, target, "not schema-enabled, so they would be flattened")

    def test_v2_single_schema_backup_into_non_schema_target(self):
        run, target = self.restore(self.backup_single, target_schema_enabled=False)
        self.assert_restored(run, target, {
            "Tables/customers": self.schema_source("dbo/customers", "lh_single"),
            "Tables/orders": self.schema_source("dbo/orders", "lh_single"),
        })

    def test_restore_specific_tables(self):
        run, target = self.restore(self.backup_schema, target_schema_enabled=False,
                                   overrides={"restore_specific_tables": ["sales.*"], "restore_files": False})
        # Without the dbo table only one schema is left, so it can be flattened
        self.assert_restored(run, target, {
            "Tables/customers": self.schema_source("sales/customers"),
            "Tables/orders": self.schema_source("sales/orders"),
        })

    # ---------------------------------------------------- overwrite behaviour

    def test_overwrite_false_skips_existing_tables_and_files(self):
        target = self.make_lakehouse("ws_tgt", self._testMethodName, schema_enabled=False,
                                     tables={"customers": ([("existing",)], "x STRING", None)},
                                     files={"reports/monthly.txt": "local edit"})
        run, _ = self.restore(self.backup_flat, target_schema_enabled=False, overwrite_existing=False)
        self.assert_ok(run)
        self.assertEqual(self.count(f"{target}/Tables/customers"), 1)
        self.assertEqual(self.count(f"{target}/Tables/orders"), len(ORDERS))
        with open(f"{target}/Files/reports/monthly.txt") as f:
            self.assertEqual(f.read(), "local edit")
        self.assertEqual(run.namespace["table_restore_result"]["tables_skipped"], 1)
        self.assertEqual(run.namespace["file_restore_result"]["files_skipped"], 1)

    def test_overwrite_replaces_table_with_a_different_schema(self):
        target = self.make_lakehouse("ws_tgt", self._testMethodName, schema_enabled=False,
                                     tables={"customers": ([("existing",)], "x STRING", None)},
                                     files={"reports/monthly.txt": "local edit"})
        run, _ = self.restore(self.backup_flat, target_schema_enabled=False)
        self.assert_ok(run)
        restored = self.spark.read.format("delta").load(f"{target}/Tables/customers")
        self.assertEqual(restored.columns, ["id", "name", "since"])
        self.assertEqual(restored.count(), len(CUSTOMERS))
        self.assertEqual(restored.filter("id = 1").collect()[0]["since"], datetime.date(1500, 1, 1))
        with open(f"{target}/Files/reports/monthly.txt") as f:
            self.assertEqual(f.read(), "monthly report")

    # --------------------------------------------------------- backup output

    def test_v2_backup_layout_manifest_and_sidecar(self):
        root = self.backup_root(self.backup_schema)
        manifest_row = self.spark.read.format("delta").load(f"{root}/_manifest").collect()[0]
        self.assertEqual((manifest_row["layout_version"], manifest_row["schema_enabled"], manifest_row["status"]), (2, True, "success"))
        manifest = json.loads(manifest_row["manifest_json"])
        tables = {t["qualified_name"]: t for t in manifest["tables"]}
        self.assertEqual(sorted(tables), ["dbo.customers", "sales.customers", "sales.orders"])
        orders = tables["sales.orders"]
        self.assertEqual((orders["delta_version"], orders["row_count"], orders["rows_written"]), (1, 5, 5))
        self.assertEqual((orders["backup_path"], orders["partition_columns"], orders["verification"]),
                         ("tables/sales/orders", ["region"], "rows_match"))

        sidecar = json.loads(self.spark.read.format("delta").load(f"{root}/tables/sales/orders_metadata").collect()[0]["metadata_json"])
        self.assertEqual((sidecar["schema"], sidecar["table"], sidecar["backup_path"], sidecar["delta_version"]),
                         ("sales", "orders", "tables/sales/orders", 1))
        self.assertTrue(sidecar["source_path"].endswith("/lh_schema.Lakehouse/Tables/sales/orders"))
        self.assertEqual(self.partition_columns(f"{root}/tables/sales/orders"), ["region"])

        metadata = json.loads(self.spark.read.format("delta").load(f"{root}/backup_metadata").collect()[0]["metadata_json"])
        self.assertEqual((metadata["layout_version"], metadata["schema_enabled"]), (2, True))
        instructions = self.spark.read.format("delta").load(f"{root}/RESTORE_INSTRUCTIONS").collect()[0]["instructions"]
        self.assertIn("SHALLOW CLONE", instructions)
        self.assertNotIn("Zero Data Duplication", instructions)

        flat_manifest = self.spark.read.format("delta").load(f"{self.backup_root(self.backup_flat)}/_manifest").collect()[0]
        self.assertFalse(flat_manifest["schema_enabled"])
        self.assertTrue(os.path.isdir(f"{self.backup_root(self.backup_flat)}/tables/orders_metadata"))

    def test_backup_without_row_counts(self):
        self.make_lakehouse("ws_src", "lh_nocount", schema_enabled=True, tables={"dbo/orders": (ORDERS, ORDERS_DDL, None)})
        folder = self.run_backup("lh_nocount", overrides={"count_rows": False})
        manifest = json.loads(self.spark.read.format("delta").load(f"{self.backup_root(folder)}/_manifest").collect()[0]["manifest_json"])
        entry = manifest["tables"][0]
        self.assertEqual((entry["row_count"], entry["rows_written"], entry["verification"]), (None, len(ORDERS), "not_compared"))
        run, target = self.restore(folder, target_schema_enabled=True)
        self.assert_restored(run, target, {"Tables/dbo/orders": self.schema_source("dbo/orders", "lh_nocount")})

    def test_partial_backup_fails_the_run_but_stays_restorable(self):
        source = self.make_lakehouse("ws_src", "lh_broken", schema_enabled=False, tables={"good": (AUDIT, AUDIT_DDL, None)})
        os.makedirs(f"{source}/Tables/broken/_delta_log")
        with open(f"{source}/Tables/broken/_delta_log/00000000000000000000.json", "w") as f:
            f.write("this is not a delta log")
        run = self.run_backup("lh_broken", expect_ok=False)
        self.assertIsInstance(run.error, RuntimeError)
        self.assertIn("Backup finished with problems", str(run.error))
        folder = run.namespace["backup_folder"]
        manifest_row = self.spark.read.format("delta").load(f"{self.backup_root(folder)}/_manifest").collect()[0]
        self.assertEqual((manifest_row["status"], manifest_row["tables_backed_up"], manifest_row["tables_failed"]), ("partial", 1, 1))

        restore_run, target = self.restore(folder, target_schema_enabled=False)
        self.assert_restored(restore_run, target, {"Tables/good": f"{source}/Tables/good"})
        self.assertIn("This backup is partial", restore_run.output)

    def test_unlistable_folder_makes_the_backup_partial(self):
        import notebookutils

        self.make_lakehouse("ws_src", "lh_locked", schema_enabled=False, tables={"t": (AUDIT, AUDIT_DDL, None)},
                            files={"ok/a.txt": "a", "locked/b.txt": "b"})
        local_fs = notebookutils.fs

        class LockedFs(type(local_fs)):
            def ls(self, path):
                if path.rstrip("/").endswith("lh_locked.Lakehouse/Files/locked"):
                    raise PermissionError("403 Forbidden")
                return super().ls(path)

        notebookutils.fs = LockedFs()
        try:
            run = self.run_backup("lh_locked", expect_ok=False)
        finally:
            notebookutils.fs = local_fs
        self.assertIsInstance(run.error, RuntimeError)
        self.assertIn("1 files or folders failed", str(run.error))
        root = self.backup_root(run.namespace["backup_folder"])
        manifest = json.loads(self.spark.read.format("delta").load(f"{root}/_manifest").collect()[0]["manifest_json"])
        self.assertEqual((manifest["status"], manifest["files_backed_up"], manifest["files_failed"]), ("partial", 1, 1))
        self.assertTrue(manifest["file_discovery_errors"][0]["path"].endswith("/Files/locked"))
        self.assertEqual(manifest["tables_backed_up"], 1)

    def test_verification_without_delta_operation_metrics(self):
        self.make_lakehouse("ws_src", "lh_nometrics", schema_enabled=False, tables={"orders": (ORDERS, ORDERS_DDL, None)})
        self.spark.conf.set("spark.databricks.delta.history.metricsEnabled", "false")
        try:
            folder = self.run_backup("lh_nometrics")
        finally:
            self.spark.conf.unset("spark.databricks.delta.history.metricsEnabled")
        manifest = json.loads(self.spark.read.format("delta").load(f"{self.backup_root(folder)}/_manifest").collect()[0]["manifest_json"])
        entry = manifest["tables"][0]
        self.assertEqual((entry["row_count"], entry["rows_written"], entry["verification"]), (len(ORDERS), len(ORDERS), "rows_match"))

    def test_failed_backup_leaves_a_manifest_and_is_not_restored(self):
        def discover_files(files_path):
            raise OSError("simulated OneLake failure")

        self.make_lakehouse("ws_src", "lh_crash", schema_enabled=False, tables={"t": (AUDIT, AUDIT_DDL, None)})
        run = self.run_backup("lh_crash", overrides={"discover_files": discover_files}, expect_ok=False)
        self.assertIsInstance(run.error, OSError)
        folder = run.namespace["backup_folder"]
        manifest_row = self.spark.read.format("delta").load(f"{self.backup_root(folder)}/_manifest").collect()[0]
        self.assertEqual(manifest_row["status"], "failed")  # so the cleanup notebook can age it out

        restore_run, target = self.restore(folder, target_schema_enabled=False)
        self.assert_nothing_written(restore_run, target, "backup failed before completing")

    def test_restore_rejects_missing_backup_folder(self):
        run, target = self.restore("does_not_exist_backup_2025-01-01_00-00-00", target_schema_enabled=False)
        self.assert_nothing_written(run, target, "Backup folder not found")

    # ---------------------------------------------------------------- cleanup

    def test_cleanup_skips_folders_without_a_manifest(self):
        bkp = self.make_lakehouse("ws_bkp", "lh_cleanup", schema_enabled=False)
        recent = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime("%Y-%m-%d_%H-%M-%S")
        folders = {
            "expired_with_manifest": "flat_ws_src_backup_2020-01-01_00-00-00",
            "expired_without_manifest": "flat_ws_src_backup_2020-01-02_00-00-00",
            "recent": f"flat_ws_src_backup_{recent}",
        }
        for key, name in folders.items():
            shutil.copytree(self.backup_root(self.backup_flat), f"{bkp}/Files/{name}")
        shutil.rmtree(f"{bkp}/Files/{folders['expired_without_manifest']}/_manifest")
        os.makedirs(f"{bkp}/Files/not_a_backup")

        run = self.run_nb("cleanup", {"backup_lakehouse_name": "lh_cleanup", "backup_workspace_name": "ws_bkp",
                                   "retention_days": 30, "dry_run": False})
        self.assert_ok(run)
        remaining = sorted(os.listdir(f"{bkp}/Files"))
        self.assertNotIn(folders["expired_with_manifest"], remaining)
        self.assertIn(folders["expired_without_manifest"], remaining)
        self.assertIn(folders["recent"], remaining)
        self.assertIn("not_a_backup", remaining)
        self.assertIn("unknown, skipped", run.output)
        result = run.namespace["cleanup_result"]
        self.assertEqual((result["deleted_backups"], result["unknown_skipped"]), (1, 1))


if __name__ == "__main__":
    unittest.main()
