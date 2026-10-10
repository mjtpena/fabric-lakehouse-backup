"""Unit tests for the pure helpers in the backup, restore and cleanup notebooks."""

import datetime
import unittest

from notebook_loader import FakeFs, load_pure_helpers

BACKUP = load_pure_helpers("backup")
RESTORE = load_pure_helpers("restore")
CLEANUP = load_pure_helpers("cleanup")

ROOT = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse"
TABLES = f"{ROOT}/Tables"

FLAT_LAKEHOUSE = [
    "Tables/customers/_delta_log/",
    "Tables/customers/part-0.parquet",
    "Tables/orders/_delta_log/",
    "Tables/orders/region=AU/part-0.parquet",
    "Tables/stray_folder/data.csv",   # not a Delta table: Fabric shows it as Unidentified
    "Tables/_temporary/",             # system folder
]

SCHEMA_LAKEHOUSE = [
    "Tables/dbo/customers/_delta_log/",
    "Tables/sales/orders/_delta_log/",
    "Tables/sales/customers/_delta_log/",
    "Tables/sales/not_a_table/readme.txt",
    "Tables/empty_schema/",
    "Tables/.hidden/",
]


def discover(helpers, paths):
    fs = FakeFs(ROOT, paths)
    return helpers["discover_lakehouse_tables"](TABLES, fs.ls, fs.exists)


def pairs(discovery):
    return [(t["schema"], t["table"]) for t in discovery["tables"]]


class DiscoveryTests(unittest.TestCase):
    """Run against both notebooks: each carries its own copy of discover_lakehouse_tables."""

    def test_non_schema_lakehouse(self):
        for helpers in (BACKUP, RESTORE):
            result = discover(helpers, FLAT_LAKEHOUSE)
            self.assertFalse(result["schema_enabled"])
            self.assertEqual(pairs(result), [(None, "customers"), (None, "orders")])
            self.assertEqual(result["tables"][0]["path"], f"{TABLES}/customers")
            self.assertEqual([s["path"] for s in result["skipped"]], [f"{TABLES}/stray_folder"])

    def test_schema_enabled_lakehouse(self):
        for helpers in (BACKUP, RESTORE):
            result = discover(helpers, SCHEMA_LAKEHOUSE)
            self.assertTrue(result["schema_enabled"])
            self.assertEqual(result["schemas"], ["dbo", "empty_schema", "sales"])
            self.assertEqual(pairs(result), [("dbo", "customers"), ("sales", "customers"), ("sales", "orders")])
            self.assertEqual(result["tables"][-1]["path"], f"{TABLES}/sales/orders")
            self.assertEqual([s["path"] for s in result["skipped"]], [f"{TABLES}/sales/not_a_table"])

    def test_dbo_is_not_mistaken_for_a_table(self):
        # The defect this refresh fixes: the old discovery returned ["dbo"] here
        for helpers in (BACKUP, RESTORE):
            result = discover(helpers, ["Tables/dbo/orders/_delta_log/"])
            self.assertEqual(pairs(result), [("dbo", "orders")])

    def test_empty_schema_enabled_lakehouse(self):
        for helpers in (BACKUP, RESTORE):
            result = discover(helpers, ["Tables/dbo/"])
            self.assertTrue(result["schema_enabled"])
            self.assertEqual(result["tables"], [])

    def test_empty_non_schema_lakehouse(self):
        for helpers in (BACKUP, RESTORE):
            result = discover(helpers, ["Tables/", "Files/"])
            self.assertFalse(result["schema_enabled"])
            self.assertEqual(result["tables"], [])

    def test_trailing_slash_names_from_ls(self):
        class SlashFs(FakeFs):
            def ls(self, path):
                entries = super().ls(path)
                for entry in entries:
                    if entry.isDir:
                        entry.name += "/"
                return entries

        fs = SlashFs(ROOT, ["Tables/dbo/orders/_delta_log/"])
        result = BACKUP["discover_lakehouse_tables"](TABLES, fs.ls, fs.exists)
        self.assertEqual(pairs(result), [("dbo", "orders")])

    def test_discovery_errors_propagate(self):
        def failing_ls(path):
            raise PermissionError("403")

        with self.assertRaises(PermissionError):
            BACKUP["discover_lakehouse_tables"](TABLES, failing_ls, lambda path: False)


class BackupHelperTests(unittest.TestCase):
    def test_backup_folder_name_contract(self):
        name = BACKUP["build_backup_folder_name"]("lh_silver1", "wsp_transform_dev", datetime.datetime(2025, 11, 10, 14, 30, 15))
        self.assertEqual(name, "silver1_transform_dev_backup_2025-11-10_14-30-15")
        # The cleanup notebook must be able to read the timestamp back
        self.assertEqual(CLEANUP["parse_backup_timestamp"](name), datetime.datetime(2025, 11, 10, 14, 30, 15))

    def test_onelake_path(self):
        self.assertEqual(
            BACKUP["onelake_lakehouse_path"](" ws ", "lh_a"),
            "abfss://ws@onelake.dfs.fabric.microsoft.com/lh_a.Lakehouse",
        )
        for helpers in (RESTORE, CLEANUP):
            self.assertEqual(BACKUP["onelake_lakehouse_path"]("ws", "lh"), helpers["onelake_lakehouse_path"]("ws", "lh"))

    def test_backup_layout_paths(self):
        self.assertEqual(BACKUP["backup_table_relpath"]("sales", "orders"), "tables/sales/orders")
        self.assertEqual(BACKUP["backup_table_relpath"](None, "orders"), "tables/orders")
        self.assertEqual(BACKUP["sidecar_relpath"]("sales", "orders"), "tables/sales/orders_metadata")
        self.assertEqual(BACKUP["sidecar_relpath"](None, "orders"), "tables/orders_metadata")
        for schema in ("sales", None):
            self.assertEqual(BACKUP["backup_table_relpath"](schema, "t"), RESTORE["backup_table_relpath"](schema, "t"))
            self.assertEqual(BACKUP["sidecar_relpath"](schema, "t"), RESTORE["sidecar_relpath"](schema, "t"))

    def test_sidecar_collisions(self):
        tables = [
            {"schema": "dbo", "table": "orders"},
            {"schema": "dbo", "table": "orders_metadata"},   # collides with orders' sidecar
            {"schema": "sales", "table": "orders_metadata"},  # no sales.orders: fine
            {"schema": "dbo", "table": "audit_metadata"},     # no dbo.audit: fine
        ]
        self.assertEqual(BACKUP["find_sidecar_collisions"](tables), [("dbo", "orders_metadata")])
        self.assertEqual(BACKUP["find_sidecar_collisions"]([{"schema": None, "table": "a"}]), [])

    def test_restore_instructions_describe_the_real_method(self):
        for schema_enabled in (True, False):
            text = BACKUP["create_restore_instructions"]("x_backup_2026-01-01_00-00-00", schema_enabled, 3, 4)
            self.assertIn("Full Delta Copy", text)
            self.assertNotIn("Zero Data Duplication", text)
            self.assertNotIn("DEEP CLONE delta", text)
            self.assertIn("DEEP CLONE is not available", text)
            self.assertIn("SHALLOW CLONE", text)
            self.assertIn("cleanup job", text)
            self.assertNotIn(".zip", text)
            self.assertIn("tables/<schema>/<table>" if schema_enabled else "tables/<table>", text)


class RestoreHelperTests(unittest.TestCase):
    plan = staticmethod(RESTORE["plan_table_restore"])

    @staticmethod
    def tables(*names):
        """'schema.table' or 'table' -> backup table entries"""
        result = []
        for name in names:
            schema, _, table = name.rpartition(".")
            schema = schema or None
            result.append({"schema": schema, "table": table,
                           "backup_path": RESTORE["backup_table_relpath"](schema, table),
                           "expected_rows": 1, "expected_rows_source": "manifest", "delta_version": 0})
        return result

    def targets(self, plan):
        return [(item["source_name"], item["target_relpath"]) for item in plan]

    def test_flat_backup_into_non_schema_target(self):
        plan = self.plan(self.tables("customers", "orders"), False)
        self.assertEqual(self.targets(plan), [("customers", "Tables/customers"), ("orders", "Tables/orders")])
        self.assertEqual(plan[0]["backup_path"], "tables/customers")

    def test_flat_backup_into_schema_target_defaults_to_dbo(self):
        plan = self.plan(self.tables("customers"), True)
        self.assertEqual(self.targets(plan), [("customers", "Tables/dbo/customers")])
        self.assertEqual(plan[0]["target_name"], "dbo.customers")

    def test_flat_backup_into_schema_target_with_target_schema(self):
        plan = self.plan(self.tables("customers"), True, " legacy ")
        self.assertEqual(self.targets(plan), [("customers", "Tables/legacy/customers")])

    def test_target_schema_needs_a_schema_enabled_target(self):
        with self.assertRaisesRegex(ValueError, "not schema-enabled"):
            self.plan(self.tables("customers"), False, "legacy")

    def test_invalid_target_schema(self):
        for bad in ("_x", "a/b", "a.b", ".x"):
            with self.subTest(target_schema=bad), self.assertRaisesRegex(ValueError, "not a valid schema name"):
                self.plan(self.tables("customers"), True, bad)

    def test_schema_backup_into_schema_target_keeps_schemas(self):
        plan = self.plan(self.tables("dbo.customers", "sales.customers", "sales.orders"), True)
        self.assertEqual(self.targets(plan), [
            ("dbo.customers", "Tables/dbo/customers"),
            ("sales.customers", "Tables/sales/customers"),
            ("sales.orders", "Tables/sales/orders"),
        ])
        self.assertEqual(plan[1]["backup_path"], "tables/sales/customers")

    def test_schema_backup_remapped_into_one_schema(self):
        plan = self.plan(self.tables("dbo.customers", "sales.orders"), True, "restored")
        self.assertEqual(self.targets(plan), [
            ("dbo.customers", "Tables/restored/customers"),
            ("sales.orders", "Tables/restored/orders"),
        ])

    def test_remap_collision_fails(self):
        with self.assertRaisesRegex(ValueError, "same target table"):
            self.plan(self.tables("dbo.customers", "sales.Customers"), True, "restored")

    def test_single_schema_backup_flattens_into_non_schema_target(self):
        plan = self.plan(self.tables("dbo.customers", "dbo.orders"), False)
        self.assertEqual(self.targets(plan), [("dbo.customers", "Tables/customers"), ("dbo.orders", "Tables/orders")])

    def test_multi_schema_backup_into_non_schema_target_fails(self):
        with self.assertRaisesRegex(ValueError, "2 schemas"):
            self.plan(self.tables("dbo.customers", "sales.orders"), False)

    def test_empty_plan(self):
        self.assertEqual(self.plan([], True), [])
        self.assertEqual(self.plan([], False), [])

    def test_table_filter(self):
        match = RESTORE["matches_table_filter"]
        self.assertTrue(match("sales", "orders", []))
        self.assertTrue(match("sales", "orders", ["orders"]))
        self.assertTrue(match("sales", "orders", ["SALES.*"]))
        self.assertFalse(match("dbo", "orders", ["sales.*"]))
        self.assertTrue(match(None, "orders", ["ord*"]))


class ManifestParsingTests(unittest.TestCase):
    parse = staticmethod(RESTORE["parse_backup_manifest"])

    def test_missing_manifest_is_treated_as_v1(self):
        parsed = self.parse(None)
        self.assertFalse(parsed["has_manifest"])
        self.assertEqual(parsed["layout_version"], 1)
        self.assertIsNone(parsed["tables"])

    def test_v1_manifest_row(self):
        # Columns written by the original backup notebook (no layout_version, no manifest_json)
        parsed = self.parse({"backup_folder_name": "a_b_backup_2025-11-10_14-30-15", "backup_type": "complete_lakehouse_direct_copy",
                             "tables_info": [{"name": "customers", "row_count": "3"}]})
        self.assertTrue(parsed["has_manifest"])
        self.assertEqual(parsed["layout_version"], 1)
        self.assertEqual(parsed["status"], "completed")
        self.assertFalse(parsed["schema_enabled"])
        self.assertIsNone(parsed["tables"])

    def test_v2_manifest_row(self):
        import json

        manifest = {"layout_version": 2, "status": "partial", "schema_enabled": True, "tables": [
            {"schema": "dbo", "table": "a", "status": "success", "backup_path": "tables/dbo/a", "row_count": 5, "delta_version": 7},
            {"schema": "dbo", "table": "b", "status": "success", "backup_path": "tables/dbo/b", "row_count": None, "rows_written": 9},
            {"schema": "dbo", "table": "c", "status": "failed"},
        ]}
        parsed = self.parse({"layout_version": 2, "manifest_json": json.dumps(manifest)})
        self.assertEqual((parsed["layout_version"], parsed["status"], parsed["schema_enabled"]), (2, "partial", True))
        tables = RESTORE["tables_from_manifest"](parsed["tables"])
        self.assertEqual([(t["table"], t["expected_rows"], t["delta_version"]) for t in tables], [("a", 5, 7), ("b", 9, None)])

    def test_listing_skips_sidecars_only_next_to_their_table(self):
        discovery = {"tables": [
            {"schema": None, "table": "customers"},
            {"schema": None, "table": "customers_metadata"},   # sidecar
            {"schema": None, "table": "audit_metadata"},       # a real table: no 'audit' next to it
        ]}
        names = [t["table"] for t in RESTORE["tables_from_backup_listing"](discovery)]
        self.assertEqual(names, ["customers", "audit_metadata"])

    def test_zip_backup_detection(self):
        self.assertTrue(RESTORE["looks_like_zip_backup"](["lakehouse_complete_backup.zip"]))
        self.assertTrue(RESTORE["looks_like_zip_backup"](["complete_backup_zip_data", "_manifest"]))
        self.assertFalse(RESTORE["looks_like_zip_backup"](["tables", "files", "_manifest"]))


class CleanupHelperTests(unittest.TestCase):
    def test_parse_backup_timestamp(self):
        parse = CLEANUP["parse_backup_timestamp"]
        self.assertEqual(parse("a_b_backup_2025-01-02_03-04-05"), datetime.datetime(2025, 1, 2, 3, 4, 5))
        self.assertIsNone(parse("a_b_backup_2025-13-02_03-04-05"))
        self.assertIsNone(parse("not_a_backup"))
        self.assertIsNone(parse("_cleanup_logs"))

    def test_format_size(self):
        self.assertEqual(CLEANUP["format_size"](0), "0.00 B")
        self.assertEqual(CLEANUP["format_size"](1536), "1.50 KB")

    def test_utc_now_is_naive_utc(self):
        for helpers in (BACKUP, RESTORE, CLEANUP):
            now = helpers["utc_now"]()
            self.assertIsNone(now.tzinfo)
            expected = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            self.assertLess(abs((expected - now).total_seconds()), 5)


if __name__ == "__main__":
    unittest.main()
