# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# PARAMETERS CELL ********************

# Microsoft Fabric Lakehouse Complete Restore Notebook
# Restores Tables and Files from a backup folder written by nb_lakehouse_backup.
# Handles schema-enabled (Tables/<schema>/<table>) and non-schema (Tables/<table>)
# lakehouses, and backups written by the original (layout v1) and current (layout v2) notebook.

# Parameters - Configure these values or override when scheduling
backup_lakehouse_name = ""  # REQUIRED: Backup lakehouse name (e.g., "lh_backup")
backup_workspace_name = ""  # REQUIRED: Backup workspace name (e.g., "wsp_lakehouse_backup_dev")
backup_path = ""  # REQUIRED: Path to the backup folder (e.g., "silver1_transform_dev_backup_2025-11-10_14-30-15")

# Restore target configuration
target_lakehouse_name = ""  # REQUIRED: Target lakehouse name (e.g., "lh_silver1_restored")
target_workspace_name = ""  # REQUIRED: Target workspace name (e.g., "wsp_transform_dev")
target_schema = ""  # OPTIONAL, schema-enabled targets only. Empty: keep each table's schema
                    # (tables from non-schema backups go to dbo). Set: restore every table into this schema.

# Restore options
restore_tables = True  # Restore tables to Tables directory
restore_files = True   # Restore files to Files directory
overwrite_existing = True  # Overwrite existing tables/files if they exist

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "tags": [
# META     "parameters"
# META   ]
# META }

# CELL ********************

import platform
import traceback
import uuid
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from delta.tables import DeltaTable
import notebookutils

# Configure Spark for handling ancient datetime values (pre-1582 dates, pre-1900 timestamps)
# This is required for Oracle EBS tables that may contain historical dates
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")

# Initialize global variables
log_entries = []

# Configuration constants (not parameters)
verify_restore = True  # Always verify restores
enable_detailed_logging = True  # Always enable detailed logging
dry_run = False  # True: validate the backup and print the restore plan without writing anything
restore_specific_tables = []  # Restore all tables; or e.g. ["customers", "sales.orders", "sales.*"]
restore_specific_files = []  # Restore all files; or fnmatch patterns on the path under Files/, e.g. ["reports/*"]
fail_on_partial_restore = True  # Fail the notebook run (and the pipeline) if any table or file fails, or verification fails

print("📚 Libraries imported successfully")
print("⚙️  Spark configured for ancient datetime handling (pre-1582 dates, pre-1900 timestamps)")
print(f"⚙️  Spark {spark.version}, Python {platform.python_version()}")

# CELL ********************

# Pure helpers: no Spark or notebookutils calls. Unit tested in tests/.
import datetime
import fnmatch
import json

SUPPORTED_LAYOUT_VERSION = 2  # Newest backup layout this notebook can restore
SIDECAR_SUFFIX = "_metadata"
DEFAULT_SCHEMA = "dbo"


def utc_now():
    """Current UTC time as a naive datetime (Fabric Spark drivers run in UTC)"""
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


def onelake_lakehouse_path(workspace_name, lakehouse_name):
    """ABFS path of a lakehouse item in OneLake"""
    return f"abfss://{workspace_name.strip()}@onelake.dfs.fabric.microsoft.com/{lakehouse_name.strip()}.Lakehouse"


def is_hidden_name(name):
    """Folders starting with '_' or '.' are system folders, never tables or schemas"""
    return name.startswith("_") or name.startswith(".")


def qualified_table_name(schema, table):
    """'schema.table' for schema-enabled lakehouses, 'table' otherwise"""
    return f"{schema}.{table}" if schema else table


def discover_lakehouse_tables(tables_path, list_dir, path_exists):
    """
    Find the Delta tables under a lakehouse Tables/ folder (or a backup's tables/ folder).

    Non-schema lakehouse:      Tables/<table>/_delta_log
    Schema-enabled lakehouse:  Tables/<schema>/<table>/_delta_log  (the dbo schema always exists)

    If any top-level folder is a Delta table, the lakehouse is treated as non-schema.
    Otherwise every top-level folder is a schema. An empty Tables/ folder is non-schema.

    list_dir(path): entries with .name and .isDir (notebookutils.fs.ls)
    path_exists(path): bool (notebookutils.fs.exists)

    Returns {"schema_enabled": bool,
             "schemas": [str],  schema folders (empty for non-schema lakehouses)
             "tables": [{"schema": str or None, "table": str, "path": str}],  sorted
             "skipped": [{"path": str, "reason": str}]}
    """
    def child_dirs(path):
        return sorted(
            entry.name.rstrip("/")
            for entry in list_dir(path)
            if entry.isDir and not is_hidden_name(entry.name)
        )

    def is_delta_table(path):
        return path_exists(f"{path}/_delta_log")

    result = {"schema_enabled": False, "schemas": [], "tables": [], "skipped": []}
    top_level = {name: is_delta_table(f"{tables_path}/{name}") for name in child_dirs(tables_path)}

    if any(top_level.values()):
        for name, is_table in top_level.items():
            path = f"{tables_path}/{name}"
            if is_table:
                result["tables"].append({"schema": None, "table": name, "path": path})
            else:
                result["skipped"].append({"path": path, "reason": "not a Delta table (no _delta_log)"})
        return result

    result["schema_enabled"] = bool(top_level)
    result["schemas"] = list(top_level)
    for schema in top_level:
        schema_path = f"{tables_path}/{schema}"
        for name in child_dirs(schema_path):
            path = f"{schema_path}/{name}"
            if is_delta_table(path):
                result["tables"].append({"schema": schema, "table": name, "path": path})
            else:
                result["skipped"].append({"path": path, "reason": "not a Delta table (no _delta_log)"})
    return result


def backup_table_relpath(schema, table):
    """Table location inside a backup folder: tables/<schema>/<table> or tables/<table>"""
    return f"tables/{schema}/{table}" if schema else f"tables/{table}"


def sidecar_relpath(schema, table):
    """Per-table metadata sidecar, next to the table copy: tables/[<schema>/]<table>_metadata"""
    return backup_table_relpath(schema, table) + SIDECAR_SUFFIX


def target_table_relpath(schema, table):
    """Table location in the target lakehouse: Tables/<schema>/<table> or Tables/<table>"""
    return f"Tables/{schema}/{table}" if schema else f"Tables/{table}"


def parse_backup_manifest(manifest_row):
    """
    Normalise a backup's _manifest row (a dict), or None when the manifest is missing or unreadable.

    Layout v2+ manifests store the full manifest as JSON in the manifest_json column.
    Manifests without layout_version come from the original backup notebook (layout v1,
    tables/<table>); their per-table details are read from the folder listing and sidecars.
    """
    if manifest_row is None:
        return {"has_manifest": False, "layout_version": 1, "status": "unknown",
                "schema_enabled": None, "tables": None}

    if manifest_row.get("manifest_json"):
        manifest = json.loads(manifest_row["manifest_json"])
        return {
            "has_manifest": True,
            "layout_version": int(manifest.get("layout_version") or 1),
            "status": manifest.get("status") or "unknown",
            "schema_enabled": manifest.get("schema_enabled"),
            "tables": manifest.get("tables"),
            "source_lakehouse_name": manifest.get("source_lakehouse_name"),
            "source_workspace_name": manifest.get("source_workspace_name"),
            "backup_timestamp": manifest.get("backup_timestamp"),
            "spark_version": manifest.get("spark_version"),
        }

    return {
        "has_manifest": True,
        "layout_version": int(manifest_row.get("layout_version") or 1),
        "status": manifest_row.get("status") or "completed",
        "schema_enabled": False,
        "tables": None,
        "source_lakehouse_name": manifest_row.get("source_lakehouse_name"),
        "source_workspace_name": manifest_row.get("source_workspace_name"),
        "backup_timestamp": manifest_row.get("backup_timestamp"),
    }


def tables_from_manifest(manifest_tables):
    """Backed-up tables listed in a v2 manifest (only those whose backup succeeded)"""
    tables = []
    for entry in manifest_tables or []:
        if entry.get("status") != "success":
            continue
        expected_rows = entry.get("row_count")
        if expected_rows is None:
            expected_rows = entry.get("rows_written")
        tables.append({
            "schema": entry.get("schema"),
            "table": entry["table"],
            "backup_path": entry.get("backup_path") or backup_table_relpath(entry.get("schema"), entry["table"]),
            "expected_rows": expected_rows,
            "expected_rows_source": "manifest" if expected_rows is not None else None,
            "delta_version": entry.get("delta_version"),
        })
    return tables


def tables_from_backup_listing(discovery):
    """
    Backed-up tables found by listing a backup's tables/ folder (layout v1, or no manifest).
    '<table>_metadata' folders are sidecars when '<table>' sits next to them.
    """
    names = {(t["schema"], t["table"]) for t in discovery["tables"]}
    tables = []
    for t in discovery["tables"]:
        base = t["table"][:-len(SIDECAR_SUFFIX)] if t["table"].endswith(SIDECAR_SUFFIX) else None
        if base and (t["schema"], base) in names:
            continue
        tables.append({
            "schema": t["schema"],
            "table": t["table"],
            "backup_path": backup_table_relpath(t["schema"], t["table"]),
            "expected_rows": None,
            "expected_rows_source": None,
            "delta_version": None,
        })
    return tables


def matches_table_filter(schema, table, patterns):
    """True when no patterns are given, or 'table' / 'schema.table' matches one (fnmatch, case-insensitive)"""
    if not patterns:
        return True
    candidates = [table.lower(), qualified_table_name(schema, table).lower()]
    return any(fnmatch.fnmatchcase(name, pattern.lower()) for pattern in patterns for name in candidates)


def plan_table_restore(backup_tables, target_schema_enabled, target_schema=""):
    """
    Decide where every backed-up table goes in the target lakehouse. Raises ValueError,
    before anything is written, for combinations that cannot work.

    Schema-enabled target:
      - tables from a schema backup keep their schema, or all go to target_schema if set
      - tables from a non-schema backup (v1, or v2 of a non-schema source) go to
        target_schema if set, else dbo
    Non-schema target:
      - target_schema must be empty
      - tables from a schema backup are flattened to Tables/<table>, which only works
        when they all come from one schema
    """
    target_schema = (target_schema or "").strip()
    if target_schema:
        if not target_schema_enabled:
            raise ValueError(
                f"target_schema='{target_schema}' was given, but the target lakehouse is not schema-enabled "
                "(it has no Tables/<schema>/ folders). Leave target_schema empty, or restore into a "
                "schema-enabled lakehouse."
            )
        if is_hidden_name(target_schema) or "/" in target_schema or "." in target_schema:
            raise ValueError(f"target_schema='{target_schema}' is not a valid schema name")

    source_schemas = sorted({t["schema"] for t in backup_tables if t["schema"]})
    if not target_schema_enabled and len(source_schemas) > 1:
        raise ValueError(
            f"The backup holds tables from {len(source_schemas)} schemas ({', '.join(source_schemas)}) "
            "but the target lakehouse is not schema-enabled, so they would be flattened into one "
            "Tables/ folder. Restore into a schema-enabled lakehouse, or set restore_specific_tables "
            "to tables from a single schema."
        )

    plan = []
    for t in backup_tables:
        if target_schema_enabled:
            destination_schema = target_schema or t["schema"] or DEFAULT_SCHEMA
        else:
            destination_schema = None
        plan.append({
            **t,
            "source_name": qualified_table_name(t["schema"], t["table"]),
            "target_schema": destination_schema,
            "target_name": qualified_table_name(destination_schema, t["table"]),
            "target_relpath": target_table_relpath(destination_schema, t["table"]),
        })

    by_target = {}
    for item in plan:
        by_target.setdefault(item["target_relpath"].lower(), []).append(item["source_name"])
    collisions = {path: names for path, names in by_target.items() if len(names) > 1}
    if collisions:
        details = "; ".join(f"{', '.join(names)} -> {path}" for path, names in sorted(collisions.items()))
        raise ValueError(
            f"Several backed-up tables would be restored to the same target table: {details}. "
            "Leave target_schema empty to keep each table in its own schema, or use restore_specific_tables."
        )
    return plan


def looks_like_zip_backup(entry_names):
    """ZIP-format backups came from the original 2025 notebooks (Fabric_Lakehouse_Backup.ipynb)"""
    return any(name.lower().endswith(".zip") or name.endswith("_zip_data") for name in entry_names)


print("✅ Pure helper functions defined successfully")

# CELL ********************

def get_current_timestamp():
    """Return current timestamp in a standardized format"""
    return utc_now().strftime("%Y-%m-%d_%H-%M-%S")

def log_message(message, level="INFO"):
    """Log a message with timestamp and level"""
    timestamp = utc_now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")

    if enable_detailed_logging and level != "DEBUG":
        log_entries.append({
            "timestamp": timestamp,
            "level": level,
            "message": message
        })

def validate_parameters():
    """Validate that required parameters are provided"""
    if not target_lakehouse_name or not target_lakehouse_name.strip():
        raise ValueError("target_lakehouse_name is required")

    if not target_workspace_name or not target_workspace_name.strip():
        raise ValueError("target_workspace_name is required")

    if not backup_path or not backup_path.strip():
        raise ValueError("backup_path is required - use format like 'silver1_transform_dev_backup_2025-11-10_14-30-15'")

    if not backup_lakehouse_name or not backup_lakehouse_name.strip():
        raise ValueError("backup_lakehouse_name is required")

    if not backup_workspace_name or not backup_workspace_name.strip():
        raise ValueError("backup_workspace_name is required")

def get_backup_source_path():
    """Construct path to the backup source in the backup lakehouse"""
    return f"{onelake_lakehouse_path(backup_workspace_name, backup_lakehouse_name)}/Files/{backup_path.strip().strip('/')}"

def get_target_paths():
    """Construct paths to the target lakehouse root, Tables and Files directories"""
    base_path = onelake_lakehouse_path(target_workspace_name, target_lakehouse_name)
    return {
        "root": base_path,
        "tables": f"{base_path}/Tables",
        "files": f"{base_path}/Files"
    }

def write_delta_rows(rows, schema, path):
    """Write rows with an explicit schema as a Delta table (no dict schema inference)"""
    spark.createDataFrame(rows, schema).write.mode("overwrite").format("delta").save(path)

print("✅ Core helper functions defined successfully")

# CELL ********************

def read_backup_manifest(backup_source_path):
    """Read the backup's _manifest row as a dict, or None if it is missing or unreadable"""
    manifest_path = f"{backup_source_path}/_manifest"
    try:
        rows = spark.read.format("delta").load(manifest_path).limit(1).collect()
        if rows:
            return rows[0].asDict(recursive=True)
        log_message("Backup manifest is empty", "WARNING")
    except Exception as e:
        log_message(f"No readable backup manifest at {manifest_path}: {str(e).splitlines()[0]}", "WARNING")
    return None

def read_sidecar(backup_source_path, schema, table):
    """Read a table's _metadata sidecar JSON (v1 and v2 backups), or None"""
    try:
        rows = spark.read.format("delta").load(f"{backup_source_path}/{sidecar_relpath(schema, table)}").collect()
        return json.loads(rows[0]["metadata_json"]) if rows else None
    except Exception:
        return None

def analyze_backup_source(backup_source_path):
    """
    Work out what the backup folder contains and which tables it can restore.
    Fails before anything is written if the folder cannot be restored by this notebook.
    """
    log_message("🔍 Analyzing backup source...", "INFO")

    if not notebookutils.fs.exists(backup_source_path):
        raise ValueError(f"Backup folder not found: {backup_source_path}")

    entry_names = [entry.name.rstrip("/") for entry in notebookutils.fs.ls(backup_source_path)]
    if looks_like_zip_backup(entry_names) and "tables" not in entry_names and "files" not in entry_names:
        raise ValueError(
            "This folder is a ZIP-format backup from the original 2025 notebooks (Fabric_Lakehouse_Backup.ipynb). "
            "This notebook does not restore that format; use Fabric_Lakehouse_Restore.ipynb from commit 0054803 "
            "of the fabric-lakehouse-backup repository."
        )

    backup = parse_backup_manifest(read_backup_manifest(backup_source_path))
    if not backup["has_manifest"]:
        log_message("⚠️  No readable manifest: treating the folder as layout v1 and discovering tables from the folder listing", "WARNING")
    if backup["layout_version"] > SUPPORTED_LAYOUT_VERSION:
        raise ValueError(
            f"Backup layout_version {backup['layout_version']} is newer than this restore notebook supports "
            f"({SUPPORTED_LAYOUT_VERSION}). Update nb_lakehouse_restore."
        )
    if backup["status"] == "failed":
        raise ValueError("The backup manifest says this backup failed before completing; choose another backup folder.")
    if backup["status"] == "partial":
        log_message("⚠️  This backup is partial: some tables or files failed to back up. Only the successful ones are restored.", "WARNING")

    tables = []
    if backup["layout_version"] >= 2 and backup["tables"] is not None:
        tables = tables_from_manifest(backup["tables"])
        backup["table_source"] = "manifest"
    else:
        backup_tables_path = f"{backup_source_path}/tables"
        if notebookutils.fs.exists(backup_tables_path):
            discovery = discover_lakehouse_tables(backup_tables_path, notebookutils.fs.ls, notebookutils.fs.exists)
            tables = tables_from_backup_listing(discovery)
            backup["schema_enabled"] = discovery["schema_enabled"]
        backup["table_source"] = "folder listing"
        for table in tables:
            sidecar = read_sidecar(backup_source_path, table["schema"], table["table"])
            if sidecar and sidecar.get("row_count") is not None:
                table["expected_rows"] = int(sidecar["row_count"])
                table["expected_rows_source"] = "sidecar"
            if sidecar:
                table["delta_version"] = sidecar.get("delta_version")

    backup["backup_tables"] = tables
    backup["has_files"] = notebookutils.fs.exists(f"{backup_source_path}/files")

    layout = "schema (tables/<schema>/<table>)" if backup["schema_enabled"] else "flat (tables/<table>)"
    log_message(f"📋 Backup layout v{backup['layout_version']}, {layout}, status: {backup['status']}", "INFO")
    log_message(f"📋 {len(tables)} restorable tables (from {backup['table_source']}), files folder present: {backup['has_files']}", "INFO")
    return backup

def discover_backup_files(backup_files_path):
    """List every file in the backup's files/ folder with its path relative to files/"""
    files = []

    def walk(path, relative_base):
        for item in notebookutils.fs.ls(path):
            name = item.name.rstrip("/")
            relative_path = f"{relative_base}/{name}".lstrip("/")
            if item.isDir:
                walk(item.path, relative_path)
            else:
                files.append({"name": name, "path": item.path, "relative_path": relative_path,
                              "size": getattr(item, "size", 0)})

    if notebookutils.fs.exists(backup_files_path):
        walk(backup_files_path, "")
    return files

print("✅ Backup analysis functions defined successfully")

# CELL ********************

def restore_tables_direct_copy(backup_source_path, target_paths, plan):
    """
    Restore tables by reading each backup copy and writing it to its planned target location.
    Works across workspaces (reads from the backup lakehouse, writes to the target lakehouse).
    Records status on each plan entry.
    """
    log_message("🎯 Starting direct copy restore of Delta tables", "INFO")

    tables_restored = 0
    tables_failed = 0
    tables_skipped = 0

    for item in plan:
        source_table_path = f"{backup_source_path}/{item['backup_path']}"
        target_table_path = f"{target_paths['root']}/{item['target_relpath']}"
        item["target_path"] = target_table_path

        try:
            version_text = f", captured at source Delta version {item['delta_version']}" if item.get("delta_version") is not None else ""
            log_message(f"  📋 Restoring table: {item['source_name']} -> {item['target_relpath']}{version_text}", "INFO")

            if not overwrite_existing and notebookutils.fs.exists(f"{target_table_path}/_delta_log"):
                log_message(f"    ⚠️  Table {item['target_name']} already exists (use overwrite_existing=True to replace)", "WARNING")
                item["status"] = "skipped"
                tables_skipped += 1
                continue

            if item.get("expected_rows") is not None:
                log_message(f"    📊 Table info: {item['expected_rows']:,} rows at backup time", "INFO")

            log_message("    📖 Reading from backup...", "INFO")
            df = spark.read.format("delta").load(source_table_path)
            partition_columns = list(DeltaTable.forPath(spark, source_table_path).detail().select("partitionColumns").collect()[0][0] or [])

            log_message("    ✍️  Writing to target lakehouse...", "INFO")
            writer = df.write.format("delta")
            if overwrite_existing:
                # Replace the table even if its columns or partitioning changed since the backup.
                # The target's Delta history is kept, so the pre-restore state stays reachable by time travel.
                writer = writer.mode("overwrite").option("overwriteSchema", "true")
            else:
                writer = writer.mode("errorifexists")
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            writer.save(target_table_path)

            item["status"] = "restored"
            tables_restored += 1
            log_message(f"    ✅ Table {item['target_name']} restored successfully", "INFO")

        except Exception as table_error:
            item["status"] = "failed"
            item["error"] = str(table_error)
            tables_failed += 1
            log_message(f"    ❌ Error restoring table {item['source_name']}: {str(table_error)}", "ERROR")

    log_message(f"✅ Table restore completed: {tables_restored} restored, {tables_failed} failed, {tables_skipped} skipped", "INFO")

    return {
        "success": tables_failed == 0,
        "method": "direct_copy_read_write",
        "tables_restored": tables_restored,
        "tables_failed": tables_failed,
        "tables_skipped": tables_skipped
    }

def restore_files_direct_copy(target_files_path, backup_files):
    """
    Restore files one by one to the same relative path under the target's Files/ folder.
    Copying to explicit file paths avoids depending on how a folder copy treats an
    existing destination folder.
    """
    log_message("🎯 Starting direct copy restore of files", "INFO")

    files_restored = 0
    files_failed = 0
    files_skipped = 0
    total_bytes = 0

    for file_info in backup_files:
        relative_path = file_info["relative_path"]
        target_file_path = f"{target_files_path}/{relative_path}"
        try:
            if notebookutils.fs.exists(target_file_path):
                if not overwrite_existing:
                    files_skipped += 1
                    log_message(f"    ⚠️  File exists, skipping: {relative_path}", "WARNING")
                    continue
                notebookutils.fs.rm(target_file_path)

            if notebookutils.fs.cp(file_info["path"], target_file_path) is False:
                raise RuntimeError("notebookutils.fs.cp returned False")

            files_restored += 1
            total_bytes += file_info.get("size") or 0
            log_message(f"    ✅ Copied: {relative_path}", "INFO")
        except Exception as file_error:
            files_failed += 1
            log_message(f"    ❌ Error copying {relative_path}: {str(file_error)}", "ERROR")

    log_message(f"✅ File restore completed: {files_restored} restored, {files_failed} failed, {files_skipped} skipped", "INFO")

    return {
        "success": files_failed == 0,
        "method": "direct_copy",
        "files_restored": files_restored,
        "files_failed": files_failed,
        "files_skipped": files_skipped,
        "total_size_mb": total_bytes / 1024 / 1024
    }

print("✅ Direct copy restore functions defined successfully")

# CELL ********************

def verify_restored_data(plan, file_restore_result):
    """
    Verify every restored table by counting its rows and comparing them, per (schema, table),
    with the row count recorded at backup time (manifest for v2 backups, sidecar for v1).
    """
    if not verify_restore:
        log_message("⏭️  Verification skipped (verify_restore=False)", "INFO")
        return {"skipped": True, "overall_success": True}

    log_message("🔍 Starting data verification...", "INFO")
    verification_results = {
        "tables": {},
        "files": {"restored": file_restore_result.get("files_restored", 0),
                  "failed": file_restore_result.get("files_failed", 0)},
        "overall_success": file_restore_result.get("files_failed", 0) == 0
    }

    for item in plan:
        if item.get("status") != "restored":
            continue
        key = f"{item['source_name']} -> {item['target_name']}"
        expected_count = item.get("expected_rows")
        try:
            actual_count = spark.read.format("delta").load(item["target_path"]).count()
            if expected_count is None:
                data_matches = None
                log_message(f"    ℹ️  {key}: {actual_count:,} rows (no row count recorded at backup time)", "INFO")
            else:
                data_matches = actual_count == expected_count
                if data_matches:
                    log_message(f"    ✅ {key}: {actual_count:,} rows verified", "INFO")
                else:
                    log_message(f"    ⚠️  {key}: Row count mismatch (expected {expected_count:,}, got {actual_count:,})", "WARNING")
                    verification_results["overall_success"] = False
            verification_results["tables"][key] = {
                "schema": item["schema"],
                "table": item["table"],
                "target_schema": item["target_schema"],
                "verified": True,
                "data_matches": data_matches,
                "actual_rows": actual_count,
                "expected_rows": expected_count,
                "expected_rows_source": item.get("expected_rows_source"),
            }
        except Exception as table_verify_error:
            log_message(f"    ❌ Failed to verify table {key}: {str(table_verify_error)}", "ERROR")
            verification_results["tables"][key] = {"verified": False, "error": str(table_verify_error)}
            verification_results["overall_success"] = False

    compared = len([t for t in verification_results["tables"].values() if t.get("data_matches") is not None])
    log_message(f"🔍 Verification complete: {len(verification_results['tables'])} tables checked, {compared} compared with backup row counts", "INFO")

    if verification_results["overall_success"]:
        log_message("✅ All verification checks passed", "INFO")
    else:
        log_message("⚠️  Some verification checks failed - see details above", "WARNING")

    return verification_results

RESTORE_MANIFEST_SCHEMA = StructType([
    StructField("restore_id", StringType(), False),
    StructField("restore_timestamp", StringType(), False),
    StructField("status", StringType(), False),
    StructField("source_backup_path", StringType(), True),
    StructField("backup_folder_name", StringType(), True),
    StructField("backup_layout_version", IntegerType(), True),
    StructField("backup_schema_enabled", BooleanType(), True),
    StructField("backup_lakehouse_name", StringType(), True),
    StructField("backup_workspace_name", StringType(), True),
    StructField("target_lakehouse_name", StringType(), True),
    StructField("target_workspace_name", StringType(), True),
    StructField("target_schema_enabled", BooleanType(), True),
    StructField("target_schema", StringType(), True),
    StructField("restore_type", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("tables_restored", IntegerType(), True),
    StructField("tables_failed", IntegerType(), True),
    StructField("tables_skipped", IntegerType(), True),
    StructField("files_restored", IntegerType(), True),
    StructField("files_failed", IntegerType(), True),
    StructField("verification_passed", BooleanType(), True),
    StructField("restore_json", StringType(), False),
])

LOG_SCHEMA = StructType([
    StructField("timestamp", StringType(), True),
    StructField("level", StringType(), True),
    StructField("message", StringType(), True)
])

print("✅ Verification functions defined successfully")

# CELL ********************

try:
    start_time = utc_now()
    log_message("🔄 Starting Microsoft Fabric Lakehouse COMPLETE RESTORE", "INFO")
    log_message("📋 Restoring Tables and Files from backup", "INFO")

    # Validate parameters
    validate_parameters()

    # Construct paths
    backup_source_path = get_backup_source_path()
    target_paths = get_target_paths()

    log_message(f"📂 Backup source: {backup_source_path}", "INFO")
    log_message(f"🎯 Target lakehouse: {target_lakehouse_name} (Workspace: {target_workspace_name})", "INFO")
    log_message(f"📊 Target tables path: {target_paths['tables']}", "INFO")
    log_message(f"📁 Target files path: {target_paths['files']}", "INFO")

    # ------------------------------------------------------------------
    # Plan: everything below is read-only until the plan is complete
    # ------------------------------------------------------------------
    backup = analyze_backup_source(backup_source_path)
    backup_info = {"backup_type": "direct_copy", "restore_method": "delta_read_write"}

    plan = []
    target_schema_enabled = None
    if restore_tables:
        backup_tables_to_restore = [
            t for t in backup["backup_tables"]
            if matches_table_filter(t["schema"], t["table"], restore_specific_tables)
        ]
        if restore_specific_tables:
            log_message(f"🎯 Restoring specific tables {restore_specific_tables}: {len(backup_tables_to_restore)} matched", "INFO")

        if not notebookutils.fs.exists(target_paths["tables"]):
            raise ValueError(f"Target lakehouse Tables folder not found: {target_paths['tables']}. Create the target lakehouse first.")
        target_discovery = discover_lakehouse_tables(target_paths["tables"], notebookutils.fs.ls, notebookutils.fs.exists)
        target_schema_enabled = target_discovery["schema_enabled"]
        log_message(f"🗂️  Target lakehouse is {'schema-enabled' if target_schema_enabled else 'not schema-enabled'}"
                    f" ({len(target_discovery['tables'])} existing tables)", "INFO")

        plan = plan_table_restore(backup_tables_to_restore, target_schema_enabled, target_schema)

        new_schemas = sorted({item["target_schema"] for item in plan
                              if item["target_schema"] and item["target_schema"] not in target_discovery["schemas"]})
        if new_schemas:
            log_message(f"⚠️  Target schemas that don't exist yet: {', '.join(new_schemas)}. Their folders are created by the restore; "
                        "if a schema doesn't appear in the Lakehouse explorer, create it there (New schema) or with CREATE SCHEMA.", "WARNING")

        for item in plan:
            log_message(f"  🧭 {item['backup_path']} -> {item['target_relpath']}", "INFO")

    backup_files = []
    if restore_files and backup["has_files"]:
        backup_files = discover_backup_files(f"{backup_source_path}/files")
        if restore_specific_files:
            backup_files = [f for f in backup_files
                            if any(fnmatch.fnmatch(f["relative_path"], pattern) for pattern in restore_specific_files)]
            log_message(f"🎯 Restoring files matching {restore_specific_files}: {len(backup_files)} matched", "INFO")
        log_message(f"📁 {len(backup_files)} files to restore", "INFO")

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------
    table_restore_result = {"success": True, "tables_restored": 0, "tables_failed": 0, "tables_skipped": 0}
    file_restore_result = {"success": True, "files_restored": 0, "files_failed": 0, "files_skipped": 0}
    verification_result = {"skipped": True, "overall_success": True}

    if dry_run:
        log_message("🔍 DRY RUN: plan validated, nothing written", "INFO")
    else:
        log_message("=" * 50, "INFO")
        log_message("🎯 DIRECT COPY RESTORE EXECUTION", "INFO")
        log_message("=" * 50, "INFO")

        if restore_tables and plan:
            table_restore_result = restore_tables_direct_copy(backup_source_path, target_paths, plan)

        if restore_files and backup_files:
            file_restore_result = restore_files_direct_copy(target_paths['files'], backup_files)

        verification_result = verify_restored_data(plan, file_restore_result)

    # Record end time
    end_time = utc_now()
    duration_seconds = (end_time - start_time).total_seconds()

    problems = (table_restore_result.get("tables_failed", 0) or file_restore_result.get("files_failed", 0)
                or not verification_result.get("overall_success", False))
    status = "dry_run" if dry_run else ("partial" if problems else "success")

    restore_manifest = {
        "restore_id": str(uuid.uuid4())[:8],
        "restore_timestamp": get_current_timestamp(),
        "status": status,
        "source_backup_path": backup_source_path,
        "backup_folder_name": backup_path,
        "backup_layout_version": backup["layout_version"],
        "backup_schema_enabled": backup["schema_enabled"],
        "backup_lakehouse_name": backup_lakehouse_name,
        "backup_workspace_name": backup_workspace_name,
        "target_lakehouse_name": target_lakehouse_name,
        "target_workspace_name": target_workspace_name,
        "target_schema_enabled": target_schema_enabled,
        "target_schema": target_schema,
        "restore_type": "complete_lakehouse_restore",
        "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "duration_seconds": duration_seconds,
        "tables_restored": table_restore_result.get("tables_restored", 0),
        "tables_failed": table_restore_result.get("tables_failed", 0),
        "tables_skipped": table_restore_result.get("tables_skipped", 0),
        "files_restored": file_restore_result.get("files_restored", 0),
        "files_failed": file_restore_result.get("files_failed", 0),
        "verification_passed": verification_result.get("overall_success", False),
        "backup_type": backup_info["backup_type"],
        "restore_method": backup_info["restore_method"],
        "dry_run": dry_run,
        "tables": [
            {key: item.get(key) for key in ("source_name", "target_name", "backup_path", "target_relpath",
                                            "delta_version", "expected_rows", "status", "error")}
            for item in plan
        ],
        "verification": verification_result,
    }

    if not dry_run:
        # Save restore manifest
        try:
            row = tuple(
                json.dumps(restore_manifest, default=str) if field.name == "restore_json" else restore_manifest.get(field.name)
                for field in RESTORE_MANIFEST_SCHEMA.fields
            )
            write_delta_rows([row], RESTORE_MANIFEST_SCHEMA, f"{target_paths['files']}/_restore_manifest_{get_current_timestamp()}")
            log_message("📝 Restore manifest saved successfully", "INFO")
        except Exception as manifest_error:
            log_message(f"Warning: Could not save restore manifest: {str(manifest_error)}", "WARNING")

    # Write logs
    if enable_detailed_logging and log_entries and not dry_run:
        try:
            log_rows = [(entry["timestamp"], entry["level"], entry["message"]) for entry in log_entries]
            write_delta_rows(log_rows, LOG_SCHEMA, f"{target_paths['files']}/_restore_logs_{get_current_timestamp()}")
            log_message("📝 Restore logs written to target lakehouse", "INFO")
        except Exception as e:
            log_message(f"Error writing restore logs: {str(e)}", "WARNING")

    log_message("", "INFO")
    log_message("🎉 " + "=" * 58, "INFO")
    log_message(f"🎉 LAKEHOUSE RESTORE COMPLETED ({status.upper()})", "INFO")
    log_message("🎉 " + "=" * 58, "INFO")
    log_message(f"✅ Restore finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC", "INFO")
    log_message(f"📂 Target: {target_lakehouse_name} ({target_workspace_name})", "INFO")
    log_message(f"💾 Source backup: {backup_path} (layout v{backup['layout_version']})", "INFO")

    if dry_run:
        log_message("🔍 DRY RUN COMPLETED - No actual changes made", "INFO")
    else:
        log_message(f"📊 Tables restored: {table_restore_result.get('tables_restored', 0)}"
                    f" (failed {table_restore_result.get('tables_failed', 0)}, skipped {table_restore_result.get('tables_skipped', 0)})", "INFO")
        log_message(f"📁 Files restored: {file_restore_result.get('files_restored', 0)}"
                    f" (failed {file_restore_result.get('files_failed', 0)}, skipped {file_restore_result.get('files_skipped', 0)})", "INFO")

        if 'total_size_mb' in file_restore_result:
            log_message(f"📦 File data restored: {file_restore_result['total_size_mb']:.2f} MB", "INFO")

        if verification_result.get("skipped"):
            log_message("⏭️  Verification: Skipped", "INFO")
        elif verification_result.get("overall_success"):
            log_message("✅ Verification: All checks passed", "INFO")
        else:
            log_message("⚠️  Verification: Some checks failed", "WARNING")

    log_message(f"⏱️  Duration: {duration_seconds:.2f} seconds", "INFO")
    log_message("=" * 60, "INFO")

    # Final result
    final_result = {
        "status": status,
        "restore_type": "complete_lakehouse",
        "target_lakehouse_name": target_lakehouse_name,
        "target_workspace_name": target_workspace_name,
        "target_schema_enabled": target_schema_enabled,
        "target_schema": target_schema,
        "source_backup": backup_source_path,
        "backup_folder_name": backup_path,
        "backup_layout_version": backup["layout_version"],
        "duration_seconds": duration_seconds,
        "dry_run": dry_run,
        "tables_restored": table_restore_result.get("tables_restored", 0),
        "tables_failed": table_restore_result.get("tables_failed", 0),
        "files_restored": file_restore_result.get("files_restored", 0),
        "files_failed": file_restore_result.get("files_failed", 0),
        "verification_passed": verification_result.get("overall_success", False),
        "backup_type": backup_info["backup_type"],
        "restore_method": backup_info["restore_method"]
    }

    print("\n🎉 LAKEHOUSE RESTORE RESULT:")
    print(json.dumps(final_result, indent=2, default=str))

    if status == "partial" and fail_on_partial_restore:
        raise RuntimeError(
            f"Restore finished with problems: {table_restore_result.get('tables_failed', 0)} tables failed, "
            f"{file_restore_result.get('files_failed', 0)} files failed, verification passed: "
            f"{verification_result.get('overall_success', False)}. See the log above."
        )

except Exception as e:
    log_message(f"💥 Lakehouse restore FAILED: {str(e)}", "ERROR")
    log_message(f"Full error trace: {traceback.format_exc()}", "ERROR")

    failure_result = {
        "status": "failed",
        "error": str(e),
        "timestamp": utc_now().strftime("%Y-%m-%d %H:%M:%S")
    }

    print("\n💥 LAKEHOUSE RESTORE RESULT:")
    print(json.dumps(failure_result, indent=2))

    # Re-raise so the Fabric job (and the Azure DevOps pipeline) reports the failure
    raise
