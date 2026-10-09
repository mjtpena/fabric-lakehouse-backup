# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

# Microsoft Fabric Lakehouse Complete Backup Notebook
# Backs up Delta tables (full read/write copy) and files (file copy) into a
# timestamped folder under the backup lakehouse's Files area.
# Supports schema-enabled lakehouses (Tables/<schema>/<table>) and
# non-schema lakehouses (Tables/<table>).

# Parameters - Configure these values or override when scheduling
source_lakehouse_name = ""  # REQUIRED: Source lakehouse name (e.g., "lh_silver1")
source_workspace_name = ""  # REQUIRED: Source workspace name (e.g., "wsp_transform_dev")
backup_lakehouse_name = ""  # REQUIRED: Backup lakehouse name (e.g., "lh_backup")
backup_workspace_name = ""  # REQUIRED: Backup workspace name (e.g., "wsp_lakehouse_backup_dev")

# Backup options
backup_tables = True  # Backup tables from Tables directory
backup_files = True   # Backup files from Files directory

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "tags": [
# META     "parameters"
# META   ]
# META }

# CELL ********************

import json
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
verify_backup = True  # Always verify backups
enable_detailed_logging = True  # Always enable detailed logging
retention_days = 30  # Backup retention period in days
count_rows = True  # Count rows of every source table before backup (a full scan on large tables); False skips it
fail_on_partial_backup = True  # Fail the notebook run (and the pipeline) if any table or file fails to back up

print("📚 Libraries imported successfully")
print("⚙️  Spark configured for ancient datetime handling (pre-1582 dates, pre-1900 timestamps)")
print(f"⚙️  Spark {spark.version}, Python {platform.python_version()}")

# CELL ********************

# Pure helpers: no Spark or notebookutils calls. Unit tested in tests/.
import datetime

LAYOUT_VERSION = 2  # Backup folder layout written by this notebook (see README "Backup Storage Structure")
BACKUP_TIMESTAMP_FORMAT = "%Y-%m-%d_%H-%M-%S"
SIDECAR_SUFFIX = "_metadata"


def utc_now():
    """Current UTC time as a naive datetime (Fabric Spark drivers run in UTC)"""
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


def onelake_lakehouse_path(workspace_name, lakehouse_name):
    """ABFS path of a lakehouse item in OneLake"""
    return f"abfss://{workspace_name.strip()}@onelake.dfs.fabric.microsoft.com/{lakehouse_name.strip()}.Lakehouse"


def build_backup_folder_name(lakehouse_name, workspace_name, timestamp):
    """
    Backup folder name: {lakehouse}_{workspace}_backup_{YYYY-MM-DD_HH-MM-SS}
    e.g. "silver1_transform_dev_backup_2025-11-10_14-30-15".
    The cleanup notebook parses the timestamp suffix, so keep this format stable.
    """
    lakehouse_short = lakehouse_name.strip().lower().replace("lh_", "").replace("_lakehouse", "")
    workspace_short = workspace_name.strip().lower().replace("wsp_", "").replace("_workspace", "")
    return f"{lakehouse_short}_{workspace_short}_backup_{timestamp.strftime(BACKUP_TIMESTAMP_FORMAT)}"


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


def find_sidecar_collisions(tables):
    """
    (schema, table) pairs whose backup location is also the metadata sidecar of another
    table, e.g. a table named 'orders_metadata' next to a table named 'orders'.
    """
    names = {(t["schema"], t["table"]) for t in tables}
    return sorted(
        ((schema, table) for schema, table in names
         if table.endswith(SIDECAR_SUFFIX) and (schema, table[:-len(SIDECAR_SUFFIX)]) in names),
        key=lambda pair: (pair[0] or "", pair[1]),
    )


def get_file_type(filename):
    """Determine file type based on extension"""
    if '.' not in filename:
        return "unknown"

    extension = filename.split('.')[-1].lower()

    file_types = {
        'jpg': 'image', 'jpeg': 'image', 'png': 'image', 'gif': 'image', 'bmp': 'image', 'svg': 'image', 'webp': 'image',
        'pdf': 'document', 'doc': 'document', 'docx': 'document', 'txt': 'text', 'rtf': 'document',
        'xls': 'spreadsheet', 'xlsx': 'spreadsheet', 'csv': 'data',
        'json': 'data', 'xml': 'data', 'yaml': 'data', 'yml': 'data', 'parquet': 'data',
        'zip': 'archive', 'rar': 'archive', '7z': 'archive', 'tar': 'archive', 'gz': 'archive',
        'py': 'code', 'sql': 'code', 'js': 'code', 'html': 'code', 'css': 'code',
        'mp4': 'video', 'avi': 'video', 'mov': 'video', 'mp3': 'audio', 'wav': 'audio'
    }

    return file_types.get(extension, 'other')


def create_restore_instructions(backup_folder, schema_enabled, tables_count, files_count):
    """Human-readable restore instructions stored with every backup (RESTORE_INSTRUCTIONS table)"""
    if schema_enabled:
        layout = "schema-enabled source lakehouse: tables/<schema>/<table>"
        table_path = "tables/<schema>/<table>"
        target_path = "Tables/<schema>/<table>"
    else:
        layout = "non-schema source lakehouse: tables/<table>"
        table_path = "tables/<table>"
        target_path = "Tables/<table>"

    return f"""# Lakehouse Backup - Restore Instructions

Backup folder: {backup_folder}
Layout version: {LAYOUT_VERSION} ({layout})

## Backup Method: Full Delta Copy + File Copy
- Every Delta table was read from the source lakehouse at a fixed Delta version and written
  here as a new, independent Delta table (spark.read + df.write). The source version is in
  the table's `_metadata` sidecar.
- Every file under Files/ was copied with notebookutils.fs.cp.
- The backup shares no data files with the source. It survives VACUUM, overwrites, dropped
  tables and deletion of the source lakehouse, and it uses as much storage as the data it holds.

## Backup Contents
- {tables_count} tables
- {files_count} files
- `_manifest` (backup summary and table list), `_logs`, `backup_metadata`, this table

Preserved: rows, column names and types, partition columns.
Not preserved: the source table's Delta history (time travel), table properties, constraints,
column comments, Spark views, and shortcuts (shortcut targets are copied as data).

## Restore Tables

### Option 1: Use the restore notebook (recommended)
Run `nb_lakehouse_restore` (or the restore pipeline) with `backup_path` = `{backup_folder}`.
It restores into schema-enabled and non-schema lakehouses; see `target_schema` in the README.

### Option 2: Manual copy of one table
```python
backup_table = "<backup lakehouse path>/Files/{backup_folder}/{table_path}"
target_table = "<target lakehouse path>/{target_path}"

df = spark.read.format("delta").load(backup_table)
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_table)
```

### Option 3: Temporary access with SHALLOW CLONE (same workspace only)
Fabric supports SHALLOW CLONE only; DEEP CLONE is not available. A shallow clone of a table in
this backup gives fast, read-mostly access without copying data:
```sql
CREATE TABLE restored_table SHALLOW CLONE delta.`<backup lakehouse path>/Files/{backup_folder}/{table_path}`
```
A shallow clone references this backup's data files instead of copying them. When this backup
folder is deleted (for example by the cleanup job once it is older than the retention period)
or its files are vacuumed, the clone breaks. Use Option 1 or 2 for a durable restore.

## Restore Files

### Option 1: Use the restore notebook (recommended)
The restore notebook copies every file back to the same relative path under Files/.

### Option 2: Manual file copy
```python
import notebookutils

source_file = "<backup lakehouse path>/Files/{backup_folder}/files/subfolder/file.csv"
target_file = "<target lakehouse path>/Files/subfolder/file.csv"
notebookutils.fs.cp(source_file, target_file)
```

## Backup Structure
```
{backup_folder}/
├── tables/
│   ├── {"<schema>/" if schema_enabled else ""}<table>/            # Delta table (full copy)
│   └── {"<schema>/" if schema_enabled else ""}<table>_metadata/   # Sidecar: schema, table, source_path, delta_version, ...
├── files/                     # Files/ contents, original formats
├── _manifest/                 # Backup summary (manifest_json holds the full table list)
├── _logs/                     # Execution log
├── backup_metadata/
└── RESTORE_INSTRUCTIONS/      # This text
```

## Verification
After a restore, compare row counts with the `row_count` recorded in `_manifest`:
```python
df = spark.read.format("delta").load("<restored table path>")
print(df.count())
```
"""


print("✅ Pure helper functions defined successfully")

# CELL ********************

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
    if not source_lakehouse_name or not source_lakehouse_name.strip():
        raise ValueError("source_lakehouse_name is required")

    if not source_workspace_name or not source_workspace_name.strip():
        raise ValueError("source_workspace_name is required")

    if not backup_lakehouse_name or not backup_lakehouse_name.strip():
        raise ValueError("backup_lakehouse_name is required")

    if not backup_workspace_name or not backup_workspace_name.strip():
        raise ValueError("backup_workspace_name is required")

def get_source_paths():
    """Construct paths to the source lakehouse Tables and Files directories"""
    base_path = onelake_lakehouse_path(source_workspace_name, source_lakehouse_name)
    return {
        "tables": f"{base_path}/Tables",
        "files": f"{base_path}/Files"
    }

def get_backup_base_path():
    """Construct the base path for the backup lakehouse"""
    return f"{onelake_lakehouse_path(backup_workspace_name, backup_lakehouse_name)}/Files"

def write_json_table(value, path, column):
    """Write a single JSON document as a one-row, one-column Delta table"""
    schema = StructType([StructField(column, StringType(), False)])
    spark.createDataFrame([(value,)], schema).write.mode("overwrite").format("delta").save(path)

print("✅ Core helper functions defined successfully")

# CELL ********************

def discover_tables(tables_path):
    """Discover Delta tables in the source lakehouse (schema-enabled or not)"""
    log_message("🔍 Discovering tables...", "INFO")
    discovery = discover_lakehouse_tables(tables_path, notebookutils.fs.ls, notebookutils.fs.exists)

    layout = "schema-enabled (Tables/<schema>/<table>)" if discovery["schema_enabled"] else "non-schema (Tables/<table>)"
    log_message(f"Lakehouse layout: {layout}", "INFO")
    for skipped in discovery["skipped"]:
        log_message(f"  ⏭️  Skipping {skipped['path']}: {skipped['reason']}", "WARNING")
    if discovery["schema_enabled"]:
        log_message(f"Schemas: {', '.join(discovery['schemas'])}", "INFO")
        if "dbo" not in discovery["schemas"]:
            log_message("No dbo schema folder found; check that this is a schema-enabled lakehouse", "WARNING")

    log_message(f"Found {len(discovery['tables'])} tables", "INFO")
    return discovery

def read_delta(table_path, version=None):
    """Read a Delta table, pinned to a version when one is given"""
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    return reader.load(table_path)

def get_table_info(table_path, schema_name, table_name):
    """Get information about a table: Delta version, partitioning, columns and (optionally) row count"""
    table_info = {
        "schema": schema_name,
        "table": table_name,
        "name": table_name,
        "qualified_name": qualified_table_name(schema_name, table_name),
        "source_path": table_path,
    }
    try:
        delta_table = DeltaTable.forPath(spark, table_path)
        table_info["delta_version"] = int(delta_table.history(1).select("version").collect()[0][0])
        table_info["partition_columns"] = list(delta_table.detail().select("partitionColumns").collect()[0][0] or [])

        # Pin reads to the captured version so the count, the copy and delta_version agree
        df = read_delta(table_path, table_info["delta_version"])
        table_info["columns"] = df.columns
        table_info["column_count"] = len(df.columns)
        table_info["data_schema"] = df.schema.simpleString()
        table_info["row_count"] = df.count() if count_rows else None
        return table_info
    except Exception as e:
        log_message(f"Error getting info for table {table_info['qualified_name']}: {str(e)}", "WARNING")
        table_info["error"] = str(e)
        return table_info

def get_rows_written(table_path):
    """Rows written by the latest commit of a Delta table, from its operation metrics (None if not recorded)"""
    try:
        metrics = DeltaTable.forPath(spark, table_path).history(1).select("operationMetrics").collect()[0][0] or {}
        value = metrics.get("numOutputRows")
        return int(value) if value is not None else None
    except Exception:
        return None

print("✅ Table discovery functions defined successfully")

# CELL ********************

def discover_files(files_path):
    """Discover all files in the Files directory"""
    try:
        log_message("🔍 Discovering files...", "INFO")

        try:
            files_info = []

            def process_path(path, relative_base=""):
                try:
                    items = notebookutils.fs.ls(path)
                    for item in items:
                        if not item.name.startswith('_') and not item.name.startswith('.'):
                            relative_path = f"{relative_base}/{item.name.rstrip('/')}".lstrip('/')
                            file_info = {
                                "name": item.name.rstrip('/'),
                                "path": item.path,
                                "relative_path": relative_path,
                                "is_directory": item.isDir,
                                "size": item.size if hasattr(item, 'size') else 0,
                                "type": "directory" if item.isDir else get_file_type(item.name)
                            }
                            files_info.append(file_info)

                            if item.isDir:
                                process_path(item.path, relative_path)
                except Exception as e:
                    log_message(f"Error processing path {path}: {str(e)}", "WARNING")

            process_path(files_path)
            log_message(f"Found {len(files_info)} files/directories", "INFO")
            return files_info

        except Exception as e:
            log_message(f"File discovery using notebookutils failed: {str(e)}", "WARNING")

        # Fallback to Spark
        try:
            files_df = spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(files_path + "/*")
            file_rows = files_df.select("path", "length", "modificationTime").collect()

            files_info = []
            for row in file_rows:
                file_path = row.path
                if '/Files/' in file_path:
                    relative_path = file_path.split('/Files/')[1]
                    file_name = relative_path.split('/')[-1]

                    files_info.append({
                        "name": file_name,
                        "path": file_path,
                        "relative_path": relative_path,
                        "is_directory": False,
                        "size": row.length,
                        "type": get_file_type(file_name),
                        "modification_time": row.modificationTime
                    })

            log_message(f"Found {len(files_info)} files using Spark", "INFO")
            return files_info

        except Exception as e:
            log_message(f"Spark file discovery failed: {str(e)}", "ERROR")
            return []

    except Exception as e:
        log_message(f"File discovery failed: {str(e)}", "ERROR")
        return []

print("✅ File discovery functions defined successfully")

# CELL ********************

def backup_tables_direct_copy(backup_path, tables_info):
    """
    Back up every table by reading it from the source (pinned to its captured Delta version)
    and writing a new Delta table into the backup folder. Works across workspaces.
    Records status, backup_path and rows_written on each entry of tables_info.
    """
    try:
        log_message("📊 Starting full-copy backup for tables", "INFO")

        tables_backed_up = 0
        tables_failed = 0

        for table_info in tables_info:
            qualified_name = table_info["qualified_name"]

            if "error" in table_info:
                log_message(f"  ⚠️  Skipping table {qualified_name} - discovery error: {table_info['error']}", "WARNING")
                table_info["status"] = "failed"
                tables_failed += 1
                continue

            try:
                row_count = table_info.get("row_count")
                rows_text = f"{row_count:,} rows" if row_count is not None else "rows not counted"
                log_message(f"  📋 Backing up table: {qualified_name} ({rows_text}, Delta version {table_info['delta_version']})", "INFO")

                relpath = backup_table_relpath(table_info["schema"], table_info["table"])
                dest_table_path = f"{backup_path}/{relpath}"

                log_message("    📖 Reading from source lakehouse...", "INFO")
                df = read_delta(table_info["source_path"], table_info["delta_version"])

                log_message("    ✍️  Writing to backup lakehouse...", "INFO")
                writer = df.write.mode("overwrite").format("delta")
                if table_info["partition_columns"]:
                    writer = writer.partitionBy(*table_info["partition_columns"])
                writer.save(dest_table_path)

                table_info["backup_path"] = relpath
                table_info["rows_written"] = get_rows_written(dest_table_path)

                # Per-table metadata sidecar, next to the table copy
                table_metadata = {
                    "layout_version": LAYOUT_VERSION,
                    "schema": table_info["schema"],
                    "table": table_info["table"],
                    "table_name": table_info["table"],
                    "qualified_name": qualified_name,
                    "source_path": table_info["source_path"],
                    "backup_path": relpath,
                    "delta_version": table_info["delta_version"],
                    "row_count": row_count,
                    "rows_written": table_info["rows_written"],
                    "columns": table_info.get("columns", []),
                    "partition_columns": table_info["partition_columns"],
                    "backup_method": "delta_read_write",
                    "backup_timestamp": utc_now().isoformat(),
                }
                write_json_table(
                    json.dumps(table_metadata, indent=2),
                    f"{backup_path}/{sidecar_relpath(table_info['schema'], table_info['table'])}",
                    "metadata_json",
                )

                table_info["status"] = "success"
                tables_backed_up += 1
                log_message(f"    ✅ Table {qualified_name} backed up successfully", "INFO")

            except Exception as table_error:
                log_message(f"    ❌ Error backing up table {qualified_name}: {str(table_error)}", "ERROR")
                table_info["status"] = "failed"
                table_info["error"] = str(table_error)
                tables_failed += 1

        log_message(f"✅ Tables backup complete: {tables_backed_up} successful, {tables_failed} failed", "INFO")

        return {
            "success": True,
            "tables_backed_up": tables_backed_up,
            "tables_failed": tables_failed
        }

    except Exception as e:
        log_message(f"Tables backup failed: {str(e)}", "ERROR")
        return {"success": False, "error": str(e), "tables_backed_up": 0, "tables_failed": len(tables_info)}

def backup_files_direct_copy(backup_path, files_info):
    """Backup all files by copying directly to backup location"""
    try:
        log_message("📁 Starting direct copy backup for files", "INFO")

        files_backed_up = 0
        files_failed = 0

        for file_info in files_info:
            if file_info.get('is_directory', False):
                continue  # Skip directories

            try:
                log_message(f"  📄 Backing up file: {file_info['relative_path']}", "INFO")

                source_file_path = file_info['path']
                dest_file_path = f"{backup_path}/files/{file_info['relative_path']}"

                # Direct file copy using notebookutils (cp returns False on failure)
                if notebookutils.fs.cp(source_file_path, dest_file_path, True) is False:
                    raise RuntimeError("notebookutils.fs.cp returned False")

                files_backed_up += 1
                log_message(f"    ✅ File {file_info['name']} backed up successfully", "INFO")

            except Exception as file_error:
                log_message(f"    ❌ Error backing up file {file_info['name']}: {str(file_error)}", "ERROR")
                files_failed += 1

        log_message(f"✅ Files backup complete: {files_backed_up} successful, {files_failed} failed", "INFO")

        return {
            "success": True,
            "files_backed_up": files_backed_up,
            "files_failed": files_failed
        }

    except Exception as e:
        log_message(f"Files backup failed: {str(e)}", "ERROR")
        return {"success": False, "error": str(e), "files_backed_up": 0, "files_failed": len(files_info)}

def create_unified_direct_copy_backup(backup_path, backup_folder, schema_enabled, tables_info, files_info):
    """Create a complete lakehouse backup: full Delta read/write copy for tables, file copy for files"""
    try:
        log_message("🎯 Creating unified direct copy backup", "INFO")

        start_time = utc_now()

        # Backup tables
        tables_result = backup_tables_direct_copy(backup_path, tables_info) if backup_tables else {"success": True, "tables_backed_up": 0, "tables_failed": 0}

        # Backup files
        files_result = backup_files_direct_copy(backup_path, files_info) if backup_files else {"success": True, "files_backed_up": 0, "files_failed": 0}

        # Create backup metadata
        backup_metadata = {
            "backup_type": "direct_copy_lakehouse_backup",
            "backup_method": "delta_read_write_and_file_copy",
            "layout_version": LAYOUT_VERSION,
            "schema_enabled": schema_enabled,
            "created_at": start_time.isoformat(),
            "source_lakehouse_name": source_lakehouse_name,
            "source_workspace_name": source_workspace_name,
            "backup_lakehouse_name": backup_lakehouse_name,
            "backup_workspace_name": backup_workspace_name,
            "tables_backed_up": tables_result.get("tables_backed_up", 0),
            "tables_failed": tables_result.get("tables_failed", 0),
            "files_backed_up": files_result.get("files_backed_up", 0),
            "files_failed": files_result.get("files_failed", 0),
            "backup_duration_seconds": (utc_now() - start_time).total_seconds(),
            "spark_version": spark.version,
        }

        # Save metadata
        write_json_table(json.dumps(backup_metadata, indent=2), f"{backup_path}/backup_metadata", "metadata_json")

        # Create restore instructions
        instructions = create_restore_instructions(
            backup_folder,
            schema_enabled,
            tables_result.get("tables_backed_up", 0),
            files_result.get("files_backed_up", 0)
        )
        write_json_table(instructions, f"{backup_path}/RESTORE_INSTRUCTIONS", "instructions")

        log_message("🎉 Direct copy backup completed", "INFO")

        return {
            "success": True,
            "method": "direct_copy",
            "tables_backed_up": tables_result.get("tables_backed_up", 0),
            "tables_failed": tables_result.get("tables_failed", 0),
            "files_backed_up": files_result.get("files_backed_up", 0),
            "files_failed": files_result.get("files_failed", 0),
            "total_items": tables_result.get("tables_backed_up", 0) + files_result.get("files_backed_up", 0),
            "duration_seconds": backup_metadata["backup_duration_seconds"]
        }

    except Exception as e:
        log_message(f"Direct copy backup failed: {str(e)}", "ERROR")
        return {"success": False, "error": str(e)}

def verify_backed_up_tables(tables_info):
    """
    Compare the rows written to each backup table (from Delta operation metrics) with the
    row count captured from the source at the same Delta version.
    Returns True when every comparable table matches.
    """
    all_match = True
    for table_info in tables_info:
        if table_info.get("status") != "success":
            continue
        expected = table_info.get("row_count")
        written = table_info.get("rows_written")
        if expected is None or written is None:
            table_info["verification"] = "not_compared"
            continue
        if expected == written:
            table_info["verification"] = "rows_match"
        else:
            table_info["verification"] = "rows_mismatch"
            all_match = False
            log_message(f"    ⚠️  {table_info['qualified_name']}: source had {expected:,} rows, backup wrote {written:,}", "WARNING")
    return all_match

MANIFEST_SCHEMA = StructType([
    StructField("layout_version", IntegerType(), False),
    StructField("backup_id", StringType(), False),
    StructField("backup_folder_name", StringType(), False),
    StructField("backup_timestamp", StringType(), True),
    StructField("status", StringType(), False),
    StructField("backup_type", StringType(), True),
    StructField("backup_method", StringType(), True),
    StructField("source_lakehouse_name", StringType(), True),
    StructField("source_workspace_name", StringType(), True),
    StructField("backup_lakehouse_name", StringType(), True),
    StructField("backup_workspace_name", StringType(), True),
    StructField("schema_enabled", BooleanType(), True),
    StructField("tables_discovered", IntegerType(), True),
    StructField("tables_backed_up", IntegerType(), True),
    StructField("tables_failed", IntegerType(), True),
    StructField("files_discovered", IntegerType(), True),
    StructField("files_backed_up", IntegerType(), True),
    StructField("files_failed", IntegerType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("spark_version", StringType(), True),
    StructField("manifest_json", StringType(), False),
])

def write_manifest(backup_path, manifest):
    """
    Write the _manifest Delta table. Scalar fields become columns for easy querying;
    the full manifest (including the per-table list) is stored as JSON in manifest_json.
    An explicit schema keeps this independent of Spark's dict schema inference.
    """
    row = tuple(
        json.dumps(manifest, default=str) if field.name == "manifest_json" else manifest.get(field.name)
        for field in MANIFEST_SCHEMA.fields
    )
    spark.createDataFrame([row], MANIFEST_SCHEMA).write.mode("overwrite").format("delta").save(f"{backup_path}/_manifest")

def write_logs(backup_path):
    """Write the execution log to the backup folder"""
    log_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("level", StringType(), True),
        StructField("message", StringType(), True)
    ])
    log_rows = [(entry["timestamp"], entry["level"], entry["message"]) for entry in log_entries]
    spark.createDataFrame(log_rows, log_schema).write.format("delta").mode("overwrite").save(f"{backup_path}/_logs")

print("✅ Direct copy backup functions defined successfully")

# CELL ********************

backup_path = None
manifest = None
try:
    start_time = utc_now()
    log_message("🚀 Starting Microsoft Fabric COMPLETE LAKEHOUSE BACKUP", "INFO")
    log_message("📋 Backing up Tables (full Delta read/write copy) + Files (file copy)", "INFO")

    # Validate parameters
    validate_parameters()

    # Construct paths
    source_paths = get_source_paths()
    backup_base_path = get_backup_base_path()
    backup_folder = build_backup_folder_name(source_lakehouse_name, source_workspace_name, start_time)
    backup_path = f"{backup_base_path}/{backup_folder}"

    log_message(f"📂 Source lakehouse: {source_lakehouse_name} (Workspace: {source_workspace_name})", "INFO")
    log_message(f"📊 Source tables path: {source_paths['tables']}", "INFO")
    log_message(f"📁 Source files path: {source_paths['files']}", "INFO")
    log_message(f"💾 Backup lakehouse: {backup_lakehouse_name} (Workspace: {backup_workspace_name})", "INFO")
    log_message(f"📦 Backup folder: {backup_folder}", "INFO")
    log_message("🎯 Backup method: full copy (Delta read/write + file copy)", "INFO")

    # Test backup path access
    try:
        test_path = f"{backup_path}/_test_access"
        test_df = spark.createDataFrame([("test",)], StructType([StructField("value", StringType(), True)]))
        test_df.write.mode("overwrite").format("delta").save(test_path)
        try:
            notebookutils.fs.rm(test_path, True)
        except Exception:
            pass
        log_message("✅ Backup path access confirmed", "INFO")
    except Exception as e:
        raise ValueError(f"Cannot write to backup location {backup_path}: {str(e)}")

    # Initialize results
    tables_info = []
    files_info = []
    schema_enabled = None  # Unknown unless tables are backed up

    # Discover and analyze tables
    if backup_tables:
        log_message("=" * 50, "INFO")
        log_message("🔍 TABLES DISCOVERY PHASE", "INFO")
        log_message("=" * 50, "INFO")

        discovery = discover_tables(source_paths['tables'])
        schema_enabled = discovery["schema_enabled"]
        collisions = set(find_sidecar_collisions(discovery["tables"]))

        if not discovery["tables"]:
            log_message("⚠️  No tables found in source lakehouse", "WARNING")
        else:
            names = ", ".join(qualified_table_name(t["schema"], t["table"]) for t in discovery["tables"])
            log_message(f"📋 Found {len(discovery['tables'])} tables: {names}", "INFO")

            for table in discovery["tables"]:
                qualified_name = qualified_table_name(table["schema"], table["table"])
                log_message(f"  🔍 Analyzing table: {qualified_name}", "INFO")

                if (table["schema"], table["table"]) in collisions:
                    sibling = qualified_table_name(table["schema"], table["table"][:-len(SIDECAR_SUFFIX)])
                    table_info = {
                        "schema": table["schema"], "table": table["table"], "name": table["table"],
                        "qualified_name": qualified_name, "source_path": table["path"],
                        "error": f"name collides with the metadata sidecar of table {sibling}; rename one of the tables to back it up",
                    }
                else:
                    table_info = get_table_info(table["path"], table["schema"], table["table"])
                tables_info.append(table_info)

                if "error" not in table_info:
                    rows_text = f"{table_info['row_count']:,} rows" if table_info["row_count"] is not None else "rows not counted"
                    log_message(f"    ✅ {qualified_name}: {rows_text}, {table_info['column_count']} columns, Delta version {table_info['delta_version']}", "INFO")
                else:
                    log_message(f"    ❌ {qualified_name}: {table_info['error']}", "WARNING")

    # Discover and analyze files
    if backup_files:
        log_message("=" * 50, "INFO")
        log_message("🔍 FILES DISCOVERY PHASE", "INFO")
        log_message("=" * 50, "INFO")

        files_info = discover_files(source_paths['files'])

        if not files_info:
            log_message("⚠️  No files found in source lakehouse Files directory", "WARNING")
        else:
            # Analyze files
            total_files = len([f for f in files_info if not f.get('is_directory', False)])
            total_size = sum(f.get('size', 0) for f in files_info if not f.get('is_directory', False))

            # Group by type
            by_type = {}
            for file_info in files_info:
                if not file_info.get('is_directory', False):
                    file_type = file_info.get('type', 'unknown')
                    if file_type not in by_type:
                        by_type[file_type] = {'count': 0, 'size': 0}
                    by_type[file_type]['count'] += 1
                    by_type[file_type]['size'] += file_info.get('size', 0)

            log_message(f"📁 File analysis: {total_files} files, {total_size/1024/1024:.2f} MB total", "INFO")
            for file_type, stats in by_type.items():
                log_message(f"   - {file_type}: {stats['count']} files ({stats['size']/1024/1024:.2f} MB)", "INFO")

    # Execute backup
    if (backup_tables and tables_info) or (backup_files and files_info):
        log_message("=" * 50, "INFO")
        log_message("🎯 DIRECT COPY BACKUP EXECUTION", "INFO")
        log_message("=" * 50, "INFO")

        backup_result = create_unified_direct_copy_backup(backup_path, backup_folder, schema_enabled, tables_info, files_info)
        if not backup_result.get("success"):
            raise RuntimeError(f"Backup failed: {backup_result.get('error')}")

    else:
        log_message("⚠️  No tables or files found to backup", "WARNING")
        backup_result = {
            "success": True,
            "status": "completed",
            "message": "No data found to backup",
            "tables_found": len(tables_info),
            "files_found": len(files_info)
        }

    # Verification: rows written to each backup table vs. rows counted at the source
    verification_result = None
    if verify_backup and tables_info:
        log_message("🔍 Starting backup verification...", "INFO")
        verification_result = verify_backed_up_tables(tables_info)
        compared = len([t for t in tables_info if t.get("verification") in ("rows_match", "rows_mismatch")])
        log_message(f"{'✅' if verification_result else '⚠️ '} Backup verification: {compared} tables compared, "
                    f"{'all row counts match' if verification_result else 'row count mismatch found'}", "INFO")

    # Record end time
    end_time = utc_now()
    duration_seconds = (end_time - start_time).total_seconds()

    tables_failed = backup_result.get("tables_failed", 0)
    files_failed = backup_result.get("files_failed", 0)
    status = "success" if not tables_failed and not files_failed and verification_result is not False else "partial"
    file_entries = [f for f in files_info if not f.get('is_directory', False)]

    # Create comprehensive manifest
    manifest = {
        "layout_version": LAYOUT_VERSION,
        "backup_id": str(uuid.uuid4()),
        "backup_folder_name": backup_folder,
        "backup_timestamp": start_time.strftime(BACKUP_TIMESTAMP_FORMAT),
        "status": status,
        "backup_type": "complete_lakehouse_direct_copy",
        "backup_method": "direct_copy",
        "source_lakehouse_name": source_lakehouse_name,
        "source_workspace_name": source_workspace_name,
        "backup_lakehouse_name": backup_lakehouse_name,
        "backup_workspace_name": backup_workspace_name,
        "schema_enabled": schema_enabled,
        "tables_discovered": len(tables_info),
        "tables_backed_up": backup_result.get("tables_backed_up", 0),
        "tables_failed": tables_failed,
        "files_discovered": len(file_entries),
        "files_backed_up": backup_result.get("files_backed_up", 0),
        "files_failed": files_failed,
        "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "duration_seconds": duration_seconds,
        "spark_version": spark.version,
        "python_version": platform.python_version(),
        "count_rows": count_rows,
        "verification_passed": verification_result,
        "backup_result": backup_result,
        "tables": [
            {key: table_info.get(key) for key in (
                "schema", "table", "qualified_name", "status", "backup_path", "source_path",
                "delta_version", "row_count", "rows_written", "column_count", "columns",
                "partition_columns", "data_schema", "verification", "error",
            )}
            for table_info in tables_info
        ],
        "files_summary": {
            "total_files": len(file_entries),
            "total_size_mb": sum(f.get('size', 0) for f in file_entries) / 1024 / 1024
        }
    }

    write_manifest(backup_path, manifest)

    # Write detailed logs
    if enable_detailed_logging and log_entries:
        try:
            write_logs(backup_path)
            log_message("📝 Detailed logs written to backup location", "INFO")
        except Exception as e:
            log_message(f"Error writing logs: {str(e)}", "WARNING")

    # Final comprehensive summary
    log_message("", "INFO")
    log_message("🎉 " + "=" * 48, "INFO")
    log_message(f"🎉 COMPLETE LAKEHOUSE BACKUP FINISHED ({status.upper()})", "INFO")
    log_message("🎉 " + "=" * 48, "INFO")
    log_message(f"✅ Backup completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC", "INFO")
    log_message(f"📂 Source: {source_lakehouse_name} ({source_workspace_name})", "INFO")
    log_message(f"🗂️  Schema-enabled source: {schema_enabled}", "INFO")
    log_message(f"💾 Backup: {backup_lakehouse_name} ({backup_workspace_name})", "INFO")
    log_message(f"📦 Backup folder: {backup_folder}", "INFO")
    log_message(f"🎯 Backup method: {backup_result.get('method', 'direct_copy')}", "INFO")
    log_message(f"📊 Tables backed up: {backup_result.get('tables_backed_up', 0)}", "INFO")
    log_message(f"📊 Tables failed: {tables_failed}", "INFO")
    log_message(f"📁 Files backed up: {backup_result.get('files_backed_up', 0)}", "INFO")
    log_message(f"📁 Files failed: {files_failed}", "INFO")
    log_message(f"🎯 Total items backed up: {backup_result.get('total_items', 0)}", "INFO")

    if 'duration_seconds' in backup_result:
        log_message(f"⏱️  Backup operation: {backup_result['duration_seconds']:.2f} seconds", "INFO")

    log_message(f"⏱️  Total duration: {duration_seconds:.2f} seconds", "INFO")
    if verification_result is not None:
        log_message(f"✅ Verification: {'PASSED' if verification_result else 'FAILED'}", "INFO")
    log_message("=" * 50, "INFO")

    # Final result
    final_result = {
        "status": status,
        "backup_type": "complete_lakehouse_direct_copy",
        "layout_version": LAYOUT_VERSION,
        "schema_enabled": schema_enabled,
        "source_lakehouse_name": source_lakehouse_name,
        "source_workspace_name": source_workspace_name,
        "backup_lakehouse_name": backup_lakehouse_name,
        "backup_workspace_name": backup_workspace_name,
        "backup_folder_name": backup_folder,
        "backup_path": backup_path,
        "duration_seconds": duration_seconds,
        "verification_passed": verification_result,
        **backup_result
    }

    print("\n🎉 COMPLETE LAKEHOUSE BACKUP RESULT:")
    print(json.dumps(final_result, indent=2, default=str))

    if status != "success" and fail_on_partial_backup:
        raise RuntimeError(
            f"Backup finished with problems: {tables_failed} tables failed, {files_failed} files failed, "
            f"verification passed: {verification_result}. The backup folder and manifest were written; "
            "see the log above. Set fail_on_partial_backup = False to accept partial backups."
        )

except Exception as e:
    log_message(f"💥 Complete lakehouse backup FAILED: {str(e)}", "ERROR")
    log_message(f"Full error trace: {traceback.format_exc()}", "ERROR")

    # Leave a manifest behind so the cleanup notebook can age out the incomplete folder
    if backup_path and manifest is None:
        try:
            write_manifest(backup_path, {
                "layout_version": LAYOUT_VERSION,
                "backup_id": str(uuid.uuid4()),
                "backup_folder_name": backup_path.rstrip("/").split("/")[-1],
                "status": "failed",
                "source_lakehouse_name": source_lakehouse_name,
                "source_workspace_name": source_workspace_name,
                "backup_lakehouse_name": backup_lakehouse_name,
                "backup_workspace_name": backup_workspace_name,
                "error": str(e),
                "end_time": utc_now().strftime("%Y-%m-%d %H:%M:%S"),
                "spark_version": spark.version,
            })
            write_logs(backup_path)
        except Exception as manifest_error:
            log_message(f"Could not write failure manifest: {str(manifest_error)}", "WARNING")

    failure_result = {
        "status": "failed",
        "error": str(e),
        "timestamp": utc_now().strftime("%Y-%m-%d %H:%M:%S")
    }

    print("\n💥 COMPLETE LAKEHOUSE BACKUP RESULT:")
    print(json.dumps(failure_result, indent=2))

    # Re-raise so the Fabric job (and the Azure DevOps pipeline) reports the failure
    raise
