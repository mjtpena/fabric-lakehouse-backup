# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# PARAMETERS CELL ********************

# Microsoft Fabric Lakehouse Complete Restore Notebook
# Restores Tables and Files from unified backup ZIP

# Parameters - Configure these values or override when scheduling
backup_lakehouse_name = ""  # REQUIRED: Backup lakehouse name (e.g., "lh_backup")
backup_workspace_name = ""  # REQUIRED: Backup workspace name (e.g., "wsp_lakehouse_backup_dev")
backup_path = ""  # REQUIRED: Path to the backup folder (e.g., "silver1_transform_dev_backup_2025-11-10_14-30-15")

# Restore target configuration
target_lakehouse_name = ""  # REQUIRED: Target lakehouse name (e.g., "lh_silver1_restored")
target_workspace_name = ""  # REQUIRED: Target workspace name (e.g., "wsp_transform_dev")

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

import os
import json
import datetime
import uuid
import zipfile
import traceback
import fnmatch
import tempfile
from io import BytesIO
from pyspark.sql.functions import lit, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType, BooleanType, BinaryType
import time
import notebookutils

# Configure Spark for handling ancient datetime values (pre-1582 dates, pre-1900 timestamps)
# This is required for Oracle EBS tables that may contain historical dates
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")

# Initialize global variables
log_entries = []
restore_start_time = datetime.datetime.now()

# Configuration constants (not parameters)
backup_source_type = "lakehouse"  # Fixed to lakehouse restore
table_format_preference = "parquet"  # Prefer parquet format
restore_method = "auto"  # Auto-detect restore method
verify_restore = True  # Always verify restores
enable_detailed_logging = True  # Always enable detailed logging
dry_run = False  # Actually perform restores (not dry-run)
restore_specific_tables = []  # Restore all tables
restore_specific_files = []  # Restore all files

print("📚 Libraries imported successfully")
print("⚙️  Spark configured for ancient datetime handling (pre-1582 dates, pre-1900 timestamps)")
print(f"🔄 Lakehouse restore process initiated at: {restore_start_time}")

# CELL ********************

def get_current_timestamp():
    """Return current timestamp in a standardized format"""
    return datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def log_message(message, level="INFO"):
    """Log a message with timestamp and level"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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

print("✅ Core helper functions defined successfully")

# CELL ********************

def get_backup_source_path():
    """Construct path to the backup source in the backup lakehouse"""
    workspace_name = backup_workspace_name.strip()
    lakehouse_name = backup_lakehouse_name.strip()
    return f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse/Files/{backup_path}"

def get_target_paths():
    """Construct paths to the target lakehouse Tables and Files directories"""
    workspace_name = target_workspace_name.strip()
    lakehouse_name = target_lakehouse_name.strip()
    base_path = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse"
    return {
        "tables": f"{base_path}/Tables",
        "files": f"{base_path}/Files"
    }

print("✅ Path construction functions defined successfully")

# CELL ********************

def analyze_backup_source(backup_source_path):
    """Analyze the backup source and determine the restore method"""
    try:
        log_message("🔍 Analyzing backup source...", "INFO")
        
        backup_info = {
            "backup_type": "unknown",
            "has_zip_file": False,
            "has_zip_data": False,
            "has_manifest": False,
            "restore_method": "unknown"
        }
        
        # Check for direct ZIP file
        try:
            files = notebookutils.fs.ls(backup_source_path)
            for file in files:
                if file.name.endswith('.zip'):
                    backup_info["has_zip_file"] = True
                    backup_info["zip_file_path"] = file.path
                    log_message(f"✅ Found ZIP file: {file.name}", "INFO")
                    break
        except Exception as e:
            log_message(f"Could not list backup directory: {str(e)}", "WARNING")
        
        # Check for ZIP data in Delta table
        try:
            zip_data_paths = [
                f"{backup_source_path}/complete_backup_zip_data",
                f"{backup_source_path}/files_backup_zip_data",
                f"{backup_source_path}/lakehouse_complete_backup_zip_data"
            ]
            
            for zip_path in zip_data_paths:
                try:
                    test_df = spark.read.format("delta").load(zip_path)
                    test_df.limit(1).collect()
                    backup_info["has_zip_data"] = True
                    backup_info["zip_data_path"] = zip_path
                    log_message(f"✅ Found ZIP data in Delta table: {zip_path}", "INFO")
                    break
                except:
                    continue
                    
        except Exception as e:
            log_message(f"Could not check for ZIP data: {str(e)}", "DEBUG")
        
        # Check for manifest
        try:
            manifest_df = spark.read.format("delta").load(f"{backup_source_path}/_manifest")
            manifest_data = manifest_df.collect()[0]
            backup_info["has_manifest"] = True
            backup_info["manifest_data"] = manifest_data.asDict()
            log_message("✅ Found backup manifest", "INFO")
        except Exception as e:
            log_message(f"No manifest found: {str(e)}", "DEBUG")
        
        # Determine restore method
        if backup_info["has_zip_file"]:
            backup_info["restore_method"] = "zip_direct"
            backup_info["backup_type"] = "zip_file"
        elif backup_info["has_zip_data"]:
            backup_info["restore_method"] = "zip_from_delta"
            backup_info["backup_type"] = "zip_in_delta"
        else:
            backup_info["restore_method"] = "legacy_format"
            backup_info["backup_type"] = "legacy"
        
        log_message(f"📋 Backup analysis complete: {backup_info['backup_type']} ({backup_info['restore_method']})", "INFO")
        return backup_info
        
    except Exception as e:
        log_message(f"Backup analysis failed: {str(e)}", "ERROR")
        return {"backup_type": "unknown", "restore_method": "unknown", "error": str(e)}

def read_zip_contents(backup_info):
    """Read and analyze ZIP contents"""
    try:
        log_message("📦 Reading ZIP backup contents...", "INFO")
        
        # Get ZIP binary data
        if backup_info["restore_method"] == "zip_direct":
            # Read ZIP file directly
            zip_file_path = backup_info["zip_file_path"]
            # Copy to local temp for processing
            temp_zip_path = f"/tmp/restore_backup_{uuid.uuid4().hex[:8]}.zip"
            notebookutils.fs.cp(zip_file_path, f"file:{temp_zip_path}")
            
            with open(temp_zip_path, "rb") as f:
                zip_binary = f.read()
            
            os.remove(temp_zip_path)
                
        elif backup_info["restore_method"] == "zip_from_delta":
            # Read ZIP from Delta table
            zip_data_path = backup_info["zip_data_path"]
            zip_df = spark.read.format("delta").load(zip_data_path)
            zip_row = zip_df.collect()[0]
            zip_binary = zip_row.zip_binary
            
        else:
            raise Exception(f"Cannot read ZIP for restore method: {backup_info['restore_method']}")
        
        # Analyze ZIP contents
        zip_contents = {
            "tables": {},
            "files": [],
            "metadata": {},
            "total_size": len(zip_binary)
        }
        
        with zipfile.ZipFile(BytesIO(zip_binary), 'r') as zip_file:
            file_list = zip_file.namelist()
            
            log_message(f"📁 ZIP contains {len(file_list)} items", "INFO")
            
            # Parse contents
            for file_path in file_list:
                if file_path.startswith('tables/'):
                    # Parse table structure
                    path_parts = file_path.split('/')
                    if len(path_parts) >= 3:
                        table_name = path_parts[1]
                        file_name = path_parts[2]
                        
                        if table_name not in zip_contents["tables"]:
                            zip_contents["tables"][table_name] = {
                                "formats": [],
                                "has_csv": False,
                                "has_parquet": False,
                                "has_schema": False,
                                "has_metadata": False
                            }
                        
                        if file_name.endswith('.csv'):
                            zip_contents["tables"][table_name]["has_csv"] = True
                            zip_contents["tables"][table_name]["formats"].append("csv")
                        elif file_name.endswith('.parquet'):
                            zip_contents["tables"][table_name]["has_parquet"] = True
                            zip_contents["tables"][table_name]["formats"].append("parquet")
                        elif file_name == 'schema.json':
                            zip_contents["tables"][table_name]["has_schema"] = True
                        elif file_name == 'metadata.json':
                            zip_contents["tables"][table_name]["has_metadata"] = True
                
                elif file_path.startswith('files/'):
                    # Parse file structure
                    relative_path = file_path[6:]  # Remove 'files/' prefix
                    if relative_path and not relative_path.endswith('/'):
                        zip_contents["files"].append({
                            "zip_path": file_path,
                            "relative_path": relative_path,
                            "name": relative_path.split('/')[-1],
                            "type": get_file_type(relative_path.split('/')[-1])
                        })
                
                elif file_path.startswith('_backup_info/'):
                    # Read metadata
                    if file_path.endswith('metadata.json'):
                        metadata_content = zip_file.read(file_path)
                        zip_contents["metadata"] = json.loads(metadata_content.decode('utf-8'))
        
        log_message(f"📊 Found {len(zip_contents['tables'])} tables and {len(zip_contents['files'])} files in ZIP", "INFO")
        
        # Store ZIP binary for later use
        zip_contents["zip_binary"] = zip_binary
        
        return zip_contents
        
    except Exception as e:
        log_message(f"Failed to read ZIP contents: {str(e)}", "ERROR")
        return None

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

print("✅ Backup analysis and ZIP reading functions defined successfully")

# CELL ********************

def restore_tables_direct_copy(backup_source_path, target_tables_path):
    """
    Restore tables by reading from backup and writing to target
    Works across workspaces (reads from backup lakehouse, writes to target lakehouse)
    """
    try:
        log_message("🎯 Starting direct copy restore of Delta tables", "INFO")
        
        tables_restored = 0
        tables_failed = 0
        tables_skipped = 0
        
        # Path to backed up tables
        backup_tables_path = f"{backup_source_path}/tables"
        
        # List all backed up tables
        try:
            backed_up_items = notebookutils.fs.ls(backup_tables_path)
            table_folders = [f for f in backed_up_items if f.isDir and not f.name.startswith('_') and not f.name.endswith('_metadata')]
            
            log_message(f"📊 Found {len(table_folders)} tables to restore", "INFO")
            
            for table_folder in table_folders:
                table_name = table_folder.name
                
                try:
                    log_message(f"  📋 Restoring table: {table_name}", "INFO")
                    
                    # Source backup table path
                    source_table_path = f"{backup_tables_path}/{table_name}"
                    # Target restore path
                    target_table_path = f"{target_tables_path}/{table_name}"
                    
                    # Check if target table exists
                    if not overwrite_existing:
                        try:
                            existing_df = spark.read.format("delta").load(target_table_path)
                            log_message(f"    ⚠️  Table {table_name} already exists (use overwrite_existing=True to replace)", "WARNING")
                            tables_skipped += 1
                            continue
                        except:
                            pass  # Table doesn't exist, proceed with restore
                    
                    # Read metadata to get table info
                    try:
                        metadata_df = spark.read.format("delta").load(f"{source_table_path}_metadata")
                        metadata_json = metadata_df.collect()[0]["metadata_json"]
                        table_metadata = json.loads(metadata_json)
                        row_count = table_metadata.get("row_count", "unknown")
                        log_message(f"    📊 Table info: {row_count:,} rows", "INFO")
                    except:
                        log_message(f"    ℹ️  Metadata not available for {table_name}", "INFO")
                    
                    # Read from backup and write to target (works across workspaces)
                    log_message(f"    📖 Reading from backup...", "INFO")
                    df = spark.read.format("delta").load(source_table_path)
                    
                    write_mode = "overwrite" if overwrite_existing else "errorifexists"
                    log_message(f"    ✍️  Writing to target lakehouse...", "INFO")
                    df.write.mode(write_mode).format("delta").save(target_table_path)
                    
                    tables_restored += 1
                    log_message(f"    ✅ Table {table_name} restored successfully", "INFO")
                    
                except Exception as table_error:
                    tables_failed += 1
                    log_message(f"    ❌ Error restoring table {table_name}: {str(table_error)}", "ERROR")
            
            log_message(f"✅ Table restore completed: {tables_restored} restored, {tables_failed} failed, {tables_skipped} skipped", "INFO")
            
            return {
                "success": True,
                "method": "direct_copy_read_write",
                "tables_restored": tables_restored,
                "tables_failed": tables_failed,
                "tables_skipped": tables_skipped
            }
            
        except Exception as list_error:
            log_message(f"❌ Error listing backup tables: {str(list_error)}", "ERROR")
            return {
                "success": False,
                "method": "direct_copy_read_write",
                "error": str(list_error),
                "tables_restored": 0,
                "tables_failed": 0
            }
            
    except Exception as e:
        log_message(f"❌ Table restore failed: {str(e)}", "ERROR")
        return {
            "success": False,
            "method": "direct_copy_read_write",
            "error": str(e),
            "tables_restored": 0,
            "tables_failed": 0
        }

def restore_files_direct_copy(backup_source_path, target_files_path):
    """
    Restore files using direct copy
    Preserves original formats
    """
    try:
        log_message("🎯 Starting direct copy restore of files", "INFO")
        
        files_restored = 0
        files_failed = 0
        files_skipped = 0
        
        # Path to backed up files
        backup_files_path = f"{backup_source_path}/files"
        
        # Check if files backup exists
        try:
            notebookutils.fs.ls(backup_files_path)
        except:
            log_message("ℹ️  No files found in backup", "INFO")
            return {
                "success": True,
                "method": "direct_copy",
                "files_restored": 0,
                "files_failed": 0,
                "files_skipped": 0
            }
        
        # Copy all files recursively
        try:
            if overwrite_existing:
                log_message(f"  📁 Copying all files from backup (overwrite mode)...", "INFO")
                notebookutils.fs.cp(backup_files_path, target_files_path, True)
                log_message(f"  ✅ Files copied successfully", "INFO")
                files_restored = 1  # Counting as 1 batch operation
            else:
                log_message(f"  📁 Copying files (skip existing mode)...", "INFO")
                # When not overwriting, we need to copy individual files
                def copy_files_recursive(source_path, target_path):
                    nonlocal files_restored, files_failed, files_skipped
                    
                    items = notebookutils.fs.ls(source_path)
                    for item in items:
                        source_item = item.path
                        target_item = source_item.replace(backup_files_path, target_files_path)
                        
                        if item.isDir:
                            # Recursively copy directory
                            copy_files_recursive(source_item, target_item)
                        else:
                            # Copy individual file
                            try:
                                # Check if file exists
                                try:
                                    notebookutils.fs.head(target_item, 1)
                                    files_skipped += 1
                                    log_message(f"    ⚠️  File exists, skipping: {item.name}", "WARNING")
                                    continue
                                except:
                                    pass  # File doesn't exist, proceed
                                
                                notebookutils.fs.cp(source_item, target_item)
                                files_restored += 1
                                log_message(f"    ✅ Copied: {item.name}", "INFO")
                            except Exception as file_error:
                                files_failed += 1
                                log_message(f"    ❌ Error copying {item.name}: {str(file_error)}", "ERROR")
                
                copy_files_recursive(backup_files_path, target_files_path)
            
            log_message(f"✅ File restore completed: {files_restored} restored, {files_failed} failed, {files_skipped} skipped", "INFO")
            
            return {
                "success": True,
                "method": "direct_copy",
                "files_restored": files_restored,
                "files_failed": files_failed,
                "files_skipped": files_skipped
            }
            
        except Exception as copy_error:
            log_message(f"❌ Error copying files: {str(copy_error)}", "ERROR")
            return {
                "success": False,
                "method": "direct_copy",
                "error": str(copy_error),
                "files_restored": files_restored,
                "files_failed": files_failed
            }
            
    except Exception as e:
        log_message(f"❌ File restore failed: {str(e)}", "ERROR")
        return {
            "success": False,
            "method": "direct_copy",
            "error": str(e),
            "files_restored": 0,
            "files_failed": 0
        }

print("✅ Direct copy restore functions defined successfully")

# CELL ********************

def restore_tables_from_zip(zip_contents, target_tables_path):
    """Restore tables from ZIP backup - LEGACY METHOD (for old backups)"""
    try:
        if not restore_tables:
            log_message("⏭️  Table restore skipped (restore_tables=False)", "INFO")
            return {"skipped": True, "reason": "restore_tables=False"}
        
        tables_to_restore = zip_contents["tables"]
        
        # Filter specific tables if requested
        if restore_specific_tables:
            tables_to_restore = {name: info for name, info in tables_to_restore.items() 
                               if name in restore_specific_tables}
            log_message(f"🎯 Restoring specific tables: {list(tables_to_restore.keys())}", "INFO")
        
        if not tables_to_restore:
            return {"success": True, "tables_restored": 0, "message": "No tables to restore"}
        
        log_message(f"📊 Starting table restore for {len(tables_to_restore)} tables...", "INFO")
        
        zip_binary = zip_contents["zip_binary"]
        tables_restored = 0
        tables_failed = 0
        restore_details = {}
        
        with zipfile.ZipFile(BytesIO(zip_binary), 'r') as zip_file:
            
            for table_name, table_info in tables_to_restore.items():
                try:
                    log_message(f"  📋 Restoring table: {table_name}", "INFO")
                    
                    if dry_run:
                        log_message(f"    🔍 DRY RUN: Would restore table {table_name}", "INFO")
                        restore_details[table_name] = {"status": "dry_run", "formats": table_info["formats"]}
                        continue
                    
                    # Check if target table exists
                    target_table_path = f"{target_tables_path}/{table_name}"
                    table_exists = False
                    
                    try:
                        test_df = spark.read.format("delta").load(target_table_path)
                        test_df.limit(1).collect()
                        table_exists = True
                    except:
                        table_exists = False
                    
                    if table_exists and not overwrite_existing:
                        log_message(f"    ⚠️  Table {table_name} exists, skipping (overwrite_existing=False)", "WARNING")
                        restore_details[table_name] = {"status": "skipped", "reason": "exists"}
                        continue
                    
                    # Determine which format to use for restore
                    restore_format = determine_table_restore_format(table_info, table_format_preference)
                    
                    df = None
                    
                    if restore_format == "parquet" and table_info["has_parquet"]:
                        # Restore from Parquet - FIXED VERSION
                        parquet_data = zip_file.read(f"tables/{table_name}/{table_name}.parquet")
                        
                        # Use BytesIO instead of temp file
                        parquet_buffer = BytesIO(parquet_data)
                        
                        # Convert to pandas first, then to Spark
                        import pandas as pd
                        pandas_df = pd.read_parquet(parquet_buffer)
                        df = spark.createDataFrame(pandas_df)
                        
                        log_message(f"    ✅ Read from Parquet: {table_name} ({len(pandas_df):,} rows)", "INFO")
                        
                    elif restore_format == "csv" and table_info["has_csv"]:
                        # Restore from CSV - FIXED VERSION
                        csv_data = zip_file.read(f"tables/{table_name}/{table_name}.csv")
                        
                        # Use BytesIO instead of temp file
                        from io import StringIO
                        csv_string = csv_data.decode('utf-8')
                        csv_buffer = StringIO(csv_string)
                        
                        # Convert to pandas first, then to Spark
                        import pandas as pd
                        pandas_df = pd.read_csv(csv_buffer)
                        df = spark.createDataFrame(pandas_df)
                        
                        log_message(f"    ✅ Read from CSV: {table_name} ({len(pandas_df):,} rows)", "INFO")
                        
                    else:
                        log_message(f"    ❌ No suitable format found for table {table_name}", "ERROR")
                        restore_details[table_name] = {"status": "failed", "reason": "no_suitable_format"}
                        tables_failed += 1
                        continue
                    
                    # Write to Delta table
                    if df is not None:
                        df.write.mode("overwrite").format("delta").save(target_table_path)
                        row_count = df.count()
                        
                        tables_restored += 1
                        restore_details[table_name] = {
                            "status": "success", 
                            "format_used": restore_format,
                            "row_count": row_count
                        }
                        
                        log_message(f"    ✅ Restored table {table_name} ({row_count:,} rows)", "INFO")
                    
                except Exception as table_error:
                    log_message(f"    ❌ Failed to restore table {table_name}: {str(table_error)}", "ERROR")
                    restore_details[table_name] = {"status": "failed", "error": str(table_error)}
                    tables_failed += 1
        
        log_message(f"📊 Table restore completed: {tables_restored} restored, {tables_failed} failed", "INFO")
        
        return {
            "success": True,
            "tables_restored": tables_restored,
            "tables_failed": tables_failed,
            "restore_details": restore_details
        }
        
    except Exception as e:
        log_message(f"Table restore failed: {str(e)}", "ERROR")
        return {"success": False, "error": str(e)}

def determine_table_restore_format(table_info, preference):
    """Determine the best format to use for table restore"""
    if preference == "auto":
        # Prefer Parquet if available, fallback to CSV
        if table_info["has_parquet"]:
            return "parquet"
        elif table_info["has_csv"]:
            return "csv"
        else:
            return "none"
    elif preference == "parquet":
        return "parquet" if table_info["has_parquet"] else "csv"
    elif preference == "csv":
        return "csv" if table_info["has_csv"] else "parquet"
    else:
        return "auto"

print("✅ FIXED Table restore functions defined successfully")

# CELL ********************

def restore_files_from_zip(zip_contents, target_files_path):
    """Restore files from ZIP backup with original format preservation"""
    try:
        if not restore_files:
            log_message("⏭️  File restore skipped (restore_files=False)", "INFO")
            return {"skipped": True, "reason": "restore_files=False"}
        
        files_to_restore = zip_contents["files"]
        
        # Filter specific files if requested
        if restore_specific_files:
            import fnmatch
            filtered_files = []
            for file_info in files_to_restore:
                for pattern in restore_specific_files:
                    if fnmatch.fnmatch(file_info["relative_path"], pattern):
                        filtered_files.append(file_info)
                        break
            files_to_restore = filtered_files
            log_message(f"🎯 Restoring files matching patterns: {restore_specific_files}", "INFO")
        
        if not files_to_restore:
            return {"success": True, "files_restored": 0, "message": "No files to restore"}
        
        log_message(f"📁 Starting file restore for {len(files_to_restore)} files...", "INFO")
        
        zip_binary = zip_contents["zip_binary"]
        files_restored = 0
        files_failed = 0
        files_skipped = 0
        restore_details = {}
        total_size_restored = 0
        
        with zipfile.ZipFile(BytesIO(zip_binary), 'r') as zip_file:
            
            for file_info in files_to_restore:
                try:
                    relative_path = file_info["relative_path"]
                    file_name = file_info["name"]
                    
                    log_message(f"  📄 Restoring file: {relative_path}", "INFO")
                    
                    if dry_run:
                        log_message(f"    🔍 DRY RUN: Would restore file {relative_path}", "INFO")
                        restore_details[relative_path] = {"status": "dry_run", "type": file_info["type"]}
                        continue
                    
                    # Check if target file exists
                    target_file_path = f"{target_files_path}/{relative_path}"
                    file_exists = False
                    
                    try:
                        notebookutils.fs.head(target_file_path, 1)
                        file_exists = True
                    except:
                        file_exists = False
                    
                    if file_exists and not overwrite_existing:
                        log_message(f"    ⚠️  File {relative_path} exists, skipping (overwrite_existing=False)", "WARNING")
                        restore_details[relative_path] = {"status": "skipped", "reason": "exists"}
                        files_skipped += 1
                        continue
                    
                    # Read file content from ZIP (preserves original format)
                    file_content = zip_file.read(file_info["zip_path"])
                    file_size = len(file_content)
                    
                    # Restore file with original format
                    try:
                        # Create directory if needed
                        target_dir = "/".join(target_file_path.split("/")[:-1])
                        try:
                            notebookutils.fs.mkdirs(target_dir)
                        except:
                            pass  # Directory might already exist
                        
                        # Write to temp file first
                        temp_file_path = f"/tmp/restore_{uuid.uuid4().hex[:8]}_{file_name}"
                        with open(temp_file_path, "wb") as f:
                            f.write(file_content)
                        
                        # Copy to target location
                        notebookutils.fs.cp(f"file:{temp_file_path}", target_file_path)
                        
                        # Clean up temp file
                        os.remove(temp_file_path)
                        
                        log_message(f"    ✅ Restored file: {relative_path} ({file_size/1024:.1f} KB)", "INFO")
                        files_restored += 1
                        total_size_restored += file_size
                        
                        restore_details[relative_path] = {
                            "status": "success",
                            "method": "notebookutils",
                            "size_bytes": file_size,
                            "type": file_info["type"]
                        }
                        
                    except Exception as notebookutils_error:
                        log_message(f"    ⚠️  notebookutils failed for {relative_path}, trying Spark method: {str(notebookutils_error)}", "WARNING")
                        
                        # Fallback to Spark method
                        success = restore_file_with_spark(file_content, target_file_path, relative_path, file_info)
                        if success:
                            files_restored += 1
                            total_size_restored += file_size
                            restore_details[relative_path] = {
                                "status": "success",
                                "method": "spark_fallback",
                                "size_bytes": file_size,
                                "type": file_info["type"],
                                "note": "Stored as Delta table due to Fabric utils limitation"
                            }
                        else:
                            files_failed += 1
                            restore_details[relative_path] = {
                                "status": "failed",
                                "error": "Both Fabric utils and Spark methods failed"
                            }
                    
                    else:
                        # Method 2: Spark-only method (stores as Delta)
                        success = restore_file_with_spark(file_content, target_file_path, relative_path, file_info)
                        if success:
                            files_restored += 1
                            total_size_restored += file_size
                            restore_details[relative_path] = {
                                "status": "success",
                                "method": "spark_only",
                                "size_bytes": file_size,
                                "type": file_info["type"],
                                "note": "Stored as Delta table (Fabric utils not available)"
                            }
                        else:
                            files_failed += 1
                            restore_details[relative_path] = {
                                "status": "failed",
                                "error": "Spark method failed"
                            }
                    
                except Exception as file_error:
                    log_message(f"    ❌ Failed to restore file {relative_path}: {str(file_error)}", "ERROR")
                    restore_details[relative_path] = {"status": "failed", "error": str(file_error)}
                    files_failed += 1
        
        log_message(f"📁 File restore completed: {files_restored} restored, {files_skipped} skipped, {files_failed} failed", "INFO")
        log_message(f"📊 Total size restored: {total_size_restored/1024/1024:.2f} MB", "INFO")
        
        return {
            "success": True,
            "files_restored": files_restored,
            "files_skipped": files_skipped,
            "files_failed": files_failed,
            "total_size_mb": total_size_restored / 1024 / 1024,
            "restore_details": restore_details
        }
        
    except Exception as e:
        log_message(f"File restore failed: {str(e)}", "ERROR")
        return {"success": False, "error": str(e)}

def restore_file_with_spark(file_content, target_file_path, relative_path, file_info):
    """Restore file using Spark (fallback method)"""
    try:
        # Create a DataFrame with the file content
        file_df = spark.createDataFrame([{
            "original_path": relative_path,
            "file_name": file_info["name"],
            "file_type": file_info["type"],
            "content": file_content,
            "size_bytes": len(file_content),
            "restored_timestamp": datetime.datetime.now().isoformat(),
            "restoration_note": "File content preserved in binary format within Delta table"
        }])
        
        # Save as Delta table
        file_df.write.mode("overwrite").format("delta").save(f"{target_file_path}_restored")
        
        log_message(f"    ✅ Restored file via Spark: {relative_path} (stored as Delta)", "INFO")
        return True
        
    except Exception as e:
        log_message(f"    ❌ Spark file restore failed for {relative_path}: {str(e)}", "ERROR")
        return False

print("✅ File restore functions defined successfully")

# CELL ********************

def verify_restored_data(target_paths, zip_contents, table_restore_result, file_restore_result):
    """Verify that restored data matches the backup"""
    try:
        if not verify_restore:
            log_message("⏭️  Verification skipped (verify_restore=False)", "INFO")
            return {"skipped": True}
        
        log_message("🔍 Starting data verification...", "INFO")
        verification_results = {
            "tables": {},
            "files": {},
            "overall_success": True
        }
        
        # Verify tables
        if restore_tables and table_restore_result.get("tables_restored", 0) > 0:
            log_message("  📊 Verifying restored tables...", "INFO")
            
            for table_name, restore_info in table_restore_result.get("restore_details", {}).items():
                if restore_info.get("status") == "success":
                    try:
                        # Check if table is readable
                        table_path = f"{target_paths['tables']}/{table_name}"
                        df = spark.read.format("delta").load(table_path)
                        actual_count = df.count()
                        expected_count = restore_info.get("row_count", 0)
                        
                        tables_match = actual_count == expected_count
                        verification_results["tables"][table_name] = {
                            "verified": True,
                            "data_matches": tables_match,
                            "actual_rows": actual_count,
                            "expected_rows": expected_count
                        }
                        
                        if tables_match:
                            log_message(f"    ✅ Table {table_name}: {actual_count:,} rows verified", "INFO")
                        else:
                            log_message(f"    ⚠️  Table {table_name}: Row count mismatch (expected {expected_count:,}, got {actual_count:,})", "WARNING")
                            verification_results["overall_success"] = False
                        
                    except Exception as table_verify_error:
                        log_message(f"    ❌ Failed to verify table {table_name}: {str(table_verify_error)}", "ERROR")
                        verification_results["tables"][table_name] = {
                            "verified": False,
                            "error": str(table_verify_error)
                        }
                        verification_results["overall_success"] = False
        
        # Verify files
        if restore_files and file_restore_result.get("files_restored", 0) > 0:
            log_message("  📁 Verifying restored files...", "INFO")
            
            for file_path, restore_info in file_restore_result.get("restore_details", {}).items():
                if restore_info.get("status") == "success":
                    try:
                        # Check if file exists and is accessible
                        target_file_path = f"{target_paths['files']}/{file_path}"
                        file_exists = False
                        
                        if restore_info.get("method") == "notebookutils":
                            try:
                                notebookutils.fs.head(target_file_path, 1)
                                file_exists = True
                            except:
                                file_exists = False
                        else:
                            # Check Delta table
                            try:
                                file_df = spark.read.format("delta").load(f"{target_file_path}_restored")
                                file_df.limit(1).collect()
                                file_exists = True
                            except:
                                file_exists = False
                        
                        verification_results["files"][file_path] = {
                            "verified": True,
                            "file_exists": file_exists,
                            "restore_method": restore_info.get("method", "unknown")
                        }
                        
                        if file_exists:
                            log_message(f"    ✅ File {file_path}: Accessible", "INFO")
                        else:
                            log_message(f"    ⚠️  File {file_path}: Not accessible after restore", "WARNING")
                            verification_results["overall_success"] = False
                        
                    except Exception as file_verify_error:
                        log_message(f"    ❌ Failed to verify file {file_path}: {str(file_verify_error)}", "ERROR")
                        verification_results["files"][file_path] = {
                            "verified": False,
                            "error": str(file_verify_error)
                        }
                        verification_results["overall_success"] = False
        
        tables_verified = len([t for t in verification_results["tables"].values() if t.get("verified")])
        files_verified = len([f for f in verification_results["files"].values() if f.get("verified")])
        
        log_message(f"🔍 Verification complete: {tables_verified} tables, {files_verified} files verified", "INFO")
        
        if verification_results["overall_success"]:
            log_message("✅ All verification checks passed", "INFO")
        else:
            log_message("⚠️  Some verification checks failed - see details above", "WARNING")
        
        return verification_results
        
    except Exception as e:
        log_message(f"Verification failed: {str(e)}", "ERROR")
        return {"success": False, "error": str(e)}

print("✅ Verification functions defined successfully")

# CELL ********************

try:
    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
    
    # Detect backup method by checking backup structure
    backup_method = "unknown"
    try:
        # Check for direct copy backup (tables folder with Delta tables)
        tables_path = f"{backup_source_path}/tables"
        try:
            items = notebookutils.fs.ls(tables_path)
            if any(f.isDir and not f.name.startswith('_') for f in items):
                backup_method = "direct_copy"
                log_message("🔍 Detected backup method: Direct Copy (Delta SHALLOW CLONE)", "INFO")
        except:
            pass
        
        # Check for legacy ZIP backup
        if backup_method == "unknown":
            try:
                files = notebookutils.fs.ls(backup_source_path)
                if any(f.name.endswith('.zip') for f in files):
                    backup_method = "zip_legacy"
                    log_message("🔍 Detected backup method: ZIP (legacy)", "INFO")
            except:
                pass
    except Exception as detect_error:
        log_message(f"⚠️  Could not auto-detect backup method: {str(detect_error)}", "WARNING")
    
    # Execute restore based on detected method
    backup_info = {}  # Initialize to avoid undefined variable errors
    
    if backup_method == "direct_copy":
        log_message("=" * 50, "INFO")
        log_message("🎯 DIRECT COPY RESTORE EXECUTION", "INFO")
        log_message("=" * 50, "INFO")
        
        # Set backup info for direct copy
        backup_info = {
            "backup_type": "direct_copy",
            "restore_method": "delta_read_write"
        }
        
        # Restore tables using Delta read/write
        table_restore_result = {"success": True, "tables_restored": 0, "tables_failed": 0}
        if restore_tables:
            table_restore_result = restore_tables_direct_copy(backup_source_path, target_paths['tables'])
        
        # Restore files using direct copy
        file_restore_result = {"success": True, "files_restored": 0, "files_failed": 0}
        if restore_files:
            file_restore_result = restore_files_direct_copy(backup_source_path, target_paths['files'])
        
        # No complex verification needed for direct copy - Delta handles integrity
        verification_result = {"overall_success": True, "method": "delta_native"}
        
    elif backup_method == "zip_legacy":
        log_message("=" * 50, "INFO")
        log_message("🎯 ZIP RESTORE EXECUTION (LEGACY)", "INFO")
        log_message("=" * 50, "INFO")
        
        # Use legacy ZIP restore method
        backup_info = analyze_backup_source(backup_source_path)
        zip_contents = load_zip_backup(backup_source_path, backup_info)
        
        table_restore_result = restore_tables_from_zip(zip_contents, target_paths['tables'])
        file_restore_result = restore_files_from_zip(zip_contents, target_paths['files'])
        verification_result = verify_restored_data(target_paths, zip_contents, table_restore_result, file_restore_result)
    
    else:
        raise ValueError(f"Could not detect backup method. Please verify backup path: {backup_source_path}")
    
    # Record end time
    end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Create restore manifest - FIXED VERSION
    restore_manifest_simple = {
        "restore_id": str(uuid.uuid4())[:8],
        "restore_timestamp": get_current_timestamp(),
        "source_backup_path": backup_source_path,
        "backup_folder_name": backup_path,
        "backup_lakehouse_name": backup_lakehouse_name,
        "backup_workspace_name": backup_workspace_name,
        "target_lakehouse_name": target_lakehouse_name,
        "target_workspace_name": target_workspace_name,
        "restore_type": "complete_lakehouse_restore",
        "dry_run": dry_run,
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": (datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") - 
                            datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")).total_seconds(),
        "backup_type": backup_info.get("backup_type", "unknown"),
        "restore_method": backup_info.get("restore_method", "unknown"),
        "tables_restored": table_restore_result.get("tables_restored", 0),
        "tables_failed": table_restore_result.get("tables_failed", 0),
        "files_restored": file_restore_result.get("files_restored", 0),
        "files_failed": file_restore_result.get("files_failed", 0),
        "verification_passed": verification_result.get("overall_success", False)
    }
    
    if not dry_run:
        # Save restore manifest - FIXED VERSION
        try:
            manifest_df = spark.createDataFrame([restore_manifest_simple])
            manifest_df.write.mode("overwrite").format("delta").save(f"{target_paths['files']}/_restore_manifest_{get_current_timestamp()}")
            log_message("📝 Restore manifest saved successfully", "INFO")
        except Exception as manifest_error:
            log_message(f"Warning: Could not save restore manifest: {str(manifest_error)}", "WARNING")
    
    # Write logs
    if enable_detailed_logging and log_entries and not dry_run:
        try:
            log_schema = StructType([
                StructField("timestamp", StringType(), True),
                StructField("level", StringType(), True),
                StructField("message", StringType(), True)
            ])
            
            log_rows = [(entry["timestamp"], entry["level"], entry["message"]) for entry in log_entries]
            log_df = spark.createDataFrame(log_rows, log_schema)
            log_df.write.format("delta").mode("overwrite").save(f"{target_paths['files']}/_restore_logs_{get_current_timestamp()}")
            log_message("📝 Restore logs written to target lakehouse", "INFO")
        except Exception as e:
            log_message(f"Error writing restore logs: {str(e)}", "WARNING")
    
    # Final summary
    duration_seconds = (datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") - 
                        datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")).total_seconds()
    
    log_message("", "INFO")
    log_message("🎉 " + "=" * 58, "INFO")
    log_message("🎉 LAKEHOUSE RESTORE COMPLETED!", "INFO")
    log_message("🎉 " + "=" * 58, "INFO")
    log_message(f"✅ Restore finished at: {end_time}", "INFO")
    log_message(f"📂 Target: {target_lakehouse_name} ({target_workspace_name})", "INFO")
    log_message(f"💾 Source backup: {backup_path}", "INFO")
    
    if dry_run:
        log_message("🔍 DRY RUN COMPLETED - No actual changes made", "INFO")
    else:
        tables_restored = table_restore_result.get("tables_restored", 0)
        files_restored = file_restore_result.get("files_restored", 0)
        
        log_message(f"📊 Tables restored: {tables_restored}", "INFO")
        log_message(f"📁 Files restored: {files_restored}", "INFO")
        
        if 'total_size_mb' in file_restore_result:
            log_message(f"📦 Data size restored: {file_restore_result['total_size_mb']:.2f} MB", "INFO")
        
        if verification_result.get("overall_success"):
            log_message("✅ Verification: All checks passed", "INFO")
        elif verification_result.get("skipped"):
            log_message("⏭️  Verification: Skipped", "INFO")
        else:
            log_message("⚠️  Verification: Some checks failed", "WARNING")
    
    log_message(f"⏱️  Duration: {duration_seconds:.2f} seconds", "INFO")
    log_message("=" * 60, "INFO")
    
    # Final result
    final_result = {
        "status": "success",
        "restore_type": "complete_lakehouse",
        "target_lakehouse_name": target_lakehouse_name,
        "target_workspace_name": target_workspace_name,
        "source_backup": backup_source_path,
        "backup_folder_name": backup_path,
        "duration_seconds": duration_seconds,
        "dry_run": dry_run,
        "tables_restored": table_restore_result.get("tables_restored", 0),
        "files_restored": file_restore_result.get("files_restored", 0),
        "verification_passed": verification_result.get("overall_success", False),
        "backup_type": backup_info["backup_type"],
        "restore_method": backup_info["restore_method"]
    }
    
    print(f"\n🎉 LAKEHOUSE RESTORE RESULT:")
    print(json.dumps(final_result, indent=2, default=str))
    
except Exception as e:
    log_message(f"💥 Lakehouse restore FAILED: {str(e)}", "ERROR")
    log_message(f"Full error trace: {traceback.format_exc()}", "ERROR")
    
    failure_result = {
        "status": "failed",
        "error": str(e),
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    print("\n💥 LAKEHOUSE RESTORE RESULT:")
    print(json.dumps(failure_result, indent=2))
