# Restore Guide

This guide provides detailed instructions for restoring Microsoft Fabric Lakehouse data from backup archives.

## 📋 Table of Contents
- [Overview](#overview)
- [Configuration](#configuration)
- [Restore Methods](#restore-methods)
- [Selective Restore](#selective-restore)
- [Verification](#verification)
- [Troubleshooting](#troubleshooting)

## 🎯 Overview

The Fabric Lakehouse Restore process can:
- **Auto-detect** backup format and choose optimal restore method
- **Restore tables** from Parquet (optimal) or CSV (human-readable) formats
- **Restore files** with perfect format preservation
- **Selective restore** specific tables or files
- **Dry run** to preview changes before execution

## ⚙️ Configuration

### Basic Parameters

```python
# Backup Source (Required)
backup_source_type = "lakehouse"  # Options: "lakehouse", "storage_account", "adls"
backup_lakehouse_name = "backup-lakehouse"
backup_workspace_id = "backup-workspace-id"
backup_path = "complete_backup_2024-01-01_12-00-00_abcd1234"

# Restore Target (Required)
target_lakehouse_name = "restored-lakehouse"
target_workspace_id = "target-workspace-id"
```

### Restore Options

```python
# What to restore
restore_tables = True           # Restore tables
restore_files = True            # Restore files

# Table format preference
table_format_preference = "parquet"  # Options: "parquet", "csv", "auto"

# Safety options
overwrite_existing = False      # Overwrite existing data
dry_run = False                 # Preview without changes
verify_restore = True           # Verify after restore
```

## 🔄 Restore Methods

The restore process automatically detects the backup format and uses the appropriate method:

### 1. Direct ZIP File Restore
When backup is stored as a ZIP file:
```
Backup Location/
└── lakehouse_complete_backup.zip
```

**Process**:
1. Downloads ZIP file
2. Extracts contents in memory
3. Restores tables and files directly

**Pros**: Fastest method, direct file access
**Cons**: Requires ZIP file storage

### 2. ZIP from Delta Table
When backup is stored in Delta table format:
```
Backup Location/
└── complete_backup_zip_data/
    └── (Delta table containing ZIP binary)
```

**Process**:
1. Reads ZIP binary from Delta table
2. Extracts contents in memory
3. Restores tables and files

**Pros**: Works with any storage backend
**Cons**: Slightly slower due to Delta read

### 3. Auto-Detection
```python
restore_method = "auto"  # Automatically chooses best method
```

## 📊 Table Restore Details

### Format Selection

#### Parquet Format (Recommended)
```python
table_format_preference = "parquet"
```
**Benefits**:
- Preserves exact data types
- Optimal performance
- Smaller file sizes
- Native Spark compatibility

#### CSV Format
```python
table_format_preference = "csv"
```
**Benefits**:
- Human-readable
- Excel compatible
- Universal format support
- Easy debugging

#### Auto Format
```python
table_format_preference = "auto"
```
**Logic**: Prefers Parquet if available, falls back to CSV

### Table Restore Process

1. **Detection**: Identifies available formats in backup
2. **Selection**: Chooses format based on preference
3. **Reading**: Reads data using optimal method
4. **Conversion**: Converts to Spark DataFrame
5. **Writing**: Saves as Delta table in target lakehouse

### Schema Preservation

Each table includes schema information:
```json
{
  "columns": ["id", "name", "email", "created_date"],
  "dtypes": ["int64", "string", "string", "timestamp"],
  "schema": "StructType(...)"
}
```

## 📁 File Restore Details

### Perfect Format Preservation
Files are restored exactly as they were backed up:
- **Images**: JPG, PNG, GIF, SVG (unchanged)
- **Documents**: PDF, Word, Excel (binary perfect)
- **Data**: CSV, JSON, XML (text preserved)
- **Archives**: ZIP, TAR (nested archives supported)

### File Structure Preservation
Original directory structure is maintained:
```
Source Files/
├── images/
│   ├── logo.png
│   └── photo.jpg
├── documents/
│   └── report.pdf
└── data/
    └── export.csv

        ↓ Restore ↓

Target Files/
├── images/
│   ├── logo.png      # Identical to original
│   └── photo.jpg     # Identical to original
├── documents/
│   └── report.pdf    # Identical to original
└── data/
    └── export.csv     # Identical to original
```

## 🎯 Selective Restore

### Restore Specific Tables
```python
# Restore only specific tables
restore_specific_tables = ["customers", "orders", "products"]
```

### Restore Specific Files
```python
# Use glob patterns for files
restore_specific_files = [
    "*.pdf",              # All PDF files
    "images/*.jpg",       # All JPG images
    "data/important.csv"  # Specific file
]
```

### Combined Selective Restore
```python
# Restore specific tables and files
restore_tables = True
restore_files = True
restore_specific_tables = ["customers"]
restore_specific_files = ["images/*.png"]
```

## 🔍 Dry Run Mode

Preview restore without making changes:

```python
dry_run = True
```

**Output Example**:
```
[INFO] DRY RUN: Would restore table customers (10,000 rows)
[INFO] DRY RUN: Would restore table orders (50,000 rows)
[INFO] DRY RUN: Would restore file images/logo.png (45 KB)
[INFO] DRY RUN: Would restore file documents/report.pdf (2.1 MB)
```

**Benefits**:
- Preview restore scope
- Estimate time and space requirements
- Validate configuration
- Check for conflicts

## ✅ Verification

### Automatic Verification
```python
verify_restore = True
```

**Checks performed**:
1. **Table row counts**: Compare with backup metadata
2. **File existence**: Verify all files restored
3. **File sizes**: Validate file integrity
4. **Schema validation**: Check table structures

### Manual Verification

#### Verify Tables
```python
# Check restored table
df = spark.read.format("delta").load("target_path/customers")
print(f"Restored rows: {df.count()}")
print(f"Columns: {df.columns}")
```

#### Verify Files
```python
# Check restored files
files = fabric_utils.fs.ls("target_files_path")
for file in files:
    print(f"File: {file.name}, Size: {file.size}")
```

## 📈 Performance Optimization

### Large Table Restore
```python
# For large tables, consider:
table_format_preference = "parquet"  # Faster than CSV
```

### Memory Management
- Restore processes tables sequentially
- Files restored in batches
- Memory usage optimized automatically

### Parallel Restore
Currently, restore is sequential for data integrity. Future versions may support parallel restore for independent items.

## 🚨 Error Handling

### Common Scenarios

#### Existing Data Conflicts
```
Warning: Table 'customers' exists, skipping (overwrite_existing=False)
```
**Solutions**:
- Set `overwrite_existing = True`
- Restore to different lakehouse
- Use selective restore for new tables only

#### Format Issues
```
Error: No suitable format found for table 'products'
```
**Solutions**:
- Check backup integrity
- Try different format preference
- Examine backup contents manually

#### Permission Issues
```
Error: Access denied to target lakehouse
```
**Solutions**:
- Verify target lakehouse permissions
- Check workspace access
- Ensure managed identity has proper roles

### Recovery Strategies

1. **Partial Failures**: Continue with remaining items
2. **Retry Logic**: Failed items can be retried individually
3. **Rollback**: Failed restores don't leave partial data

## 📋 Restore Manifest

Each restore creates a detailed manifest:

```json
{
  "restore_id": "restore_87654321",
  "restore_timestamp": "2024-01-01_14-30-00",
  "backup_source": "complete_backup_2024-01-01_12-00-00_abcd1234",
  "target_lakehouse": "restored-lakehouse",
  "tables_restored": 3,
  "files_restored": 15,
  "total_items": 18,
  "duration_seconds": 127.5,
  "verification_passed": true
}
```

## 🏆 Best Practices

### 1. Planning
- Use dry run before production restores
- Verify backup integrity before restore
- Plan for sufficient time and resources

### 2. Safety
- Test restore in non-production environment first
- Keep original backups until restore is verified
- Use selective restore for targeted recovery

### 3. Performance
- Use Parquet format for large tables
- Restore during low-usage periods
- Monitor resource usage during restore

### 4. Verification
- Always enable verification
- Manually check critical data
- Document restore procedures

## 📊 Monitoring Restore Progress

### Real-time Monitoring
```
[2024-01-01 14:30:00] [INFO] Starting restore process...
[2024-01-01 14:30:01] [INFO] Analyzing backup source...
[2024-01-01 14:30:02] [INFO] Found ZIP backup with 3 tables, 15 files
[2024-01-01 14:30:03] [INFO] Starting table restore...
[2024-01-01 14:30:15] [INFO] ✅ Restored table customers (10,000 rows)
[2024-01-01 14:30:28] [INFO] ✅ Restored table orders (50,000 rows)
[2024-01-01 14:30:35] [INFO] ✅ Restored table products (5,000 rows)
[2024-01-01 14:30:36] [INFO] Starting file restore...
[2024-01-01 14:32:07] [INFO] ✅ Restored 15 files (125.6 MB)
[2024-01-01 14:32:08] [INFO] Starting verification...
[2024-01-01 14:32:15] [INFO] ✅ Verification passed
[2024-01-01 14:32:15] [INFO] 🎉 Restore completed successfully!
```

### Progress Metrics
- Items processed vs. total
- Data volume restored
- Time elapsed and estimated completion
- Success/failure counts

## 📞 Support

For restore-related issues:
- Check backup integrity first
- Review restore logs for errors
- Try dry run to identify issues
- Use selective restore to isolate problems

---

**Next**: [Troubleshooting Guide](troubleshooting.md) | [Advanced Usage](advanced-usage.md)
