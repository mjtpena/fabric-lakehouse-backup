# Backup Guide

This guide provides detailed instructions for backing up Microsoft Fabric Lakehouse data using the backup notebook.

## 📋 Table of Contents
- [Overview](#overview)
- [Configuration](#configuration)
- [Backup Types](#backup-types)
- [Storage Options](#storage-options)
- [Advanced Configuration](#advanced-configuration)
- [Monitoring](#monitoring)
- [Best Practices](#best-practices)

## 🎯 Overview

The Fabric Lakehouse Combined Backup creates a unified ZIP archive containing:
- **Tables**: Delta format data converted to Parquet + CSV + Schema
- **Files**: Original files with perfect format preservation
- **Metadata**: Complete backup information and restore instructions

## ⚙️ Configuration

### Basic Parameters

```python
# Source Configuration (Required)
source_lakehouse_name = "your-lakehouse-name"
source_workspace_id = "your-workspace-id"

# Backup Destination (Required)
backup_type = "lakehouse"  # Options: "lakehouse", "storage_account", "adls"
backup_lakehouse_name = "backup-lakehouse"
backup_workspace_id = "backup-workspace-id"
```

### Backup Options

```python
# What to backup
backup_tables = True        # Include tables
backup_files = True         # Include files
backup_method = "unified_zip"  # Method (currently only unified_zip)

# Quality and verification
verify_backup = True        # Verify after backup
enable_detailed_logging = True  # Detailed logs
```

## 🗄️ Backup Types

### 1. Lakehouse to Lakehouse
Store backup in another Fabric Lakehouse (recommended for most scenarios).

```python
backup_type = "lakehouse"
backup_lakehouse_name = "my-backup-lakehouse"
backup_workspace_id = "backup-workspace-id"
```

**Pros:**
- Native Fabric integration
- No additional storage costs
- Easy access and management

**Cons:**
- Requires another lakehouse
- Subject to Fabric quotas

### 2. Azure Storage Account
Store backup in external Azure Storage Account.

```python
backup_type = "storage_account"
backup_storage_account = "mystorageaccount"
backup_container = "lakehouse-backups"
use_managed_identity = True
```

**Pros:**
- Independent of Fabric quotas
- Long-term retention options
- Cost-effective for large backups

**Cons:**
- Requires Azure Storage setup
- Additional authentication complexity

### 3. Azure Data Lake Storage Gen2
Store backup in ADLS Gen2 for enterprise scenarios.

```python
backup_type = "adls"
backup_adls_account = "myadlsaccount"
backup_adls_container = "backups"
use_managed_identity = True
```

**Pros:**
- Enterprise-grade security
- Hierarchical namespace
- Analytics integration

**Cons:**
- More complex setup
- Higher costs

## 🔧 Advanced Configuration

### Size and Compression Settings

```python
# Table size limits
max_table_rows_in_zip = 100000  # Max rows per table in ZIP
max_single_file_mb = 100        # Max file size in MB

# Compression
compression_level = 6           # ZIP compression (1-9)
```

### Format Options

```python
# Table format options
include_table_csv = True        # Human-readable CSV
include_table_parquet = True    # Optimized Parquet
```

### Retention and Cleanup

```python
# Retention settings
retention_days = 30             # Backup retention period
```

## 📊 Monitoring

### Log Levels
The backup process provides detailed logging:

```
[2024-01-01 12:00:00] [INFO] Starting backup process...
[2024-01-01 12:00:01] [INFO] Found 5 tables and 23 files
[2024-01-01 12:00:02] [INFO] Creating unified ZIP backup...
[2024-01-01 12:05:00] [INFO] Backup completed successfully
```

### Progress Tracking
Monitor backup progress through:
- Real-time log messages
- Table and file counts
- Compression statistics
- Duration tracking

### Verification
Built-in verification includes:
- ZIP integrity checks
- File count verification
- Size validation
- Metadata consistency

## 📈 Performance Considerations

### Large Tables
For tables with many rows:
```python
# Option 1: Increase row limit
max_table_rows_in_zip = 500000

# Option 2: Metadata-only backup for large tables
# (automatically applied when table exceeds limit)
```

### Large Files
For large files:
```python
# Increase file size limit
max_single_file_mb = 500

# Or exclude large files and backup separately
```

### Memory Optimization
```python
# Higher compression saves space but uses more memory
compression_level = 9

# Lower compression for memory-constrained environments
compression_level = 3
```

## 🏆 Best Practices

### 1. Regular Backups
- Schedule daily backups for active lakehouses
- Use weekly backups for stable data
- Test restore process regularly

### 2. Storage Strategy
- Use lakehouse backup for quick recovery
- Use external storage for long-term retention
- Implement backup rotation policies

### 3. Security
```python
# Always use managed identity
use_managed_identity = True

# Restrict backup location access
# Configure proper RBAC on storage accounts
```

### 4. Monitoring
- Enable detailed logging for production
- Monitor backup sizes and duration
- Set up alerts for backup failures

### 5. Testing
- Test backups with sample data first
- Verify restore process works
- Document recovery procedures

## 🚨 Error Handling

### Common Issues and Solutions

#### Authentication Errors
```
Error: Cannot write to backup location
```
**Solution**: Check managed identity permissions on target storage.

#### Memory Issues
```
Error: OutOfMemoryError during ZIP creation
```
**Solutions**:
- Reduce `max_table_rows_in_zip`
- Increase notebook compute resources
- Use higher compression level

#### Large File Issues
```
Warning: Skipping large file (150 MB)
```
**Solutions**:
- Increase `max_single_file_mb`
- Backup large files separately
- Use external storage with higher limits

### Error Recovery
- Failed backups can be safely retried
- Partial backups are automatically cleaned up
- Error details logged for troubleshooting

## 📋 Backup Manifest

Each backup creates a comprehensive manifest:

```json
{
  "backup_id": "12345678",
  "backup_timestamp": "2024-01-01_12-00-00",
  "backup_type": "complete_lakehouse_unified",
  "source_lakehouse_name": "my-lakehouse",
  "tables_included": 5,
  "files_included": 23,
  "total_items": 28,
  "compressed_size_mb": 45.6,
  "space_saved_mb": 123.4
}
```

## 🔍 Verification Steps

After backup completion:

1. **Check backup location**: Verify files are created
2. **Review logs**: Check for warnings or errors
3. **Validate manifest**: Ensure all items included
4. **Test restore**: Perform test restore to verify

## 📞 Support

For backup-related issues:
- Check the troubleshooting guide
- Review detailed logs
- Test with smaller datasets
- Report issues via GitHub

---

**Next**: [Restore Guide](restore-guide.md) | [Troubleshooting](troubleshooting.md)
