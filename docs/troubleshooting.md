# Troubleshooting Guide

This guide helps you diagnose and resolve common issues with Microsoft Fabric Lakehouse backup and restore operations.

## 📋 Table of Contents
- [Quick Diagnostic Steps](#quick-diagnostic-steps)
- [Backup Issues](#backup-issues)
- [Restore Issues](#restore-issues)
- [Authentication Problems](#authentication-problems)
- [Performance Issues](#performance-issues)
- [Storage Issues](#storage-issues)
- [Advanced Troubleshooting](#advanced-troubleshooting)

## 🔍 Quick Diagnostic Steps

Before diving into specific issues, run these quick checks:

### 1. Environment Check
```python
# Check Fabric utilities availability
print(f"Fabric utilities available: {fabric_utils_available} ({utils_name})")

# Check workspace ID
workspace_id = get_workspace_id()
print(f"Current workspace ID: {workspace_id}")

# Check Spark configuration
print(f"Spark app name: {spark.sparkContext.appName}")
print(f"Spark version: {spark.version}")
```

### 2. Permissions Check
```python
# Test source lakehouse access
try:
    test_df = spark.read.format("delta").load(f"{source_tables_path}/test_table")
    print("✅ Source access: OK")
except Exception as e:
    print(f"❌ Source access: {str(e)}")

# Test backup location access
try:
    test_df = spark.createDataFrame([("test",)], ["value"])
    test_df.write.mode("overwrite").format("delta").save(f"{backup_path}/_test")
    print("✅ Backup access: OK")
except Exception as e:
    print(f"❌ Backup access: {str(e)}")
```

### 3. Resource Check
```python
# Check available memory
import psutil
memory = psutil.virtual_memory()
print(f"Available memory: {memory.available / 1024**3:.2f} GB")

# Check disk space (if applicable)
disk = psutil.disk_usage('/')
print(f"Available disk space: {disk.free / 1024**3:.2f} GB")
```

## 🗄️ Backup Issues

### Issue: "Source Lakehouse Name is required"

**Error Message:**
```
ValueError: Source Lakehouse Name is required
```

**Cause:** Missing or empty `source_lakehouse_name` parameter.

**Solution:**
```python
source_lakehouse_name = "your-actual-lakehouse-name"  # Don't leave empty
```

### Issue: "Cannot write to backup location"

**Error Message:**
```
ValueError: Cannot write to backup location abfss://...
```

**Causes & Solutions:**

1. **Permissions Issue**
   ```python
   # Check managed identity permissions
   use_managed_identity = True
   
   # Or configure specific authentication
   spark.conf.set("fs.azure.account.auth.type...", "OAuth")
   ```

2. **Wrong Storage Account Name**
   ```python
   # Verify storage account name
   backup_storage_account = "correct-storage-account-name"
   ```

3. **Container Doesn't Exist**
   ```python
   # Create container first or use existing one
   backup_container = "existing-container-name"
   ```

### Issue: "No tables found in source lakehouse"

**Error Message:**
```
⚠️ No tables found in source lakehouse
```

**Causes & Solutions:**

1. **Wrong Lakehouse Name**
   ```python
   # Verify exact lakehouse name (case-sensitive)
   source_lakehouse_name = "exact-lakehouse-name"
   ```

2. **Empty Lakehouse**
   ```python
   # Check if lakehouse actually has tables
   # Create test table to verify connection
   ```

3. **Permissions Issue**
   ```python
   # Ensure read permissions on source lakehouse
   # Check workspace access
   ```

### Issue: "OutOfMemoryError during ZIP creation"

**Error Message:**
```
OutOfMemoryError: Java heap space
```

**Solutions:**

1. **Reduce Table Size Limit**
   ```python
   max_table_rows_in_zip = 50000  # Reduce from default 100000
   ```

2. **Reduce File Size Limit**
   ```python
   max_single_file_mb = 50  # Reduce from default 100
   ```

3. **Increase Compression**
   ```python
   compression_level = 9  # Higher compression, less memory usage
   ```

4. **Use Selective Backup**
   ```python
   # Backup specific tables only
   restore_specific_tables = ["critical_table1", "critical_table2"]
   ```

### Issue: Large Tables Metadata Only

**Warning Message:**
```
⚠️ Table 'large_table' too large (500,000 rows) - metadata only
```

**Explanation:** This is expected behavior for tables exceeding `max_table_rows_in_zip`.

**Solutions:**

1. **Increase Row Limit**
   ```python
   max_table_rows_in_zip = 1000000  # Allow larger tables
   ```

2. **Separate Backup Strategy**
   ```python
   # Backup large tables separately using Delta copy
   large_table_df = spark.read.format("delta").load(f"{source_path}/large_table")
   large_table_df.write.format("delta").save(f"{backup_path}/large_table_separate")
   ```

## 🔄 Restore Issues

### Issue: "Backup path is required"

**Error Message:**
```
ValueError: Backup path is required
```

**Solution:**
```python
# Specify the exact backup folder path
backup_path = "complete_backup_2024-01-01_12-00-00_abcd1234"
```

### Issue: "No suitable format found for table"

**Error Message:**
```
Error: No suitable format found for table 'products'
```

**Causes & Solutions:**

1. **Corrupted Backup**
   ```python
   # Check backup integrity
   backup_info = analyze_backup_source(backup_source_path)
   print(backup_info)
   ```

2. **Missing Format Files**
   ```python
   # Try different format preference
   table_format_preference = "csv"  # If Parquet is missing
   # or
   table_format_preference = "auto"  # Auto-detect best format
   ```

### Issue: "Table exists, skipping"

**Warning Message:**
```
⚠️ Table 'customers' exists, skipping (overwrite_existing=False)
```

**Solutions:**

1. **Enable Overwrite**
   ```python
   overwrite_existing = True  # Overwrite existing tables
   ```

2. **Restore to Different Lakehouse**
   ```python
   target_lakehouse_name = "new-restore-lakehouse"
   ```

3. **Use Selective Restore**
   ```python
   # Restore only non-existing tables
   restore_specific_tables = ["new_table1", "new_table2"]
   ```

### Issue: ZIP Reading Failures

**Error Message:**
```
Failed to read ZIP contents: BadZipFile
```

**Solutions:**

1. **Check Backup Method**
   ```python
   # Try different restore method
   restore_method = "zip_from_delta"  # If direct ZIP fails
   ```

2. **Verify Backup Integrity**
   ```python
   # Check if backup completed successfully
   # Look for backup summary and manifest
   ```

## 🔐 Authentication Problems

### Issue: Managed Identity Authentication Failed

**Error Message:**
```
AuthenticationException: Managed Identity authentication failed
```

**Solutions:**

1. **Check Managed Identity Assignment**
   - Verify system-assigned or user-assigned managed identity is enabled
   - Check role assignments on storage account

2. **Required Role Assignments**
   ```
   Storage Account:
   - Storage Blob Data Contributor (or Owner)
   - Storage Account Contributor (for container operations)
   
   Lakehouse:
   - Fabric Admin or Contributor role
   ```

3. **Alternative Authentication**
   ```python
   use_managed_identity = False
   # Configure SAS token or other authentication method
   ```

### Issue: Cross-Workspace Access Denied

**Error Message:**
```
Access denied to workspace or lakehouse
```

**Solutions:**

1. **Check Workspace Permissions**
   - Verify access to both source and target workspaces
   - Ensure proper Fabric capacity allocation

2. **Verify Workspace IDs**
   ```python
   # Use correct workspace IDs (GUIDs)
   source_workspace_id = "12345678-1234-1234-1234-123456789012"
   target_workspace_id = "87654321-4321-4321-4321-210987654321"
   ```

## ⚡ Performance Issues

### Issue: Slow Backup Performance

**Symptoms:** Backup takes excessive time to complete.

**Solutions:**

1. **Optimize Compression**
   ```python
   compression_level = 6  # Balance between speed and compression
   ```

2. **Reduce Data Volume**
   ```python
   # Use selective backup
   max_table_rows_in_zip = 100000
   max_single_file_mb = 100
   ```

3. **Increase Compute Resources**
   - Use larger Spark pool
   - Increase driver and executor memory

### Issue: Memory Pressure During Restore

**Symptoms:** Restore fails with memory errors.

**Solutions:**

1. **Sequential Processing**
   ```python
   # Restore tables one by one
   restore_specific_tables = ["table1"]  # Restore individually
   ```

2. **Reduce Batch Sizes**
   - Process smaller data chunks
   - Use streaming reads where possible

## 💾 Storage Issues

### Issue: Insufficient Storage Space

**Error Message:**
```
No space left on device
```

**Solutions:**

1. **Clean Up Old Backups**
   ```python
   # Implement retention policy
   retention_days = 30
   ```

2. **Use External Storage**
   ```python
   # Move to Azure Storage Account with more space
   backup_type = "storage_account"
   ```

### Issue: Storage Account Access Issues

**Error Message:**
```
This request is not authorized to perform this operation
```

**Solutions:**

1. **Check Storage Account Configuration**
   - Verify account name and container
   - Check firewall and network rules

2. **Verify Permissions**
   ```python
   # Required permissions on storage account:
   # - Storage Blob Data Contributor
   # - Storage Account Contributor (for container operations)
   ```

## 🔧 Advanced Troubleshooting

### Enable Debug Logging

```python
# Enable maximum logging
enable_detailed_logging = True

# Add custom debug prints
def debug_log(message):
    if enable_detailed_logging:
        print(f"[DEBUG] {message}")

# Use throughout your debugging
debug_log("Starting table discovery...")
```

### Manual Backup Inspection

```python
# Manually inspect backup contents
try:
    # Check manifest
    manifest_df = spark.read.format("delta").load(f"{backup_path}/_manifest")
    manifest_data = manifest_df.collect()[0].asDict()
    print("Backup manifest:", manifest_data)
    
    # Check logs
    log_df = spark.read.format("delta").load(f"{backup_path}/_logs")
    log_df.show(50, truncate=False)
    
except Exception as e:
    print(f"Cannot read backup metadata: {e}")
```

### Network and Connectivity Issues

```python
# Test connectivity to storage
import requests
try:
    # Test Azure Storage connectivity
    response = requests.get(f"https://{backup_storage_account}.blob.core.windows.net", timeout=10)
    print(f"Storage connectivity: {response.status_code}")
except Exception as e:
    print(f"Connectivity issue: {e}")
```

### Resource Monitoring During Operations

```python
import time
import psutil

def monitor_resources():
    while True:
        memory = psutil.virtual_memory()
        cpu = psutil.cpu_percent()
        print(f"Memory: {memory.percent}%, CPU: {cpu}%")
        time.sleep(30)

# Run in background during backup/restore operations
```

## 📞 Getting Help

### Information to Collect

When reporting issues, please provide:

1. **Environment Details**
   ```python
   print(f"Fabric utilities: {fabric_utils_available} ({utils_name})")
   print(f"Spark version: {spark.version}")
   print(f"Python version: {sys.version}")
   ```

2. **Configuration Used**
   ```python
   # Sanitized version of your configuration
   # (remove sensitive information like account names)
   ```

3. **Complete Error Messages**
   - Full stack traces
   - All error messages and warnings

4. **Log Entries**
   - Relevant log entries from the operation
   - Timeline of events

### Support Channels

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and community help
- **Documentation**: Check all guides first

### Before Reporting

1. Check this troubleshooting guide
2. Try the issue with a smaller dataset
3. Test with the latest version
4. Search existing GitHub issues

---

**Related**: [Backup Guide](backup-guide.md) | [Restore Guide](restore-guide.md) | [Advanced Usage](advanced-usage.md)
