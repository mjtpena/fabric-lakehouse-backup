# Backup Examples

This document provides practical examples for different backup scenarios using Microsoft Fabric Lakehouse backup tools.

## 📋 Table of Contents
- [Basic Backup Examples](#basic-backup-examples)
- [Storage Options](#storage-options)
- [Selective Backup](#selective-backup)
- [Enterprise Scenarios](#enterprise-scenarios)
- [Automation Examples](#automation-examples)

## 🚀 Basic Backup Examples

### Example 1: Simple Lakehouse to Lakehouse Backup

```python
# Complete backup of one lakehouse to another
source_lakehouse_name = "sales-analytics"
source_workspace_id = "12345678-1234-1234-1234-123456789012"

# Backup destination
backup_type = "lakehouse"
backup_lakehouse_name = "sales-analytics-backup"
backup_workspace_id = "12345678-1234-1234-1234-123456789012"

# Options
backup_tables = True
backup_files = True
backup_method = "unified_zip"
verify_backup = True
enable_detailed_logging = True
```

**Result**: Creates a unified ZIP backup containing all tables and files from the source lakehouse.

### Example 2: Tables-Only Backup

```python
# Backup only tables, skip files
source_lakehouse_name = "customer-data"
source_workspace_id = "your-workspace-id"

backup_type = "lakehouse"
backup_lakehouse_name = "customer-data-backup"
backup_workspace_id = "backup-workspace-id"

# Backup configuration
backup_tables = True   # Include tables
backup_files = False   # Skip files
backup_method = "unified_zip"

# Table-specific options
include_table_csv = True      # Include CSV format
include_table_parquet = True  # Include Parquet format
max_table_rows_in_zip = 200000  # Allow larger tables
```

**Use Case**: When you only need to backup structured data and want to exclude binary files.

### Example 3: Files-Only Backup

```python
# Backup only files, skip tables
source_lakehouse_name = "document-storage"
source_workspace_id = "your-workspace-id"

backup_type = "lakehouse"
backup_lakehouse_name = "document-backup"
backup_workspace_id = "backup-workspace-id"

# Backup configuration
backup_tables = False  # Skip tables
backup_files = True    # Include files
backup_method = "unified_zip"

# File-specific options
max_single_file_mb = 500  # Allow larger files
compression_level = 9     # Maximum compression for files
```

**Use Case**: Document management systems or when you only need to backup unstructured data.

## 💾 Storage Options

### Example 4: Azure Storage Account Backup

```python
# Backup to external Azure Storage Account
source_lakehouse_name = "production-data"
source_workspace_id = "prod-workspace-id"

# Azure Storage configuration
backup_type = "storage_account"
backup_storage_account = "companybackups"
backup_container = "fabric-lakehouse-backups"
use_managed_identity = True

# Backup options
backup_tables = True
backup_files = True
retention_days = 90  # 3 months retention
```

**Benefits**:
- Independent of Fabric quotas
- Long-term retention capabilities
- Cost-effective for large backups

### Example 5: Azure Data Lake Storage Gen2 Backup

```python
# Enterprise backup to ADLS Gen2
source_lakehouse_name = "enterprise-analytics"
source_workspace_id = "enterprise-workspace-id"

# ADLS Gen2 configuration
backup_type = "adls"
backup_adls_account = "enterprisedatalake"
backup_adls_container = "lakehouse-archives"
use_managed_identity = True

# Enterprise-grade options
backup_tables = True
backup_files = True
verify_backup = True
enable_detailed_logging = True
retention_days = 2555  # 7 years for compliance
```

**Use Case**: Enterprise environments with compliance requirements and long-term retention needs.

### Example 6: Cross-Region Backup

```python
# Backup to different Azure region for disaster recovery
source_lakehouse_name = "critical-systems"
source_workspace_id = "primary-region-workspace"

# Cross-region storage account
backup_type = "storage_account"
backup_storage_account = "drbackupaccount"  # In different region
backup_container = "disaster-recovery"
use_managed_identity = True

# High reliability options
backup_tables = True
backup_files = True
verify_backup = True
compression_level = 9  # Maximum compression for transfer
```

## 🎯 Selective Backup

### Example 7: Specific Tables Backup

```python
# Backup only critical tables
source_lakehouse_name = "ecommerce-platform"
source_workspace_id = "ecommerce-workspace-id"

backup_type = "lakehouse"
backup_lakehouse_name = "critical-data-backup"
backup_workspace_id = "backup-workspace-id"

# Selective backup - only specific tables
backup_tables = True
backup_files = False

# Note: For selective table backup, you would need to modify the notebook
# to include only specific tables. Example modification:
critical_tables = ["customers", "orders", "payments", "inventory"]

# In the backup notebook, you would filter the discovered tables:
# tables_to_backup = [table for table in discovered_tables if table in critical_tables]
```

### Example 8: File Type Specific Backup

```python
# Backup only specific file types
source_lakehouse_name = "media-assets"
source_workspace_id = "media-workspace-id"

backup_type = "storage_account"
backup_storage_account = "mediabackups"
backup_container = "image-archives"

backup_tables = False
backup_files = True

# Configuration for image files only
max_single_file_mb = 1000  # Allow large image files

# Note: For selective file backup by type, you would modify the backup logic:
# allowed_extensions = ['.jpg', '.png', '.gif', '.svg', '.webp']
# files_to_backup = [f for f in discovered_files if any(f['name'].lower().endswith(ext) for ext in allowed_extensions)]
```

### Example 9: Size-Based Selective Backup

```python
# Backup with size restrictions
source_lakehouse_name = "mixed-data"
source_workspace_id = "your-workspace-id"

backup_type = "lakehouse"
backup_lakehouse_name = "size-optimized-backup"
backup_workspace_id = "backup-workspace-id"

backup_tables = True
backup_files = True

# Size-based filtering
max_table_rows_in_zip = 50000   # Smaller tables only
max_single_file_mb = 50         # Smaller files only
compression_level = 9           # Maximum compression

# Large items will be metadata-only or skipped with warnings
```

## 🏢 Enterprise Scenarios

### Example 10: Multi-Environment Backup Strategy

```python
# Dynamic configuration based on environment
import os

# Detect environment (could be set by pipeline parameter)
environment = os.environ.get("FABRIC_ENVIRONMENT", "development")

# Environment-specific configurations
environments = {
    "production": {
        "source_lakehouse_name": "prod-analytics",
        "source_workspace_id": "prod-workspace-id",
        "backup_type": "storage_account",
        "backup_storage_account": "prodbackups",
        "retention_days": 90,
        "verify_backup": True,
        "compression_level": 6
    },
    "staging": {
        "source_lakehouse_name": "staging-analytics", 
        "source_workspace_id": "staging-workspace-id",
        "backup_type": "lakehouse",
        "backup_lakehouse_name": "staging-backup",
        "retention_days": 30,
        "verify_backup": True,
        "compression_level": 7
    },
    "development": {
        "source_lakehouse_name": "dev-analytics",
        "source_workspace_id": "dev-workspace-id", 
        "backup_type": "lakehouse",
        "backup_lakehouse_name": "dev-backup",
        "retention_days": 7,
        "verify_backup": False,
        "compression_level": 9
    }
}

# Apply environment configuration
config = environments[environment]
source_lakehouse_name = config["source_lakehouse_name"]
source_workspace_id = config["source_workspace_id"]
backup_type = config["backup_type"]
retention_days = config["retention_days"]
verify_backup = config["verify_backup"]
compression_level = config["compression_level"]

# Apply storage-specific settings
if backup_type == "storage_account":
    backup_storage_account = config["backup_storage_account"]
    backup_container = f"{environment}-backups"
elif backup_type == "lakehouse":
    backup_lakehouse_name = config["backup_lakehouse_name"]
    backup_workspace_id = config.get("backup_workspace_id", source_workspace_id)
```

### Example 11: Compliance-Focused Backup

```python
# Backup with compliance and audit requirements
source_lakehouse_name = "financial-data"
source_workspace_id = "finance-workspace-id"

# Secure backup location
backup_type = "adls"
backup_adls_account = "compliancebackups"
backup_adls_container = "financial-archives"
use_managed_identity = True

# Compliance settings
backup_tables = True
backup_files = True
verify_backup = True
enable_detailed_logging = True
retention_days = 2555  # 7 years for financial compliance

# Enhanced options for compliance
include_table_csv = True      # Human-readable format for audits
include_table_parquet = True  # Optimized format for analytics
compression_level = 9         # Maximum compression for storage efficiency

# Custom backup folder with compliance naming
backup_folder_path = f"financial_backup_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_compliance"
```

### Example 12: High-Frequency Backup for Critical Systems

```python
# Configuration for systems requiring frequent backups
source_lakehouse_name = "real-time-trading"
source_workspace_id = "trading-workspace-id"

# Fast backup configuration
backup_type = "lakehouse"
backup_lakehouse_name = "trading-backup-hourly"
backup_workspace_id = "trading-workspace-id"

backup_tables = True
backup_files = False  # Skip files for speed

# Speed-optimized settings
compression_level = 3         # Lower compression for speed
max_table_rows_in_zip = 1000000  # Allow large tables
verify_backup = False         # Skip verification for speed
enable_detailed_logging = False  # Minimal logging

# Include timestamp in backup path
backup_folder_path = f"hourly_backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
```

## 🤖 Automation Examples

### Example 13: Parameterized Backup for Fabric Pipelines

```python
# Parameters that can be passed from Fabric Pipeline
import json

# Default values
default_config = {
    "source_lakehouse_name": "",
    "source_workspace_id": "",
    "backup_type": "lakehouse",
    "backup_lakehouse_name": "",
    "backup_workspace_id": "",
    "notification_webhook": "",
    "job_id": ""
}

# Override with pipeline parameters (if available)
try:
    # In Fabric Pipeline, these would be passed as parameters
    pipeline_params = {
        "source_lakehouse_name": "dynamic-lakehouse",
        "backup_type": "storage_account",
        "backup_storage_account": "automated-backups",
        "notification_webhook": "https://company.com/webhooks/backup-complete",
        "job_id": "backup-job-12345"
    }
    
    # Merge configurations
    config = {**default_config, **pipeline_params}
    
    # Apply parameters
    source_lakehouse_name = config["source_lakehouse_name"]
    backup_type = config["backup_type"]
    backup_storage_account = config.get("backup_storage_account", "")
    notification_webhook = config.get("notification_webhook", "")
    job_id = config.get("job_id", "")
    
except Exception as e:
    print(f"Using default configuration: {e}")
    config = default_config
```

### Example 14: Scheduled Daily Backup with Notifications

```python
# Daily backup with notification integration
source_lakehouse_name = "daily-operations"
source_workspace_id = "operations-workspace-id"

backup_type = "storage_account"
backup_storage_account = "dailybackups"
backup_container = "scheduled-backups"

# Daily backup settings
backup_tables = True
backup_files = True
verify_backup = True
enable_detailed_logging = True

# Time-based backup path
backup_folder_path = f"daily_{datetime.datetime.now().strftime('%Y-%m-%d')}"

# Notification settings (would be implemented in the backup notebook)
notification_config = {
    "webhook_url": "https://company.slack.com/webhooks/backup-notifications",
    "email_recipients": ["admin@company.com", "backup-team@company.com"],
    "include_metrics": True
}
```

### Example 15: Conditional Backup Based on Data Changes

```python
# Backup only if data has changed since last backup
source_lakehouse_name = "customer-analytics"
source_workspace_id = "analytics-workspace-id"

# Check for changes since last backup
def should_backup():
    try:
        # Check last backup timestamp from manifest
        last_backup_df = spark.read.format("delta").load(f"{backup_path}/_manifest")
        last_backup_time = last_backup_df.select("backup_timestamp").collect()[0][0]
        
        # Check if source data has been modified since then
        source_tables_df = spark.read.format("delta").load(f"{source_tables_path}/*")
        source_modified_time = source_tables_df.select("_commit_timestamp").agg({"_commit_timestamp": "max"}).collect()[0][0]
        
        # Backup if data is newer than last backup
        return source_modified_time > last_backup_time
        
    except Exception:
        # If can't determine, backup anyway
        return True

# Conditional backup configuration
if should_backup():
    backup_tables = True
    backup_files = True
    print("Data changes detected, proceeding with backup...")
else:
    print("No changes detected, skipping backup...")
    # Set backup flags to False or exit early
```

## 📊 Performance Examples

### Example 16: Memory-Optimized Backup for Large Datasets

```python
# Configuration optimized for large datasets with limited memory
source_lakehouse_name = "big-data-warehouse"
source_workspace_id = "warehouse-workspace-id"

backup_type = "storage_account"
backup_storage_account = "bigdatabackups"
backup_container = "warehouse-archives"

# Memory-conscious settings
backup_tables = True
backup_files = True

# Conservative limits to avoid memory issues
max_table_rows_in_zip = 25000   # Smaller batches
max_single_file_mb = 25         # Smaller files
compression_level = 9           # Maximum compression to save space

# Skip verification for large datasets (can be done separately)
verify_backup = False
enable_detailed_logging = False  # Reduce memory usage
```

### Example 17: High-Performance Backup

```python
# Configuration optimized for speed
source_lakehouse_name = "time-sensitive-data"
source_workspace_id = "urgent-workspace-id"

backup_type = "lakehouse"  # Fastest option
backup_lakehouse_name = "urgent-backup"
backup_workspace_id = "urgent-workspace-id"

# Speed-optimized settings
backup_tables = True
backup_files = False  # Skip files for maximum speed

# Performance settings
compression_level = 1           # Minimal compression for speed
max_table_rows_in_zip = 2000000 # Large batches
verify_backup = False           # Skip verification
enable_detailed_logging = False # Minimal logging

# Include only essential table formats
include_table_csv = False       # Skip CSV for speed
include_table_parquet = True    # Keep only Parquet
```

---

These examples provide practical configurations for various backup scenarios. Modify the parameters according to your specific requirements and environment.

**Related**: [Restore Examples](restore-examples.md) | [Backup Guide](../docs/backup-guide.md)
