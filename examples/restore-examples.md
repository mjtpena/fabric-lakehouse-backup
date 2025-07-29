# Restore Examples

This document provides practical examples for different restore scenarios using Microsoft Fabric Lakehouse restore tools.

## 📋 Table of Contents
- [Basic Restore Examples](#basic-restore-examples)
- [Selective Restore](#selective-restore)
- [Format-Specific Restore](#format-specific-restore)
- [Enterprise Scenarios](#enterprise-scenarios)
- [Disaster Recovery](#disaster-recovery)

## 🚀 Basic Restore Examples

### Example 1: Complete Restore from Lakehouse Backup

```python
# Restore everything from a lakehouse backup
backup_source_type = "lakehouse"
backup_lakehouse_name = "sales-analytics-backup"
backup_workspace_id = "backup-workspace-id"
backup_path = "complete_backup_2024-01-15_14-30-45_abc123"

# Target configuration
target_lakehouse_name = "sales-analytics-restored"
target_workspace_id = "restore-workspace-id"

# Restore options
restore_tables = True
restore_files = True
table_format_preference = "parquet"  # Optimal performance
overwrite_existing = False
verify_restore = True
dry_run = False
```

**Result**: Restores all tables and files from the backup to a new lakehouse.

### Example 2: Quick Preview with Dry Run

```python
# Preview what would be restored without making changes
backup_source_type = "lakehouse"
backup_lakehouse_name = "customer-data-backup"
backup_workspace_id = "backup-workspace-id"
backup_path = "complete_backup_2024-01-15_09-15-30_def456"

target_lakehouse_name = "customer-data-preview"
target_workspace_id = "test-workspace-id"

# Dry run configuration
restore_tables = True
restore_files = True
dry_run = True  # Preview only, no actual changes
enable_detailed_logging = True
```

**Output Example**:
```
[INFO] DRY RUN: Would restore table customers (50,000 rows)
[INFO] DRY RUN: Would restore table orders (120,000 rows)
[INFO] DRY RUN: Would restore file documents/report.pdf (2.1 MB)
[INFO] DRY RUN: Would restore file images/logo.png (45 KB)
[INFO] DRY RUN: Total: 2 tables, 2 files, estimated 15.2 MB
```

### Example 3: Restore from Azure Storage Account

```python
# Restore from external Azure Storage Account backup
backup_source_type = "storage_account"
backup_storage_account = "companybackups"
backup_container = "fabric-lakehouse-backups"
backup_path = "complete_backup_2024-01-10_16-20-15_ghi789"
use_managed_identity = True

# Target configuration
target_lakehouse_name = "production-data-restored"
target_workspace_id = "production-workspace-id"

# Restore options
restore_tables = True
restore_files = True
table_format_preference = "auto"  # Let system choose best format
verify_restore = True
```

**Use Case**: Restoring from long-term storage or cross-region backup.

## 🎯 Selective Restore

### Example 4: Restore Specific Tables Only

```python
# Restore only critical business tables
backup_source_type = "lakehouse"
backup_lakehouse_name = "ecommerce-backup"
backup_workspace_id = "backup-workspace-id"
backup_path = "complete_backup_2024-01-12_11-45-20_jkl012"

target_lakehouse_name = "critical-data-restore"
target_workspace_id = "recovery-workspace-id"

# Selective restore - tables only
restore_tables = True
restore_files = False  # Skip files

# Specific tables to restore
restore_specific_tables = ["customers", "orders", "payments", "inventory"]

table_format_preference = "parquet"
overwrite_existing = True  # Overwrite if exists
verify_restore = True
```

**Use Case**: When you only need specific business-critical tables for analysis or recovery.

### Example 5: Restore Specific File Types

```python
# Restore only document files
backup_source_type = "storage_account"
backup_storage_account = "documentbackups"
backup_container = "archives"
backup_path = "complete_backup_2024-01-08_13-30-00_mno345"

target_lakehouse_name = "document-recovery"
target_workspace_id = "recovery-workspace-id"

# Files only restore
restore_tables = False
restore_files = True

# Specific file patterns
restore_specific_files = [
    "*.pdf",                    # All PDF documents
    "*.docx",                   # All Word documents
    "contracts/*",              # All files in contracts folder
    "reports/quarterly_*.xlsx"  # Specific Excel reports
]

verify_restore = True
```

**Use Case**: Document recovery or when you only need specific file types.

### Example 6: Restore Single Table for Analysis

```python
# Restore just one table for quick analysis
backup_source_type = "lakehouse"
backup_lakehouse_name = "analytics-backup"
backup_workspace_id = "backup-workspace-id"
backup_path = "complete_backup_2024-01-14_08-15-45_pqr678"

target_lakehouse_name = "single-table-analysis"
target_workspace_id = "analysis-workspace-id"

# Single table restore
restore_tables = True
restore_files = False
restore_specific_tables = ["customer_transactions"]  # Only this table

table_format_preference = "parquet"  # Best for analysis
overwrite_existing = True
verify_restore = True
dry_run = False
```

**Use Case**: Quick data analysis or troubleshooting specific data issues.

## 📊 Format-Specific Restore

### Example 7: CSV Format for Excel Compatibility

```python
# Restore tables in CSV format for Excel analysis
backup_source_type = "lakehouse"
backup_lakehouse_name = "business-data-backup"
backup_workspace_id = "backup-workspace-id"
backup_path = "complete_backup_2024-01-13_15-45-30_stu901"

target_lakehouse_name = "excel-compatible-data"
target_workspace_id = "business-workspace-id"

# CSV format preference
restore_tables = True
restore_files = False
table_format_preference = "csv"  # Human-readable, Excel compatible

# Specific business tables
restore_specific_tables = ["sales_summary", "customer_metrics", "product_performance"]

overwrite_existing = True
verify_restore = True
```

**Use Case**: When business users need to analyze data in Excel or other tools that prefer CSV format.

### Example 8: Parquet Format for Analytics

```python
# Restore in Parquet format for optimal analytics performance
backup_source_type = "adls"
backup_adls_account = "enterprisedatalake"
backup_adls_container = "lakehouse-archives"
backup_path = "complete_backup_2024-01-11_10-20-35_vwx234"

target_lakehouse_name = "analytics-optimized"
target_workspace_id = "analytics-workspace-id"

# Parquet format for best performance
restore_tables = True
restore_files = False  # Analytics typically doesn't need files
table_format_preference = "parquet"  # Optimal for Spark analytics

# Large dataset configuration
overwrite_existing = True
verify_restore = True
enable_detailed_logging = True
```

**Use Case**: Data science and analytics workloads requiring optimal performance.

### Example 9: Auto Format Selection

```python
# Let the system choose the best available format
backup_source_type = "lakehouse"
backup_lakehouse_name = "mixed-data-backup"
backup_workspace_id = "backup-workspace-id"
backup_path = "complete_backup_2024-01-09_14-10-25_yzab567"

target_lakehouse_name = "auto-format-restore"
target_workspace_id = "flexible-workspace-id"

# Auto format selection
restore_tables = True
restore_files = True
table_format_preference = "auto"  # System chooses best available

# Let system handle all decisions
overwrite_existing = False  # Be conservative
verify_restore = True
```

**Use Case**: When you're not sure about the backup format or want optimal automatic selection.

## 🏢 Enterprise Scenarios

### Example 10: Production System Recovery

```python
# Full production system recovery scenario
backup_source_type = "storage_account"
backup_storage_account = "prodbackups"
backup_container = "disaster-recovery"
backup_path = "complete_backup_2024-01-14_02-00-00_prod789"  # Last good backup

# Production recovery target
target_lakehouse_name = "production-system-recovered"
target_workspace_id = "production-workspace-id"

# Complete recovery configuration
restore_tables = True
restore_files = True
table_format_preference = "parquet"  # Production performance
overwrite_existing = True  # Replace corrupted data
verify_restore = True
enable_detailed_logging = True

# No dry run - this is actual recovery
dry_run = False
```

**Use Case**: Disaster recovery when production system needs to be restored from backup.

### Example 11: Environment Refresh (Production to Staging)

```python
# Refresh staging environment with production data
backup_source_type = "storage_account"
backup_storage_account = "prodbackups"
backup_container = "environment-refresh"
backup_path = "complete_backup_2024-01-15_06-00-00_refresh123"

# Staging environment target
target_lakehouse_name = "staging-refreshed"
target_workspace_id = "staging-workspace-id"

# Environment refresh settings
restore_tables = True
restore_files = True
table_format_preference = "parquet"
overwrite_existing = True  # Replace existing staging data

# Verify to ensure staging is properly refreshed
verify_restore = True
enable_detailed_logging = True
```

**Use Case**: Refreshing lower environments with production data for testing.

### Example 12: Compliance Audit Restore

```python
# Restore historical data for compliance audit
backup_source_type = "adls"
backup_adls_account = "compliancebackups"
backup_adls_container = "financial-archives"
backup_path = "complete_backup_2023-12-31_23-59-59_audit456"  # Year-end backup

target_lakehouse_name = "audit-2023-data"
target_workspace_id = "compliance-workspace-id"

# Audit-specific configuration
restore_tables = True
restore_files = True
table_format_preference = "csv"  # Human-readable for auditors

# Specific audit tables
restore_specific_tables = [
    "financial_transactions",
    "customer_accounts", 
    "regulatory_reports",
    "audit_trails"
]

# Specific audit documents
restore_specific_files = [
    "compliance/*.pdf",
    "reports/2023_*.xlsx",
    "audit_evidence/*"
]

verify_restore = True
enable_detailed_logging = True
```

**Use Case**: Compliance audits requiring historical data restoration.

## 🚨 Disaster Recovery

### Example 13: Cross-Region Disaster Recovery

```python
# Restore from backup in different Azure region
backup_source_type = "storage_account"
backup_storage_account = "drbackupaccount"  # In DR region
backup_container = "disaster-recovery"
backup_path = "complete_backup_2024-01-14_18-00-00_dr789"

# DR environment target
target_lakehouse_name = "disaster-recovery-system"
target_workspace_id = "dr-workspace-id"  # In DR region

# DR configuration
restore_tables = True
restore_files = True
table_format_preference = "parquet"
overwrite_existing = True

# Critical for DR scenarios
verify_restore = True
enable_detailed_logging = True

# Immediate restore - no dry run
dry_run = False
```

**Use Case**: Activating disaster recovery site when primary region is unavailable.

### Example 14: Point-in-Time Recovery

```python
# Restore to specific point in time
backup_source_type = "storage_account"
backup_storage_account = "timeseriesbackups"
backup_container = "point-in-time"

# Choose specific backup timestamp (before incident occurred)
backup_path = "complete_backup_2024-01-14_14-30-00_before_incident"

target_lakehouse_name = "point-in-time-recovery"
target_workspace_id = "recovery-workspace-id"

# Point-in-time recovery settings
restore_tables = True
restore_files = True
table_format_preference = "parquet"
overwrite_existing = True
verify_restore = True

# Document the recovery point
recovery_notes = f"Restored to backup taken at 2024-01-14 14:30:00 (before incident)"
```

**Use Case**: Recovering to a specific point in time before data corruption or other incidents.

### Example 15: Partial System Recovery

```python
# Restore only affected components after partial system failure
backup_source_type = "lakehouse"
backup_lakehouse_name = "system-backup"
backup_workspace_id = "backup-workspace-id"
backup_path = "complete_backup_2024-01-15_12-00-00_partial123"

target_lakehouse_name = "partial-recovery"
target_workspace_id = "recovery-workspace-id"

# Restore only affected tables (other tables are still good)
restore_tables = True
restore_files = False  # Files weren't affected

# Only restore corrupted tables
restore_specific_tables = ["corrupted_table1", "corrupted_table2"]

table_format_preference = "parquet"
overwrite_existing = True  # Replace corrupted data
verify_restore = True
```

**Use Case**: When only part of the system is affected and needs restoration.

## 🔧 Testing and Validation

### Example 16: Backup Validation Restore

```python
# Restore to validate backup integrity (testing purposes)
backup_source_type = "lakehouse"
backup_lakehouse_name = "test-backup"
backup_workspace_id = "test-workspace-id"
backup_path = "complete_backup_2024-01-15_16-45-30_validation789"

# Temporary validation target
target_lakehouse_name = "backup-validation-test"
target_workspace_id = "test-workspace-id"

# Validation configuration
restore_tables = True
restore_files = True
table_format_preference = "auto"

# Sample restore for validation
restore_specific_tables = ["sample_table"]  # Just one table for testing
restore_specific_files = ["sample_file.pdf"]  # Just one file for testing

verify_restore = True
enable_detailed_logging = True
dry_run = False
```

**Use Case**: Regular testing to ensure backups are restorable.

### Example 17: Performance Testing Restore

```python
# Restore for performance testing
backup_source_type = "storage_account"
backup_storage_account = "performancetest"
backup_container = "test-data"
backup_path = "complete_backup_2024-01-15_10-30-00_perftest456"

target_lakehouse_name = "performance-test-data"
target_workspace_id = "performance-workspace-id"

# Performance test configuration
restore_tables = True
restore_files = False  # Focus on table performance

# Test different formats
table_format_preference = "parquet"  # Test optimal format

# Large dataset for performance testing
restore_specific_tables = ["large_table1", "large_table2"]

verify_restore = False  # Skip verification for speed
enable_detailed_logging = False  # Minimal logging for speed
```

**Use Case**: Setting up performance testing environments with realistic data.

---

These examples provide practical configurations for various restore scenarios. Modify the parameters according to your specific requirements and environment.

**Related**: [Backup Examples](backup-examples.md) | [Restore Guide](../docs/restore-guide.md)
