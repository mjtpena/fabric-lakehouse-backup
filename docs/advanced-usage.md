# Advanced Usage Guide

This guide covers advanced scenarios and configurations for Microsoft Fabric Lakehouse backup and restore operations.

## 📋 Table of Contents
- [Enterprise Deployment](#enterprise-deployment)
- [Automation & Scheduling](#automation--scheduling)
- [Cross-Region Backup](#cross-region-backup)
- [Large Scale Operations](#large-scale-operations)
- [Custom Authentication](#custom-authentication)
- [Integration Patterns](#integration-patterns)
- [Performance Tuning](#performance-tuning)
- [Monitoring & Alerting](#monitoring--alerting)

## 🏢 Enterprise Deployment

### Multi-Environment Strategy

```python
# Production backup configuration
environments = {
    "production": {
        "source_workspace_id": "prod-workspace-id",
        "backup_workspace_id": "prod-backup-workspace-id",
        "retention_days": 90,
        "backup_frequency": "daily"
    },
    "staging": {
        "source_workspace_id": "staging-workspace-id", 
        "backup_workspace_id": "staging-backup-workspace-id",
        "retention_days": 30,
        "backup_frequency": "weekly"
    },
    "development": {
        "source_workspace_id": "dev-workspace-id",
        "backup_workspace_id": "dev-backup-workspace-id", 
        "retention_days": 7,
        "backup_frequency": "weekly"
    }
}

# Dynamic configuration based on environment
current_env = "production"  # Set based on deployment
config = environments[current_env]

source_workspace_id = config["source_workspace_id"]
backup_workspace_id = config["backup_workspace_id"]
retention_days = config["retention_days"]
```

### Governance and Compliance

```python
# Compliance-focused configuration
backup_config = {
    # Data classification
    "include_sensitive_tables": False,  # Exclude PII data
    "encrypt_backup": True,             # Additional encryption
    "audit_logging": True,              # Enhanced audit trail
    
    # Retention policies
    "retention_days": 2555,             # 7 years for compliance
    "archive_after_days": 365,          # Move to archive tier
    
    # Access controls
    "require_approval": True,           # Backup approval workflow
    "backup_encryption_key": "key-vault-reference"
}
```

### Data Classification Integration

```python
# Classify and handle data appropriately
sensitive_tables = ["customer_pii", "financial_data", "health_records"]
public_tables = ["product_catalog", "public_content"]

def backup_with_classification():
    # Backup public data normally
    backup_tables = True
    restore_specific_tables = public_tables
    
    # Handle sensitive data separately with enhanced security
    if include_sensitive_data:
        # Use encrypted storage account
        backup_type = "storage_account"
        backup_storage_account = "encrypted-storage-account"
        use_encryption_key = True
```

## 🕐 Automation & Scheduling

### Parameterized Notebooks for Automation

```python
# Parameters for Azure Data Factory or Fabric Pipeline
import json

# Configuration from pipeline parameters
pipeline_params = {
    "source_lakehouse": "dynamic-lakehouse-name",
    "backup_type": "storage_account", 
    "backup_location": "automated-backups",
    "notification_endpoint": "webhook-url",
    "job_id": "backup-job-12345"
}

# Apply parameters
source_lakehouse_name = pipeline_params.get("source_lakehouse")
backup_storage_account = pipeline_params.get("backup_location")
job_id = pipeline_params.get("job_id")
```

### Fabric Pipeline Integration

```yaml
# Example Fabric Pipeline YAML
pipeline:
  activities:
    - name: "DailyLakehouseBackup"
      type: "Notebook"
      notebook: "Fabric_Lakehouse_Combined_Backup"
      parameters:
        source_lakehouse_name: "@pipeline().parameters.LakehouseName"
        backup_type: "storage_account"
        backup_storage_account: "@pipeline().parameters.BackupAccount"
        enable_notifications: true
        job_id: "@pipeline().RunId"
      
    - name: "NotifyBackupComplete"
      type: "WebActivity"
      dependsOn: ["DailyLakehouseBackup"]
      method: "POST"
      url: "@pipeline().parameters.NotificationWebhook"
```

### Scheduling Best Practices

```python
# Time-based configuration
import datetime

current_hour = datetime.datetime.now().hour

# Adjust performance based on time
if 2 <= current_hour <= 6:  # Overnight hours
    # Use more resources for faster backup
    compression_level = 9
    max_table_rows_in_zip = 500000
    max_single_file_mb = 500
else:  # Business hours
    # Use fewer resources
    compression_level = 6
    max_table_rows_in_zip = 100000
    max_single_file_mb = 100
```

## 🌍 Cross-Region Backup

### Geo-Redundant Backup Strategy

```python
# Primary backup (same region)
primary_backup = {
    "backup_type": "lakehouse",
    "backup_lakehouse_name": "primary-backup-lakehouse",
    "backup_workspace_id": "primary-workspace-id"
}

# Secondary backup (different region)
secondary_backup = {
    "backup_type": "storage_account", 
    "backup_storage_account": "crossregionbackups",
    "backup_container": "disaster-recovery",
    "storage_region": "different-azure-region"
}

# Execute both backups
def execute_geo_redundant_backup():
    # Primary backup
    log_message("Starting primary backup...", "INFO")
    primary_result = create_unified_zip_backup(source_paths, primary_backup_path, tables_info, files_info)
    
    if primary_result.get("success"):
        # Secondary backup (copy primary to different region)
        log_message("Starting cross-region backup...", "INFO")
        copy_backup_cross_region(primary_backup_path, secondary_backup)
```

### Multi-Cloud Strategy

```python
# Backup to multiple cloud providers
backup_destinations = [
    {
        "type": "azure_storage",
        "account": "azure-backup-account",
        "container": "fabric-backups"
    },
    {
        "type": "aws_s3",
        "bucket": "fabric-backups-s3",
        "region": "us-east-1"
    }
]

# Note: AWS S3 integration would require additional setup
```

## 📊 Large Scale Operations

### Parallel Backup Strategy

```python
# Divide tables into chunks for parallel processing
def partition_tables_for_parallel_backup(tables_list, chunk_size=5):
    chunks = []
    for i in range(0, len(tables_list), chunk_size):
        chunks.append(tables_list[i:i + chunk_size])
    return chunks

# Process table chunks in parallel (future enhancement)
table_chunks = partition_tables_for_parallel_backup(discovered_tables)
```

### Memory-Optimized Large Dataset Handling

```python
# Configuration for very large datasets
large_scale_config = {
    "streaming_backup": True,           # Stream data instead of loading all
    "chunk_size_mb": 50,               # Process in smaller chunks
    "temp_storage_path": "/tmp/backup", # Use temp storage for processing
    "cleanup_temp": True,              # Clean up temp files
    "progress_checkpoints": True       # Save progress for resume capability
}

def stream_large_table_backup(table_path, table_name):
    """Backup large tables using streaming approach"""
    df = spark.read.format("delta").load(table_path)
    
    # Process in batches
    total_partitions = df.rdd.getNumPartitions()
    batch_size = max(1, total_partitions // 10)  # 10 batches
    
    for i in range(0, total_partitions, batch_size):
        batch_df = df.limit(batch_size).offset(i * batch_size)
        # Process batch...
```

### Distributed Backup Coordination

```python
# Coordinate multiple backup jobs
backup_jobs = [
    {"lakehouse": "lakehouse1", "priority": "high"},
    {"lakehouse": "lakehouse2", "priority": "medium"},
    {"lakehouse": "lakehouse3", "priority": "low"}
]

def execute_distributed_backup():
    results = {}
    
    # Process high priority first
    high_priority = [job for job in backup_jobs if job["priority"] == "high"]
    for job in high_priority:
        results[job["lakehouse"]] = execute_backup(job)
    
    # Then medium and low priority
    # (Implementation would use actual job queue or orchestration)
```

## 🔐 Custom Authentication

### Service Principal Authentication

```python
# Service principal configuration
service_principal_config = {
    "client_id": "your-app-id",
    "client_secret": "your-app-secret",  # Store in Key Vault
    "tenant_id": "your-tenant-id"
}

def setup_service_principal_auth():
    """Configure service principal for backup operations"""
    spark.conf.set("fs.azure.account.auth.type.{}.dfs.core.windows.net".format(backup_storage_account), "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type.{}.dfs.core.windows.net".format(backup_storage_account), 
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id.{}.dfs.core.windows.net".format(backup_storage_account), 
                   service_principal_config["client_id"])
    spark.conf.set("fs.azure.account.oauth2.client.secret.{}.dfs.core.windows.net".format(backup_storage_account), 
                   service_principal_config["client_secret"])
    spark.conf.set("fs.azure.account.oauth2.client.endpoint.{}.dfs.core.windows.net".format(backup_storage_account), 
                   f"https://login.microsoftonline.com/{service_principal_config['tenant_id']}/oauth2/token")
```

### Azure Key Vault Integration

```python
# Key Vault integration for secure credential management
def get_secret_from_keyvault(secret_name):
    """Retrieve secrets from Azure Key Vault"""
    try:
        # Using fabric_utils if available
        if fabric_utils_available:
            return fabric_utils.credentials.getSecret(secret_name)
        else:
            # Alternative: direct Key Vault API call
            return get_secret_direct(secret_name)
    except Exception as e:
        log_message(f"Failed to retrieve secret {secret_name}: {e}", "ERROR")
        return None

# Use secrets for authentication
backup_storage_account_key = get_secret_from_keyvault("backup-storage-key")
service_principal_secret = get_secret_from_keyvault("sp-secret")
```

## 🔗 Integration Patterns

### REST API Integration

```python
# Webhook notifications for backup completion
import requests
import json

def send_backup_notification(backup_result, webhook_url):
    """Send backup completion notification"""
    notification_payload = {
        "event": "backup_completed",
        "timestamp": datetime.datetime.now().isoformat(),
        "lakehouse": source_lakehouse_name,
        "status": "success" if backup_result.get("success") else "failed",
        "backup_id": backup_result.get("backup_id"),
        "tables_backed_up": backup_result.get("tables_included", 0),
        "files_backed_up": backup_result.get("files_included", 0),
        "size_mb": backup_result.get("compressed_size_mb", 0)
    }
    
    try:
        response = requests.post(
            webhook_url,
            json=notification_payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        log_message(f"Notification sent: {response.status_code}", "INFO")
    except Exception as e:
        log_message(f"Failed to send notification: {e}", "WARNING")
```

### Event-Driven Architecture

```python
# Event Hub integration for backup events
def publish_backup_event(event_data):
    """Publish backup events to Azure Event Hub"""
    try:
        # Event Hub connection string from Key Vault
        connection_string = get_secret_from_keyvault("eventhub-connection")
        
        # Create event payload
        event_payload = {
            "eventType": "LakehouseBackupCompleted",
            "subject": f"lakehouse/{source_lakehouse_name}",
            "data": event_data,
            "eventTime": datetime.datetime.now().isoformat()
        }
        
        # Send to Event Hub (implementation would use Event Hub SDK)
        # send_to_event_hub(connection_string, event_payload)
        
    except Exception as e:
        log_message(f"Failed to publish event: {e}", "WARNING")
```

### Power BI Integration

```python
# Create backup status dashboard data
def create_backup_metrics():
    """Create metrics for Power BI dashboard"""
    metrics = {
        "backup_timestamp": datetime.datetime.now().isoformat(),
        "lakehouse_name": source_lakehouse_name,
        "backup_size_mb": backup_result.get("compressed_size_mb", 0),
        "compression_ratio": backup_result.get("compression_ratio", 0),
        "duration_minutes": backup_duration / 60,
        "tables_count": backup_result.get("tables_included", 0),
        "files_count": backup_result.get("files_included", 0),
        "success": backup_result.get("success", False)
    }
    
    # Write to metrics table for Power BI
    metrics_df = spark.createDataFrame([metrics])
    metrics_df.write.mode("append").format("delta").save("backup_metrics_table")
```

## ⚡ Performance Tuning

### Spark Configuration Optimization

```python
# Optimize Spark settings for backup operations
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Memory optimization
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.executor.memoryFraction", "0.8")
```

### Adaptive Configuration

```python
def get_adaptive_config(data_size_gb):
    """Get configuration based on data size"""
    if data_size_gb < 1:
        return {
            "compression_level": 9,
            "max_table_rows_in_zip": 200000,
            "max_single_file_mb": 200
        }
    elif data_size_gb < 10:
        return {
            "compression_level": 6,
            "max_table_rows_in_zip": 100000,
            "max_single_file_mb": 100
        }
    else:
        return {
            "compression_level": 3,
            "max_table_rows_in_zip": 50000,
            "max_single_file_mb": 50
        }

# Apply adaptive configuration
estimated_size = estimate_data_size(source_paths)
adaptive_config = get_adaptive_config(estimated_size)
compression_level = adaptive_config["compression_level"]
max_table_rows_in_zip = adaptive_config["max_table_rows_in_zip"]
```

## 📊 Monitoring & Alerting

### Custom Metrics Collection

```python
# Collect detailed performance metrics
class BackupMetrics:
    def __init__(self):
        self.start_time = datetime.datetime.now()
        self.metrics = {
            "memory_usage": [],
            "processing_times": {},
            "error_counts": {"tables": 0, "files": 0},
            "throughput": {"tables_per_minute": 0, "mb_per_minute": 0}
        }
    
    def record_table_processing(self, table_name, processing_time, row_count):
        self.metrics["processing_times"][table_name] = {
            "time_seconds": processing_time,
            "rows": row_count,
            "rows_per_second": row_count / processing_time if processing_time > 0 else 0
        }
    
    def get_summary(self):
        duration = (datetime.datetime.now() - self.start_time).total_seconds()
        return {
            "total_duration_seconds": duration,
            "average_memory_usage": sum(self.metrics["memory_usage"]) / len(self.metrics["memory_usage"]) if self.metrics["memory_usage"] else 0,
            "error_rate": (self.metrics["error_counts"]["tables"] + self.metrics["error_counts"]["files"]) / max(1, len(self.metrics["processing_times"])),
            "throughput": self.metrics["throughput"]
        }

# Usage
metrics = BackupMetrics()
# ... during backup operations ...
metrics.record_table_processing("customers", 45.2, 100000)
```

### Health Checks and Alerts

```python
def perform_backup_health_check():
    """Perform comprehensive health check"""
    health_status = {
        "timestamp": datetime.datetime.now().isoformat(),
        "checks": {}
    }
    
    # Check 1: Storage accessibility
    try:
        test_df = spark.createDataFrame([("test",)], ["value"])
        test_df.write.mode("overwrite").format("delta").save(f"{backup_path}/_health_check")
        health_status["checks"]["storage_access"] = "PASS"
    except Exception as e:
        health_status["checks"]["storage_access"] = f"FAIL: {str(e)}"
    
    # Check 2: Memory availability
    import psutil
    memory = psutil.virtual_memory()
    if memory.percent < 80:
        health_status["checks"]["memory"] = "PASS"
    else:
        health_status["checks"]["memory"] = f"WARNING: {memory.percent}% used"
    
    # Check 3: Recent backup success rate
    # (Query backup history and calculate success rate)
    
    return health_status

def send_alert_if_needed(health_status):
    """Send alert if health check fails"""
    failed_checks = [check for check, status in health_status["checks"].items() if "FAIL" in status]
    
    if failed_checks:
        alert_payload = {
            "alert_type": "backup_health_failure",
            "failed_checks": failed_checks,
            "health_status": health_status
        }
        send_backup_notification(alert_payload, alert_webhook_url)
```

### Backup History and Trends

```python
def analyze_backup_trends():
    """Analyze backup performance trends"""
    try:
        # Read backup history
        history_df = spark.read.format("delta").load("backup_metrics_table")
        
        # Calculate trends
        trends = {
            "average_duration_minutes": history_df.agg({"duration_minutes": "avg"}).collect()[0][0],
            "average_size_mb": history_df.agg({"backup_size_mb": "avg"}).collect()[0][0],
            "success_rate": history_df.filter("success = true").count() / history_df.count(),
            "compression_efficiency": history_df.agg({"compression_ratio": "avg"}).collect()[0][0]
        }
        
        return trends
        
    except Exception as e:
        log_message(f"Failed to analyze trends: {e}", "WARNING")
        return None
```

---

This advanced usage guide provides enterprise-ready patterns for deploying and managing Fabric Lakehouse backup solutions at scale. For specific implementation details, refer to the individual guides and examples.

**Related**: [Backup Guide](backup-guide.md) | [Restore Guide](restore-guide.md) | [Troubleshooting](troubleshooting.md)
