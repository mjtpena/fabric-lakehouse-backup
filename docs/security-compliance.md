# Security and Compliance Guide

## Overview

This guide covers security best practices, compliance considerations, and audit capabilities for the Fabric Lakehouse backup and restore tools.

## 🔐 Security Features

### Authentication & Authorization

#### Managed Identity (Recommended)
```python
# Enable managed identity authentication
use_managed_identity = True

# Managed identity automatically handles:
# - Azure Storage authentication
# - Azure Data Lake authentication  
# - No credential management required
```

#### Service Principal Authentication
```python
# For environments without managed identity
spark.conf.set("fs.azure.account.auth.type.mystorageaccount.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.mystorageaccount.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.mystorageaccount.dfs.core.windows.net", "your-client-id")
spark.conf.set("fs.azure.account.oauth2.client.secret.mystorageaccount.dfs.core.windows.net", "your-client-secret")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.mystorageaccount.dfs.core.windows.net", 
               "https://login.microsoftonline.com/your-tenant-id/oauth2/token")
```

### Data Protection

#### Data at Rest
- **Azure Storage Encryption**: Automatic encryption using Microsoft-managed keys
- **Customer-Managed Keys**: Support for bring-your-own-key (BYOK) scenarios
- **Backup Integrity**: SHA-256 checksums for data verification

#### Data in Transit
- **HTTPS/TLS**: All data transfers encrypted in transit
- **OneLake Security**: Native Fabric security controls
- **VNet Integration**: Support for private endpoints

#### Data Classification
```python
# Example: Add data classification to backup metadata
backup_metadata = {
    "data_classification": {
        "sensitivity": "confidential",  # public, internal, confidential, restricted
        "data_types": ["pii", "financial"],
        "retention_policy": "7_years",
        "geographical_restrictions": ["eu_only"]
    }
}
```

## 📋 Compliance Framework

### Supported Standards

#### GDPR (General Data Protection Regulation)
- **Right to be Forgotten**: Tools to identify and remove personal data
- **Data Portability**: Export capabilities for data subject requests
- **Consent Management**: Tracking of data processing consent
- **Breach Notification**: Audit trails for incident response

#### SOX (Sarbanes-Oxley)
- **Data Integrity**: Checksums and verification processes
- **Access Controls**: Role-based access with audit trails
- **Change Management**: Version control and approval workflows
- **Retention Policies**: Automated retention and deletion

#### HIPAA (Health Insurance Portability and Accountability Act)
- **PHI Protection**: Encryption and access controls for health data
- **Audit Logging**: Comprehensive access and modification logs
- **Business Associate Agreements**: Support for BAA compliance
- **Minimum Necessary**: Tools to limit data exposure

#### PCI DSS (Payment Card Industry Data Security Standard)
- **Cardholder Data Protection**: Encryption and tokenization support
- **Access Monitoring**: Real-time access monitoring and alerting
- **Network Security**: Secure data transmission protocols
- **Regular Testing**: Automated security validation

### Compliance Configuration

#### Enable Audit Logging
```python
# Import security utilities
from scripts.security_utils import SecurityManager

# Initialize with audit path
security_manager = SecurityManager(audit_path="/secure/audit/logs")

# Log all backup operations
security_manager.log_audit_event(
    user_id=get_current_user(),
    operation="lakehouse_backup",
    resource_type="lakehouse", 
    resource_name=source_lakehouse_name,
    action="data_export",
    status="success",
    details={
        "data_classification": "confidential",
        "record_count": total_records,
        "justification": "scheduled_backup"
    }
)
```

#### Data Lineage Tracking
```python
# Generate data lineage for compliance
lineage = security_manager.generate_data_lineage(backup_manifest)

# Include upstream data sources
lineage["upstream_sources"] = [
    {
        "system": "source_system_name",
        "last_update": "2024-01-15T10:30:00Z",
        "data_owner": "data.owner@company.com"
    }
]
```

## 🛡️ Access Controls

### Role-Based Access Control (RBAC)

#### Backup Administrator
```python
permissions = {
    "lakehouse_read": True,
    "backup_create": True,
    "backup_delete": True,
    "restore_execute": True,
    "audit_view": True,
    "config_modify": True
}
```

#### Backup Operator  
```python
permissions = {
    "lakehouse_read": True,
    "backup_create": True,
    "backup_delete": False,
    "restore_execute": True,
    "audit_view": False,
    "config_modify": False
}
```

#### Read-Only Auditor
```python
permissions = {
    "lakehouse_read": False,
    "backup_create": False,
    "backup_delete": False,
    "restore_execute": False,
    "audit_view": True,
    "config_modify": False
}
```

### Implementation Example
```python
def check_permissions(user_id: str, operation: str) -> bool:
    """Check if user has permission for operation"""
    
    # Get user role from Azure AD/Fabric
    user_role = get_user_role(user_id)
    
    # Define role permissions
    role_permissions = {
        "backup_admin": ["read", "backup", "restore", "delete", "audit"],
        "backup_operator": ["read", "backup", "restore"],
        "auditor": ["audit"],
        "read_only": ["read"]
    }
    
    allowed_operations = role_permissions.get(user_role, [])
    return operation in allowed_operations
```

## 📊 Audit and Monitoring

### Audit Trail Requirements

#### What to Log
- **Access Events**: Who accessed what data and when
- **Data Operations**: Backup, restore, delete operations
- **Configuration Changes**: Parameter modifications
- **Security Events**: Failed authentication, permission denials
- **Data Lineage**: Source systems and transformations

#### Audit Log Format
```json
{
    "timestamp": "2024-01-15T10:30:00.123Z",
    "user_id": "user@company.com",
    "session_id": "abc123",
    "operation": "lakehouse_backup",
    "resource_type": "lakehouse",
    "resource_name": "sales_data_lakehouse",
    "action": "data_export",
    "status": "success",
    "ip_address": "10.0.1.100",
    "user_agent": "Fabric-Notebook/2.0",
    "details": {
        "record_count": 1000000,
        "data_classification": "confidential",
        "backup_location": "encrypted_backup_storage",
        "checksum": "sha256:abc123...",
        "retention_days": 2555
    }
}
```

### Compliance Reporting

#### Generate Compliance Reports
```python
# Monthly compliance report
report = security_manager.generate_compliance_report(
    start_date="2024-01-01",
    end_date="2024-01-31"
)

# Key metrics included:
# - Total data access events
# - Failed access attempts  
# - Data export operations
# - Retention policy compliance
# - Geographic data movement
```

#### Automated Alerting
```python
# Configure alerts for security events
alerts = {
    "failed_authentication": {
        "threshold": 5,
        "time_window": "15_minutes",
        "notification": "security_team@company.com"
    },
    "large_data_export": {
        "threshold": "1GB",
        "notification": "data_governance@company.com"
    },
    "after_hours_access": {
        "time_range": "18:00-06:00",
        "notification": "soc@company.com"
    }
}
```

## 🔒 Data Privacy

### Personal Data Handling

#### Data Discovery
```python
# Identify personal data in backups
pii_scanner = {
    "patterns": [
        r"\b\d{3}-\d{2}-\d{4}\b",  # SSN
        r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",  # Email
        r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b"  # Credit Card
    ],
    "columns": ["ssn", "email", "phone", "address", "credit_card"]
}

# Scan backup for PII
pii_found = scan_backup_for_pii(backup_data, pii_scanner)
```

#### Data Anonymization
```python
# Anonymize sensitive data before backup
def anonymize_data(df, sensitive_columns):
    """Anonymize sensitive columns in DataFrame"""
    for column in sensitive_columns:
        if column in df.columns:
            # Hash sensitive values
            df = df.withColumn(column, sha2(col(column), 256))
    return df

# Apply before backup
anonymized_df = anonymize_data(original_df, ["ssn", "email", "phone"])
```

#### Right to be Forgotten
```python
# Remove specific user data from backups
def remove_user_data(backup_path: str, user_id: str):
    """Remove all data for specific user"""
    
    # Read backup
    backup_df = spark.read.format("delta").load(backup_path)
    
    # Filter out user data
    filtered_df = backup_df.filter(col("user_id") != user_id)
    
    # Overwrite backup
    filtered_df.write.mode("overwrite").format("delta").save(backup_path)
    
    # Log deletion
    security_manager.log_audit_event(
        user_id="system",
        operation="data_deletion",
        resource_type="backup",
        resource_name=backup_path,
        action="gdpr_deletion",
        status="success",
        details={"deleted_user_id": user_id}
    )
```

## 🚨 Incident Response

### Security Incident Procedures

#### Data Breach Response
1. **Immediate Actions**
   - Isolate affected systems
   - Preserve audit logs
   - Notify security team
   - Document timeline

2. **Investigation**
   - Analyze audit trails
   - Identify scope of breach
   - Determine root cause
   - Collect evidence

3. **Containment**
   - Revoke compromised credentials
   - Update access controls
   - Apply security patches
   - Monitor for further activity

4. **Recovery**
   - Restore from clean backups
   - Validate data integrity
   - Resume normal operations
   - Update procedures

#### Example Incident Response Script
```python
def incident_response(incident_type: str, affected_resources: list):
    """Automated incident response actions"""
    
    # Log incident
    security_manager.log_audit_event(
        user_id="system",
        operation="incident_response",
        resource_type="security",
        resource_name="fabric_backup_system",
        action=incident_type,
        status="initiated",
        details={"affected_resources": affected_resources}
    )
    
    if incident_type == "data_breach":
        # Immediate containment
        for resource in affected_resources:
            disable_resource_access(resource)
        
        # Preserve evidence
        create_forensic_copy(affected_resources)
        
        # Notify stakeholders
        send_breach_notification()
    
    elif incident_type == "unauthorized_access":
        # Reset credentials
        force_credential_reset()
        
        # Enhance monitoring
        enable_enhanced_logging()
```

## 📚 Additional Resources

### Documentation
- [Azure Security Best Practices](https://docs.microsoft.com/azure/security/)
- [Microsoft Fabric Security](https://docs.microsoft.com/fabric/security/)
- [GDPR Compliance Guide](https://docs.microsoft.com/compliance/regulatory/gdpr)

### Tools and Extensions
- **Azure Sentinel**: SIEM/SOAR for security monitoring
- **Microsoft Purview**: Data governance and compliance
- **Azure Policy**: Compliance automation and enforcement
- **Key Vault**: Secure key and secret management

### Training and Certification
- **Microsoft Security, Compliance, and Identity Fundamentals (SC-900)**
- **Microsoft Azure Security Engineer (AZ-500)**
- **Microsoft Information Protection Administrator (SC-400)**
- **GDPR and Data Protection Certification**
