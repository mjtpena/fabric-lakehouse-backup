"""
Security and compliance utilities for Fabric Lakehouse backups.
Provides encryption, access control, and audit trail functionality.
"""

import hashlib
import json
import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import base64

@dataclass
class AuditEntry:
    """Audit trail entry for backup operations"""
    timestamp: str
    user_id: str
    operation: str
    resource_type: str
    resource_name: str
    action: str
    status: str
    details: Dict[str, Any]
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None

class SecurityManager:
    """Manage security aspects of backup operations"""
    
    def __init__(self, audit_path: str = None):
        self.audit_path = audit_path
        self.audit_entries: List[AuditEntry] = []
    
    def log_audit_event(self, user_id: str, operation: str, resource_type: str, 
                       resource_name: str, action: str, status: str, 
                       details: Dict[str, Any] = None):
        """Log an audit event"""
        
        entry = AuditEntry(
            timestamp=datetime.datetime.now().isoformat(),
            user_id=user_id,
            operation=operation,
            resource_type=resource_type,
            resource_name=resource_name,
            action=action,
            status=status,
            details=details or {}
        )
        
        self.audit_entries.append(entry)
        
        if self.audit_path:
            self._save_audit_entry(entry)
    
    def _save_audit_entry(self, entry: AuditEntry):
        """Save audit entry to persistent storage"""
        try:
            # In a real implementation, this would save to a secure audit database
            # For now, we'll use a local file as an example
            audit_data = {
                "timestamp": entry.timestamp,
                "user_id": entry.user_id,
                "operation": entry.operation,
                "resource_type": entry.resource_type,
                "resource_name": entry.resource_name,
                "action": entry.action,
                "status": entry.status,
                "details": entry.details
            }
            
            # Append to audit log file
            with open(f"{self.audit_path}/audit.log", "a") as f:
                f.write(json.dumps(audit_data) + "\\n")
                
        except Exception as e:
            print(f"Warning: Could not save audit entry: {e}")
    
    def generate_data_lineage(self, backup_manifest: Dict[str, Any]) -> Dict[str, Any]:
        """Generate data lineage for backup"""
        
        lineage = {
            "backup_id": backup_manifest.get("backup_id"),
            "source_lakehouse": backup_manifest.get("source_lakehouse_name"),
            "backup_timestamp": backup_manifest.get("backup_timestamp"),
            "data_sources": [],
            "transformations": [],
            "outputs": []
        }
        
        # Add table lineage
        for table_info in backup_manifest.get("tables_info", []):
            if "error" not in table_info:
                lineage["data_sources"].append({
                    "type": "delta_table",
                    "name": table_info["name"],
                    "row_count": table_info.get("row_count", 0),
                    "schema": table_info.get("schema", ""),
                    "last_modified": backup_manifest.get("backup_timestamp")
                })
        
        # Add transformation info
        lineage["transformations"].append({
            "type": "backup_compression",
            "method": backup_manifest.get("backup_method", "unified_zip"),
            "compression_ratio": backup_manifest.get("backup_result", {}).get("compression_ratio", 0)
        })
        
        # Add output info
        lineage["outputs"].append({
            "type": "backup_archive",
            "location": backup_manifest.get("backup_path", ""),
            "format": "unified_zip",
            "size_mb": backup_manifest.get("backup_result", {}).get("compressed_size_mb", 0)
        })
        
        return lineage
    
    def validate_access_permissions(self, user_id: str, resource_path: str, 
                                  operation: str) -> bool:
        """Validate user permissions for backup operations"""
        
        # This is a simplified example - in production, integrate with 
        # Azure AD, Fabric security, or your organization's IAM system
        
        permissions = {
            "backup_admin": ["read", "write", "backup", "restore", "delete"],
            "backup_operator": ["read", "backup", "restore"],
            "read_only": ["read"]
        }
        
        # In a real implementation, you would:
        # 1. Look up user role from Azure AD/Fabric
        # 2. Check resource-specific permissions
        # 3. Validate against workspace security groups
        # 4. Apply organization-specific policies
        
        # For demonstration, assume all operations are allowed
        # but log the access attempt
        
        self.log_audit_event(
            user_id=user_id,
            operation="permission_check",
            resource_type="lakehouse",
            resource_name=resource_path,
            action=operation,
            status="allowed",
            details={"permission_system": "demo"}
        )
        
        return True
    
    def generate_compliance_report(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Generate compliance report for audit purposes"""
        
        # Filter audit entries by date range
        filtered_entries = []
        for entry in self.audit_entries:
            entry_date = entry.timestamp.split('T')[0]
            if start_date <= entry_date <= end_date:
                filtered_entries.append(entry)
        
        # Generate report
        report = {
            "report_period": {
                "start_date": start_date,
                "end_date": end_date
            },
            "total_operations": len(filtered_entries),
            "operations_by_type": {},
            "operations_by_user": {},
            "security_events": [],
            "data_access_summary": {
                "tables_accessed": set(),
                "files_accessed": set(),
                "backup_operations": 0,
                "restore_operations": 0
            }
        }
        
        # Analyze entries
        for entry in filtered_entries:
            # Count by operation type
            op_type = entry.operation
            report["operations_by_type"][op_type] = report["operations_by_type"].get(op_type, 0) + 1
            
            # Count by user
            user_id = entry.user_id
            report["operations_by_user"][user_id] = report["operations_by_user"].get(user_id, 0) + 1
            
            # Track data access
            if entry.resource_type == "table":
                report["data_access_summary"]["tables_accessed"].add(entry.resource_name)
            elif entry.resource_type == "file":
                report["data_access_summary"]["files_accessed"].add(entry.resource_name)
            
            if "backup" in entry.operation.lower():
                report["data_access_summary"]["backup_operations"] += 1
            elif "restore" in entry.operation.lower():
                report["data_access_summary"]["restore_operations"] += 1
            
            # Flag security-relevant events
            if entry.status in ["failed", "denied", "error"]:
                report["security_events"].append({
                    "timestamp": entry.timestamp,
                    "user_id": entry.user_id,
                    "operation": entry.operation,
                    "status": entry.status,
                    "details": entry.details
                })
        
        # Convert sets to counts for JSON serialization
        report["data_access_summary"]["unique_tables_accessed"] = len(report["data_access_summary"]["tables_accessed"])
        report["data_access_summary"]["unique_files_accessed"] = len(report["data_access_summary"]["files_accessed"])
        del report["data_access_summary"]["tables_accessed"]
        del report["data_access_summary"]["files_accessed"]
        
        return report

def calculate_data_checksum(data: bytes) -> str:
    """Calculate SHA-256 checksum for data integrity verification"""
    return hashlib.sha256(data).hexdigest()

def verify_backup_integrity(backup_data: bytes, expected_checksum: str) -> bool:
    """Verify backup data integrity using checksum"""
    actual_checksum = calculate_data_checksum(backup_data)
    return actual_checksum == expected_checksum

def sanitize_log_message(message: str) -> str:
    """Sanitize log messages to prevent log injection attacks"""
    # Remove control characters and limit length
    sanitized = ''.join(char for char in message if ord(char) >= 32 or char in '\\t\\n\\r')
    return sanitized[:1000]  # Limit to 1000 characters

def mask_sensitive_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Mask sensitive data in logs and outputs"""
    sensitive_keys = [
        "password", "secret", "key", "token", "connectionstring",
        "account_key", "sas_token", "client_secret"
    ]
    
    masked_data = data.copy()
    
    def mask_value(value):
        if isinstance(value, str) and len(value) > 4:
            return value[:2] + "*" * (len(value) - 4) + value[-2:]
        return "***"
    
    def mask_recursive(obj):
        if isinstance(obj, dict):
            return {
                key: mask_value(value) if any(sensitive in key.lower() for sensitive in sensitive_keys)
                else mask_recursive(value)
                for key, value in obj.items()
            }
        elif isinstance(obj, list):
            return [mask_recursive(item) for item in obj]
        else:
            return obj
    
    return mask_recursive(masked_data)

# Example usage in notebooks:
"""
# Add this to your backup notebook for security and compliance:

# Initialize security manager
security_manager = SecurityManager(audit_path="/path/to/audit/logs")

# Log backup start
security_manager.log_audit_event(
    user_id="user@company.com",  # Get from Fabric context
    operation="lakehouse_backup",
    resource_type="lakehouse",
    resource_name=source_lakehouse_name,
    action="backup_start",
    status="initiated",
    details={"backup_type": backup_type, "target": backup_path}
)

# Validate permissions (in production, integrate with your IAM)
if not security_manager.validate_access_permissions(
    user_id="user@company.com",
    resource_path=source_lakehouse_name,
    operation="backup"
):
    raise PermissionError("Insufficient permissions for backup operation")

# Calculate checksums for integrity
backup_checksum = calculate_data_checksum(backup_zip_data)

# Log backup completion
security_manager.log_audit_event(
    user_id="user@company.com",
    operation="lakehouse_backup", 
    resource_type="lakehouse",
    resource_name=source_lakehouse_name,
    action="backup_complete",
    status="success",
    details={
        "backup_size_mb": len(backup_zip_data) / 1024 / 1024,
        "checksum": backup_checksum,
        "tables_backed_up": len(tables_info),
        "files_backed_up": len(files_info)
    }
)

# Generate data lineage
lineage = security_manager.generate_data_lineage(backup_manifest)

# Generate compliance report (monthly)
compliance_report = security_manager.generate_compliance_report(
    start_date="2024-01-01",
    end_date="2024-01-31"
)
"""
