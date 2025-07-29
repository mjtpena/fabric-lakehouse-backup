"""
Configuration loader for Fabric Lakehouse backup tools.
Provides environment-specific configurations and validation.
"""

import json
import os
from typing import Dict, Any, Optional

class BackupConfig:
    """Configuration manager for backup operations"""
    
    def __init__(self, config_path: str = None, environment: str = "development"):
        self.environment = environment
        self.config_path = config_path or self._find_config_file()
        self.config = self._load_config()
    
    def _find_config_file(self) -> str:
        """Find configuration file in common locations"""
        possible_paths = [
            "config.json",
            "config/config.json", 
            os.path.expanduser("~/.fabric-backup/config.json"),
            "/etc/fabric-backup/config.json"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return path
        
        # Return template path if no config found
        return "config/config.template.json"
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            
            # Validate required sections
            required_sections = ["environments", "storage_configurations"]
            for section in required_sections:
                if section not in config:
                    raise ValueError(f"Missing required configuration section: {section}")
            
            return config
        
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
    
    def get_environment_config(self, environment: str = None) -> Dict[str, Any]:
        """Get configuration for specific environment"""
        env = environment or self.environment
        
        if env not in self.config["environments"]:
            available = list(self.config["environments"].keys())
            raise ValueError(f"Environment '{env}' not found. Available: {available}")
        
        return self.config["environments"][env]
    
    def get_storage_config(self, storage_name: str = "default") -> Dict[str, Any]:
        """Get storage configuration by name"""
        storage_configs = self.config.get("storage_configurations", {})
        
        if storage_name not in storage_configs:
            available = list(storage_configs.keys())
            raise ValueError(f"Storage config '{storage_name}' not found. Available: {available}")
        
        return storage_configs[storage_name]
    
    def get_backup_schedule(self, schedule_name: str) -> Dict[str, Any]:
        """Get backup schedule configuration"""
        schedules = self.config.get("backup_schedules", {})
        
        if schedule_name not in schedules:
            available = list(schedules.keys())
            raise ValueError(f"Schedule '{schedule_name}' not found. Available: {available}")
        
        return schedules[schedule_name]
    
    def get_notification_config(self) -> Dict[str, Any]:
        """Get notification settings"""
        return self.config.get("notification_settings", {})
    
    def get_advanced_options(self) -> Dict[str, Any]:
        """Get advanced configuration options"""
        return self.config.get("advanced_options", {})
    
    def validate_config(self) -> bool:
        """Validate configuration completeness and correctness"""
        errors = []
        
        # Validate environments
        for env_name, env_config in self.config["environments"].items():
            required_keys = ["source_workspace_id", "backup_workspace_id"]
            for key in required_keys:
                if not env_config.get(key):
                    errors.append(f"Environment '{env_name}' missing '{key}'")
        
        # Validate storage configurations
        for storage_name, storage_config in self.config["storage_configurations"].items():
            backup_type = storage_config.get("backup_type")
            if backup_type == "storage_account" and not storage_config.get("backup_storage_account"):
                errors.append(f"Storage '{storage_name}' missing 'backup_storage_account'")
            elif backup_type == "adls" and not storage_config.get("backup_adls_account"):
                errors.append(f"Storage '{storage_name}' missing 'backup_adls_account'")
        
        if errors:
            raise ValueError("Configuration validation failed:\\n" + "\\n".join(errors))
        
        return True

def load_backup_parameters(config: BackupConfig, environment: str = None, storage: str = "default") -> Dict[str, Any]:
    """Load backup parameters from configuration for notebook use"""
    
    env_config = config.get_environment_config(environment)
    storage_config = config.get_storage_config(storage)
    advanced_options = config.get_advanced_options()
    
    # Merge configurations
    parameters = {
        # Environment settings
        "source_workspace_id": env_config["source_workspace_id"],
        "backup_workspace_id": env_config.get("backup_workspace_id"),
        "retention_days": env_config.get("retention_days", 30),
        "compression_level": env_config.get("compression_level", 6),
        "max_table_rows_in_zip": env_config.get("max_table_rows_in_zip", 100000),
        
        # Storage settings
        "backup_type": storage_config["backup_type"],
        "use_managed_identity": storage_config.get("use_managed_identity", True),
        
        # Advanced options
        "parallel_table_processing": advanced_options.get("parallel_table_processing", False),
        "max_parallel_tables": advanced_options.get("max_parallel_tables", 3),
        "enable_incremental_backup": advanced_options.get("enable_incremental_backup", False),
    }
    
    # Add storage-specific parameters
    if storage_config["backup_type"] == "storage_account":
        parameters.update({
            "backup_storage_account": storage_config["backup_storage_account"],
            "backup_container": storage_config.get("backup_container", "lakehouse-backups")
        })
    elif storage_config["backup_type"] == "adls":
        parameters.update({
            "backup_adls_account": storage_config["backup_adls_account"],
            "backup_adls_container": storage_config.get("backup_adls_container", "backups")
        })
    
    return parameters

# Example usage for notebooks:
"""
# Add this cell to your notebook to use configuration-driven parameters:

try:
    from scripts.config_loader import BackupConfig, load_backup_parameters
    
    # Load configuration
    config = BackupConfig(environment="production")  # or "development", "staging"
    config.validate_config()
    
    # Get parameters for this backup
    params = load_backup_parameters(config, environment="production", storage="external_storage")
    
    # Override notebook parameters with config values
    source_workspace_id = params["source_workspace_id"]
    backup_workspace_id = params["backup_workspace_id"]
    backup_type = params["backup_type"]
    retention_days = params["retention_days"]
    compression_level = params["compression_level"]
    
    print("✅ Configuration loaded successfully")
    print(f"📂 Environment: production")
    print(f"💾 Storage: external_storage") 
    print(f"🔧 Backup type: {backup_type}")
    
except Exception as e:
    print(f"⚠️  Could not load configuration: {e}")
    print("Using notebook default parameters...")
"""
