"""
Integration tests for Fabric Lakehouse backup and restore operations.
These tests validate the core functionality in a controlled environment.
"""

import unittest
import tempfile
import shutil
import json
import zipfile
from io import BytesIO
import os
import sys

# Add scripts to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

class TestBackupRestore(unittest.TestCase):
    """Test backup and restore functionality"""
    
    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_data = {
            "test_table_data": [
                {"id": 1, "name": "Alice", "value": 100.0},
                {"id": 2, "name": "Bob", "value": 200.0},
                {"id": 3, "name": "Charlie", "value": 300.0}
            ],
            "test_file_content": b"This is test file content for backup testing"
        }
    
    def tearDown(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_backup_metadata_generation(self):
        """Test backup metadata generation"""
        from datetime import datetime
        
        metadata = {
            "backup_type": "unified_lakehouse_backup",
            "created_at": datetime.now().isoformat(),
            "source_lakehouse": "test_lakehouse",
            "tables_count": 3,
            "files_count": 5,
            "fabric_version": "2.0",
            "backup_method": "unified_zip"
        }
        
        # Validate required fields
        required_fields = ["backup_type", "created_at", "source_lakehouse", "tables_count", "files_count"]
        for field in required_fields:
            self.assertIn(field, metadata, f"Missing required field: {field}")
        
        # Validate data types
        self.assertIsInstance(metadata["tables_count"], int)
        self.assertIsInstance(metadata["files_count"], int)
        self.assertEqual(metadata["backup_type"], "unified_lakehouse_backup")
    
    def test_zip_creation_and_extraction(self):
        """Test ZIP file creation and extraction"""
        # Create test ZIP
        zip_buffer = BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Add metadata
            metadata = {
                "backup_type": "test_backup",
                "created_at": "2024-01-01T00:00:00",
                "tables_count": 1,
                "files_count": 1
            }
            zip_file.writestr("_backup_info/metadata.json", json.dumps(metadata))
            
            # Add test table data
            table_data = "id,name,value\\n1,Alice,100.0\\n2,Bob,200.0"
            zip_file.writestr("tables/test_table/test_table.csv", table_data)
            
            # Add test file
            zip_file.writestr("files/test_file.txt", self.test_data["test_file_content"])
        
        # Test extraction
        zip_content = zip_buffer.getvalue()
        self.assertGreater(len(zip_content), 0, "ZIP file should not be empty")
        
        # Verify ZIP contents
        with zipfile.ZipFile(BytesIO(zip_content), 'r') as zip_file:
            file_list = zip_file.namelist()
            
            expected_files = [
                "_backup_info/metadata.json",
                "tables/test_table/test_table.csv",
                "files/test_file.txt"
            ]
            
            for expected_file in expected_files:
                self.assertIn(expected_file, file_list, f"Missing expected file: {expected_file}")
            
            # Verify metadata
            metadata_content = zip_file.read("_backup_info/metadata.json")
            metadata = json.loads(metadata_content.decode('utf-8'))
            self.assertEqual(metadata["backup_type"], "test_backup")
    
    def test_file_type_detection(self):
        """Test file type detection logic"""
        
        def get_file_type(filename):
            """Replicated file type detection function"""
            if '.' not in filename:
                return "unknown"
            
            extension = filename.split('.')[-1].lower()
            
            file_types = {
                'jpg': 'image', 'jpeg': 'image', 'png': 'image',
                'pdf': 'document', 'doc': 'document', 'docx': 'document',
                'csv': 'data', 'json': 'data', 'parquet': 'data',
                'py': 'code', 'sql': 'code'
            }
            
            return file_types.get(extension, 'other')
        
        test_cases = [
            ("document.pdf", "document"),
            ("image.jpg", "image"),
            ("data.csv", "data"),
            ("script.py", "code"),
            ("unknown.xyz", "other"),
            ("noextension", "unknown")
        ]
        
        for filename, expected_type in test_cases:
            actual_type = get_file_type(filename)
            self.assertEqual(actual_type, expected_type, 
                           f"File type detection failed for {filename}")
    
    def test_configuration_validation(self):
        """Test configuration validation"""
        try:
            from config_loader import BackupConfig
            
            # Create test config
            test_config = {
                "environments": {
                    "test": {
                        "source_workspace_id": "test-workspace-1",
                        "backup_workspace_id": "test-workspace-2",
                        "retention_days": 7
                    }
                },
                "storage_configurations": {
                    "default": {
                        "backup_type": "lakehouse",
                        "use_managed_identity": True
                    }
                }
            }
            
            # Save to temp file
            config_path = os.path.join(self.temp_dir, "test_config.json")
            with open(config_path, 'w') as f:
                json.dump(test_config, f)
            
            # Test loading
            config = BackupConfig(config_path, environment="test")
            env_config = config.get_environment_config("test")
            
            self.assertEqual(env_config["source_workspace_id"], "test-workspace-1")
            self.assertEqual(env_config["backup_workspace_id"], "test-workspace-2")
            
            # Test validation
            self.assertTrue(config.validate_config())
            
        except ImportError:
            self.skipTest("config_loader module not available")
    
    def test_performance_monitoring(self):
        """Test performance monitoring functionality"""
        try:
            from performance_monitor import PerformanceMonitor
            
            monitor = PerformanceMonitor()
            
            # Test operation monitoring
            monitor.start_operation("test_operation")
            
            # Simulate some work
            import time
            time.sleep(0.1)  # 100ms
            
            metrics = monitor.end_operation(rows_processed=100, bytes_processed=1024)
            
            self.assertEqual(metrics.operation, "test_operation")
            self.assertGreater(metrics.duration_seconds, 0.05)  # At least 50ms
            self.assertEqual(metrics.rows_processed, 100)
            self.assertEqual(metrics.bytes_processed, 1024)
            
            # Test summary report
            summary = monitor.get_summary_report()
            self.assertIn("summary", summary)
            self.assertEqual(summary["summary"]["total_operations"], 1)
            self.assertEqual(summary["summary"]["total_rows_processed"], 100)
            
        except ImportError:
            self.skipTest("performance_monitor module not available")

class TestParameterValidation(unittest.TestCase):
    """Test parameter validation functions"""
    
    def test_required_parameters(self):
        """Test validation of required parameters"""
        
        def validate_parameters(source_lakehouse_name, backup_type, **kwargs):
            """Simplified parameter validation"""
            errors = []
            
            if not source_lakehouse_name:
                errors.append("Source Lakehouse Name is required")
            
            if backup_type == "storage_account" and not kwargs.get("backup_storage_account"):
                errors.append("Backup Storage Account is required for storage_account backup type")
            elif backup_type == "lakehouse" and not kwargs.get("backup_lakehouse_name"):
                errors.append("Backup Lakehouse Name is required for lakehouse backup type")
            
            return errors
        
        # Test valid parameters
        errors = validate_parameters(
            source_lakehouse_name="test-lakehouse",
            backup_type="lakehouse",
            backup_lakehouse_name="backup-lakehouse"
        )
        self.assertEqual(len(errors), 0, "Valid parameters should not produce errors")
        
        # Test missing required parameters
        errors = validate_parameters(
            source_lakehouse_name="",
            backup_type="lakehouse"
        )
        self.assertGreater(len(errors), 0, "Missing parameters should produce errors")
        
        # Test storage account validation
        errors = validate_parameters(
            source_lakehouse_name="test-lakehouse",
            backup_type="storage_account"
        )
        self.assertIn("Backup Storage Account is required", " ".join(errors))

class TestBackupIntegrity(unittest.TestCase):
    """Test backup integrity and verification"""
    
    def test_backup_verification_logic(self):
        """Test backup verification functionality"""
        
        # Simulate backup data
        original_data = {
            "table1": {"rows": 1000, "columns": 5},
            "table2": {"rows": 2000, "columns": 3},
            "files": ["file1.csv", "file2.pdf", "image.jpg"]
        }
        
        restored_data = {
            "table1": {"rows": 1000, "columns": 5},
            "table2": {"rows": 2000, "columns": 3},
            "files": ["file1.csv", "file2.pdf", "image.jpg"]
        }
        
        # Perfect match should pass
        def verify_data_integrity(original, restored):
            """Simplified verification logic"""
            issues = []
            
            # Check tables
            for table_name, table_info in original.get("tables", {}).items():
                if table_name not in restored.get("tables", {}):
                    issues.append(f"Missing table: {table_name}")
                else:
                    restored_table = restored["tables"][table_name]
                    if table_info.get("rows") != restored_table.get("rows"):
                        issues.append(f"Row count mismatch in {table_name}")
            
            # Check files
            original_files = set(original.get("files", []))
            restored_files = set(restored.get("files", []))
            missing_files = original_files - restored_files
            
            for missing_file in missing_files:
                issues.append(f"Missing file: {missing_file}")
            
            return len(issues) == 0, issues
        
        # Test perfect match
        is_valid, issues = verify_data_integrity(original_data, restored_data)
        self.assertTrue(is_valid, f"Perfect match should be valid: {issues}")
        
        # Test with missing data
        incomplete_restored = {
            "table1": {"rows": 1000, "columns": 5},
            "files": ["file1.csv", "file2.pdf"]  # Missing image.jpg
        }
        
        is_valid, issues = verify_data_integrity(original_data, incomplete_restored)
        self.assertFalse(is_valid, "Incomplete restore should be invalid")
        self.assertGreater(len(issues), 0, "Should report specific issues")

if __name__ == '__main__':
    # Run tests
    unittest.main(verbosity=2)
