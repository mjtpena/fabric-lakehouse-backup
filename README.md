# Microsoft Fabric Lakehouse Backup & Restore Tools

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Microsoft Fabric](https://img.shields.io/badge/Microsoft-Fabric-blue)](https://www.microsoft.com/en-us/microsoft-fabric)

A comprehensive solution for backing up and restoring Microsoft Fabric Lakehouse data, including both tables (Delta format) and files with perfect format preservation.

## 🚀 Features

### Complete Lakehouse Backup
- **Unified ZIP Backup**: Creates a single ZIP file containing both tables and files
- **Multiple Table Formats**: Tables stored as Delta/Parquet + CSV + Schema for maximum compatibility
- **Perfect File Preservation**: All file formats (images, PDFs, documents) preserved exactly as originals
- **Intelligent Compression**: Configurable compression with space-saving optimization
- **Metadata Rich**: Complete backup metadata and restore instructions included

### Flexible Restore Options
- **Smart Format Detection**: Automatically detects backup format and chooses optimal restore method
- **Format Preference**: Choose between Parquet (optimal) or CSV (human-readable) for table restoration
- **Selective Restore**: Restore specific tables or files using patterns
- **Dry Run Mode**: Preview what would be restored without making changes
- **Verification**: Built-in backup/restore verification

### Storage Flexibility
- **OneLake Integration**: Native support for Fabric Lakehouse storage
- **Azure Storage Account**: Backup to external Azure Storage accounts
- **ADLS Gen2**: Support for Azure Data Lake Storage Gen2
- **Managed Identity**: Secure authentication using Azure Managed Identity

## 📋 Prerequisites

- Microsoft Fabric workspace with Lakehouse
- PySpark environment (Fabric Notebook runtime)
- Required permissions:
  - Read access to source lakehouse
  - Write access to backup location
  - Write access to target lakehouse (for restore)

## 🔧 Quick Start

### 1. Backup Your Lakehouse

Open the `Fabric_Lakehouse_Combined_Backup.ipynb` notebook and configure these parameters:

```python
# Source Configuration
source_lakehouse_name = "your-source-lakehouse"
source_workspace_id = "your-workspace-id"

# Backup Configuration
backup_type = "lakehouse"  # or "storage_account", "adls"
backup_lakehouse_name = "your-backup-lakehouse"
backup_workspace_id = "your-backup-workspace-id"

# Options
backup_tables = True
backup_files = True
backup_method = "unified_zip"
```

Run the notebook to create a complete backup with:
- ✅ All tables in multiple formats (Delta/Parquet/CSV)
- ✅ All files in original formats
- ✅ Complete metadata and restore instructions
- ✅ Compressed ZIP with space optimization

### 2. Restore Your Data

Open the `Fabric_Lakehouse_Restore.ipynb` notebook and configure:

```python
# Backup Source
backup_source_type = "lakehouse"
backup_lakehouse_name = "your-backup-lakehouse"
backup_path = "complete_backup_2024-01-01_12-00-00_abcd1234"

# Restore Target
target_lakehouse_name = "your-target-lakehouse"
target_workspace_id = "your-workspace-id"

# Options
restore_tables = True
restore_files = True
table_format_preference = "parquet"  # or "csv", "auto"
```

## 🏗️ Architecture

### Backup Process
```
Source Lakehouse
├── Tables/ (Delta format)
│   ├── customers
│   ├── products
│   └── orders
└── Files/
    ├── images/
    ├── documents/
    └── data/

        ↓ Backup Process ↓

Unified ZIP Backup
├── _backup_info/
│   ├── metadata.json
│   ├── contents.json
│   └── RESTORE_INSTRUCTIONS.md
├── tables/
│   ├── customers/
│   │   ├── customers.csv      # Human-readable
│   │   ├── customers.parquet  # Optimized
│   │   ├── schema.json        # Data types
│   │   └── metadata.json      # Statistics
│   └── ...
└── files/
    ├── images/photo.jpg       # Original format
    ├── documents/report.pdf   # Original format
    └── data/data.csv          # Original format
```

### Storage Options
- **OneLake**: Store backups in another Fabric Lakehouse
- **Azure Storage**: External Azure Storage Account with containers
- **ADLS Gen2**: Azure Data Lake Storage Gen2 for enterprise scenarios

## 📊 Configuration Options

### Backup Configuration
| Parameter | Description | Default |
|-----------|-------------|---------|
| `backup_tables` | Include tables in backup | `True` |
| `backup_files` | Include files in backup | `True` |
| `backup_method` | Backup method | `"unified_zip"` |
| `max_table_rows_in_zip` | Max rows per table in ZIP | `100000` |
| `max_single_file_mb` | Max file size in ZIP (MB) | `100` |
| `compression_level` | ZIP compression (1-9) | `6` |
| `include_table_csv` | Include CSV format | `True` |
| `include_table_parquet` | Include Parquet format | `True` |

### Restore Configuration
| Parameter | Description | Default |
|-----------|-------------|---------|
| `restore_tables` | Restore tables | `True` |
| `restore_files` | Restore files | `True` |
| `table_format_preference` | Preferred format | `"parquet"` |
| `overwrite_existing` | Overwrite existing data | `False` |
| `verify_restore` | Verify after restore | `True` |
| `dry_run` | Preview without changes | `False` |

## 📁 Project Structure

```
fabric-lakehouse-backup/
├── Fabric_Lakehouse_Combined_Backup.ipynb   # Complete backup notebook
├── Fabric_Lakehouse_Restore.ipynb           # Restore notebook
├── README.md                                 # This file
├── LICENSE                                   # MIT License
├── CONTRIBUTING.md                          # Contribution guidelines
├── docs/                                    # Documentation
│   ├── backup-guide.md                     # Detailed backup guide
│   ├── restore-guide.md                    # Detailed restore guide
│   ├── troubleshooting.md                  # Common issues & solutions
│   └── advanced-usage.md                   # Advanced scenarios
├── examples/                               # Example configurations
│   ├── backup-examples.md                 # Backup examples
│   └── restore-examples.md                # Restore examples
└── scripts/                               # Helper scripts
    ├── validate-backup.py                 # Backup validation
    └── migration-helper.py                # Migration utilities
```

## 🔍 Backup Contents

Each backup includes:

1. **Tables** (multiple formats for maximum compatibility):
   - **CSV**: Human-readable, Excel-compatible
   - **Parquet**: Optimized, type-preserving
   - **Schema**: Column definitions and data types
   - **Metadata**: Row counts, statistics

2. **Files** (original formats perfectly preserved):
   - Images (JPG, PNG, etc.)
   - Documents (PDF, Word, Excel)
   - Data files (CSV, JSON, etc.)
   - Any other file type

3. **Metadata**:
   - Backup timestamp and configuration
   - File inventory and structure
   - Restore instructions
   - Verification checksums

## 🛠️ Advanced Usage

### Selective Backup
```python
# Backup specific tables only
restore_specific_tables = ["customers", "orders"]

# Backup specific file patterns
restore_specific_files = ["*.pdf", "images/*.jpg"]
```

### Custom Storage Authentication
```python
# Use specific credentials
use_managed_identity = False
# Configure custom authentication in notebook
```

### Large Dataset Handling
```python
# Adjust limits for large datasets
max_table_rows_in_zip = 500000
max_single_file_mb = 500
compression_level = 9  # Maximum compression
```

## 🔒 Security & Best Practices

- **Managed Identity**: Use Azure Managed Identity for secure authentication
- **Encryption**: All data encrypted in transit and at rest
- **Access Control**: Implement proper RBAC on storage accounts
- **Retention**: Configure retention policies for backup cleanup
- **Verification**: Always verify backups before deleting source data

## 🆘 Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Ensure Managed Identity has proper permissions
   - Check workspace and storage account access

2. **Large File Handling**
   - Adjust `max_single_file_mb` for large files
   - Consider separate backup for very large files

3. **Memory Issues**
   - Reduce `max_table_rows_in_zip` for large tables
   - Use higher compression levels

See [troubleshooting.md](docs/troubleshooting.md) for detailed solutions.

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Microsoft Fabric team for the amazing platform
- Open source community for inspiration and feedback

## 📞 Support

- 🐛 **Issues**: Report bugs via GitHub Issues
- 💡 **Feature Requests**: Suggest features via GitHub Issues
- 📖 **Documentation**: Check the `docs/` folder
- 💬 **Discussions**: Use GitHub Discussions for questions

---

**Made with ❤️ for the Microsoft Fabric community**
