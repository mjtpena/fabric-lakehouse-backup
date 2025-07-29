# Release Notes

## Version 2.0.0 (Current)

### 🚀 New Features
- **Unified ZIP Backup**: Single ZIP file containing both tables and files
- **Multiple Table Formats**: Tables stored as Delta/Parquet + CSV + Schema
- **Perfect File Preservation**: All file formats preserved exactly as originals
- **Intelligent Restore**: Auto-detects backup format and chooses optimal restore method
- **Selective Restore**: Restore specific tables or files using patterns
- **Dry Run Mode**: Preview restore operations without making changes
- **Performance Monitoring**: Built-in performance tracking and optimization suggestions
- **Configuration Management**: Environment-specific configurations with validation

### 💾 Storage Support
- OneLake (Fabric Lakehouse)
- Azure Storage Account
- Azure Data Lake Storage Gen2
- Managed Identity authentication

### 🔧 Technical Improvements
- Comprehensive error handling with retry logic
- Memory-efficient processing for large datasets
- Parallel processing capabilities
- Backup verification and integrity checks
- Detailed logging and audit trails

### 📋 Quality & Documentation
- Complete test suite with integration tests
- Comprehensive documentation and examples
- GitHub Actions CI/CD pipeline
- Performance benchmarking tools
- Migration helper scripts

---

## Version 1.0.0 (Legacy)

### Basic Features
- Simple table backup to CSV format
- Basic file copying
- Manual restore process
- Limited error handling

### Limitations
- No unified backup format
- Manual configuration required
- Limited storage options
- No verification capabilities

---

## Upgrade Path

### From Version 1.0.0 to 2.0.0

1. **Backup Format Migration**:
   - Use the migration helper script: `scripts/migration-helper.py`
   - Old backups remain compatible but lack new features
   - Recommend re-backing up with new format for full benefits

2. **Configuration Updates**:
   - Move from inline parameters to `config.json`
   - Update notebook parameter cells
   - Enable new features like performance monitoring

3. **New Capabilities**:
   - Enable unified ZIP backups for space efficiency
   - Use selective restore for faster partial recoveries
   - Implement performance monitoring for optimization

### Breaking Changes
- Parameter structure has changed (backwards compatible with warnings)
- New dependency on `zipfile` and `BytesIO` for unified backups
- Enhanced error handling may surface previously hidden issues

### Migration Script Usage
```python
from scripts.migration_helper import migrate_backup_format

# Migrate old backup to new format
migrate_backup_format(
    old_backup_path="path/to/old/backup",
    new_backup_path="path/to/new/backup",
    include_verification=True
)
```

---

## Roadmap

### Version 2.1.0 (Planned)
- [ ] Incremental backup support
- [ ] Advanced scheduling with cron expressions  
- [ ] Email/Teams notifications
- [ ] Multi-region backup replication
- [ ] Delta table time travel integration

### Version 2.2.0 (Future)
- [ ] Backup encryption at rest
- [ ] Cross-tenant backup support
- [ ] Advanced compression algorithms
- [ ] Backup deduplication
- [ ] REST API for backup operations

### Version 3.0.0 (Vision)
- [ ] Real-time backup streaming
- [ ] AI-powered backup optimization
- [ ] Advanced backup analytics dashboard
- [ ] Multi-cloud backup support
- [ ] Backup marketplace integration

---

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### How to Report Issues
1. Check existing issues in GitHub
2. Provide minimal reproduction case
3. Include notebook parameters and error logs
4. Specify Fabric environment details

### How to Suggest Features
1. Open a GitHub issue with "Feature Request" label
2. Describe the use case and expected behavior
3. Provide mockups or examples if applicable
4. Participate in design discussions

---

## Support

- **Documentation**: See `/docs` folder for detailed guides
- **Examples**: Check `/examples` for common scenarios  
- **Issues**: Report bugs via GitHub Issues
- **Discussions**: Community support via GitHub Discussions
- **Enterprise**: Contact maintainers for enterprise support
