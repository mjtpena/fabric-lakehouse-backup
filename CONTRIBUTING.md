# Contributing to Fabric Lakehouse Backup & Restore

First off, thank you for considering contributing to this project! 🎉

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check the existing issues to avoid duplicates. When you create a bug report, include as many details as possible:

- **Use a clear and descriptive title**
- **Describe the exact steps to reproduce the problem**
- **Provide specific examples** (e.g., lakehouse names, workspace configurations)
- **Describe the behavior you observed and what you expected**
- **Include logs** from Azure DevOps pipeline runs or notebook execution
- **Include your environment details** (Fabric capacity type, region, etc.)

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion:

- **Use a clear and descriptive title**
- **Provide a detailed description** of the suggested enhancement
- **Explain why this enhancement would be useful**
- **List any alternatives you've considered**

### Pull Requests

1. **Fork the repository** and create your branch from `main`
2. **Make your changes** following the coding standards below
3. **Test your changes** with your own Fabric environment
4. **Update documentation** if needed
5. **Submit your pull request**

## Development Setup

### Prerequisites

- Microsoft Fabric workspace with Lakehouse capabilities
- Azure DevOps organization
- Azure subscription for Service Principal authentication

### Local Development

1. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/fabric-lakehouse-backup.git
   cd fabric-lakehouse-backup
   ```

2. Create a branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. Make your changes and test them in your Fabric environment

4. Commit your changes:
   ```bash
   git commit -m "Add: brief description of your changes"
   ```

5. Push to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

6. Open a Pull Request

## Coding Standards

### Python (Notebooks)

- Follow PEP 8 style guidelines
- Use meaningful variable and function names
- Add docstrings to all functions
- Include logging for important operations
- Handle exceptions gracefully with informative error messages

Example:
```python
def backup_table(source_path: str, destination_path: str, table_name: str) -> dict:
    """
    Backup a single Delta table from source to destination.
    
    Args:
        source_path: The ABFS path to the source table
        destination_path: The ABFS path for the backup
        table_name: Name of the table being backed up
    
    Returns:
        dict: Result containing success status and metadata
    
    Raises:
        ValueError: If paths are invalid
    """
    try:
        log_message(f"Starting backup of table: {table_name}", "INFO")
        # ... implementation
    except Exception as e:
        log_message(f"Failed to backup table {table_name}: {str(e)}", "ERROR")
        raise
```

### YAML (Pipelines)

- Use clear, descriptive names for stages, jobs, and steps
- Add comments explaining complex logic
- Use variables for values that might change
- Follow consistent indentation (2 spaces)

Example:
```yaml
# Stage: Backup the specified lakehouse
- stage: LakehouseBackup
  displayName: "Lakehouse Backup"
  jobs:
    - job: backup_lakehouse
      displayName: Backup Lakehouse
      steps:
        # Step 1: Authenticate and find the notebook
        - task: AzurePowerShell@5
          displayName: Acquire Backup Notebook
```

### PowerShell (Pipeline Scripts)

- Use Write-Host with emojis for visual feedback
- Include proper error handling with try/catch
- Log important variables and steps
- Use meaningful variable names

## Commit Messages

Use clear and descriptive commit messages:

- `Add:` for new features
- `Fix:` for bug fixes
- `Update:` for changes to existing functionality
- `Docs:` for documentation changes
- `Refactor:` for code refactoring

Examples:
- `Add: Support for selective table restore`
- `Fix: Token refresh issue during long-running backups`
- `Docs: Update README with new parameters`

## Testing

Before submitting a PR:

1. **Test backup functionality** with your own lakehouse
2. **Test restore functionality** to a new or existing lakehouse
3. **Verify pipeline execution** in Azure DevOps
4. **Check logging** for appropriate messages
5. **Validate error handling** with intentionally invalid inputs

## Questions?

Feel free to open an issue with your question or reach out through GitHub Discussions.

Thank you for contributing! 🙏
