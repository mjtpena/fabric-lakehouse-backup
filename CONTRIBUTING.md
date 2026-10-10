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

3. Make your changes, run the local checks (see [Testing](#testing)) and test them in your Fabric environment

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
- Handle exceptions gracefully with informative error messages, and use `except Exception:` rather than a bare `except:`
- Keep notebooks runnable on Fabric Runtime 1.3 (Python 3.11) and Runtime 2.0 (Python 3.13)
- Use `notebookutils`, not `mssparkutils`
- Write Delta tables from Python data with an explicit schema; don't rely on Spark inferring a schema from dicts (Spark 4 merges map value types and rejects mixed types)
- Put logic that doesn't need Spark or `notebookutils` (path building, discovery decisions, restore planning) in the notebook's `# Pure helpers: no Spark or notebookutils calls.` cell, and pass I/O in as functions, so it can be unit tested
- Don't change the Fabric cell markers (`# CELL ********************`, `# PARAMETERS CELL ********************`, `# METADATA ********************`); the parameters cell must stay the first code cell, tagged `"parameters"`

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

### Local checks (run these before every PR)

The [Validate Notebooks](.github/workflows/validate-notebooks.yml) workflow runs the same checks on every pull request.

```bash
# Syntax on the Fabric runtime Python versions (3.11 for Runtime 1.3, 3.13 for Runtime 2.0)
for nb in nb_lakehouse_*.Notebook; do python3.13 -m py_compile "$nb/notebook-content.py"; done

# Lint: unused imports, undefined names, bare except (settings in ruff.toml)
pip install ruff==0.16.8
ruff check .

# Notebook structure and pure-helper unit tests (standard library only)
python -m unittest discover -s tests -v
```

### End-to-end tests on local Spark

`tests/test_spark_matrix.py` runs all three notebooks, cell by cell, on open-source Spark and Delta Lake. A local folder stands in for OneLake and `tests/spark_harness.py` stands in for `notebookutils`. The tests cover backup and restore across non-schema and schema-enabled sources and targets, backups in the original (v1) layout, `target_schema`, overwrite behaviour, partial backups and the cleanup manifest guard. They are skipped when PySpark isn't installed. The workflow runs them on two lanes that match the Fabric runtimes:

```bash
# Like Fabric Runtime 2.0 (needs Java 17 or 21)
python3.13 -m venv .venv && . .venv/bin/activate
pip install "pyspark==4.1.*" "delta-spark==4.2.*"
cd tests && REQUIRE_SPARK_TESTS=1 python -m unittest -v test_spark_matrix

# Like Fabric Runtime 1.3: Python 3.11 with "pyspark==3.5.*" "delta-spark==3.2.*"
```

The first run downloads the Delta Lake jars from Maven Central.

### Testing in a Fabric workspace

The local tests don't exercise OneLake, Fabric's `notebookutils`, the lakehouse catalog or service principal permissions. Before releasing a change to the notebooks, run it in a Fabric workspace on the runtimes you support (1.3 and 2.0):

1. **Backup** a non-schema lakehouse and a schema-enabled lakehouse (with tables in `dbo` and one other schema, a partitioned table, and some files). Check `_manifest` (`layout_version = 2`, `schema_enabled`, `status = success`) and the `tables/` layout.
2. **Restore** each combination below into a new lakehouse and check that the tables appear in the Lakehouse explorer under the expected schema, with the expected row counts:

   | Backup | Non-schema target | Schema-enabled target |
   |---|---|---|
   | v1 backup of a non-schema source (made with the original notebook) | `Tables/<table>` | `Tables/dbo/<table>`; also try `target_schema` |
   | v2 backup of a non-schema source | `Tables/<table>` | `Tables/dbo/<table>` |
   | v2 backup of a schema-enabled source | Fails before writing if it has several schemas; flattens a single schema | `Tables/<schema>/<table>`; also try `target_schema` |

   (The original notebook couldn't back up schema-enabled lakehouses, so there is no v1 schema-enabled case.)
3. **Restore into a new schema** with `target_schema` and confirm whether Fabric lists the new schema without running `CREATE SCHEMA`.
4. **Run the pipelines** from Azure DevOps with the service principal, including a restore with the Target Schema parameter, and confirm that a deliberately failing run (for example a wrong lakehouse name) fails the pipeline.
5. **Cleanup** with `dry_run = true`, then `false`, on a backup lakehouse that holds an expired backup without `_manifest`; it must be reported as "unknown, skipped" and kept.

## Questions?

Feel free to open an issue with your question or reach out through GitHub Discussions.

Thank you for contributing! 🙏
