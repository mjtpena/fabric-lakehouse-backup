# Microsoft Fabric Lakehouse Backup & Restore

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A complete backup and restore solution for Microsoft Fabric Lakehouses, featuring automated Azure DevOps pipelines and PySpark notebooks for reliable data protection.

## 🌟 Features

- **Full Lakehouse Backup**: Backup both Delta tables and files from any Fabric Lakehouse
- **Schema-Enabled Lakehouses**: Backs up and restores `Tables/<schema>/<table>` lakehouses (the default for new lakehouses) as well as non-schema `Tables/<table>` lakehouses
- **Cross-Workspace Support**: Backup and restore across different Fabric workspaces
- **Automated Scheduling**: Schedule daily backups via Azure DevOps pipelines
- **Flexible Restore**: Restore complete backups or selective tables/files
- **Automatic Retention**: Cleanup old backups based on configurable retention policy
- **Service Principal Authentication**: Secure, automated execution without user interaction
- **Detailed Logging**: Comprehensive logging and manifest generation for audit trails
- **Verified Copies**: Row counts captured at a fixed Delta version and checked after backup and after restore
- **Ancient Date Handling**: Pre-configured for legacy data with pre-1582 dates (e.g., Oracle EBS)

## 🧭 How this fits with Fabric's built-in protection

Fabric has no native backup, restore point or point-in-time restore for Lakehouses (restore points are a Warehouse feature). Its built-in protection covers deleted *items*, deleted *files* for a short window, and regional outages. This library covers the gap in between: a dropped or overwritten table, a bad load, or deleted files, recovered from a copy you control, kept as long as you choose. Microsoft's own [disaster recovery guidance for Lakehouse](https://learn.microsoft.com/en-us/fabric/security/experience-specific-guidance) recommends the same approach: copy Delta tables and files to another lakehouse with a notebook.

| Protection | Protects against | Retention | Does not cover |
|---|---|---|---|
| [Item Recovery](https://learn.microsoft.com/en-us/fabric/admin/item-recovery) (workspace recycle bin) | A deleted Lakehouse item (or other Fabric item) | On by default with 3 days for tenants that never configured it; admins can set a longer period, up to 90 days | Dropped or overwritten tables and deleted files inside a lakehouse that still exists |
| [OneLake soft delete](https://learn.microsoft.com/en-us/fabric/onelake/soft-delete) | Deleted files | 7 days | Overwritten data; it is file level and not table aware, and recovery is via Azure Storage Explorer or Az.Storage PowerShell, file by file |
| [Delta time travel](https://docs.delta.io/latest/delta-batch.html#data-retention) | Bad writes, updates and deletes in a table that still exists | `delta.logRetentionDuration` (30 days by default), and only as far back as `VACUUM` and `delta.deletedFileRetentionDuration` (7 days by default) allow | Dropped tables, deleted lakehouses, anything older than the retention window or already vacuumed |
| [Warehouse restore points](https://learn.microsoft.com/en-us/fabric/data-warehouse/restore-in-place) | Warehouse data | 30 days by default | Not available for Lakehouses |
| [OneLake / capacity disaster recovery](https://learn.microsoft.com/en-us/fabric/onelake/onelake-disaster-recovery) | Loss of an Azure region (data is replicated asynchronously to the paired region; enabled per capacity) | Current state only | Accidental deletes and overwrites (they replicate too); item definitions such as notebooks and pipelines; data not yet replicated at failover |
| **This library** | Dropped or overwritten tables, deleted files, a deleted lakehouse after its recycle-bin window, bad loads | Your backup schedule and the cleanup job's retention (30 days by default) | Changes between backups; Delta history, table properties and Spark views; a regional outage unless the backup lakehouse is in a different region or on a capacity with disaster recovery enabled |

The Learn pages above were checked in October 2026. Settings such as the Item Recovery retention change over time, so check them for your tenant.

## 📁 Repository Structure

```
fabric-lakehouse-backup/
├── README.md                                    # This file
├── LICENSE                                      # MIT License
├── nb_lakehouse_backup.Notebook/
│   └── notebook-content.py                      # Backup notebook (PySpark)
├── nb_lakehouse_restore.Notebook/
│   └── notebook-content.py                      # Restore notebook (PySpark)
├── nb_lakehouse_cleanup.Notebook/
│   └── notebook-content.py                      # Cleanup/retention notebook (PySpark)
├── pipeline.notebook.lakehouse.backup.yml       # Manual backup pipeline
├── pipeline.notebook.lakehouse.backup.scheduled.yml  # Scheduled backup pipeline
├── pipeline.notebook.lakehouse.restore.yml      # Restore pipeline
├── pipeline.notebook.lakehouse.cleanup.yml      # Cleanup/retention pipeline
├── ruff.toml                                    # Lint settings used by CI
└── tests/                                       # Unit tests and local Spark end-to-end tests (see CONTRIBUTING.md)
```

## 🚀 Quick Start

### Prerequisites

Before you begin, ensure you have:

1. **Microsoft Fabric Workspace** with appropriate permissions
2. **Azure DevOps Organization** with pipelines enabled
3. **Azure Subscription** for Service Principal authentication
4. **Service Principal** configured with Fabric API access

### Step 1: Set Up Azure Service Principal

1. **Create a Service Principal in Azure AD**:
   ```bash
   az ad sp create-for-rbac --name "sp-fabric-backup" --role Contributor
   ```

2. **Grant Fabric API Permissions**:
   - Navigate to Azure Portal → Microsoft Entra ID → App registrations
   - Select your Service Principal
   - Go to **API permissions** → **Add a permission**
   - Select **Power BI Service** (this covers Fabric API)
   - Add delegated permissions:
     - `Workspace.ReadWrite.All`
     - `Item.Execute.All`
     - `Item.ReadWrite.All`
   - Click **Grant admin consent**

3. **Add Service Principal to Fabric Workspace**:
   - Open Microsoft Fabric portal
   - Go to your workspace → **Manage access**
   - Add the Service Principal with **Admin** or **Member** role

### Step 2: Configure Azure DevOps

1. **Create Service Connection**:
   - Go to Azure DevOps → Project Settings → Service connections
   - Create new **Azure Resource Manager** connection
   - Select **Service Principal (manual)** or **Service Principal (automatic)**
   - Name it following the pattern: `fabric-service-connection-{environment}`
   - Example: `fabric-service-connection-dev`

2. **Create Variable Groups**:
   Create variable groups for each environment (`fabric-workspaces-dev`, `fabric-workspaces-tst`, `fabric-workspaces-prd`) with these variables:

   | Variable Name | Description | Example |
   |--------------|-------------|---------|
   | `lakehouse_backup_workspace_id` | Workspace ID where backup notebook resides | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
   | `silver1_lakehouse_name` | Source lakehouse name | `lh_silver1` |
   | `silver2_lakehouse_name` | Source lakehouse name | `lh_silver2` |
   | `transform_workspace_name` | Source workspace name | `wsp_transform_dev` |

3. **Create Settings File** (if using templates):
   Create a `settings.yml` file in the parent directory with shared variables:
   ```yaml
   # settings.yml - Shared pipeline variables
   variables:
     - name: organization_name
       value: "your-org"
   ```

### Step 3: Deploy Notebooks to Fabric

1. **Import Notebooks**:
   - Open Microsoft Fabric portal
   - Navigate to your backup workspace
   - Click **Import** → **Notebook**
   - Upload `nb_lakehouse_backup.Notebook/notebook-content.py`
   - Upload `nb_lakehouse_restore.Notebook/notebook-content.py`
   - Upload `nb_lakehouse_cleanup.Notebook/notebook-content.py`
   - Rename them to `nb_lakehouse_backup`, `nb_lakehouse_restore`, and `nb_lakehouse_cleanup`

2. **Create Backup Lakehouse**:
   - In your backup workspace, create a new Lakehouse
   - Name it following the pattern: `lh_{env}_backup` (e.g., `lh_dev_backup`)

### Step 4: Create Azure DevOps Pipelines

1. **Import Pipeline Files**:
   - Go to Azure DevOps → Pipelines → New pipeline
   - Select your repository
   - Choose "Existing Azure Pipelines YAML file"
   - Select the appropriate pipeline file

2. **Available Pipelines**:
   - `pipeline.notebook.lakehouse.backup.yml` - Manual on-demand backup
   - `pipeline.notebook.lakehouse.backup.scheduled.yml` - Automated daily backup
   - `pipeline.notebook.lakehouse.restore.yml` - Restore from backup
   - `pipeline.notebook.lakehouse.cleanup.yml` - Backup retention cleanup

## 📖 Usage

### Running a Manual Backup

1. Go to Azure DevOps Pipelines
2. Select **Lakehouse Backup** pipeline
3. Click **Run pipeline**
4. Fill in the parameters:
   - **Environment**: `dev`, `tst`, or `prd`
   - **Source Lakehouse Name**: e.g., `lh_silver1`
   - **Source Workspace Name**: e.g., `wsp_transform_dev`
   - **Backup Tables**: `true` or `false`
   - **Backup Files**: `true` or `false`

### Automated Scheduled Backups

The scheduled pipeline runs automatically:
- **Schedule**: Daily at 12:30 AM AEST (14:30 UTC)
- **Default**: Backs up all configured lakehouses

To modify the schedule, edit the cron expression in `pipeline.notebook.lakehouse.backup.scheduled.yml`:
```yaml
schedules:
  - cron: "30 14 * * *"  # Modify this cron expression
```

### Restoring from Backup

1. Go to Azure DevOps Pipelines
2. Select **Lakehouse Restore** pipeline
3. Click **Run pipeline**
4. Fill in the parameters:
   - **Environment**: `dev`, `tst`, or `prd`
   - **Backup Path**: e.g., `silver1_transform_dev_backup_2025-11-10_14-30-15`
   - **Target Lakehouse Name**: e.g., `lh_silver1_restored`
   - **Target Workspace Name**: e.g., `wsp_transform_dev`
   - **Target Schema** (optional): leave blank to keep each table's schema; see [Restoring into schema-enabled and non-schema lakehouses](#restoring-into-schema-enabled-and-non-schema-lakehouses)
   - **Restore Tables**: `true` or `false`
   - **Restore Files**: `true` or `false`
   - **Overwrite Existing**: `true` or `false`

The restore notebook checks the whole plan before it writes anything: if the backup can't be restored into the target as requested (for example a backup with tables from several schemas into a non-schema lakehouse), it fails with an explanation and leaves the target untouched. Set `dry_run = True` in the notebook's configuration cell to print the plan without restoring.

When `overwrite_existing` is `true`, an existing target table is replaced even if its columns or partitioning changed since the backup. The replacement is a new Delta version of that table, so the pre-restore state stays reachable with time travel until it is vacuumed.

## 🗂️ Schema-Enabled Lakehouses

Schema-enabled lakehouses are [generally available](https://blog.fabric.microsoft.com/en-au/blog/lakehouse-schemas-generally-available) and are the default for new lakehouses created in the Fabric portal (lakehouses created through the REST API need `"enableSchemas": true`). Their tables live at `Tables/<schema>/<table>` in OneLake, with a mandatory `dbo` schema; non-schema lakehouses keep tables at `Tables/<table>`. See [Lakehouse schemas](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas).

**Backup.** The backup notebook detects the layout from OneLake: if any folder directly under `Tables/` is a Delta table (has a `_delta_log`), the lakehouse is non-schema; otherwise every folder under `Tables/` is a schema and its Delta-table subfolders are backed up. Folders that aren't Delta tables are skipped with a warning (Fabric shows them as *Unidentified*). The result is recorded as `schema_enabled` in `_manifest` and `backup_metadata`.

### Restoring into schema-enabled and non-schema lakehouses

The restore notebook detects whether the target is schema-enabled the same way (an empty `Tables/` folder counts as non-schema; an empty schema-enabled lakehouse still has `Tables/dbo`).

| Backup | Target: non-schema lakehouse | Target: schema-enabled lakehouse |
|---|---|---|
| Non-schema source (any layout version, including backups from the original notebook) | `Tables/<table>` | `Tables/dbo/<table>`, or `Tables/<target_schema>/<table>` |
| Schema-enabled source, one schema | `Tables/<table>` | `Tables/<schema>/<table>`, or `Tables/<target_schema>/<table>` |
| Schema-enabled source, several schemas | Fails before writing (would flatten several schemas into one folder); restore into a schema-enabled lakehouse or pick one schema with `restore_specific_tables` | `Tables/<schema>/<table>`, or every table into `Tables/<target_schema>/<table>` (fails before writing if two tables would get the same name) |

`target_schema` only applies to schema-enabled targets; setting it for a non-schema target fails before writing. If a schema the restore creates (for example a new `target_schema`) doesn't appear in the Lakehouse explorer, create it with **New schema** or `CREATE SCHEMA`; the restored tables are already in place under `Tables/<schema>/`.

## 🔧 Configuration

### Backup Naming Convention

Backups are automatically named using this pattern:
```
{lakehouse_short}_{workspace_short}_backup_{timestamp}
```
Example: `silver1_transform_dev_backup_2025-11-10_14-30-15`

### Backup Method

Each backup is a **full, independent copy**:

- **Tables**: every Delta table is read from the source at a fixed Delta version (`versionAsOf`) and written as a new Delta table in the backup folder, keeping its partition columns. Reads and writes go through OneLake, so this works across workspaces.
- **Files**: every file under `Files/` is copied with `notebookutils.fs.cp`, keeping its relative path. Files and folders whose names start with `_` or `.` are skipped.

The backup shares no data files with the source, so it survives `VACUUM`, overwrites, dropped tables and deletion of the source lakehouse, and it takes as much storage as the data it copies. Not preserved: the source tables' Delta history, table properties, constraints, column comments and Spark views; shortcuts are backed up as the data they point to. Tables that need Delta column mapping (column names with spaces or special characters) can't be backed up yet; see [Troubleshooting](#-troubleshooting).

Fabric supports `SHALLOW CLONE` only, not `DEEP CLONE` ([Delta Lake clone](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-clone)). A shallow clone of a table *in a backup* is a fast way to look at backed-up data in the same workspace, but it keeps pointing at the backup's files and breaks once the cleanup job deletes that backup. Use the restore notebook for a durable restore.

### Backup Storage Structure

Backups written by the current notebook use **layout version 2**:

```
backup_lakehouse/Files/
├── silver1_transform_dev_backup_2025-11-10_14-30-15/    # Schema-enabled source
│   ├── tables/
│   │   ├── dbo/
│   │   │   ├── customers/              # Delta table (full copy)
│   │   │   └── customers_metadata/     # Sidecar: schema, table, source_path, backup_path, delta_version, row_count
│   │   └── sales/
│   │       ├── orders/
│   │       └── orders_metadata/
│   ├── files/
│   │   ├── reports/
│   │   │   └── monthly_report.pdf
│   │   └── data/
│   │       └── import.csv
│   ├── _manifest/                      # Backup manifest (see below)
│   ├── _logs/                          # Execution logs
│   ├── backup_metadata/                # Summary incl. layout_version and schema_enabled
│   └── RESTORE_INSTRUCTIONS/           # Manual restore instructions
└── silver2_transform_dev_backup_2025-11-10_14-35-02/    # Non-schema source
    ├── tables/
    │   ├── customers/
    │   ├── customers_metadata/
    │   ├── orders/
    │   └── orders_metadata/
    ├── files/
    └── _manifest/, _logs/, backup_metadata/, RESTORE_INSTRUCTIONS/
```

Non-schema sources keep the same `tables/<table>` layout as backups from the original notebook (layout version 1), so those older backups remain restorable. A table whose name collides with another table's sidecar (for example `orders_metadata` next to `orders` in the same schema) is reported as failed rather than overwriting the sidecar.

### Backup Manifest

`_manifest` is a one-row Delta table. Scalar fields are columns, so you can query them across backups; the full manifest, including the per-table list, is stored as JSON in `manifest_json`.

| Field | Description |
|---|---|
| `layout_version` | `2` for backups written by the current notebook. Backups without it are layout version 1 |
| `status` | `success`, `partial` (some tables or files failed; the rest are restorable) or `failed` (the run stopped before completing; the restore notebook refuses these) |
| `schema_enabled` | Whether the source lakehouse is schema-enabled (null if tables weren't backed up) |
| `backup_folder_name`, `backup_timestamp`, `backup_id` | Folder name, its UTC timestamp, and a unique ID |
| `source_*`, `backup_*` | Source and backup lakehouse and workspace names |
| `tables_discovered`, `tables_backed_up`, `tables_failed` | Table counts |
| `files_discovered`, `files_backed_up`, `files_failed` | File counts |
| `start_time`, `end_time`, `duration_seconds` | Timing (UTC) |
| `spark_version` | Spark version of the session that wrote the backup |
| `manifest_json` | Everything above plus `tables`: per table `schema`, `table`, `status`, `backup_path`, `source_path`, `delta_version`, `row_count`, `rows_written`, `columns`, `partition_columns`, `verification`; and `file_discovery_errors`, the folders under `Files/` that couldn't be listed (each counts as a failed file) |

Each table's `_metadata` sidecar holds the same per-table fields, so a single table can be inspected or restored by hand.

### Notebook Parameters

#### Backup Notebook (`nb_lakehouse_backup`)

| Parameter | Type | Description |
|-----------|------|-------------|
| `source_lakehouse_name` | string | Name of the lakehouse to backup |
| `source_workspace_name` | string | Name of the source workspace |
| `backup_lakehouse_name` | string | Name of the backup lakehouse |
| `backup_workspace_name` | string | Name of the backup workspace |
| `backup_tables` | boolean | Whether to backup tables |
| `backup_files` | boolean | Whether to backup files |

#### Restore Notebook (`nb_lakehouse_restore`)

| Parameter | Type | Description |
|-----------|------|-------------|
| `backup_lakehouse_name` | string | Name of the backup lakehouse |
| `backup_workspace_name` | string | Name of the backup workspace |
| `backup_path` | string | Path to the backup folder |
| `target_lakehouse_name` | string | Name of the target lakehouse |
| `target_workspace_name` | string | Name of the target workspace |
| `target_schema` | string | Optional, schema-enabled targets only. Empty: keep each table's schema (tables from non-schema backups go to `dbo`). Set: restore every table into this schema |
| `restore_tables` | boolean | Whether to restore tables |
| `restore_files` | boolean | Whether to restore files |
| `overwrite_existing` | boolean | Whether to overwrite existing data |

Settings in each notebook's configuration cell (not pipeline parameters):

| Notebook | Setting | Default | Description |
|---|---|---|---|
| Backup | `count_rows` | `True` | Count rows of every source table before the backup (a full scan on large tables). With `False`, verification uses the rows written as recorded in the Delta log |
| Backup | `fail_on_partial_backup` | `True` | Fail the run, and the pipeline, if any table or file fails or verification fails. The manifest is written first |
| Restore | `dry_run` | `False` | Validate the backup and print the restore plan without writing |
| Restore | `restore_specific_tables` | `[]` | Restore only matching tables, e.g. `["customers", "sales.*"]` |
| Restore | `restore_specific_files` | `[]` | Restore only files matching these patterns (relative to `Files/`) |
| Restore | `fail_on_partial_restore` | `True` | Fail the run if any table or file fails, or verification fails |

## ⚙️ Fabric Runtimes

The notebooks are written for **Fabric Runtime 1.3** (Spark 3.5, Delta Lake 3.2, Python 3.11) and **Runtime 2.0** (Spark 4.1, Delta Lake 4.2, Python 3.13, Java 21; see [Runtime 2.0](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0)). They use `notebookutils` (not `mssparkutils`).

What is tested automatically (`.github/workflows/validate-notebooks.yml`):

- **Static checks** on Python 3.11 and 3.13: every notebook compiles, `ruff` finds no unused imports, undefined names or bare `except`, the Fabric cell markers and parameters cell are intact, and every parameter the pipelines pass exists in the notebook.
- **Unit tests** for table discovery, backup layout paths, restore planning and manifest parsing.
- **End-to-end runs** of all three notebooks on open-source Spark 3.5 + Delta 3.2 and Spark 4.1 + Delta 4.2, with a local stand-in for OneLake and `notebookutils`. They cover backup and restore across non-schema and schema-enabled sources and targets, backups in the original (v1) layout, `target_schema`, overwrite behaviour, partial backups and the cleanup manifest guard.

What still needs a run in a Fabric workspace before you rely on it: OneLake `abfss://` paths, permissions and cross-workspace access under your service principal; the real `notebookutils.fs` behaviour; Fabric registering schemas and tables written by a restore; and Fabric-specific Delta defaults (V-Order, Optimize Write). CONTRIBUTING.md has a [checklist for testing in a Fabric workspace](CONTRIBUTING.md#testing-in-a-fabric-workspace).

Restore with the same runtime that wrote the backup, or a newer one: Delta tables written on Runtime 2.0 can use table features that Runtime 1.3 can't read.

### Upgrading from the original notebooks

- New backups use layout version 2. Backups written by the original notebook still restore with the new restore notebook (non-schema lakehouses only: the original notebook couldn't back up tables in schema-enabled lakehouses).
- The original backup notebook failed silently on Runtime 2.0: it copied tables and files but its `_manifest` write failed, and the run still reported success. Those folders restore fine, but they have no manifest, so the cleanup notebook now skips them. Delete them by hand once they're older than your retention period.
- Failed and partial backups and restores now fail the Fabric job, so the Azure DevOps pipeline reports them.

## 🔐 Security Considerations

### Service Principal Best Practices

1. **Least Privilege**: Grant only necessary permissions to the Service Principal
2. **Secret Rotation**: Regularly rotate Service Principal secrets
3. **Audit Logging**: Enable Azure AD sign-in logs for the Service Principal
4. **Conditional Access**: Consider applying conditional access policies

### Azure DevOps Security

1. **Variable Groups**: Store sensitive values as secrets in variable groups
2. **Pipeline Permissions**: Restrict who can run backup/restore pipelines
3. **Approval Gates**: Add approval gates for production restores
4. **Branch Policies**: Protect pipeline YAML files with branch policies

## 🐛 Troubleshooting

### Common Issues

**Issue**: `Notebook not found in workspace`
- **Solution**: Ensure the notebook is imported with the exact name (`nb_lakehouse_backup` or `nb_lakehouse_restore`)

**Issue**: `Failed to resolve workspace ID`
- **Solution**: Verify the variable group contains `lakehouse_backup_workspace_id`

**Issue**: `Access denied to Fabric API`
- **Solution**: 
  1. Verify Service Principal has Fabric API permissions
  2. Check Service Principal is added to the workspace with sufficient role
  3. Ensure admin consent is granted for API permissions

**Issue**: `The backup holds tables from N schemas ... but the target lakehouse is not schema-enabled`
- **Solution**: Restore into a schema-enabled lakehouse, or restore one schema at a time with `restore_specific_tables` (e.g. `["sales.*"]`).

**Issue**: `target_schema='...' was given, but the target lakehouse is not schema-enabled`
- **Solution**: Leave the Target Schema parameter blank for non-schema targets.

**Issue**: `Several backed-up tables would be restored to the same target table`
- **Solution**: `target_schema` would put two tables with the same name (from different schemas) into one schema. Leave it blank, or restore the schemas separately with `restore_specific_tables`.

**Issue**: A restored schema doesn't show in the Lakehouse explorer
- **Solution**: Create the schema (**New schema**, or `CREATE SCHEMA <name>`); the tables are already under `Tables/<schema>/`.

**Issue**: `Backup finished with problems` / `Restore finished with problems`
- **Solution**: Some tables or files failed. The log above the error lists them; the backup manifest is still written with `status = partial`. Set `fail_on_partial_backup` / `fail_on_partial_restore` to `False` to accept partial runs.

**Issue**: `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES` when backing up a table
- **Cause**: The table uses Delta column mapping (column names with spaces or characters such as `,;{}()=`). The backup writes a new Delta table without carrying over table properties, so these tables can't be copied yet; they are reported as failed and the run fails.
- **Workaround**: Rename the columns in the source table, or copy such tables separately (for example `CREATE TABLE ... TBLPROPERTIES ('delta.columnMapping.mode' = 'name')` followed by an `INSERT`).

**Issue**: `Ancient datetime handling errors`
- **Solution**: The notebooks are pre-configured for legacy dates. If issues persist, verify Spark configuration in the notebook.

### Monitoring Backups

1. **Fabric Monitoring Hub**: View notebook execution status at [https://app.fabric.microsoft.com/monitoringhub](https://app.fabric.microsoft.com/monitoringhub)
2. **Azure DevOps**: Check pipeline run logs for detailed output
3. **Backup Manifest**: Each backup includes a `_manifest` table with execution details

## 📊 Backup Retention & Cleanup

The cleanup notebook and pipeline automatically manage backup retention by deleting backups older than the specified retention period.

### Cleanup Pipeline Features

- **Configurable retention period** (default: 30 days)
- **Dry-run mode** to preview deletions before executing
- **Pattern filtering** to clean specific lakehouses only
- **Detailed reporting** of space reclaimed
- **Manifest guard**: an expired folder is deleted only if its `_manifest` can be read; folders with a missing or unreadable manifest are logged as "unknown, skipped" and left in place
- **Scheduled execution** (default: weekly)

### Running Cleanup Manually

1. Go to Azure DevOps Pipelines
2. Select **Lakehouse Cleanup** pipeline
3. Click **Run pipeline**
4. Fill in the parameters:
   - **Environment**: `dev`, `tst`, or `prd`
   - **Retention Days**: Number of days to keep (default: 30)
   - **Dry Run**: `true` to preview, `false` to delete
   - **Name Pattern**: Optional filter (e.g., `silver1_` for specific lakehouse)

### Automated Scheduled Cleanup

The cleanup pipeline runs automatically:
- **Schedule**: Weekly on Sunday at 2:00 AM AEST (16:00 UTC Saturday)
- **Default Retention**: 30 days

To modify the schedule, edit the cron expression in `pipeline.notebook.lakehouse.cleanup.yml`:
```yaml
schedules:
  - cron: "0 16 * * 6"  # Modify this cron expression
```

### Cleanup Notebook Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `backup_lakehouse_name` | string | Name of the backup lakehouse |
| `backup_workspace_name` | string | Name of the backup workspace |
| `retention_days` | integer | Days to retain backups (default: 30) |
| `dry_run` | boolean | Preview mode - no deletions if true |
| `name_pattern` | string | Filter backups by name pattern |

### Cleanup Best Practices

1. **Always run dry-run first**: Preview what will be deleted before actual cleanup
2. **Set appropriate retention**: Balance storage costs vs recovery needs
3. **Monitor cleanup logs**: Check for failed deletions and for folders reported as "unknown, skipped"
4. **Stagger cleanup schedules**: Don't run cleanup during backup windows

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Microsoft Fabric team for the powerful lakehouse capabilities
- Azure DevOps for CI/CD pipeline support
- The open-source community for continuous improvement

## 📬 Support

If you encounter any issues or have questions:

1. Check the [Troubleshooting](#-troubleshooting) section
2. Open an issue in this repository
3. Review Microsoft Fabric documentation at [https://learn.microsoft.com/fabric](https://learn.microsoft.com/fabric)
