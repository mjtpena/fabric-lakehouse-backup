# Microsoft Fabric Lakehouse Backup & Restore

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A complete backup and restore solution for Microsoft Fabric Lakehouses, featuring automated Azure DevOps pipelines and PySpark notebooks for reliable data protection.

## 🌟 Features

- **Full Lakehouse Backup**: Backup both Delta tables and files from any Fabric Lakehouse
- **Cross-Workspace Support**: Backup and restore across different Fabric workspaces
- **Automated Scheduling**: Schedule daily backups via Azure DevOps pipelines
- **Flexible Restore**: Restore complete backups or selective tables/files
- **Service Principal Authentication**: Secure, automated execution without user interaction
- **Detailed Logging**: Comprehensive logging and manifest generation for audit trails
- **Ancient Date Handling**: Pre-configured for legacy data with pre-1582 dates (e.g., Oracle EBS)

## 📁 Repository Structure

```
fabric-lakehouse-backup/
├── README.md                                    # This file
├── LICENSE                                      # MIT License
├── nb_lakehouse_backup.Notebook/
│   └── notebook-content.py                      # Backup notebook (PySpark)
├── nb_lakehouse_restore.Notebook/
│   └── notebook-content.py                      # Restore notebook (PySpark)
├── pipeline.notebook.lakehouse.backup.yml       # Manual backup pipeline
├── pipeline.notebook.lakehouse.backup.scheduled.yml  # Scheduled backup pipeline
└── pipeline.notebook.lakehouse.restore.yml      # Restore pipeline
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
   - Name it following the pattern: `sp-{subscription-type}-{environment}-01`
   - Example: `sp-alz-dpora-dev-01`

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
   - Rename them to `nb_lakehouse_backup` and `nb_lakehouse_restore`

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
   - **Restore Tables**: `true` or `false`
   - **Restore Files**: `true` or `false`
   - **Overwrite Existing**: `true` or `false`

## 🔧 Configuration

### Backup Naming Convention

Backups are automatically named using this pattern:
```
{lakehouse_short}_{workspace_short}_backup_{timestamp}
```
Example: `silver1_transform_dev_backup_2025-11-10_14-30-15`

### Backup Storage Structure

```
backup_lakehouse/Files/
└── silver1_transform_dev_backup_2025-11-10_14-30-15/
    ├── tables/
    │   ├── customers/                 # Delta table
    │   ├── customers_metadata/        # Table metadata
    │   ├── orders/
    │   └── orders_metadata/
    ├── files/
    │   ├── reports/
    │   │   └── monthly_report.pdf
    │   └── data/
    │       └── import.csv
    ├── _manifest/                     # Backup manifest
    └── _logs/                         # Execution logs
```

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
| `restore_tables` | boolean | Whether to restore tables |
| `restore_files` | boolean | Whether to restore files |
| `overwrite_existing` | boolean | Whether to overwrite existing data |

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

**Issue**: `Ancient datetime handling errors`
- **Solution**: The notebooks are pre-configured for legacy dates. If issues persist, verify Spark configuration in the notebook.

### Monitoring Backups

1. **Fabric Monitoring Hub**: View notebook execution status at [https://app.fabric.microsoft.com/monitoringhub](https://app.fabric.microsoft.com/monitoringhub)
2. **Azure DevOps**: Check pipeline run logs for detailed output
3. **Backup Manifest**: Each backup includes a `_manifest` table with execution details

## 📊 Backup Retention

The default retention period is **30 days**. To implement automatic cleanup:

1. Create a separate cleanup notebook
2. Schedule it to run periodically
3. Delete backups older than the retention period

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
