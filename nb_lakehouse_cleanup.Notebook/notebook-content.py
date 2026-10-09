# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

# =============================================================================
# Microsoft Fabric Lakehouse Backup Cleanup/Retention Notebook
# =============================================================================
# This notebook manages backup retention by deleting backups older than the
# specified retention period. It helps maintain storage costs and keeps the
# backup lakehouse organized.
#
# Features:
#   - Configurable retention period (days)
#   - Dry-run mode to preview deletions
#   - Detailed logging and reporting
#   - Support for selective cleanup by lakehouse name pattern
#   - Never deletes a folder whose _manifest is missing or unreadable
#     (logged as "unknown, skipped"; delete such folders by hand)
# =============================================================================

# Parameters - Configure these values or override when scheduling
backup_lakehouse_name = ""  # REQUIRED: Backup lakehouse name (e.g., "lh_dev_backup")
backup_workspace_name = ""  # REQUIRED: Backup workspace name (e.g., "ws-dev-backup")

# Retention settings
retention_days = 30  # Number of days to retain backups (default: 30)

# Cleanup options
dry_run = True  # If True, only report what would be deleted without actually deleting
name_pattern = ""  # Optional: Only clean backups matching this pattern (e.g., "silver1_")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "tags": [
# META     "parameters"
# META   ]
# META }

# CELL ********************

# =============================================================================
# Cell 1: Import Libraries and Initialize
# =============================================================================

import json
import datetime
import notebookutils

# Initialize global variables
log_entries = []
cleanup_start_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)  # UTC, like backup folder names

print("=" * 60)
print("🧹 MICROSOFT FABRIC LAKEHOUSE BACKUP CLEANUP")
print("=" * 60)
print(f"📅 Started at: {cleanup_start_time}")
print("📚 Libraries imported successfully")

# CELL ********************

# =============================================================================
# Cell 2: Helper Functions
# =============================================================================
# Pure helpers: no Spark or notebookutils calls. Unit tested in tests/.
import datetime
import re


def utc_now():
    """Current UTC time as a naive datetime (backup folder timestamps are UTC)"""
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

def onelake_lakehouse_path(workspace_name, lakehouse_name):
    """ABFS path of a lakehouse item in OneLake"""
    return f"abfss://{workspace_name.strip()}@onelake.dfs.fabric.microsoft.com/{lakehouse_name.strip()}.Lakehouse"

def log_message(message, level="INFO"):
    """Log a message with timestamp and level"""
    timestamp = utc_now().strftime("%Y-%m-%d %H:%M:%S")
    emoji_map = {
        "INFO": "ℹ️",
        "WARNING": "⚠️",
        "ERROR": "❌",
        "SUCCESS": "✅",
        "DELETE": "🗑️",
        "SKIP": "⏭️"
    }
    emoji = emoji_map.get(level, "📝")
    print(f"[{timestamp}] {emoji} [{level}] {message}")
    
    log_entries.append({
        "timestamp": timestamp,
        "level": level,
        "message": message
    })

def validate_parameters():
    """Validate that required parameters are provided"""
    errors = []
    
    if not backup_lakehouse_name or not backup_lakehouse_name.strip():
        errors.append("backup_lakehouse_name is required")
    
    if not backup_workspace_name or not backup_workspace_name.strip():
        errors.append("backup_workspace_name is required")
    
    if retention_days < 1:
        errors.append("retention_days must be at least 1")
    
    if errors:
        for error in errors:
            log_message(error, "ERROR")
        raise ValueError("Parameter validation failed: " + ", ".join(errors))
    
    log_message("Parameter validation passed", "SUCCESS")

def get_backup_base_path():
    """Construct the base path for the backup lakehouse Files directory"""
    return f"{onelake_lakehouse_path(backup_workspace_name, backup_lakehouse_name)}/Files"

def parse_backup_timestamp(backup_name):
    """
    Extract timestamp from backup folder name.
    Expected format: {lakehouse}_{workspace}_backup_{YYYY-MM-DD_HH-MM-SS}
    Returns datetime object or None if parsing fails.
    """
    # Pattern to match timestamp at the end: YYYY-MM-DD_HH-MM-SS
    pattern = r'(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})$'
    match = re.search(pattern, backup_name)
    
    if match:
        timestamp_str = match.group(1)
        try:
            return datetime.datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S")
        except ValueError:
            return None
    return None

def format_size(size_bytes):
    """Format bytes into human-readable size"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

print("✅ Helper functions defined successfully")

# CELL ********************

# =============================================================================
# Cell 3: Discover Backups
# =============================================================================

def discover_backups(base_path):
    """
    Discover all backup folders in the backup lakehouse.
    Returns a list of dictionaries with backup information.
    """
    log_message(f"Discovering backups in: {base_path}", "INFO")
    
    backups = []
    
    try:
        items = notebookutils.fs.ls(base_path)
        
        for item in items:
            if item.isDir and not item.name.startswith('_') and not item.name.startswith('.'):
                backup_name = item.name.rstrip('/')
                backup_timestamp = parse_backup_timestamp(backup_name)
                
                backup_info = {
                    "name": backup_name,
                    "path": item.path,
                    "timestamp": backup_timestamp,
                    "age_days": None,
                    "size_bytes": 0,
                    "is_expired": False,
                    "matches_pattern": True,
                    "manifest_readable": None  # Checked only for backups that would be deleted
                }
                
                # Calculate age if timestamp was parsed
                if backup_timestamp:
                    age = utc_now() - backup_timestamp
                    backup_info["age_days"] = age.days
                    backup_info["is_expired"] = age.days > retention_days
                
                # Check pattern match if specified
                if name_pattern and name_pattern.strip():
                    backup_info["matches_pattern"] = name_pattern.strip().lower() in backup_name.lower()
                
                # Try to get size (this can be slow for large backups)
                try:
                    backup_info["size_bytes"] = get_folder_size(item.path)
                except Exception:
                    backup_info["size_bytes"] = 0
                
                backups.append(backup_info)
        
        log_message(f"Found {len(backups)} backup folders", "SUCCESS")
        
    except Exception as e:
        log_message(f"Error discovering backups: {str(e)}", "ERROR")
        raise
    
    return backups

def get_folder_size(folder_path):
    """
    Calculate total size of a folder recursively.
    Note: This can be slow for large folders.
    """
    total_size = 0
    try:
        items = notebookutils.fs.ls(folder_path)
        for item in items:
            if item.isDir:
                total_size += get_folder_size(item.path)
            else:
                total_size += item.size if hasattr(item, 'size') else 0
    except Exception:
        pass
    return total_size

def manifest_is_readable(backup_folder_path):
    """True if the backup folder has a _manifest Delta table that can be read and has a row"""
    try:
        return len(spark.read.format("delta").load(f"{backup_folder_path}/_manifest").limit(1).collect()) == 1
    except Exception:
        return False

print("✅ Backup discovery functions defined successfully")

# CELL ********************

# =============================================================================
# Cell 4: Analyze Backups
# =============================================================================

def analyze_backups(backups):
    """
    Analyze discovered backups and categorize them.
    Returns analysis summary.
    """
    log_message("Analyzing backups...", "INFO")
    
    analysis = {
        "total_backups": len(backups),
        "valid_backups": 0,
        "invalid_backups": 0,
        "expired_backups": 0,
        "retained_backups": 0,
        "pattern_matched": 0,
        "unknown_skipped": 0,
        "to_delete": [],
        "to_retain": [],
        "to_skip_unknown": [],
        "total_size_to_delete": 0,
        "total_size_to_retain": 0
    }
    
    for backup in backups:
        # Check if backup has valid timestamp
        if backup["timestamp"] is None:
            analysis["invalid_backups"] += 1
            log_message(f"⚠️ Invalid backup (no timestamp): {backup['name']}", "WARNING")
            continue
        
        analysis["valid_backups"] += 1
        
        # Check pattern match
        if not backup["matches_pattern"]:
            analysis["to_retain"].append(backup)
            analysis["retained_backups"] += 1
            analysis["total_size_to_retain"] += backup["size_bytes"]
            continue
        
        analysis["pattern_matched"] += 1
        
        # Check if expired
        if backup["is_expired"]:
            # Guard: only delete folders that are recognisably backups. A folder without a
            # readable _manifest may be an in-progress backup or something else entirely.
            backup["manifest_readable"] = manifest_is_readable(backup["path"])
            if not backup["manifest_readable"]:
                analysis["unknown_skipped"] += 1
                analysis["to_skip_unknown"].append(backup)
                log_message(f"Backup {backup['name']}: manifest missing or unreadable - unknown, skipped", "SKIP")
                continue
            analysis["expired_backups"] += 1
            analysis["to_delete"].append(backup)
            analysis["total_size_to_delete"] += backup["size_bytes"]
        else:
            analysis["retained_backups"] += 1
            analysis["to_retain"].append(backup)
            analysis["total_size_to_retain"] += backup["size_bytes"]
    
    return analysis

def print_analysis_report(analysis):
    """Print a formatted analysis report"""
    print("")
    print("=" * 60)
    print("📊 BACKUP ANALYSIS REPORT")
    print("=" * 60)
    print("")
    print(f"📁 Total backup folders found:     {analysis['total_backups']}")
    print(f"✅ Valid backups (with timestamp): {analysis['valid_backups']}")
    print(f"⚠️  Invalid backups (no timestamp): {analysis['invalid_backups']}")
    print("")
    print(f"📅 Retention period:               {retention_days} days")
    print(f"🔍 Name pattern filter:            {name_pattern if name_pattern else '(none)'}")
    print("")
    print(f"🗑️  Backups to DELETE:              {analysis['expired_backups']}")
    print(f"💾 Backups to RETAIN:              {analysis['retained_backups']}")
    print(f"⏭️  Unknown, skipped (no manifest):  {analysis['unknown_skipped']}")
    print("")
    print(f"📦 Storage to free:                {format_size(analysis['total_size_to_delete'])}")
    print(f"📦 Storage to retain:              {format_size(analysis['total_size_to_retain'])}")
    print("=" * 60)
    print("")
    
    # List backups to delete
    if analysis["to_delete"]:
        print("🗑️  BACKUPS SCHEDULED FOR DELETION:")
        print("-" * 60)
        for backup in sorted(analysis["to_delete"], key=lambda x: x["timestamp"]):
            age_str = f"{backup['age_days']} days old" if backup['age_days'] else "unknown age"
            size_str = format_size(backup['size_bytes'])
            print(f"   • {backup['name']}")
            print(f"     Age: {age_str} | Size: {size_str}")
        print("")
    
    # List expired folders that were skipped because their manifest is missing or unreadable
    if analysis["to_skip_unknown"]:
        print("⏭️  UNKNOWN, SKIPPED (manifest missing or unreadable - delete by hand if not needed):")
        print("-" * 60)
        for backup in sorted(analysis["to_skip_unknown"], key=lambda x: x["timestamp"]):
            print(f"   • {backup['name']} ({backup['age_days']} days old, {format_size(backup['size_bytes'])})")
        print("")

    # List backups to retain (limited to 10 most recent)
    if analysis["to_retain"]:
        print("💾 BACKUPS TO RETAIN (most recent 10):")
        print("-" * 60)
        sorted_retain = sorted(
            [b for b in analysis["to_retain"] if b["timestamp"]], 
            key=lambda x: x["timestamp"], 
            reverse=True
        )[:10]
        for backup in sorted_retain:
            age_str = f"{backup['age_days']} days old" if backup['age_days'] else "unknown age"
            print(f"   • {backup['name']} ({age_str})")
        if len(analysis["to_retain"]) > 10:
            print(f"   ... and {len(analysis['to_retain']) - 10} more")
        print("")

print("✅ Analysis functions defined successfully")

# CELL ********************

# =============================================================================
# Cell 5: Delete Expired Backups
# =============================================================================

def delete_backup(backup_path, backup_name):
    """
    Delete a single backup folder and all its contents.
    Returns True if successful, False otherwise.
    """
    try:
        log_message(f"Deleting backup: {backup_name}", "DELETE")
        notebookutils.fs.rm(backup_path, recurse=True)
        log_message(f"Successfully deleted: {backup_name}", "SUCCESS")
        return True
    except Exception as e:
        log_message(f"Failed to delete {backup_name}: {str(e)}", "ERROR")
        return False

def cleanup_expired_backups(analysis):
    """
    Delete all expired backups identified in the analysis.
    Returns cleanup results.
    """
    results = {
        "attempted": 0,
        "succeeded": 0,
        "failed": 0,
        "space_freed": 0,
        "failed_backups": []
    }
    
    if not analysis["to_delete"]:
        log_message("No backups to delete", "INFO")
        return results
    
    if dry_run:
        print("")
        print("=" * 60)
        print("🔍 DRY RUN MODE - NO CHANGES WILL BE MADE")
        print("=" * 60)
        print("")
        print("The following backups WOULD be deleted:")
        for backup in analysis["to_delete"]:
            print(f"   🗑️ {backup['name']} ({format_size(backup['size_bytes'])})")
        print("")
        print(f"Total space that WOULD be freed: {format_size(analysis['total_size_to_delete'])}")
        print("")
        print("💡 Set dry_run=False to actually delete these backups")
        print("=" * 60)
        return results
    
    print("")
    print("=" * 60)
    print("🗑️  STARTING BACKUP CLEANUP")
    print("=" * 60)
    print("")
    
    for backup in analysis["to_delete"]:
        results["attempted"] += 1
        
        if delete_backup(backup["path"], backup["name"]):
            results["succeeded"] += 1
            results["space_freed"] += backup["size_bytes"]
        else:
            results["failed"] += 1
            results["failed_backups"].append(backup["name"])
    
    return results

def print_cleanup_results(results):
    """Print cleanup results summary"""
    print("")
    print("=" * 60)
    print("📋 CLEANUP RESULTS")
    print("=" * 60)
    print("")
    print(f"🎯 Backups attempted:  {results['attempted']}")
    print(f"✅ Successfully deleted: {results['succeeded']}")
    print(f"❌ Failed to delete:    {results['failed']}")
    print("")
    print(f"💾 Space freed:        {format_size(results['space_freed'])}")
    print("")
    
    if results["failed_backups"]:
        print("❌ Failed deletions:")
        for name in results["failed_backups"]:
            print(f"   • {name}")
        print("")
    
    print("=" * 60)

print("✅ Cleanup functions defined successfully")

# CELL ********************

# =============================================================================
# Cell 6: Main Execution
# =============================================================================

def main():
    """Main execution function"""
    try:
        # Step 1: Validate parameters
        print("")
        log_message("=" * 50, "INFO")
        log_message("STEP 1: Validating parameters", "INFO")
        log_message("=" * 50, "INFO")
        validate_parameters()
        
        print("")
        print("📋 Configuration:")
        print(f"   Backup Lakehouse: {backup_lakehouse_name}")
        print(f"   Backup Workspace: {backup_workspace_name}")
        print(f"   Retention Days:   {retention_days}")
        print(f"   Dry Run:          {dry_run}")
        print(f"   Name Pattern:     {name_pattern if name_pattern else '(all backups)'}")
        print("")
        
        # Step 2: Discover backups
        log_message("=" * 50, "INFO")
        log_message("STEP 2: Discovering backups", "INFO")
        log_message("=" * 50, "INFO")
        
        base_path = get_backup_base_path()
        backups = discover_backups(base_path)
        
        if not backups:
            log_message("No backups found. Nothing to clean up.", "INFO")
            return
        
        # Step 3: Analyze backups
        log_message("=" * 50, "INFO")
        log_message("STEP 3: Analyzing backups", "INFO")
        log_message("=" * 50, "INFO")
        
        analysis = analyze_backups(backups)
        print_analysis_report(analysis)
        
        # Step 4: Cleanup expired backups
        log_message("=" * 50, "INFO")
        log_message("STEP 4: Cleaning up expired backups", "INFO")
        log_message("=" * 50, "INFO")
        
        results = cleanup_expired_backups(analysis)
        
        if not dry_run and results["attempted"] > 0:
            print_cleanup_results(results)
        
        # Final summary
        cleanup_end_time = utc_now()
        duration = cleanup_end_time - cleanup_start_time
        
        print("")
        print("=" * 60)
        print("🏁 CLEANUP COMPLETED")
        print("=" * 60)
        print(f"⏱️  Duration: {duration}")
        print(f"📅 Completed at: {cleanup_end_time}")
        print("=" * 60)
        
        # Return summary for pipeline
        return {
            "status": "success",
            "dry_run": dry_run,
            "total_backups": analysis["total_backups"],
            "expired_backups": analysis["expired_backups"],
            "unknown_skipped": analysis["unknown_skipped"],
            "deleted_backups": results["succeeded"] if not dry_run else 0,
            "space_freed_bytes": results["space_freed"] if not dry_run else 0,
            "failed_deletions": results["failed"] if not dry_run else 0
        }
        
    except Exception as e:
        log_message(f"Cleanup failed with error: {str(e)}", "ERROR")
        import traceback
        traceback.print_exc()
        raise

print("✅ Main execution function defined")
print("")
print("💡 Run the next cell to execute the cleanup process")

# CELL ********************

# =============================================================================
# Cell 7: Execute Cleanup
# =============================================================================

# Run the main cleanup process
cleanup_result = main()

# Display final result
if cleanup_result:
    print("")
    print("📊 Result Summary (for pipeline integration):")
    print(json.dumps(cleanup_result, indent=2))

# CELL ********************

# =============================================================================
# Cell 8: Save Cleanup Log (Optional)
# =============================================================================

def save_cleanup_log():
    """Save cleanup log to the backup lakehouse for audit purposes"""
    try:
        if not log_entries:
            print("No log entries to save")
            return
        
        log_data = {
            "cleanup_timestamp": cleanup_start_time.isoformat(),
            "parameters": {
                "backup_lakehouse_name": backup_lakehouse_name,
                "backup_workspace_name": backup_workspace_name,
                "retention_days": retention_days,
                "dry_run": dry_run,
                "name_pattern": name_pattern
            },
            "log_entries": log_entries
        }
        
        # Create log path
        log_folder = f"{get_backup_base_path()}/_cleanup_logs"
        log_filename = f"cleanup_log_{cleanup_start_time.strftime('%Y-%m-%d_%H-%M-%S')}.json"
        log_path = f"{log_folder}/{log_filename}"
        
        # Save as JSON using Spark
        log_df = spark.createDataFrame([(json.dumps(log_data),)], ["log_json"])
        log_df.coalesce(1).write.mode("overwrite").text(log_path)
        
        print(f"✅ Cleanup log saved to: {log_path}")
        
    except Exception as e:
        print(f"⚠️ Could not save cleanup log: {str(e)}")

# Uncomment the line below to save the cleanup log
# save_cleanup_log()
