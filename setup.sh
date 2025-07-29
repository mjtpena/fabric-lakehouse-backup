#!/bin/bash

# Fabric Lakehouse Backup Setup Script
# This script helps set up the backup environment and validates prerequisites

set -e

echo "🚀 Fabric Lakehouse Backup Setup"
echo "================================="

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    case $2 in
        "success") echo -e "${GREEN}✅ $1${NC}" ;;
        "warning") echo -e "${YELLOW}⚠️  $1${NC}" ;;
        "error") echo -e "${RED}❌ $1${NC}" ;;
        "info") echo -e "${BLUE}ℹ️  $1${NC}" ;;
        *) echo "$1" ;;
    esac
}

# Check if running in correct directory
if [ ! -f "Fabric_Lakehouse_Backup.ipynb" ]; then
    print_status "Please run this script from the fabric-lakehouse-backup directory" "error"
    exit 1
fi

print_status "Setting up Fabric Lakehouse Backup environment..." "info"

# Create necessary directories
print_status "Creating directory structure..." "info"
mkdir -p config
mkdir -p logs
mkdir -p temp
mkdir -p .azure

# Copy configuration template if config.json doesn't exist
if [ ! -f "config/config.json" ]; then
    if [ -f "config/config.template.json" ]; then
        cp config/config.template.json config/config.json
        print_status "Created config/config.json from template" "success"
        print_status "Please edit config/config.json with your workspace IDs" "warning"
    else
        print_status "Configuration template not found" "error"
    fi
else
    print_status "Configuration file already exists" "info"
fi

# Check Python dependencies
print_status "Checking Python environment..." "info"

if command -v python3 &> /dev/null; then
    PYTHON_CMD=python3
elif command -v python &> /dev/null; then
    PYTHON_CMD=python
else
    print_status "Python not found. Please install Python 3.8 or later" "error"
    exit 1
fi

PYTHON_VERSION=$($PYTHON_CMD --version | cut -d' ' -f2)
print_status "Found Python $PYTHON_VERSION" "success"

# Check required Python packages
print_status "Checking required packages..." "info"

REQUIRED_PACKAGES=("pandas" "zipfile" "json" "datetime")
MISSING_PACKAGES=()

for package in "${REQUIRED_PACKAGES[@]}"; do
    if ! $PYTHON_CMD -c "import $package" &> /dev/null; then
        MISSING_PACKAGES+=($package)
    fi
done

if [ ${#MISSING_PACKAGES[@]} -eq 0 ]; then
    print_status "All required packages are available" "success"
else
    print_status "Missing packages: ${MISSING_PACKAGES[*]}" "warning"
    print_status "These are typically available in Fabric notebooks" "info"
fi

# Validate notebook structure
print_status "Validating notebook files..." "info"

if [ -f "Fabric_Lakehouse_Backup.ipynb" ]; then
    if $PYTHON_CMD -c "import json; json.load(open('Fabric_Lakehouse_Backup.ipynb'))" 2> /dev/null; then
        print_status "Backup notebook structure is valid" "success"
    else
        print_status "Backup notebook has invalid JSON structure" "error"
    fi
else
    print_status "Backup notebook not found" "error"
fi

if [ -f "Fabric_Lakehouse_Restore.ipynb" ]; then
    if $PYTHON_CMD -c "import json; json.load(open('Fabric_Lakehouse_Restore.ipynb'))" 2> /dev/null; then
        print_status "Restore notebook structure is valid" "success"
    else
        print_status "Restore notebook has invalid JSON structure" "error"
    fi
else
    print_status "Restore notebook not found" "error"
fi

# Check script files
print_status "Checking helper scripts..." "info"

SCRIPT_FILES=("scripts/validate-backup.py" "scripts/migration-helper.py" "scripts/config_loader.py" "scripts/performance_monitor.py")

for script in "${SCRIPT_FILES[@]}"; do
    if [ -f "$script" ]; then
        if $PYTHON_CMD -m py_compile "$script" 2> /dev/null; then
            print_status "$(basename $script) is valid" "success"
        else
            print_status "$(basename $script) has syntax errors" "error"
        fi
    else
        print_status "$(basename $script) not found" "warning"
    fi
done

# Generate README for quick start
if [ ! -f "QUICK_START.md" ]; then
    cat > QUICK_START.md << 'EOF'
# Quick Start Guide

## 1. Configure Your Environment

Edit `config/config.json`:
```json
{
  "environments": {
    "production": {
      "source_workspace_id": "YOUR_SOURCE_WORKSPACE_ID",
      "backup_workspace_id": "YOUR_BACKUP_WORKSPACE_ID"
    }
  }
}
```

## 2. Run Your First Backup

1. Open `Fabric_Lakehouse_Backup.ipynb` in Fabric
2. Update the first cell with your lakehouse names:
   ```python
   source_lakehouse_name = "your-source-lakehouse"
   backup_lakehouse_name = "your-backup-lakehouse"
   ```
3. Run all cells

## 3. Test Restore

1. Open `Fabric_Lakehouse_Restore.ipynb` in Fabric  
2. Update with your backup path and target lakehouse
3. Set `dry_run = True` to preview
4. Run all cells

## 4. Validate Backup

Run the validation script:
```python
from scripts.validate_backup import validate_backup_integrity
result = validate_backup_integrity("path/to/your/backup")
```

## Need Help?

- Check `docs/` for detailed guides
- See `examples/` for common scenarios
- Review `TROUBLESHOOTING.md` for common issues
EOF

    print_status "Created QUICK_START.md" "success"
fi

# Check git configuration
if [ -d ".git" ]; then
    print_status "Git repository detected" "info"
    
    # Check if GitHub Actions workflow exists
    if [ -f ".github/workflows/validate-notebooks.yml" ]; then
        print_status "GitHub Actions workflow configured" "success"
    else
        print_status "Consider setting up GitHub Actions for CI/CD" "info"
    fi
else
    print_status "Not a Git repository. Consider initializing git for version control" "info"
fi

# Summary
echo ""
print_status "Setup Summary" "info"
echo "=============="

if [ -f "config/config.json" ]; then
    print_status "✅ Configuration file ready"
else
    print_status "❌ Configuration file missing"
fi

if [ -f "Fabric_Lakehouse_Backup.ipynb" ] && [ -f "Fabric_Lakehouse_Restore.ipynb" ]; then
    print_status "✅ Notebook files present"
else
    print_status "❌ Missing notebook files"
fi

if [ -d "scripts" ] && [ -d "docs" ] && [ -d "examples" ]; then
    print_status "✅ Documentation and scripts available"
else
    print_status "❌ Missing supporting files"
fi

echo ""
print_status "Next Steps:" "info"
echo "1. Edit config/config.json with your workspace IDs"
echo "2. Open notebooks in Microsoft Fabric"
echo "3. Follow QUICK_START.md for first backup"
echo "4. Review docs/ for advanced usage"

echo ""
print_status "Setup completed! 🎉" "success"
