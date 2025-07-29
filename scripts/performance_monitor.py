"""
Performance monitoring and optimization tools for Fabric Lakehouse backups.
Provides insights into backup performance and suggests optimizations.
"""

import time
import datetime
import json
from typing import Dict, List, Any
from dataclasses import dataclass, asdict

@dataclass
class PerformanceMetrics:
    """Performance metrics for backup operations"""
    operation: str
    start_time: float
    end_time: float
    duration_seconds: float
    rows_processed: int = 0
    bytes_processed: int = 0
    memory_peak_mb: float = 0
    error_count: int = 0
    warnings_count: int = 0

class PerformanceMonitor:
    """Monitor and track backup performance"""
    
    def __init__(self):
        self.metrics: List[PerformanceMetrics] = []
        self.current_operation = None
        self.start_time = None
    
    def start_operation(self, operation_name: str):
        """Start monitoring an operation"""
        self.current_operation = operation_name
        self.start_time = time.time()
    
    def end_operation(self, rows_processed: int = 0, bytes_processed: int = 0, 
                     error_count: int = 0, warnings_count: int = 0):
        """End monitoring and record metrics"""
        if self.current_operation and self.start_time:
            end_time = time.time()
            duration = end_time - self.start_time
            
            # Try to get memory usage (if psutil available)
            memory_peak = 0
            try:
                import psutil
                process = psutil.Process()
                memory_peak = process.memory_info().rss / 1024 / 1024  # MB
            except ImportError:
                pass
            
            metrics = PerformanceMetrics(
                operation=self.current_operation,
                start_time=self.start_time,
                end_time=end_time,
                duration_seconds=duration,
                rows_processed=rows_processed,
                bytes_processed=bytes_processed,
                memory_peak_mb=memory_peak,
                error_count=error_count,
                warnings_count=warnings_count
            )
            
            self.metrics.append(metrics)
            self.current_operation = None
            self.start_time = None
            
            return metrics
    
    def get_summary_report(self) -> Dict[str, Any]:
        """Generate performance summary report"""
        if not self.metrics:
            return {"message": "No performance data collected"}
        
        total_duration = sum(m.duration_seconds for m in self.metrics)
        total_rows = sum(m.rows_processed for m in self.metrics)
        total_bytes = sum(m.bytes_processed for m in self.metrics)
        total_errors = sum(m.error_count for m in self.metrics)
        total_warnings = sum(m.warnings_count for m in self.metrics)
        
        # Calculate rates
        rows_per_second = total_rows / total_duration if total_duration > 0 else 0
        mb_per_second = (total_bytes / 1024 / 1024) / total_duration if total_duration > 0 else 0
        
        # Find slowest operations
        slowest_operations = sorted(self.metrics, key=lambda x: x.duration_seconds, reverse=True)[:5]
        
        return {
            "summary": {
                "total_operations": len(self.metrics),
                "total_duration_seconds": round(total_duration, 2),
                "total_rows_processed": total_rows,
                "total_mb_processed": round(total_bytes / 1024 / 1024, 2),
                "processing_rate": {
                    "rows_per_second": round(rows_per_second, 2),
                    "mb_per_second": round(mb_per_second, 2)
                },
                "quality_metrics": {
                    "total_errors": total_errors,
                    "total_warnings": total_warnings,
                    "success_rate": round(((len(self.metrics) - total_errors) / len(self.metrics)) * 100, 2)
                }
            },
            "slowest_operations": [
                {
                    "operation": op.operation,
                    "duration_seconds": round(op.duration_seconds, 2),
                    "rows_processed": op.rows_processed,
                    "mb_processed": round(op.bytes_processed / 1024 / 1024, 2)
                }
                for op in slowest_operations
            ]
        }
    
    def get_optimization_suggestions(self) -> List[str]:
        """Generate optimization suggestions based on performance data"""
        suggestions = []
        
        if not self.metrics:
            return ["No performance data available for analysis"]
        
        total_duration = sum(m.duration_seconds for m in self.metrics)
        avg_duration = total_duration / len(self.metrics)
        
        # Analyze patterns
        table_operations = [m for m in self.metrics if 'table' in m.operation.lower()]
        file_operations = [m for m in self.metrics if 'file' in m.operation.lower()]
        
        # Duration-based suggestions
        if avg_duration > 300:  # 5 minutes
            suggestions.append("⚡ Consider enabling parallel processing to reduce backup time")
            suggestions.append("📊 Large tables detected - consider using incremental backup for frequently updated tables")
        
        # Memory-based suggestions
        if any(m.memory_peak_mb > 8000 for m in self.metrics):  # 8GB
            suggestions.append("🧠 High memory usage detected - consider reducing max_table_rows_in_zip parameter")
            suggestions.append("💾 Enable streaming processing for large datasets")
        
        # Error-based suggestions
        error_rate = sum(m.error_count for m in self.metrics) / len(self.metrics)
        if error_rate > 0.1:  # 10% error rate
            suggestions.append("🚨 High error rate detected - check network connectivity and permissions")
            suggestions.append("🔄 Enable retry logic with exponential backoff")
        
        # File-specific suggestions
        if file_operations:
            avg_file_duration = sum(f.duration_seconds for f in file_operations) / len(file_operations)
            if avg_file_duration > 60:  # 1 minute per file
                suggestions.append("📁 File processing is slow - consider filtering out large/unnecessary files")
                suggestions.append("🗜️ Increase compression level to reduce transfer time")
        
        # Table-specific suggestions
        if table_operations:
            slow_tables = [t for t in table_operations if t.duration_seconds > 180]  # 3 minutes
            if slow_tables:
                suggestions.append(f"📋 {len(slow_tables)} slow tables detected - consider partitioning large tables")
                suggestions.append("🎯 Use selective backup for tables that don't change frequently")
        
        return suggestions if suggestions else ["✅ Performance looks good! No specific optimizations needed."]

def create_performance_report(monitor: PerformanceMonitor, output_path: str = None) -> str:
    """Create a detailed performance report"""
    
    report_data = {
        "report_generated": datetime.datetime.now().isoformat(),
        "performance_summary": monitor.get_summary_report(),
        "optimization_suggestions": monitor.get_optimization_suggestions(),
        "detailed_metrics": [asdict(m) for m in monitor.metrics]
    }
    
    if output_path:
        with open(output_path, 'w') as f:
            json.dump(report_data, f, indent=2)
    
    # Generate markdown report
    summary = report_data["performance_summary"]["summary"]
    suggestions = report_data["optimization_suggestions"]
    
    markdown_report = f"""# Backup Performance Report

Generated: {report_data["report_generated"]}

## Summary
- **Total Operations**: {summary["total_operations"]}
- **Total Duration**: {summary["total_duration_seconds"]} seconds
- **Rows Processed**: {summary["total_rows_processed"]:,}
- **Data Processed**: {summary["total_mb_processed"]:.2f} MB
- **Processing Rate**: {summary["processing_rate"]["rows_per_second"]:.0f} rows/sec, {summary["processing_rate"]["mb_per_second"]:.2f} MB/sec
- **Success Rate**: {summary["quality_metrics"]["success_rate"]:.1f}%

## Performance Issues
- **Errors**: {summary["quality_metrics"]["total_errors"]}
- **Warnings**: {summary["quality_metrics"]["total_warnings"]}

## Optimization Suggestions
"""
    
    for suggestion in suggestions:
        markdown_report += f"- {suggestion}\\n"
    
    if "slowest_operations" in report_data["performance_summary"]:
        markdown_report += "\\n## Slowest Operations\\n"
        for op in report_data["performance_summary"]["slowest_operations"]:
            markdown_report += f"- **{op['operation']}**: {op['duration_seconds']}s ({op['rows_processed']:,} rows, {op['mb_processed']:.1f} MB)\\n"
    
    return markdown_report

# Example usage in notebooks:
"""
# Add this to your backup notebook:

# Initialize performance monitor
performance_monitor = PerformanceMonitor()

# Monitor table discovery
performance_monitor.start_operation("table_discovery")
tables = discover_tables(source_paths['tables'])
performance_monitor.end_operation(rows_processed=len(tables))

# Monitor table backup
for table_name in tables:
    performance_monitor.start_operation(f"backup_table_{table_name}")
    # ... table backup code ...
    performance_monitor.end_operation(rows_processed=row_count, bytes_processed=table_size)

# Monitor file backup
performance_monitor.start_operation("file_backup")
# ... file backup code ...
performance_monitor.end_operation(bytes_processed=total_file_size)

# Generate performance report
report = create_performance_report(performance_monitor)
print(report)

# Save detailed report
performance_monitor.get_summary_report()
"""
