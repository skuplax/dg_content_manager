#!/usr/bin/env python3
"""
Report Generator Module
Generates markdown reports from the catalog database with comprehensive metrics.
"""

from typing import Dict, List, Tuple
from database import CatalogDatabase


def format_bytes(bytes_value: int) -> str:
    """Format bytes into human-readable format (KB, MB, GB, TB)."""
    if bytes_value == 0:
        return "0 B"
    
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"


def format_percentage(value: float, total: float) -> str:
    """Format a percentage value."""
    if total == 0:
        return "0.00%"
    return f"{(value / total * 100):.2f}%"


class ReportGenerator:
    """Generates markdown reports from catalog database."""
    
    def __init__(self, catalog_db: CatalogDatabase):
        """Initialize report generator with database connection."""
        self.db = catalog_db
    
    def get_total_projects(self) -> int:
        """Get total number of unique projects scanned."""
        row = self.db.conn.execute(
            "SELECT COUNT(DISTINCT project_name) AS count FROM files WHERE project_name IS NOT NULL;"
        ).fetchone()
        return row["count"] if row else 0
    
    def get_total_videos(self) -> int:
        """Get total number of videos cataloged."""
        row = self.db.conn.execute(
            "SELECT COUNT(*) AS count FROM files;"
        ).fetchone()
        return row["count"] if row else 0
    
    def get_duplicate_stats(self) -> Tuple[int, int, float]:
        """Get duplicate statistics: (unique_count, duplicate_count, duplicate_percentage)."""
        total = self.get_total_videos()
        if total == 0:
            return (0, 0, 0.0)
        
        duplicate_row = self.db.conn.execute(
            "SELECT COUNT(*) AS count FROM files WHERE is_duplicate = 1;"
        ).fetchone()
        duplicate_count = duplicate_row["count"] if duplicate_row else 0
        unique_count = total - duplicate_count
        duplicate_percentage = (duplicate_count / total * 100) if total > 0 else 0.0
        
        return (unique_count, duplicate_count, duplicate_percentage)
    
    def get_total_size(self) -> int:
        """Get total file size in bytes."""
        row = self.db.conn.execute(
            "SELECT COALESCE(SUM(file_size_bytes), 0) AS total FROM files;"
        ).fetchone()
        return row["total"] if row else 0
    
    def get_space_saved(self) -> Tuple[int, float]:
        """Get space saved by deduplication: (bytes_saved, percentage_saved)."""
        total_size = self.get_total_size()
        if total_size == 0:
            return (0, 0.0)
        
        row = self.db.conn.execute(
            "SELECT COALESCE(SUM(space_saved_bytes), 0) AS saved FROM duplicate_groups;"
        ).fetchone()
        bytes_saved = row["saved"] if row else 0
        percentage_saved = (bytes_saved / total_size * 100) if total_size > 0 else 0.0
        
        return (bytes_saved, percentage_saved)
    
    def get_duplicate_groups_count(self) -> int:
        """Get number of duplicate groups."""
        row = self.db.conn.execute(
            "SELECT COUNT(*) AS count FROM duplicate_groups WHERE duplicate_count > 0;"
        ).fetchone()
        return row["count"] if row else 0
    
    def get_breakdown_by_year(self) -> List[Dict]:
        """Get breakdown of files by year."""
        rows = self.db.conn.execute(
            """
            SELECT 
                year,
                COUNT(*) AS file_count,
                COALESCE(SUM(file_size_bytes), 0) AS total_size,
                SUM(CASE WHEN is_duplicate = 1 THEN 1 ELSE 0 END) AS duplicate_count
            FROM files
            WHERE year IS NOT NULL
            GROUP BY year
            ORDER BY year DESC;
            """
        ).fetchall()
        
        return [
            {
                "year": row["year"],
                "file_count": row["file_count"],
                "total_size": row["total_size"],
                "duplicate_count": row["duplicate_count"]
            }
            for row in rows
        ]
    
    def get_breakdown_by_month(self) -> List[Dict]:
        """Get breakdown of files by year and month."""
        rows = self.db.conn.execute(
            """
            SELECT 
                year,
                month,
                COUNT(*) AS file_count,
                COALESCE(SUM(file_size_bytes), 0) AS total_size,
                SUM(CASE WHEN is_duplicate = 1 THEN 1 ELSE 0 END) AS duplicate_count
            FROM files
            WHERE year IS NOT NULL AND month IS NOT NULL
            GROUP BY year, month
            ORDER BY year DESC, month DESC;
            """
        ).fetchall()
        
        return [
            {
                "year": row["year"],
                "month": row["month"],
                "file_count": row["file_count"],
                "total_size": row["total_size"],
                "duplicate_count": row["duplicate_count"]
            }
            for row in rows
        ]
    
    def get_top_projects_by_count(self, limit: int = 10) -> List[Dict]:
        """Get top projects by file count."""
        rows = self.db.conn.execute(
            """
            SELECT 
                project_name,
                COUNT(*) AS file_count,
                COALESCE(SUM(file_size_bytes), 0) AS total_size,
                SUM(CASE WHEN is_duplicate = 1 THEN 1 ELSE 0 END) AS duplicate_count
            FROM files
            WHERE project_name IS NOT NULL
            GROUP BY project_name
            ORDER BY file_count DESC
            LIMIT ?;
            """,
            (limit,)
        ).fetchall()
        
        return [
            {
                "project_name": row["project_name"],
                "file_count": row["file_count"],
                "total_size": row["total_size"],
                "duplicate_count": row["duplicate_count"]
            }
            for row in rows
        ]
    
    def get_top_projects_by_size(self, limit: int = 10) -> List[Dict]:
        """Get top projects by total size."""
        rows = self.db.conn.execute(
            """
            SELECT 
                project_name,
                COUNT(*) AS file_count,
                COALESCE(SUM(file_size_bytes), 0) AS total_size,
                SUM(CASE WHEN is_duplicate = 1 THEN 1 ELSE 0 END) AS duplicate_count
            FROM files
            WHERE project_name IS NOT NULL
            GROUP BY project_name
            ORDER BY total_size DESC
            LIMIT ?;
            """,
            (limit,)
        ).fetchall()
        
        return [
            {
                "project_name": row["project_name"],
                "file_count": row["file_count"],
                "total_size": row["total_size"],
                "duplicate_count": row["duplicate_count"]
            }
            for row in rows
        ]
    
    def get_average_file_size(self) -> float:
        """Get average file size in bytes."""
        row = self.db.conn.execute(
            "SELECT COALESCE(AVG(file_size_bytes), 0) AS avg_size FROM files;"
        ).fetchone()
        return row["avg_size"] if row else 0.0
    
    def get_deduplication_status_breakdown(self) -> Dict[str, int]:
        """Get breakdown of files by deduplication status."""
        rows = self.db.conn.execute(
            """
            SELECT 
                deduplication_status,
                COUNT(*) AS count
            FROM files
            GROUP BY deduplication_status
            ORDER BY count DESC;
            """
        ).fetchall()
        
        return {row["deduplication_status"]: row["count"] for row in rows}
    
    def generate_report(self) -> str:
        """Generate a comprehensive markdown report."""
        # Collect all metrics
        total_projects = self.get_total_projects()
        total_videos = self.get_total_videos()
        unique_count, duplicate_count, duplicate_percentage = self.get_duplicate_stats()
        total_size = self.get_total_size()
        space_saved_bytes, space_saved_pct = self.get_space_saved()
        duplicate_groups = self.get_duplicate_groups_count()
        avg_file_size = self.get_average_file_size()
        
        # Build markdown report
        report = []
        report.append("# Video Catalog Report\n")
        report.append(f"*Generated: {self._get_current_timestamp()}*\n")
        report.append("---\n")
        
        # Executive Summary
        report.append("## Executive Summary\n")
        report.append(f"- **Total Projects Scanned**: {total_projects:,}")
        report.append(f"- **Total Videos Cataloged**: {total_videos:,}")
        report.append(f"- **Unique Videos**: {unique_count:,} ({format_percentage(unique_count, total_videos)})")
        report.append(f"- **Duplicate Videos**: {duplicate_count:,} ({format_percentage(duplicate_count, total_videos)})")
        report.append(f"- **Total Storage**: {format_bytes(total_size)}")
        report.append(f"- **Space Saved (Deduplication)**: {format_bytes(space_saved_bytes)} ({space_saved_pct:.2f}%)")
        report.append(f"- **Duplicate Groups**: {duplicate_groups:,}")
        report.append(f"- **Average File Size**: {format_bytes(avg_file_size)}\n")
        
        # Detailed Statistics
        report.append("## Detailed Statistics\n")
        report.append("### File Overview\n")
        report.append(f"| Metric | Value |")
        report.append(f"|--------|-------|")
        report.append(f"| Total Videos | {total_videos:,} |")
        report.append(f"| Unique Videos | {unique_count:,} ({format_percentage(unique_count, total_videos)}) |")
        report.append(f"| Duplicate Videos | {duplicate_count:,} ({format_percentage(duplicate_count, total_videos)}) |")
        report.append(f"| Total Storage Size | {format_bytes(total_size)} |")
        report.append(f"| Average File Size | {format_bytes(avg_file_size)} |\n")
        
        report.append("### Deduplication Statistics\n")
        report.append(f"| Metric | Value |")
        report.append(f"|--------|-------|")
        report.append(f"| Duplicate Groups | {duplicate_groups:,} |")
        report.append(f"| Space Saved | {format_bytes(space_saved_bytes)} |")
        report.append(f"| Space Saved Percentage | {space_saved_pct:.2f}% |\n")
        
        # Breakdown by Year
        year_breakdown = self.get_breakdown_by_year()
        if year_breakdown:
            report.append("## Breakdown by Year\n")
            report.append("| Year | Files | Total Size | Duplicates |")
            report.append("|------|-------|------------|------------|")
            for item in year_breakdown:
                report.append(
                    f"| {item['year']} | {item['file_count']:,} | "
                    f"{format_bytes(item['total_size'])} | {item['duplicate_count']:,} |"
                )
            report.append("")
        
        # Breakdown by Month
        month_breakdown = self.get_breakdown_by_month()
        if month_breakdown:
            report.append("## Breakdown by Month\n")
            report.append("| Year | Month | Files | Total Size | Duplicates |")
            report.append("|------|-------|-------|------------|------------|")
            for item in month_breakdown:
                report.append(
                    f"| {item['year']} | {item['month']} | {item['file_count']:,} | "
                    f"{format_bytes(item['total_size'])} | {item['duplicate_count']:,} |"
                )
            report.append("")
        
        # Top Projects by File Count
        top_by_count = self.get_top_projects_by_count(10)
        if top_by_count:
            report.append("## Top 10 Projects by File Count\n")
            report.append("| Project Name | Files | Total Size | Duplicates |")
            report.append("|--------------|-------|------------|------------|")
            for item in top_by_count:
                report.append(
                    f"| {item['project_name']} | {item['file_count']:,} | "
                    f"{format_bytes(item['total_size'])} | {item['duplicate_count']:,} |"
                )
            report.append("")
        
        # Top Projects by Size
        top_by_size = self.get_top_projects_by_size(10)
        if top_by_size:
            report.append("## Top 10 Projects by Total Size\n")
            report.append("| Project Name | Files | Total Size | Duplicates |")
            report.append("|--------------|-------|------------|------------|")
            for item in top_by_size:
                report.append(
                    f"| {item['project_name']} | {item['file_count']:,} | "
                    f"{format_bytes(item['total_size'])} | {item['duplicate_count']:,} |"
                )
            report.append("")
        
        # Deduplication Status
        status_breakdown = self.get_deduplication_status_breakdown()
        if status_breakdown:
            report.append("## Files by Deduplication Status\n")
            report.append("| Status | Count |")
            report.append("|--------|-------|")
            for status, count in status_breakdown.items():
                report.append(f"| {status} | {count:,} |")
            report.append("")
        
        return "\n".join(report)
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp as formatted string."""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

