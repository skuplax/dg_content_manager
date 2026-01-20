#!/usr/bin/env python3
"""
Statistics repository - handles statistics operations.
"""

import sqlite3
from typing import Dict

from config import utc_now


class StatisticsRepository:
    """Repository for statistics operations."""
    
    def __init__(self, conn: sqlite3.Connection, retry_operation):
        """Initialize statistics repository.
        
        Args:
            conn: SQLite database connection
            retry_operation: Function to retry database operations
        """
        self.conn = conn
        self._retry_db_operation = retry_operation
    
    def _get_stat_value(self, stat_name: str, default: int = 0) -> int:
        """Get a statistic value from the database, returning default if not found."""
        def _do_query():
            row = self.conn.execute(
                "SELECT stat_value FROM statistics WHERE stat_name = ?;",
                (stat_name,),
            ).fetchone()
            if row:
                try:
                    return int(row["stat_value"])
                except (ValueError, TypeError):
                    return default
            return default
        
        return self._retry_db_operation(_do_query)
    
    def update_statistics(self) -> None:
        """Recalculate and persist aggregate statistics."""
        def _do_update():
            stats_cursor = self.conn.cursor()

            total_files = stats_cursor.execute(
                "SELECT COUNT(*) AS c FROM files;"
            ).fetchone()["c"]

            total_size = stats_cursor.execute(
                "SELECT COALESCE(SUM(file_size_bytes), 0) AS s FROM files;"
            ).fetchone()["s"]

            duplicate_files = stats_cursor.execute(
                "SELECT COUNT(*) AS c FROM files WHERE is_duplicate = 1;"
            ).fetchone()["c"]

            unique_files = total_files - duplicate_files

            duplicate_groups = stats_cursor.execute(
                "SELECT COUNT(*) AS c FROM duplicate_groups WHERE duplicate_count > 0;"
            ).fetchone()["c"]

            space_saved = stats_cursor.execute(
                "SELECT COALESCE(SUM(space_saved_bytes), 0) AS s FROM duplicate_groups;"
            ).fetchone()["s"]

            space_saved_pct = (space_saved / total_size * 100) if total_size else 0

            last_scan = stats_cursor.execute(
                "SELECT MAX(scan_timestamp) AS ts FROM files;"
            ).fetchone()["ts"]

            stats = {
                "total_files": (total_files, "integer"),
                "total_size_bytes": (total_size, "integer"),
                "unique_files": (unique_files, "integer"),
                "duplicate_files": (duplicate_files, "integer"),
                "duplicate_groups": (duplicate_groups, "integer"),
                "space_saved_bytes": (space_saved, "integer"),
                "space_saved_percentage": (space_saved_pct, "float"),
                "files_deduplicated": (self._get_stat_value("files_deduplicated", 0), "integer"),
                "symlinks_created": (self._get_stat_value("symlinks_created", 0), "integer"),
                "files_consolidated": (self._get_stat_value("files_consolidated", 0), "integer"),
                "last_scan_timestamp": (last_scan or "", "datetime"),
            }

            now = utc_now()
            for name, (value, stat_type) in stats.items():
                self.conn.execute(
                    """
                    INSERT INTO statistics (stat_name, stat_value, stat_type, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(stat_name) DO UPDATE SET
                        stat_value = excluded.stat_value,
                        stat_type = excluded.stat_type,
                        updated_at = excluded.updated_at;
                    """,
                    (name, str(value), stat_type, now),
                )

            self.conn.commit()
        
        self._retry_db_operation(_do_update)
    
    def update_deduplication_statistics(self, files_consolidated: int, symlinks_created: int, files_deduplicated: int) -> None:
        """Update deduplication statistics in statistics table."""
        def _do_update():
            now = utc_now()
            
            # Update or insert statistics
            stats = {
                "files_consolidated": (files_consolidated, "integer"),
                "symlinks_created": (symlinks_created, "integer"),
                "files_deduplicated": (files_deduplicated, "integer"),
            }
            
            for name, (value, stat_type) in stats.items():
                self.conn.execute(
                    """
                    INSERT INTO statistics (stat_name, stat_value, stat_type, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(stat_name) DO UPDATE SET
                        stat_value = excluded.stat_value,
                        stat_type = excluded.stat_type,
                        updated_at = excluded.updated_at;
                    """,
                    (name, str(value), stat_type, now),
                )
            
            self.conn.commit()
        
        self._retry_db_operation(_do_update)
