#!/usr/bin/env python3
"""
Path repository - handles path operations.
"""

import sqlite3
from typing import Optional


class PathRepository:
    """Repository for path operations."""
    
    def __init__(self, conn: sqlite3.Connection, retry_operation):
        """Initialize path repository.
        
        Args:
            conn: SQLite database connection
            retry_operation: Function to retry database operations
        """
        self.conn = conn
        self._retry_db_operation = retry_operation
    
    def record_path(self, file_id: int, path: str, path_type: str = "original") -> None:
        """Record a path entry if it doesn't already exist."""
        def _do_record():
            self.conn.execute(
                """
                INSERT OR IGNORE INTO paths (file_id, path_type, path)
                VALUES (?, ?, ?);
                """,
                (file_id, path_type, path),
            )
            self.conn.commit()
        
        self._retry_db_operation(_do_record)
    
    def consolidated_path_exists(self, consolidated_path: str) -> bool:
        """Check if a consolidated path already exists in the database."""
        def _do_check():
            row = self.conn.execute(
                """
                SELECT 1 FROM paths 
                WHERE path_type = 'consolidated' AND path = ?
                LIMIT 1;
                """,
                (consolidated_path,),
            ).fetchone()
            return row is not None
        
        return self._retry_db_operation(_do_check)
    
    def update_consolidated_path(self, file_id: int, consolidated_path: str) -> None:
        """Record consolidated path in paths table."""
        def _do_update():
            self.conn.execute(
                """
                INSERT OR IGNORE INTO paths (file_id, path_type, path)
                VALUES (?, 'consolidated', ?);
                """,
                (file_id, consolidated_path),
            )
            self.conn.commit()
        
        self._retry_db_operation(_do_update)
    
    def update_symlink_path(self, file_id: int, symlink_path: str) -> None:
        """Record symlink path in paths table."""
        def _do_update():
            self.conn.execute(
                """
                INSERT OR IGNORE INTO paths (file_id, path_type, path)
                VALUES (?, 'symlink', ?);
                """,
                (file_id, symlink_path),
            )
            self.conn.commit()
        
        self._retry_db_operation(_do_update)
    
    def get_master_file_path(self, file_id: int) -> Optional[str]:
        """Get the consolidated path for a master file, or None if not found."""
        def _do_query():
            row = self.conn.execute(
                """
                SELECT path FROM paths
                WHERE file_id = ? AND path_type = 'consolidated'
                LIMIT 1;
                """,
                (file_id,),
            ).fetchone()
            return row["path"] if row else None
        
        return self._retry_db_operation(_do_query)
