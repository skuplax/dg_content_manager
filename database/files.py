#!/usr/bin/env python3
"""
File repository - handles file CRUD operations.
"""

import sqlite3
from typing import List, Optional

from config import utc_now


class FileRepository:
    """Repository for file operations."""
    
    def __init__(self, conn: sqlite3.Connection, retry_operation):
        """Initialize file repository.
        
        Args:
            conn: SQLite database connection
            retry_operation: Function to retry database operations
        """
        self.conn = conn
        self._retry_db_operation = retry_operation
    
    def record_file(
        self,
        *,
        original_path: str,
        file_name: str,
        file_size_bytes: int,
        created_at: Optional[str],
        year: str,
        month: str,
        month_day: str,
        project_name: str,
    ) -> int:
        """Insert or update a file record and return its id."""
        scan_ts = utc_now()
        
        def _do_record():
            cursor = self.conn.execute(
                """
                INSERT INTO files (
                    original_path,
                    file_name,
                    file_size_bytes,
                    created_at,
                    year,
                    month,
                    month_day,
                    project_name,
                    scan_timestamp
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(original_path) DO UPDATE SET
                    file_name=excluded.file_name,
                    file_size_bytes=excluded.file_size_bytes,
                    created_at=excluded.created_at,
                    year=excluded.year,
                    month=excluded.month,
                    month_day=excluded.month_day,
                    project_name=excluded.project_name,
                    scan_timestamp=excluded.scan_timestamp;
                """,
                (
                    original_path,
                    file_name,
                    file_size_bytes,
                    created_at,
                    year,
                    month,
                    month_day,
                    project_name,
                    scan_ts,
                ),
            )

            if cursor.lastrowid:
                file_id = cursor.lastrowid
            else:
                row = self.conn.execute(
                    "SELECT id FROM files WHERE original_path = ?;",
                    (original_path,),
                ).fetchone()
                file_id = row["id"]

            self.conn.commit()
            return file_id
        
        return self._retry_db_operation(_do_record)
    
    def file_exists_by_path(self, original_path: str) -> bool:
        """Check if a file with this path already exists in the database."""
        def _do_check():
            row = self.conn.execute(
                "SELECT 1 FROM files WHERE original_path = ? LIMIT 1;",
                (original_path,),
            ).fetchone()
            return row is not None
        
        return self._retry_db_operation(_do_check)
    
    def find_files_by_size(self, file_size_bytes: int) -> List:
        """Query database for all files with the same size.
        
        Returns list of Row objects with id, original_path, file_name, 
        file_size_bytes, and file_hash fields.
        """
        def _do_query():
            return self.conn.execute(
                """
                SELECT id, original_path, file_name, file_size_bytes, file_hash
                FROM files
                WHERE file_size_bytes = ?
                ORDER BY id ASC;
                """,
                (file_size_bytes,),
            ).fetchall()
        
        return self._retry_db_operation(_do_query)
    
    def update_file_hash(self, file_id: int, file_hash: str) -> None:
        """Set the hash for a file record."""
        if not file_hash or file_hash in {"Error", "Skipped"}:
            return

        def _do_update():
            self.conn.execute(
                """
                UPDATE files
                SET file_hash = ?, scan_timestamp = ?
                WHERE id = ?;
                """,
                (file_hash, utc_now(), file_id),
            )
            self.conn.commit()
        
        self._retry_db_operation(_do_update)
    
    def get_files_for_deduplication(self) -> List:
        """Get all files that need deduplication processing.
        
        Returns list of Row objects for files where deduplication_status = 'not_processed'.
        Includes both unique files and master files from duplicate groups.
        """
        def _do_query():
            return self.conn.execute(
                """
                SELECT id, original_path, file_name, file_size_bytes, file_hash,
                       is_duplicate, master_file_id, deduplication_status
                FROM files
                WHERE deduplication_status = 'not_processed'
                ORDER BY id ASC;
                """
            ).fetchall()
        
        return self._retry_db_operation(_do_query)
    
    def mark_deduplicated(self, file_id: int, status: str = 'deduplicated') -> None:
        """Update deduplication_status and deduplication_timestamp for a file."""
        def _do_update():
            self.conn.execute(
                """
                UPDATE files
                SET deduplication_status = ?,
                    deduplication_timestamp = ?
                WHERE id = ?;
                """,
                (status, utc_now(), file_id),
            )
            self.conn.commit()
        
        self._retry_db_operation(_do_update)
