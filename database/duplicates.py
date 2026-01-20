#!/usr/bin/env python3
"""
Duplicate repository - handles duplicate tracking operations.
"""

import sqlite3
from typing import List

from config import utc_now


class DuplicateRepository:
    """Repository for duplicate tracking operations."""
    
    def __init__(self, conn: sqlite3.Connection, retry_operation):
        """Initialize duplicate repository.
        
        Args:
            conn: SQLite database connection
            retry_operation: Function to retry database operations
        """
        self.conn = conn
        self._retry_db_operation = retry_operation
    
    def handle_duplicate_hash(self, file_hash: str) -> None:
        """Update duplicate status and group data for a given hash."""
        if not file_hash or file_hash in {"Error", "Skipped"}:
            return

        def _do_handle():
            rows = self.conn.execute(
                """
                SELECT id, file_size_bytes
                FROM files
                WHERE file_hash = ?
                ORDER BY id ASC;
                """,
                (file_hash,),
            ).fetchall()

            if len(rows) <= 1:
                # No duplicates remain for this hash.
                self.conn.execute(
                    "DELETE FROM duplicate_groups WHERE group_hash = ?;",
                    (file_hash,),
                )
                self.conn.execute(
                    """
                    UPDATE files
                    SET is_duplicate = 0,
                        master_file_id = NULL
                    WHERE file_hash = ?;
                    """,
                    (file_hash,),
                )
                self.conn.commit()
                return

            master_id = rows[0]["id"]
            total_size = sum(row["file_size_bytes"] for row in rows)
            master_size = rows[0]["file_size_bytes"]
            duplicate_count = len(rows) - 1
            space_saved = total_size - master_size

            self.conn.execute(
                """
                INSERT INTO duplicate_groups (
                    group_hash,
                    file_size_bytes,
                    master_file_id,
                    duplicate_count,
                    total_size_bytes,
                    space_saved_bytes
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(group_hash) DO UPDATE SET
                    master_file_id=excluded.master_file_id,
                    duplicate_count=excluded.duplicate_count,
                    total_size_bytes=excluded.total_size_bytes,
                    space_saved_bytes=excluded.space_saved_bytes;
                """,
                (
                    file_hash,
                    rows[0]["file_size_bytes"],
                    master_id,
                    duplicate_count,
                    total_size,
                    space_saved,
                ),
            )

            for row in rows:
                is_duplicate = 0 if row["id"] == master_id else 1
                master_file_id = None if row["id"] == master_id else master_id
                self.conn.execute(
                    """
                    UPDATE files
                    SET is_duplicate = ?,
                        master_file_id = ?
                    WHERE id = ?;
                    """,
                    (is_duplicate, master_file_id, row["id"]),
                )

            self.conn.commit()
        
        self._retry_db_operation(_do_handle)
    
    def get_duplicate_group_files(self, group_hash: str) -> List:
        """Get all files in a duplicate group by group hash.
        
        Returns list of Row objects with id, original_path, file_name, 
        is_duplicate, master_file_id, file_hash, and deduplication_status.
        """
        def _do_query():
            return self.conn.execute(
                """
                SELECT id, original_path, file_name, file_size_bytes, file_hash,
                       is_duplicate, master_file_id, deduplication_status
                FROM files
                WHERE file_hash = ?
                ORDER BY id ASC;
                """,
                (group_hash,),
            ).fetchall()
        
        return self._retry_db_operation(_do_query)
    
    def get_duplicate_groups_for_processing(self) -> List:
        """Get all duplicate groups that need processing.
        
        Returns list of Row objects with id, group_hash, master_file_id, 
        duplicate_count, and deduplicated_at.
        """
        def _do_query():
            return self.conn.execute(
                """
                SELECT id, group_hash, master_file_id, duplicate_count, deduplicated_at
                FROM duplicate_groups
                WHERE duplicate_count > 0
                ORDER BY id ASC;
                """
            ).fetchall()
        
        return self._retry_db_operation(_do_query)
    
    def update_group_deduplicated(self, group_hash: str) -> None:
        """Set deduplicated_at timestamp in duplicate_groups table."""
        def _do_update():
            self.conn.execute(
                """
                UPDATE duplicate_groups
                SET deduplicated_at = ?
                WHERE group_hash = ?;
                """,
                (utc_now(), group_hash),
            )
            self.conn.commit()
        
        self._retry_db_operation(_do_update)
