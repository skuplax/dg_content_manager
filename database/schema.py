#!/usr/bin/env python3
"""
Database schema management - table creation and migrations.
"""

import sqlite3


class SchemaManager:
    """Manages database schema creation and updates."""
    
    def __init__(self, conn: sqlite3.Connection):
        """Initialize schema manager.
        
        Args:
            conn: SQLite database connection
        """
        self.conn = conn
    
    def create_tables(self) -> None:
        """Create all database tables and indexes if they don't exist."""
        cursor = self.conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_path TEXT NOT NULL UNIQUE,
                file_name TEXT NOT NULL,
                file_size_bytes INTEGER NOT NULL,
                file_hash TEXT,
                created_at TEXT,
                year TEXT,
                month TEXT,
                month_day TEXT,
                project_name TEXT,
                scan_timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                is_duplicate INTEGER NOT NULL DEFAULT 0,
                master_file_id INTEGER,
                deduplication_status TEXT NOT NULL DEFAULT 'not_processed',
                deduplication_timestamp TEXT,
                FOREIGN KEY(master_file_id) REFERENCES files(id) ON DELETE SET NULL
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS paths (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id INTEGER NOT NULL,
                path_type TEXT NOT NULL,
                path TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(file_id, path_type, path),
                FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS duplicate_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_hash TEXT NOT NULL UNIQUE,
                file_size_bytes INTEGER NOT NULL,
                master_file_id INTEGER,
                duplicate_count INTEGER NOT NULL DEFAULT 0,
                total_size_bytes INTEGER NOT NULL DEFAULT 0,
                space_saved_bytes INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                deduplicated_at TEXT,
                FOREIGN KEY(master_file_id) REFERENCES files(id) ON DELETE SET NULL
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS statistics (
                stat_name TEXT PRIMARY KEY,
                stat_value TEXT NOT NULL,
                stat_type TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_files_hash ON files(file_hash);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_files_size ON files(file_size_bytes);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_files_master ON files(master_file_id);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_paths_file ON paths(file_id);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_paths_type ON paths(path_type);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_dup_groups_master ON duplicate_groups(master_file_id);"
        )

        self.conn.commit()
