#!/usr/bin/env python3
"""
SQLite catalog management for video file scanning and deduplication prep.

Creates and manages the dg_catalog.db inside the hidden consolidation folder
located at <scan_root>/.dg_consolidation by default.
"""

import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

CONSOLIDATION_DIR_NAME = ".dg_consolidation"
DB_FILENAME = "dg_catalog.db"


def _utc_now() -> str:
    """Return current UTC timestamp as ISO string."""
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()


def _ensure_hidden_directory(directory: Path) -> None:
    """Create directory if missing and attempt to hide it from Finder/Explorer."""
    directory.mkdir(parents=True, exist_ok=True)

    if sys.platform == "darwin":
        subprocess.run(
            ["chflags", "hidden", str(directory)],
            check=False,
            capture_output=True,
        )
    elif sys.platform.startswith("win"):
        subprocess.run(
            ["attrib", "+H", str(directory)],
            check=False,
            capture_output=True,
        )


class CatalogDatabase:
    """High-level helper for interacting with the catalog database."""

    def __init__(self, conn: sqlite3.Connection, db_path: Path, consolidation_dir: Path):
        self.conn = conn
        self.db_path = db_path
        self.consolidation_dir = consolidation_dir

    @classmethod
    def initialize(
        cls,
        scan_root: Path,
        consolidation_root: Optional[Path] = None,
        db_path: Optional[Path] = None,
    ) -> "CatalogDatabase":
        """Initialize database, creating directories and tables as needed."""
        scan_root = scan_root.resolve()
        consolidation_root = consolidation_root.resolve() if consolidation_root else scan_root

        hidden_dir = consolidation_root / CONSOLIDATION_DIR_NAME
        _ensure_hidden_directory(hidden_dir)

        final_db_path = Path(db_path).resolve() if db_path else hidden_dir / DB_FILENAME
        final_db_path.parent.mkdir(parents=True, exist_ok=True)

        # Add timeout to prevent locking issues (30 seconds)
        # Enable WAL mode for better concurrency and reliability
        conn = sqlite3.connect(final_db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")

        catalog = cls(conn, final_db_path, hidden_dir)
        catalog._create_tables()
        return catalog

    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()

    def _reconnect(self) -> None:
        """Recreate the database connection if it's in a bad state."""
        try:
            self.conn.close()
        except Exception:
            pass
        
        # Ensure the database directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Small delay to allow filesystem to stabilize
        time.sleep(0.1)
        
        self.conn = sqlite3.connect(self.db_path, timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.execute("PRAGMA journal_mode = WAL;")

    def _retry_db_operation(self, operation, max_retries=5, retry_delay=0.5):
        """Retry a database operation if it fails due to locking/timeout issues."""
        for attempt in range(max_retries):
            try:
                return operation()
            except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
                error_str = str(e).lower()
                # Check for various database connection/locking errors
                is_retryable = (
                    "unable to open database file" in error_str or
                    "database is locked" in error_str or
                    "database disk image is malformed" in error_str or
                    "no such table" in error_str  # In case connection was lost during table creation
                )
                
                if is_retryable:
                    if attempt < max_retries - 1:
                        # Always reconnect on "unable to open database file" errors
                        if "unable to open database file" in error_str:
                            self._reconnect()
                        else:
                            # For other errors, check connection first
                            try:
                                self.conn.execute("SELECT 1")
                            except (sqlite3.OperationalError, sqlite3.ProgrammingError, sqlite3.DatabaseError):
                                # Connection is bad, recreate it
                                self._reconnect()
                        
                        # Exponential backoff with longer delays
                        time.sleep(retry_delay * (2 ** attempt))
                        continue
                raise
            except Exception as e:
                # For any other exception, check if it's connection-related
                error_str = str(e).lower()
                if "unable to open" in error_str or "database" in error_str:
                    if attempt < max_retries - 1:
                        self._reconnect()
                        time.sleep(retry_delay * (2 ** attempt))
                        continue
                raise
        raise

    # ------------------------------------------------------------------
    # Table creation
    # ------------------------------------------------------------------
    def _create_tables(self) -> None:
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

    # ------------------------------------------------------------------
    # File + path recording
    # ------------------------------------------------------------------
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
        scan_ts = _utc_now()
        
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

    # ------------------------------------------------------------------
    # Hash + duplicate tracking
    # ------------------------------------------------------------------
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
                (file_hash, _utc_now(), file_id),
            )
            self.conn.commit()
        
        self._retry_db_operation(_do_update)

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

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------
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
                "files_deduplicated": (0, "integer"),
                "symlinks_created": (0, "integer"),
                "last_scan_timestamp": (last_scan or "", "datetime"),
            }

            now = _utc_now()
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


