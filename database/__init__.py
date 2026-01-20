#!/usr/bin/env python3
"""
Database package - CatalogDatabase facade for backward compatibility.
"""

from pathlib import Path
from typing import List, Optional

from config import CONSOLIDATION_DIR_NAME, DB_FILENAME

from .connection import DatabaseConnection, ensure_hidden_directory
from .schema import SchemaManager
from .files import FileRepository
from .paths import PathRepository
from .duplicates import DuplicateRepository
from .statistics import StatisticsRepository


class CatalogDatabase:
    """High-level helper for interacting with the catalog database.
    
    This facade maintains backward compatibility with the original CatalogDatabase API
    while delegating to specialized repository classes.
    """
    
    def __init__(self, conn, db_path: Path, consolidation_dir: Path):
        """Initialize catalog database.
        
        Args:
            conn: SQLite connection object
            db_path: Path to database file
            consolidation_dir: Path to consolidation directory
        """
        self.conn = conn
        self.db_path = db_path
        self.consolidation_dir = consolidation_dir
        
        # Create connection wrapper for retry logic (but use existing connection)
        self._db_conn = DatabaseConnection(db_path)
        # Close the connection that DatabaseConnection created and use the existing one
        try:
            self._db_conn.close()
        except Exception:
            pass
        self._db_conn.conn = conn  # Use existing connection
        
        # Initialize repositories
        self._files = FileRepository(conn, self._db_conn.retry_operation)
        self._paths = PathRepository(conn, self._db_conn.retry_operation)
        self._duplicates = DuplicateRepository(conn, self._db_conn.retry_operation)
        self._statistics = StatisticsRepository(conn, self._db_conn.retry_operation)
    
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
        ensure_hidden_directory(hidden_dir)

        final_db_path = Path(db_path).resolve() if db_path else hidden_dir / DB_FILENAME
        final_db_path.parent.mkdir(parents=True, exist_ok=True)

        # Create connection
        db_conn = DatabaseConnection(final_db_path)
        conn = db_conn.conn

        # Create schema
        schema = SchemaManager(conn)
        schema.create_tables()

        catalog = cls(conn, final_db_path, hidden_dir)
        return catalog
    
    def close(self) -> None:
        """Close the database connection."""
        self._db_conn.close()
    
    def _reconnect(self) -> None:
        """Recreate the database connection if it's in a bad state."""
        self._db_conn.reconnect()
        self.conn = self._db_conn.conn
    
    def _retry_db_operation(self, operation, max_retries=5, retry_delay=0.5):
        """Retry a database operation if it fails due to locking/timeout issues."""
        return self._db_conn.retry_operation(operation, max_retries, retry_delay)
    
    # File operations - delegate to FileRepository
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
        return self._files.record_file(
            original_path=original_path,
            file_name=file_name,
            file_size_bytes=file_size_bytes,
            created_at=created_at,
            year=year,
            month=month,
            month_day=month_day,
            project_name=project_name,
        )
    
    def file_exists_by_path(self, original_path: str) -> bool:
        """Check if a file with this path already exists in the database."""
        return self._files.file_exists_by_path(original_path)
    
    def find_files_by_size(self, file_size_bytes: int) -> List:
        """Query database for all files with the same size."""
        return self._files.find_files_by_size(file_size_bytes)
    
    def update_file_hash(self, file_id: int, file_hash: str) -> None:
        """Set the hash for a file record."""
        return self._files.update_file_hash(file_id, file_hash)
    
    def get_files_for_deduplication(self) -> List:
        """Get all files that need deduplication processing."""
        return self._files.get_files_for_deduplication()
    
    def mark_deduplicated(self, file_id: int, status: str = 'deduplicated') -> None:
        """Update deduplication_status and deduplication_timestamp for a file."""
        return self._files.mark_deduplicated(file_id, status)
    
    # Path operations - delegate to PathRepository
    def record_path(self, file_id: int, path: str, path_type: str = "original") -> None:
        """Record a path entry if it doesn't already exist."""
        return self._paths.record_path(file_id, path, path_type)
    
    def update_consolidated_path(self, file_id: int, consolidated_path: str) -> None:
        """Record consolidated path in paths table."""
        return self._paths.update_consolidated_path(file_id, consolidated_path)
    
    def update_symlink_path(self, file_id: int, symlink_path: str) -> None:
        """Record symlink path in paths table."""
        return self._paths.update_symlink_path(file_id, symlink_path)
    
    def get_master_file_path(self, file_id: int) -> Optional[str]:
        """Get the consolidated path for a master file, or None if not found."""
        return self._paths.get_master_file_path(file_id)
    
    def consolidated_path_exists(self, consolidated_path: str) -> bool:
        """Check if a consolidated path already exists in the database."""
        return self._paths.consolidated_path_exists(consolidated_path)
    
    # Duplicate operations - delegate to DuplicateRepository
    def handle_duplicate_hash(self, file_hash: str) -> None:
        """Update duplicate status and group data for a given hash."""
        return self._duplicates.handle_duplicate_hash(file_hash)
    
    def get_duplicate_group_files(self, group_hash: str) -> List:
        """Get all files in a duplicate group by group hash."""
        return self._duplicates.get_duplicate_group_files(group_hash)
    
    def get_duplicate_groups_for_processing(self) -> List:
        """Get all duplicate groups that need processing."""
        return self._duplicates.get_duplicate_groups_for_processing()
    
    def update_group_deduplicated(self, group_hash: str) -> None:
        """Set deduplicated_at timestamp in duplicate_groups table."""
        return self._duplicates.update_group_deduplicated(group_hash)
    
    # Statistics operations - delegate to StatisticsRepository
    def update_statistics(self) -> None:
        """Recalculate and persist aggregate statistics."""
        return self._statistics.update_statistics()
    
    def update_deduplication_statistics(self, files_consolidated: int, symlinks_created: int, files_deduplicated: int) -> None:
        """Update deduplication statistics in statistics table."""
        return self._statistics.update_deduplication_statistics(files_consolidated, symlinks_created, files_deduplicated)
