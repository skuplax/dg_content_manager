#!/usr/bin/env python3
"""
Database connection management with retry logic.
"""

import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Callable, Optional

from config import CONSOLIDATION_DIR_NAME, DB_FILENAME


def ensure_hidden_directory(directory: Path) -> None:
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


class DatabaseConnection:
    """Manages SQLite database connection with retry logic."""
    
    def __init__(self, db_path: Path):
        """Initialize database connection.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self._connect()
    
    def _connect(self) -> None:
        """Create database connection."""
        # Ensure the database directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Add timeout to prevent locking issues (30 seconds)
        # Enable WAL mode for better concurrency and reliability
        self.conn = sqlite3.connect(self.db_path, timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.execute("PRAGMA journal_mode = WAL;")
    
    def reconnect(self) -> None:
        """Recreate the database connection if it's in a bad state."""
        try:
            if self.conn:
                self.conn.close()
        except Exception:
            pass
        
        # Ensure the database directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Small delay to allow filesystem to stabilize
        time.sleep(0.1)
        
        self._connect()
    
    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def retry_operation(self, operation: Callable, max_retries: int = 5, retry_delay: float = 0.5):
        """Retry a database operation if it fails due to locking/timeout issues.
        
        Args:
            operation: Callable that performs the database operation
            max_retries: Maximum number of retry attempts
            retry_delay: Initial delay between retries (exponential backoff)
        
        Returns:
            Result of the operation
        
        Raises:
            Exception: If operation fails after all retries
        """
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
                            self.reconnect()
                        else:
                            # For other errors, check connection first
                            try:
                                self.conn.execute("SELECT 1")
                            except (sqlite3.OperationalError, sqlite3.ProgrammingError, sqlite3.DatabaseError):
                                # Connection is bad, recreate it
                                self.reconnect()
                        
                        # Exponential backoff with longer delays
                        time.sleep(retry_delay * (2 ** attempt))
                        continue
                raise
            except Exception as e:
                # For any other exception, check if it's connection-related
                error_str = str(e).lower()
                if "unable to open" in error_str or "database" in error_str:
                    if attempt < max_retries - 1:
                        self.reconnect()
                        time.sleep(retry_delay * (2 ** attempt))
                        continue
                raise
        raise
