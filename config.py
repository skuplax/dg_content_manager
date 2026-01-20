#!/usr/bin/env python3
"""
Configuration constants for DG Content Manager.
"""

from datetime import datetime, timezone

# Directory and file names
CONSOLIDATION_DIR_NAME = ".dg_consolidation"
DB_FILENAME = "dg_catalog.db"

# Video file extensions
VIDEO_EXTENSIONS = {
    '.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv', '.webm', '.m4v',
    '.3gp', '.ogv', '.mts', '.m2ts', '.ts', '.vob', '.asf', '.rm',
    '.rmvb', '.divx', '.xvid', '.mpg', '.mpeg', '.m2v', '.mpe'
}

# Hash calculation constants
HASH_CHUNK_SIZE = 1024  # 1KB
HASH_MIN_SIZE_FOR_MULTI_CHUNK = 5120  # 5KB


def utc_now() -> str:
    """Return current UTC timestamp as ISO string."""
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
