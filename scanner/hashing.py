#!/usr/bin/env python3
"""
Hash calculation utilities for file scanning.
"""

import datetime
import hashlib
import os
from pathlib import Path

from config import HASH_CHUNK_SIZE, HASH_MIN_SIZE_FOR_MULTI_CHUNK


def get_file_size_bytes(file_path):
    """Get file size in bytes."""
    try:
        return os.path.getsize(file_path)
    except OSError:
        return 0


def calculate_file_hash_multi_chunk(file_path):
    """Calculate MD5 hash using multi-chunk approach for duplicate detection.
    
    For files < 5KB: hash the entire file.
    For files >= 5KB: hash first 1KB, last 1KB, and middle 1KB, then combine.
    """
    try:
        file_size = os.path.getsize(file_path)
        
        # For files smaller than 5KB, hash the entire file
        if file_size < HASH_MIN_SIZE_FOR_MULTI_CHUNK:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                hash_md5.update(f.read())
            return hash_md5.hexdigest()
        
        # For files >= 5KB, hash three chunks: first, middle, last
        with open(file_path, "rb") as f:
            # Hash first 1KB
            first_chunk = f.read(HASH_CHUNK_SIZE)
            first_hash = hashlib.md5(first_chunk).hexdigest()
            
            # Hash last 1KB
            f.seek(-HASH_CHUNK_SIZE, os.SEEK_END)
            last_chunk = f.read(HASH_CHUNK_SIZE)
            last_hash = hashlib.md5(last_chunk).hexdigest()
            
            # Hash middle 1KB (from the middle position)
            middle_position = file_size // 2
            f.seek(middle_position - HASH_CHUNK_SIZE // 2)
            middle_chunk = f.read(HASH_CHUNK_SIZE)
            middle_hash = hashlib.md5(middle_chunk).hexdigest()
            
            # Combine all three hashes by concatenating and hashing the result
            combined = first_hash + middle_hash + last_hash
            final_hash = hashlib.md5(combined.encode()).hexdigest()
            
            return final_hash
        
    except (OSError, IOError) as e:
        print(f"  Error calculating hash: {e}")
        return "Error"


def get_file_creation_time(file_path):
    """Get file creation time."""
    try:
        stat = os.stat(file_path)
        # Try to get creation time (works on macOS)
        if hasattr(stat, 'st_birthtime'):
            creation_time = stat.st_birthtime
        else:
            # Fallback to modification time
            creation_time = stat.st_mtime
        
        return datetime.datetime.fromtimestamp(creation_time)
    except OSError:
        return None
