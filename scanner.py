#!/usr/bin/env python3
"""
Video File Scanner Module
Scans project folders and stores metadata in the catalog database.
"""

import datetime
import hashlib
import os
from pathlib import Path
from typing import Any, Dict, List

from database import CatalogDatabase


def get_video_extensions():
    """Return a set of common video file extensions."""
    return {
        '.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv', '.webm', '.m4v',
        '.3gp', '.ogv', '.mts', '.m2ts', '.ts', '.vob', '.asf', '.rm',
        '.rmvb', '.divx', '.xvid', '.mpg', '.mpeg', '.m2v', '.mpe'
    }


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
    CHUNK_SIZE = 1024  # 1KB
    MIN_SIZE_FOR_MULTI_CHUNK = 5120  # 5KB
    
    try:
        file_size = os.path.getsize(file_path)
        
        # For files smaller than 5KB, hash the entire file
        if file_size < MIN_SIZE_FOR_MULTI_CHUNK:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                hash_md5.update(f.read())
            return hash_md5.hexdigest()
        
        # For files >= 5KB, hash three chunks: first, middle, last
        with open(file_path, "rb") as f:
            # Hash first 1KB
            first_chunk = f.read(CHUNK_SIZE)
            first_hash = hashlib.md5(first_chunk).hexdigest()
            
            # Hash last 1KB
            f.seek(-CHUNK_SIZE, os.SEEK_END)
            last_chunk = f.read(CHUNK_SIZE)
            last_hash = hashlib.md5(last_chunk).hexdigest()
            
            # Hash middle 1KB (from the middle position)
            middle_position = file_size // 2
            f.seek(middle_position - CHUNK_SIZE // 2)
            middle_chunk = f.read(CHUNK_SIZE)
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


def scan_project_folder(root_path, catalog_db: CatalogDatabase, skip_hash=False):
    """Scan the project folder and store metadata inside the catalog database.
    
    Assumes structure: root/year/month/month_day/project_name/
    """
    # Track files by size to detect potential duplicates
    # Format: {file_size: [{'path': Path, 'hash': str or None, 'file_id': int}, ...]}
    files_by_size: Dict[int, List[Dict[str, Any]]] = {}
    
    root_path = Path(root_path)
    
    # Level 1: Year folders
    for year_folder in root_path.iterdir():
        if not year_folder.is_dir() or year_folder.name.startswith('.'):
            continue
        year = year_folder.name
        
        # Level 2: Month folders
        for month_folder in year_folder.iterdir():
            if not month_folder.is_dir() or month_folder.name.startswith('.'):
                continue
            month = month_folder.name
            
            # Level 3: Month_day folders
            for month_day_folder in month_folder.iterdir():
                if not month_day_folder.is_dir() or month_day_folder.name.startswith('.'):
                    continue
                month_day = month_day_folder.name
                
                # Level 4: Project folders - scan when found
                for project_folder in month_day_folder.iterdir():
                    if not project_folder.is_dir() or project_folder.name.startswith('.'):
                        continue
                    project_name = project_folder.name
                    scan_videos_in_folder(
                        project_folder,
                        catalog_db,
                        year,
                        month,
                        month_day,
                        project_name,
                        skip_hash,
                        files_by_size,
                    )


def scan_videos_in_folder(
    folder_path,
    catalog_db: CatalogDatabase,
    year,
    month,
    month_day,
    project_name,
    skip_hash=False,
    files_by_size=None,
):
    """Scan for video files in a specific folder."""
    if files_by_size is None:
        files_by_size = {}
    
    video_extensions = get_video_extensions()
    video_files = []
    
    # First, collect all video files
    for file_path in folder_path.rglob('*'):
        if file_path.is_file() and file_path.suffix.lower() in video_extensions:
            video_files.append(file_path)
    
    # Skip empty project folders
    if len(video_files) == 0:
        return
    
    print(f"Found {len(video_files)} video files in {project_name}")
    
    for i, file_path in enumerate(video_files, 1):
        print(f"Processing {i}/{len(video_files)}: {file_path.name}")
        
        video_name = file_path.name
        file_size_bytes = get_file_size_bytes(file_path)
        creation_time = get_file_creation_time(file_path)
        created_at_iso = creation_time.isoformat() if creation_time else None
        
        file_id = catalog_db.record_file(
            original_path=str(file_path),
            file_name=video_name,
            file_size_bytes=file_size_bytes,
            created_at=created_at_iso,
            year=year,
            month=month,
            month_day=month_day,
            project_name=project_name,
        )
        catalog_db.record_path(file_id, str(file_path), path_type="original")
        
        if file_size_bytes not in files_by_size:
            files_by_size[file_size_bytes] = []
        
        file_entry = {
            'path': file_path,
            'hash': None,
            'file_id': file_id
        }
        files_by_size[file_size_bytes].append(file_entry)
        
        file_hash_display = "Skipped" if skip_hash else "No duplicate"
        
        if not skip_hash:
            same_size_entries = files_by_size[file_size_bytes]
            if len(same_size_entries) > 1:
                print("  Potential duplicate detected (size match), calculating multi-chunk hash...")
                file_hash_value = calculate_file_hash_multi_chunk(file_path)
                if file_hash_value not in {"Error", "Skipped"}:
                    file_entry['hash'] = file_hash_value
                    catalog_db.update_file_hash(file_id, file_hash_value)
                    catalog_db.handle_duplicate_hash(file_hash_value)
                    file_hash_display = file_hash_value
                else:
                    file_hash_display = file_hash_value
                
                # Also hash the existing file(s) with same size if not already hashed
                for existing_file in same_size_entries[:-1]:
                    if existing_file['hash'] is None:
                        print(f"  Hashing existing file: {existing_file['path'].name}")
                        existing_hash = calculate_file_hash_multi_chunk(existing_file['path'])
                        existing_file['hash'] = existing_hash
                        if existing_hash not in {"Error", "Skipped"}:
                            catalog_db.update_file_hash(existing_file['file_id'], existing_hash)
                            catalog_db.handle_duplicate_hash(existing_hash)
            else:
                file_hash_display = "No duplicate"
        else:
            print("  Skipping hash calculation")
        
        created_at_display = creation_time.strftime('%Y-%m-%d %H:%M:%S') if creation_time else "Unknown"
        print(
            f"  Added to catalog: {video_name} ({file_size_bytes:,} bytes) | "
            f"Created: {created_at_display} | Hash: {file_hash_display}"
        )

