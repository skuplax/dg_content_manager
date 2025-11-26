#!/usr/bin/env python3
"""
Video File Scanner Module
Scans project folders and creates a CSV with video file information.
"""

import os
import csv
import datetime
from pathlib import Path
import hashlib


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


def scan_project_folder(root_path, output_csv, skip_hash=False):
    """Scan the project folder and create CSV with video file information.
    
    Assumes structure: root/year/month/month_day/project_name/
    """
    # CSV headers
    headers = ['year', 'month', 'month_day', 'project_name', 'video_file_name', 'video_file_size_bytes', 'video_created_at', 'file_hash']
    
    # Track files by size to detect potential duplicates
    # Format: {file_size: [{'path': Path, 'hash': str or None, ...}, ...]}
    files_by_size = {}
    
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        
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
                        scan_videos_in_folder(project_folder, writer, year, month, month_day, project_name, skip_hash, files_by_size)


def scan_videos_in_folder(folder_path, writer, year, month, month_day, project_name, skip_hash=False, files_by_size=None):
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
        
        # Determine if we need to calculate hash (only for duplicates)
        file_hash = None
        if not skip_hash:
            # Check if we've seen a file with the same size (potential duplicate)
            if file_size_bytes in files_by_size:
                # Potential duplicate found - calculate multi-chunk hash
                print(f"  Potential duplicate detected (size match), calculating multi-chunk hash...")
                file_hash = calculate_file_hash_multi_chunk(file_path)
                
                # Also hash the existing file(s) with same size if not already hashed
                for existing_file in files_by_size[file_size_bytes]:
                    if existing_file['hash'] is None:
                        print(f"  Hashing existing file: {existing_file['path'].name}")
                        existing_file['hash'] = calculate_file_hash_multi_chunk(existing_file['path'])
            else:
                # First file with this size - no hash needed yet
                files_by_size[file_size_bytes] = []
            
            # Store this file in the tracking dict
            file_entry = {
                'path': file_path,
                'hash': file_hash
            }
            files_by_size[file_size_bytes].append(file_entry)
            
            # Set hash display value
            if file_hash is None:
                file_hash = "No duplicate"
        else:
            file_hash = "Skipped"
            print(f"  Skipping hash calculation")
        
        # Format creation time
        if creation_time:
            created_at = creation_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
            created_at = "Unknown"
        
        # Write row to CSV
        writer.writerow([
            year,
            month,
            month_day,
            project_name,
            video_name,
            file_size_bytes,
            created_at,
            file_hash
        ])
        
        print(f"  Added to CSV: {video_name} ({file_size_bytes:,} bytes)")

