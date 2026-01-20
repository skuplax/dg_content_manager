#!/usr/bin/env python3
"""
Video File Scanner Module
Scans project folders and stores metadata in the catalog database.
"""

from pathlib import Path
from typing import Any, Dict, List

from config import VIDEO_EXTENSIONS
from database import CatalogDatabase
from scanner.hashing import (
    calculate_file_hash_multi_chunk,
    get_file_creation_time,
    get_file_size_bytes,
)


def scan_project_folder(root_path, catalog_db: CatalogDatabase, skip_hash=False, subfolder=None):
    """Scan the project folder and store metadata inside the catalog database.
    
    Assumes structure: root/year/month/month_day/project_name/
    
    Args:
        root_path: Root path to scan
        catalog_db: Database instance
        skip_hash: Whether to skip hash calculation
        subfolder: Optional subfolder path relative to root_path (e.g., '2025/october')
    """
    # Track files by size to detect potential duplicates
    # Format: {file_size: [{'path': Path, 'hash': str or None, 'file_id': int}, ...]}
    files_by_size: Dict[int, List[Dict[str, Any]]] = {}
    
    root_path = Path(root_path)
    
    if subfolder:
        # Limit scan to specific subfolder
        subfolder_path = (root_path / subfolder).resolve()
        if not subfolder_path.exists():
            print(f"Error: Subfolder '{subfolder}' does not exist in root path")
            return
        
        if not subfolder_path.is_dir():
            print(f"Error: Subfolder '{subfolder}' is not a directory")
            return
        
        # Extract year and month from the resolved subfolder path
        # Expected: root/year/month -> subfolder_path points to month folder
        # So: month = subfolder_path.name, year = subfolder_path.parent.name
        month = subfolder_path.name
        year = subfolder_path.parent.name
        
        # Verify we're still within the root path structure
        try:
            subfolder_path.relative_to(root_path)
        except ValueError:
            print(f"Error: Subfolder '{subfolder}' is not within root path")
            return
        
        # Start from month_day level (Level 3)
        for month_day_folder in subfolder_path.iterdir():
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
    else:
        # Full scan from root
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
    
    video_files = []
    
    # First, collect all video files
    try:
        for file_path in folder_path.rglob('*'):
            try:
                # Skip symlinks - they're already deduplicated and in the database
                if file_path.is_symlink():
                    continue
                if file_path.is_file() and file_path.suffix.lower() in VIDEO_EXTENSIONS:
                    video_files.append(file_path)
            except OSError as e:
                # Handle files with names too long (errno 63 on macOS)
                if e.errno == 63:
                    print(f"  Warning: Skipping file with name too long: {str(file_path)[:100]}...")
                    continue
                raise
    except OSError as e:
        # Handle errors during directory traversal
        if e.errno == 63:
            print(f"  Warning: Skipping directory/file with name too long in {project_name}")
            return
        raise
    
    # Skip empty project folders
    if len(video_files) == 0:
        return
    
    print(f"Found {len(video_files)} video files in {project_name}")
    
    for i, file_path in enumerate(video_files, 1):
        try:
            # Try to get filename - may fail if name is too long
            try:
                video_name = file_path.name
                print(f"Processing {i}/{len(video_files)}: {video_name}")
            except OSError as e:
                if e.errno == 63:
                    # File name too long - use truncated path
                    full_path = str(file_path)
                    video_name = f"...{full_path[-100:]}" if len(full_path) > 100 else full_path
                    print(f"Processing {i}/{len(video_files)}: [File name too long - truncated]")
                else:
                    raise
            
            # Skip if file already scanned
            if catalog_db.file_exists_by_path(str(file_path)):
                print(f"  Skipping (already in catalog)")
                continue
            
            file_size_bytes = get_file_size_bytes(file_path)
            creation_time = get_file_creation_time(file_path)
            created_at_iso = creation_time.isoformat() if creation_time else None
            
            # Record file first to get file_id
            try:
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
            except OSError as e:
                if e.errno == 63:
                    print(f"  Error: Cannot record file - path too long (skipping)")
                    continue
                raise
            
            # Initialize in-memory dict entry if needed
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
                # Size-based duplicate detection: check in-memory dict (current scan)
                same_size_entries = files_by_size[file_size_bytes]
                
                # Get in-memory entries (excluding current file)
                in_memory_matches = [
                    entry for entry in same_size_entries[:-1]  # Exclude current file (last entry)
                ]
                
                # Query DB for files with same size (previous scans) - only if size match found in memory
                # This minimizes DB queries while still detecting cross-run duplicates
                db_matches = []
                if len(same_size_entries) > 1:
                    db_files_same_size = catalog_db.find_files_by_size(file_size_bytes)
                    # Filter DB results by filename (case-insensitive)
                    db_matches = [
                        row for row in db_files_same_size
                        if row['file_name'].lower() == video_name.lower()
                        and row['id'] != file_id  # Exclude current file
                    ]
                
                # Check if we have any potential duplicates
                has_potential_duplicates = len(in_memory_matches) > 0 or len(db_matches) > 0
                
                if has_potential_duplicates:
                    print("  Potential duplicate detected (size + name match), calculating hash...")
                    
                    # Hash the new file first
                    file_hash_value = calculate_file_hash_multi_chunk(file_path)
                    
                    if file_hash_value not in {"Error", "Skipped"}:
                        file_entry['hash'] = file_hash_value
                        catalog_db.update_file_hash(file_id, file_hash_value)
                        
                        # Check if any existing files already have that hash stored
                        hash_match_found = False
                        
                        # Check in-memory entries with hashes
                        for existing_entry in in_memory_matches:
                            if existing_entry['hash'] == file_hash_value:
                                hash_match_found = True
                                break
                        
                        # Check DB matches with hashes
                        if not hash_match_found:
                            for db_match in db_matches:
                                if db_match['file_hash'] == file_hash_value:
                                    hash_match_found = True
                                    break
                        
                        # If hash match found, we're done
                        if hash_match_found:
                            catalog_db.handle_duplicate_hash(file_hash_value)
                            file_hash_display = file_hash_value
                        else:
                            # No hash match - hash existing files that don't have hashes yet
                            # Hash in-memory entries without hashes
                            for existing_entry in in_memory_matches:
                                if existing_entry['hash'] is None:
                                    print(f"  Hashing existing file: {existing_entry['path'].name}")
                                    existing_hash = calculate_file_hash_multi_chunk(existing_entry['path'])
                                    existing_entry['hash'] = existing_hash
                                    if existing_hash not in {"Error", "Skipped"}:
                                        catalog_db.update_file_hash(existing_entry['file_id'], existing_hash)
                                        if existing_hash == file_hash_value:
                                            hash_match_found = True
                                        catalog_db.handle_duplicate_hash(existing_hash)
                            
                            # Hash DB matches without hashes
                            for db_match in db_matches:
                                if not db_match['file_hash'] or db_match['file_hash'] in {"Error", "Skipped"}:
                                    print(f"  Hashing existing file: {db_match['file_name']}")
                                    existing_hash = calculate_file_hash_multi_chunk(Path(db_match['original_path']))
                                    if existing_hash not in {"Error", "Skipped"}:
                                        catalog_db.update_file_hash(db_match['id'], existing_hash)
                                        if existing_hash == file_hash_value:
                                            hash_match_found = True
                                        catalog_db.handle_duplicate_hash(existing_hash)
                            
                            # Always call handle_duplicate_hash for the new file's hash to update relationships
                            # This ensures all files with the same hash are properly grouped
                            catalog_db.handle_duplicate_hash(file_hash_value)
                            file_hash_display = file_hash_value
                    else:
                        file_hash_display = file_hash_value
                else:
                    file_hash_display = "No duplicate"
            else:
                print("  Skipping hash calculation")
            
            created_at_display = creation_time.strftime('%Y-%m-%d %H:%M:%S') if creation_time else "Unknown"
            print(
                f"  Added to catalog: {video_name} ({file_size_bytes:,} bytes) | "
                f"Created: {created_at_display} | Hash: {file_hash_display}"
            )
        except OSError as e:
            if e.errno == 63:
                print(f"  Error: File name/path too long - skipping this file")
                continue
            raise
