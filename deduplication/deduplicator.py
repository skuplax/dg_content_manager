#!/usr/bin/env python3
"""
Deduplication Module
Handles file consolidation and symlink creation for deduplication.
"""

import os
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from database import CatalogDatabase


class Deduplicator:
    """Handles file consolidation and symlink creation for deduplication."""
    
    def __init__(self, catalog_db: CatalogDatabase, dry_run: bool = False):
        """Initialize deduplicator with database connection.
        
        Args:
            catalog_db: CatalogDatabase instance
            dry_run: If True, show what would be done without making changes
        """
        self.db = catalog_db
        self.dry_run = dry_run
        self.consolidation_base = catalog_db.consolidation_dir / "files"
        self.stats = {
            'files_consolidated': 0,
            'symlinks_created': 0,
            'files_deduplicated': 0,
            'errors': []
        }
    
    def consolidate_files(self) -> Dict:
        """Main entry point for deduplication process.
        
        Returns:
            Dictionary with statistics about the operation
        """
        print("Starting deduplication process...")
        if self.dry_run:
            print("DRY RUN MODE - No files will be moved or symlinks created\n")
        
        # Pre-flight checks
        if not self._preflight_checks():
            return self.stats
        
        # Process unique files (not duplicates, no master)
        unique_files = self._get_unique_files()
        print(f"\nFound {len(unique_files)} unique files to consolidate")
        for file_row in unique_files:
            self._consolidate_unique_file(file_row)
        
        # Process duplicate groups
        duplicate_groups = self.db.get_duplicate_groups_for_processing()
        print(f"\nFound {len(duplicate_groups)} duplicate groups to process")
        
        for group in duplicate_groups:
            if group['deduplicated_at']:
                continue  # Already processed
            
            group_files = self.db.get_duplicate_group_files(group['group_hash'])
            if not group_files:
                continue
            
            master_file = next((f for f in group_files if f['id'] == group['master_file_id']), None)
            if not master_file:
                print(f"  Warning: Master file not found for group {group['id']}")
                continue
            
            # Get or consolidate master file (handles already-consolidated case)
            master_consolidated_path = self._consolidate_master_file(master_file)
            if not master_consolidated_path:
                print(f"  Warning: Could not get consolidated path for master in group {group['id']}")
                continue
            
            # Create symlinks for duplicates
            for dup_file in group_files:
                if dup_file['id'] != master_file['id']:
                    # Skip if already processed
                    dup_status = dup_file['deduplication_status'] if 'deduplication_status' in dup_file.keys() else 'not_processed'
                    dup_status = dup_status or 'not_processed'
                    if dup_status != 'not_processed':
                        continue
                    self._create_symlink_for_duplicate(dup_file, master_consolidated_path)
            
            # Mark group as deduplicated
            if not self.dry_run:
                self.db.update_group_deduplicated(group['group_hash'])
        
        # Update statistics
        if not self.dry_run:
            self.db.update_deduplication_statistics(
                self.stats['files_consolidated'],
                self.stats['symlinks_created'],
                self.stats['files_deduplicated']
            )
        
        return self.stats
    
    def _preflight_checks(self) -> bool:
        """Perform pre-flight checks before deduplication.
        
        Returns:
            True if checks pass, False otherwise
        """
        print("Running pre-flight checks...")
        
        # Check consolidation folder is writable
        try:
            self.consolidation_base.mkdir(parents=True, exist_ok=True)
            test_file = self.consolidation_base / ".test_write"
            test_file.write_text("test")
            test_file.unlink()
        except Exception as e:
            print(f"  ERROR: Cannot write to consolidation folder: {e}")
            return False
        
        # Check disk space (rough estimate)
        files_to_process = self.db.get_files_for_deduplication()
        total_size = sum(f['file_size_bytes'] for f in files_to_process)
        
        # Get available space
        statvfs = os.statvfs(self.consolidation_base)
        available_bytes = statvfs.f_frsize * statvfs.f_bavail
        
        if available_bytes < total_size:
            print(f"  WARNING: Available space ({available_bytes:,} bytes) may be insufficient")
            print(f"           Estimated needed: {total_size:,} bytes")
            response = input("  Continue anyway? (y/N): ")
            if response.lower() != 'y':
                return False
        
        print("  Pre-flight checks passed")
        return True
    
    def _get_unique_files(self) -> List:
        """Get unique files that need consolidation (not duplicates, no master)."""
        def _do_query():
            return self.db.conn.execute(
                """
                SELECT id, original_path, file_name, file_size_bytes, file_hash, deduplication_status
                FROM files
                WHERE deduplication_status = 'not_processed'
                  AND is_duplicate = 0
                  AND master_file_id IS NULL
                ORDER BY id ASC;
                """
            ).fetchall()
        
        return self.db._retry_db_operation(_do_query)
    
    def _consolidate_unique_file(self, file_row) -> Optional[str]:
        """Consolidate a unique file (not part of duplicate group)."""
        original_path = Path(file_row['original_path'])
        
        if not original_path.exists():
            print(f"  ERROR: File not found: {original_path}")
            self.stats['errors'].append(f"File not found: {original_path}")
            return None
        
        consolidated_path = self._get_consolidated_path(file_row['file_hash'], file_row['file_name'])
        
        if self.dry_run:
            print(f"  [DRY RUN] Would move: {original_path.name}")
            print(f"            To: {consolidated_path}")
            self.stats['files_consolidated'] += 1
            self.stats['files_deduplicated'] += 1
            return str(consolidated_path)
        
        try:
            # Move file
            consolidated_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(original_path), str(consolidated_path))
            
            # Update database
            self.db.update_consolidated_path(file_row['id'], str(consolidated_path))
            self.db.mark_deduplicated(file_row['id'], 'deduplicated')
            
            self.stats['files_consolidated'] += 1
            self.stats['files_deduplicated'] += 1
            print(f"  Consolidated: {original_path.name}")
            return str(consolidated_path)
        
        except Exception as e:
            print(f"  ERROR consolidating {original_path.name}: {e}")
            self.stats['errors'].append(f"Error consolidating {original_path}: {e}")
            return None
    
    def _consolidate_master_file(self, master_file) -> Optional[str]:
        """Consolidate a master file from a duplicate group."""
        # Check if already consolidated FIRST (before checking original path)
        existing_path = self.db.get_master_file_path(master_file['id'])
        if existing_path and Path(existing_path).exists():
            print(f"  Master already consolidated: {master_file['file_name']}")
            return existing_path
        
        original_path = Path(master_file['original_path'])
        
        if not original_path.exists():
            print(f"  ERROR: Master file not found: {original_path}")
            self.stats['errors'].append(f"Master file not found: {original_path}")
            return None
        
        consolidated_path = self._get_consolidated_path(master_file['file_hash'], master_file['file_name'])
        
        if self.dry_run:
            print(f"  [DRY RUN] Would consolidate master: {master_file['file_name']}")
            print(f"            To: {consolidated_path}")
            self.stats['files_consolidated'] += 1
            return str(consolidated_path)
        
        try:
            # Move file
            consolidated_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(original_path), str(consolidated_path))
            
            # Update database
            self.db.update_consolidated_path(master_file['id'], str(consolidated_path))
            self.db.mark_deduplicated(master_file['id'], 'deduplicated')
            
            self.stats['files_consolidated'] += 1
            print(f"  Consolidated master: {master_file['file_name']}")
            return str(consolidated_path)
        
        except Exception as e:
            print(f"  ERROR consolidating master {master_file['file_name']}: {e}")
            self.stats['errors'].append(f"Error consolidating master {original_path}: {e}")
            return None
    
    def _create_symlink_for_duplicate(self, dup_file, master_consolidated_path: str) -> bool:
        """Create a symlink for a duplicate file pointing to master."""
        original_path = Path(dup_file['original_path'])
        master_path = Path(master_consolidated_path)
        
        if not master_path.exists():
            print(f"  ERROR: Master file not found at: {master_path}")
            self.stats['errors'].append(f"Master file not found: {master_path}")
            return False
        
        if not original_path.exists():
            print(f"  WARNING: Duplicate file already missing: {original_path}")
            # File may have been manually deleted, mark as processed
            if not self.dry_run:
                self.db.mark_deduplicated(dup_file['id'], 'deduplicated')
            return False
        
        if self.dry_run:
            print(f"  [DRY RUN] Would create symlink: {original_path.name}")
            print(f"            Pointing to: {master_path}")
            self.stats['symlinks_created'] += 1
            self.stats['files_deduplicated'] += 1
            return True
        
        try:
            # Resolve both paths to absolute to ensure we have real filesystem paths
            # This is important for cross-platform compatibility on network shares
            try:
                original_path_resolved = original_path.resolve()
                master_path_resolved = master_path.resolve()
            except (OSError, RuntimeError) as e:
                # If resolution fails, we can't reliably create a cross-platform symlink
                print(f"  SKIPPED: Cannot resolve paths for {original_path.name}: {e}")
                print(f"           Cannot create cross-platform compatible symlink")
                self.stats['errors'].append(f"Cannot resolve paths for {original_path}: {e}")
                return False

            # Require relative paths for cross-platform compatibility
            # Relative paths work on network shares accessed from different OSes
            # Skip deduplication if we can't create a relative path
            try:
                relative_path = os.path.relpath(master_path_resolved, original_path_resolved.parent)
                # Check if relative path is reasonable (not too many levels up)
                if relative_path.count('..') >= 20:
                    # Too many levels up - skip to avoid cross-platform issues
                    print(f"  SKIPPED: Relative path too deep ({relative_path.count('..')} levels) for {original_path.name}")
                    print(f"           Cannot create cross-platform compatible symlink")
                    self.stats['errors'].append(f"Relative path too deep for {original_path}: {relative_path.count('..')} levels")
                    return False
                
                # Use relative path - this ensures cross-platform compatibility
                symlink_target = relative_path
            except (ValueError, OSError) as e:
                # Can't compute relative path (different drives, etc.) - skip deduplication
                print(f"  SKIPPED: Cannot compute relative path for {original_path.name}: {e}")
                print(f"           Cannot create cross-platform compatible symlink")
                self.stats['errors'].append(f"Cannot compute relative path for {original_path}: {e}")
                return False
            
            # Remove original file and create symlink
            original_path.unlink()
            original_path.symlink_to(symlink_target)
            
            # Update database
            self.db.update_symlink_path(dup_file['id'], str(original_path))
            self.db.mark_deduplicated(dup_file['id'], 'deduplicated')
            
            self.stats['symlinks_created'] += 1
            self.stats['files_deduplicated'] += 1
            print(f"  Created symlink: {original_path.name} -> {symlink_target}")
            return True
        
        except OSError as e:
            # Windows-specific error handling
            if sys.platform.startswith('win') and e.winerror == 1314:
                print(f"  ERROR creating symlink for {original_path.name}: Permission denied")
                print(f"  Windows requires Developer Mode or admin privileges to create symlinks.")
                print(f"  Enable Developer Mode: Settings > Update & Security > For developers > Developer Mode")
            else:
                print(f"  ERROR creating symlink for {original_path.name}: {e}")
            self.stats['errors'].append(f"Error creating symlink {original_path}: {e}")
            return False
        except Exception as e:
            print(f"  ERROR creating symlink for {original_path.name}: {e}")
            self.stats['errors'].append(f"Error creating symlink {original_path}: {e}")
            return False
    
    def _get_consolidated_path(self, file_hash: Optional[str], file_name: str) -> Path:
        """Generate consolidated path for a file.
        
        Format: .dg_consolidation/files/{hash[0:2]}/{hash[2:4]}/{file_hash}_{sanitized_filename}
        
        Args:
            file_hash: File hash (MD5)
            file_name: Original filename
            
        Returns:
            Path object for consolidated location
        """
        if not file_hash or file_hash in {"Error", "Skipped"}:
            # Fallback for files without hash - use a default prefix
            hash_prefix1 = "00"
            hash_prefix2 = "00"
            hash_part = "no_hash"
        else:
            hash_prefix1 = file_hash[0:2]
            hash_prefix2 = file_hash[2:4]
            hash_part = file_hash
        
        # Sanitize filename
        sanitized_name = self._sanitize_filename(file_name)
        
        # Base filename: {hash}_{sanitized_name}
        base_filename = f"{hash_part}_{sanitized_name}"
        
        # Build path
        consolidated_dir = self.consolidation_base / hash_prefix1 / hash_prefix2
        consolidated_path = consolidated_dir / base_filename
        
        # Handle collisions (same hash + same filename)
        counter = 0
        while consolidated_path.exists() or self.db.consolidated_path_exists(str(consolidated_path)):
            counter += 1
            name_part, ext_part = os.path.splitext(base_filename)
            consolidated_path = consolidated_dir / f"{name_part}_{counter}{ext_part}"
        
        return consolidated_path
    
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for filesystem compatibility.
        
        Removes or replaces characters that might cause issues.
        """
        # Remove path separators
        sanitized = filename.replace('/', '_').replace('\\', '_')
        
        # Remove other problematic characters (keep it simple)
        problematic_chars = ['<', '>', ':', '"', '|', '?', '*']
        for char in problematic_chars:
            sanitized = sanitized.replace(char, '_')
        
        # Limit length (keep extension)
        if len(sanitized) > 200:
            name, ext = os.path.splitext(sanitized)
            sanitized = name[:200-len(ext)] + ext
        
        return sanitized
    
    def print_summary(self):
        """Print summary of deduplication operation."""
        print("\n" + "="*60)
        print("Deduplication Summary")
        print("="*60)
        print(f"Files consolidated: {self.stats['files_consolidated']:,}")
        print(f"Symlinks created: {self.stats['symlinks_created']:,}")
        print(f"Total files processed: {self.stats['files_deduplicated']:,}")
        
        if self.stats['errors']:
            print(f"\nErrors encountered: {len(self.stats['errors'])}")
            for error in self.stats['errors'][:10]:  # Show first 10 errors
                print(f"  - {error}")
            if len(self.stats['errors']) > 10:
                print(f"  ... and {len(self.stats['errors']) - 10} more errors")
        
        if self.dry_run:
            print("\n[DRY RUN] No actual changes were made")
