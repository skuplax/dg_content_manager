#!/usr/bin/env python3
"""
Video File Scanner - Main Entry Point
Scans project folders for video files and stores metadata in SQLite.
"""

import argparse
import os
from pathlib import Path

from database import CatalogDatabase
from scanner import scan_project_folder


def main():
    parser = argparse.ArgumentParser(description='Scan project folders for video files and catalog them in SQLite')
    parser.add_argument('root_path', help='Root path to scan (e.g., /Volumes/digitalcatalyst/digitalcatalyst/Project Folder)')
    parser.add_argument('--consolidation-root', help='Base path for hidden consolidation folder (defaults to root_path)')
    parser.add_argument('--db-path', help='Optional path to SQLite database (overrides default inside consolidation folder)')
    parser.add_argument('--skip-hash', action='store_true', help='Skip hash calculation for faster scanning')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.root_path):
        print(f"Error: Path '{args.root_path}' does not exist")
        return 1
    
    scan_root = Path(args.root_path).resolve()
    consolidation_root = Path(args.consolidation_root).resolve() if args.consolidation_root else scan_root
    db_path = Path(args.db_path).resolve() if args.db_path else None

    print(f"Scanning: {scan_root}")
    print(f"Consolidation root: {consolidation_root}")
    
    try:
        catalog_db = CatalogDatabase.initialize(scan_root=scan_root, consolidation_root=consolidation_root, db_path=db_path)
        print(f"Catalog database: {catalog_db.db_path}")

        scan_project_folder(scan_root, catalog_db, args.skip_hash)
        catalog_db.update_statistics()

        print("Scan completed! Catalog updated.")
        return 0
    except Exception as e:
        print(f"Error during scanning: {e}")
        return 1
    finally:
        try:
            catalog_db.close()
        except UnboundLocalError:
            pass


if __name__ == "__main__":
    exit(main())

