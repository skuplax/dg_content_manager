#!/usr/bin/env python3
"""
Video File Scanner - Main Entry Point
Scans project folders for video files and stores metadata in SQLite.
"""

import argparse
import os
from pathlib import Path

from database import CatalogDatabase
from deduplication import Deduplicator
from reports import ReportGenerator
from scanner import scan_project_folder


def main():
    parser = argparse.ArgumentParser(description='Scan project folders for video files and catalog them in SQLite')
    parser.add_argument('root_path', nargs='?', help='Root path to scan (e.g., /Volumes/digitalcatalyst/digitalcatalyst/Project Folder). Required unless --report is used with --db-path.')
    parser.add_argument('--consolidation-root', help='Base path for hidden consolidation folder (defaults to root_path)')
    parser.add_argument('--db-path', help='Optional path to SQLite database (overrides default inside consolidation folder)')
    parser.add_argument('--skip-hash', action='store_true', help='Skip hash calculation for faster scanning')
    parser.add_argument('--subfolder', help='Limit scan to a specific subfolder (e.g., 2025/october). Path is relative to root_path.')
    parser.add_argument('--report', action='store_true', help='Generate and display a markdown report from the database. Exits without scanning.')
    parser.add_argument('--deduplicate', action='store_true', help='Run deduplication process (consolidate files and create symlinks). Exits without scanning.')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes (use with --deduplicate)')
    
    args = parser.parse_args()
    
    # Handle deduplication
    if args.deduplicate:
        # For deduplication, we need either db_path or root_path to locate the database
        if args.db_path:
            db_path = Path(args.db_path).resolve()
            if not db_path.exists():
                print(f"Error: Database path '{db_path}' does not exist")
                return 1
            scan_root = db_path.parent
            catalog_db = CatalogDatabase.initialize(scan_root=scan_root, db_path=db_path)
        elif args.root_path:
            scan_root = Path(args.root_path).resolve()
            if not os.path.exists(scan_root):
                print(f"Error: Path '{scan_root}' does not exist")
                return 1
            consolidation_root = Path(args.consolidation_root).resolve() if args.consolidation_root else scan_root
            catalog_db = CatalogDatabase.initialize(scan_root=scan_root, consolidation_root=consolidation_root, db_path=None)
        else:
            print("Error: --deduplicate requires either --db-path or root_path to locate the database")
            return 1
        
        try:
            deduplicator = Deduplicator(catalog_db, dry_run=args.dry_run)
            stats = deduplicator.consolidate_files()
            deduplicator.print_summary()
            return 0 if not stats['errors'] else 1
        except Exception as e:
            print(f"Error during deduplication: {e}")
            import traceback
            traceback.print_exc()
            return 1
        finally:
            try:
                catalog_db.close()
            except UnboundLocalError:
                pass
    
    # Handle report generation
    if args.report:
        # For reports, we need either db_path or root_path to locate the database
        if args.db_path:
            db_path = Path(args.db_path).resolve()
            if not db_path.exists():
                print(f"Error: Database path '{db_path}' does not exist")
                return 1
            # Use db_path directly - scan_root can be the parent directory
            scan_root = db_path.parent
            catalog_db = CatalogDatabase.initialize(scan_root=scan_root, db_path=db_path)
        elif args.root_path:
            scan_root = Path(args.root_path).resolve()
            if not os.path.exists(scan_root):
                print(f"Error: Path '{scan_root}' does not exist")
                return 1
            consolidation_root = Path(args.consolidation_root).resolve() if args.consolidation_root else scan_root
            catalog_db = CatalogDatabase.initialize(scan_root=scan_root, consolidation_root=consolidation_root, db_path=None)
        else:
            print("Error: --report requires either --db-path or root_path to locate the database")
            return 1
        
        try:
            generator = ReportGenerator(catalog_db)
            report = generator.generate_report()
            print(report)
            
            # Save report to .data/ directory
            from datetime import datetime
            data_dir = Path(".data")
            data_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            report_filename = data_dir / f"report_{timestamp}.md"
            
            with open(report_filename, 'w', encoding='utf-8') as f:
                f.write(report)
            
            print(f"\nReport saved to: {report_filename}")
            return 0
        except Exception as e:
            print(f"Error generating report: {e}")
            import traceback
            traceback.print_exc()
            return 1
        finally:
            try:
                catalog_db.close()
            except UnboundLocalError:
                pass
    
    # Normal scanning mode
    if not args.root_path:
        parser.error("root_path is required for scanning (or use --report to generate a report)")
    
    if not os.path.exists(args.root_path):
        print(f"Error: Path '{args.root_path}' does not exist")
        return 1
    
    scan_root = Path(args.root_path).resolve()
    consolidation_root = Path(args.consolidation_root).resolve() if args.consolidation_root else scan_root
    db_path = Path(args.db_path).resolve() if args.db_path else None

    print(f"Scanning: {scan_root}")
    if args.subfolder:
        print(f"Subfolder: {args.subfolder}")
    print(f"Consolidation root: {consolidation_root}")
    
    try:
        catalog_db = CatalogDatabase.initialize(scan_root=scan_root, consolidation_root=consolidation_root, db_path=db_path)
        print(f"Catalog database: {catalog_db.db_path}")

        scan_project_folder(scan_root, catalog_db, args.skip_hash, subfolder=args.subfolder)
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

