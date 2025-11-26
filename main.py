#!/usr/bin/env python3
"""
Video File Scanner - Main Entry Point
Scans project folders for video files and creates CSV.
"""

import os
import argparse
from scanner import scan_project_folder


def main():
    parser = argparse.ArgumentParser(description='Scan project folders for video files and create CSV')
    parser.add_argument('root_path', help='Root path to scan (e.g., /Volumes/digitalcatalyst/digitalcatalyst/Project Folder)')
    parser.add_argument('-o', '--output', default='video_files.csv', help='Output CSV file name')
    parser.add_argument('--skip-hash', action='store_true', help='Skip hash calculation for faster scanning')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.root_path):
        print(f"Error: Path '{args.root_path}' does not exist")
        return 1
    
    print(f"Scanning: {args.root_path}")
    print(f"Output CSV: {args.output}")
    
    try:
        scan_project_folder(args.root_path, args.output, args.skip_hash)
        print(f"Scan completed! Results saved to {args.output}")
        return 0
    except Exception as e:
        print(f"Error during scanning: {e}")
        return 1


if __name__ == "__main__":
    exit(main())

