# DG Content Manager

CLI utility that walks a structured project tree (year/month/day/project) and catalogs every video file it finds. Metadata, file paths, hashes, and duplicate statistics are stored in a SQLite database inside a hidden consolidation folder so future deduplication work can happen safely.

## Requirements
- macOS or Windows (hidden folder handling is built in); Linux works but the folder will stay visible
- Python 3.9+ with the standard library (uses `sqlite3`, `hashlib`, `pathlib`, etc.)
- Access to the storage volume you want to scan

## Quick Start
```bash
cd /path/to/dg_content_manager
python3 main.py /Volumes/your_volume/your_project_root
```

What happens:
1. A hidden `.dg_consolidation` directory is created under the scan root (or a location you pass via `--consolidation-root`).
2. `dg_catalog.db` is created/updated inside that directory (or a custom path via `--db-path`).
3. Every nested project folder is scanned recursively for common video extensions.
4. File metadata is written to the `files` and `paths` tables, duplicate groups are tracked, and aggregate statistics are refreshed.

## CLI Options
| Flag | Description |
| --- | --- |
| `root_path` | Required. Root directory to scan. Should contain year/month/day/project folders. |
| `--consolidation-root PATH` | Puts the hidden `.dg_consolidation` folder somewhere else (e.g., another volume). Defaults to `root_path`. |
| `--db-path PATH` | Override the exact SQLite file location. Useful for debugging or sharing a catalog snapshot. |
| `--skip-hash` | Skip the multi-chunk MD5 hashing pass. Scanning is faster but duplicates are only flagged by size. |

## Outputs to Know About
- **Hidden catalog folder**: `<consolidation-root>/.dg_consolidation/`
- **Database**: `dg_catalog.db` with `files`, `paths`, `duplicate_groups`, and `statistics` tables
- **Console logs**: Running progress (files found, hashes, duplicate alerts, summary)

### Duplicate Tracking
- Files are first grouped by size to avoid unnecessary hashing.
- When two files share a size, the scanner hashes the first, middle, and last KB, combines those hashes, and compares the result.
- Duplicates are marked in `files.is_duplicate`, grouped in `duplicate_groups`, and stats such as `space_saved_bytes` are updated.

## Operational Tips
- Run from a machine that can maintain stable access to the storage volume; hashing large files over flaky links will slow things down.
- Keep the `.dg_consolidation` folder with the volume so that catalog paths remain valid.
- Re-run the scanner after ingesting new footage; existing rows are updated in place, so repeat scans are safe.
- If you only need a metadata snapshot (e.g., for reporting), point `--db-path` at a temp location and copy that SQLite file elsewhere.

## Troubleshooting
- **`Path ... does not exist`**: Double-check that the root path is mounted and spelled correctly.
- **Permission errors**: Ensure you have read access to the footage and write access where the consolidation folder/database live.
- **Slow hashing**: Use `--skip-hash` for the initial sweep, then re-run without the flag once you’re ready to identify duplicates precisely.

That’s it—run the command, let the scanner walk the tree, and inspect `dg_catalog.db` with any SQLite browser when you want to verify results.

