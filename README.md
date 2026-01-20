# DG Content Manager

CLI utility that walks a structured project tree (year/month/day/project) and catalogs every video file it finds. Metadata, file paths, hashes, and duplicate statistics are stored in a SQLite database inside a hidden consolidation folder so future deduplication work can happen safely.

## Requirements
- macOS or Windows (hidden folder handling is built in); Linux works but the folder will stay visible
- Python 3.9+ with the standard library (uses `sqlite3`, `hashlib`, `pathlib`, etc.)
- Access to the storage volume you want to scan

## Quick Start

### Basic Scanning
```bash
cd /path/to/dg_content_manager
python3 main.py /Volumes/your_volume/your_project_root
```

What happens:
1. A hidden `.dg_consolidation` directory is created under the scan root (or a location you pass via `--consolidation-root`).
2. `dg_catalog.db` is created/updated inside that directory (or a custom path via `--db-path`).
3. Every nested project folder is scanned recursively for common video extensions.
4. File metadata is written to the `files` and `paths` tables, duplicate groups are tracked, and aggregate statistics are refreshed.

### Generate a Report
```bash
# Using root_path (will locate database automatically)
python3 main.py /Volumes/your_volume/your_project_root --report

# Using explicit database path
python3 main.py --db-path /path/to/dg_catalog.db --report
```

## CLI Options

### Scanning Options
| Flag | Description |
| --- | --- |
| `root_path` | Root directory to scan. Required for scanning (unless using `--report` with `--db-path`). Should contain year/month/day/project folders. |
| `--consolidation-root PATH` | Puts the hidden `.dg_consolidation` folder somewhere else (e.g., another volume). Defaults to `root_path`. |
| `--db-path PATH` | Override the exact SQLite file location. Useful for debugging or sharing a catalog snapshot. |
| `--skip-hash` | Skip the multi-chunk MD5 hashing pass. Scanning is faster but duplicates are only flagged by size. |
| `--subfolder PATH` | Limit scan to a specific subfolder relative to root_path (e.g., `2025/october`). Useful for incremental scanning. |

### Report Options
| Flag | Description |
| --- | --- |
| `--report` | Generate and display a markdown report from the database. Exits without scanning. Requires either `root_path` or `--db-path` to locate the database. |

### Deduplication Options
| Flag | Description |
| --- | --- |
| `--deduplicate` | Run deduplication process (consolidate files and create symlinks). Exits without scanning. Requires either `root_path` or `--db-path` to locate the database. |
| `--dry-run` | Show what would be done without making changes. Use with `--deduplicate` to preview operations. |

## Usage Examples

### Scan a Full Project Tree
```bash
python3 main.py /Volumes/digitalcatalyst/digitalcatalyst/Project\ Folder
```

### Scan Only a Specific Month
```bash
python3 main.py /Volumes/digitalcatalyst/digitalcatalyst/Project\ Folder --subfolder 2025/october
```

### Fast Scan Without Hashing (Initial Pass)
```bash
python3 main.py /Volumes/digitalcatalyst/digitalcatalyst/Project\ Folder --skip-hash
```

### Generate Report After Scanning
```bash
# Generate report using root path
python3 main.py /Volumes/digitalcatalyst/digitalcatalyst/Project\ Folder --report

# Generate report using explicit database path
python3 main.py --db-path /Volumes/digitalcatalyst/.dg_consolidation/dg_catalog.db --report
```

### Custom Database Location
```bash
python3 main.py /Volumes/digitalcatalyst/digitalcatalyst/Project\ Folder --db-path /tmp/my_catalog.db
```

### Consolidation Folder on Different Volume
```bash
python3 main.py /Volumes/source/Project\ Folder --consolidation-root /Volumes/destination
```

### Deduplicate Files (Preview with Dry Run)
```bash
# Preview what would be done without making changes
python3 main.py /Volumes/digitalcatalyst/digitalcatalyst/Project\ Folder --deduplicate --dry-run
```

### Deduplicate Files (Actual Operation)
```bash
# Consolidate unique files and replace duplicates with symlinks
python3 main.py /Volumes/digitalcatalyst/digitalcatalyst/Project\ Folder --deduplicate

# Using explicit database path
python3 main.py --db-path /Volumes/digitalcatalyst/.dg_consolidation/dg_catalog.db --deduplicate
```

## Outputs to Know About
- **Hidden catalog folder**: `<consolidation-root>/.dg_consolidation/`
- **Database**: `dg_catalog.db` with `files`, `paths`, `duplicate_groups`, and `statistics` tables
- **Consolidated files**: `.dg_consolidation/files/{hash[0:2]}/{hash[2:4]}/{file_hash}_{filename}` - Unique files moved here during deduplication
- **Console logs**: Running progress (files found, hashes, duplicate alerts, summary)
- **Reports**: Markdown reports with comprehensive statistics, breakdowns by year/month, top projects, and deduplication metrics

### Duplicate Tracking
- Files are first grouped by size to avoid unnecessary hashing.
- When two files share a size, the scanner hashes the first, middle, and last KB, combines those hashes, and compares the result.
- Duplicates are marked in `files.is_duplicate`, grouped in `duplicate_groups`, and stats such as `space_saved_bytes` are updated.

### Report Generation
The `--report` flag generates a comprehensive markdown report including:
- Executive summary with totals and percentages
- Detailed statistics (file counts, sizes, duplicates)
- Breakdown by year and month
- Top 10 projects by file count and total size
- Deduplication status breakdown

Reports can be generated without scanning by providing either:
- `root_path` (will automatically locate the database in `.dg_consolidation/dg_catalog.db`)
- `--db-path` pointing directly to the database file

### Deduplication Process
The `--deduplicate` flag performs file consolidation and symlink creation:

**What it does:**
1. **Consolidates unique files**: Moves unique files (non-duplicates) to `.dg_consolidation/files/` using a 2-level hash-based directory structure
2. **Consolidates master files**: Moves the master file from each duplicate group to the consolidation folder
3. **Creates symlinks**: Replaces duplicate files with symlinks pointing to their consolidated master files
4. **Updates database**: Records all paths (original, consolidated, symlink) and marks files as processed

**File Organization:**
- Consolidated files are stored in: `.dg_consolidation/files/{hash[0:2]}/{hash[2:4]}/{file_hash}_{filename}`
- This 2-level structure efficiently distributes files (handles 30K+ files with excellent performance)
- Filename collisions are handled automatically with counter suffixes

**Safety Features:**
- **Always use `--dry-run` first** to preview what will happen
- Pre-flight checks verify disk space and write permissions
- Original file structure is preserved (symlinks maintain original paths)
- Database tracks all operations for audit and recovery

**Important Notes:**
- Deduplication is **irreversible** - make sure you have backups before running
- Symlinks replace original duplicate files - ensure your applications can handle symlinks
- The consolidation folder must remain accessible for symlinks to work
- Run `--dry-run` first to see exactly what will be changed
- **macOS**: Symlinks may appear as "aliases" in Finder, but they're real symlinks and work correctly with applications like Premiere Pro
- **Windows**: Creating symlinks requires **Developer Mode** (Settings > Update & Security > For developers) or admin privileges. Without it, symlink creation will fail with a permission error

## Operational Tips
- Run from a machine that can maintain stable access to the storage volume; hashing large files over flaky links will slow things down.
- Keep the `.dg_consolidation` folder with the volume so that catalog paths remain valid.
- Re-run the scanner after ingesting new footage; existing rows are updated in place, so repeat scans are safe.
- Use `--subfolder` for incremental scanning of specific months or date ranges.
- Use `--skip-hash` for initial fast scans, then re-run without it to identify duplicates precisely.
- Generate reports regularly to track catalog growth and duplicate statistics.
- If you only need a metadata snapshot (e.g., for reporting), point `--db-path` at a temp location and copy that SQLite file elsewhere.
- **Before deduplication**: Always run with `--dry-run` first to preview changes, ensure you have backups, and verify your applications can handle symlinks.
- **After deduplication**: Keep the `.dg_consolidation` folder with the volume - symlinks depend on it. Reports will show deduplication statistics.

## Troubleshooting
- **`Path ... does not exist`**: Double-check that the root path is mounted and spelled correctly.
- **Permission errors**: Ensure you have read access to the footage and write access where the consolidation folder/database live.
- **Slow hashing**: Use `--skip-hash` for the initial sweep, then re-run without the flag once you’re ready to identify duplicates precisely.
- **Symlink errors**: Ensure the consolidation folder is accessible and the master file exists at the consolidated path.
- **Windows symlink errors**: If you see `[WinError 1314]`, enable Developer Mode (Settings > Update & Security > For developers) or run with admin privileges.
- **macOS "alias" display**: Symlinks showing as "aliases" in Finder is normal - they're real symlinks and work correctly with applications.
- **Deduplication fails**: Check available disk space (consolidation needs space for all unique files), verify write permissions, and ensure files aren't locked by other applications.

That’s it—run the command, let the scanner walk the tree, and inspect `dg_catalog.db` with any SQLite browser when you want to verify results.

