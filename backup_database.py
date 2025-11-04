#!/usr/bin/env python3
"""
Backup Database Before Re-ingestion
Creates a backup copy of the database before making changes
"""

import shutil
from pathlib import Path
from datetime import datetime
import sys

def backup_database(db_path: str, backup_dir: str = "database_backups"):
    """Create a backup of the database"""
    
    db_file = Path(db_path)
    if not db_file.exists():
        print(f"❌ Database file not found: {db_path}")
        return None
    
    # Create backup directory
    backup_path = Path(backup_dir)
    backup_path.mkdir(exist_ok=True)
    
    # Generate backup filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"{db_file.stem}_backup_{timestamp}.db"
    backup_file = backup_path / backup_filename
    
    print(f"📦 Creating backup...")
    print(f"   Source: {db_path}")
    print(f"   Destination: {backup_file}")
    
    # Copy database file
    try:
        shutil.copy2(db_file, backup_file)
        
        # Get file sizes
        source_size = db_file.stat().st_size / (1024**3)  # GB
        backup_size = backup_file.stat().st_size / (1024**3)  # GB
        
        print(f"✅ Backup created successfully!")
        print(f"   Source size: {source_size:.2f} GB")
        print(f"   Backup size: {backup_size:.2f} GB")
        
        return str(backup_file)
        
    except Exception as e:
        print(f"❌ Backup failed: {e}")
        return None

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Backup database before re-ingestion")
    parser.add_argument("--db", default="tick_data_production.db", help="Database path")
    parser.add_argument("--backup-dir", default="database_backups", help="Backup directory")
    
    args = parser.parse_args()
    
    backup_file = backup_database(args.db, args.backup_dir)
    
    if backup_file:
        print(f"\n💾 Backup saved to: {backup_file}")
        print(f"   You can restore it with: cp {backup_file} {args.db}")

if __name__ == "__main__":
    main()

