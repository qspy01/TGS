import sqlite3
import hashlib
import argparse
import os
import sys
from tqdm import tqdm

DB_NAME = "logs.db"
CHUNK_SIZE = 10000  # Commit every 10k lines for balance between RAM and Speed

def get_line_hash(text: str) -> str:
    """Generate SHA256 hash for deduplication."""
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

def init_db_sync():
    """Synchronous DB init for the importer script."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Same schema as database.py
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs_raw (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL,
            content_hash TEXT UNIQUE NOT NULL
        );
    """)
    cursor.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS logs_fts 
        USING fts5(content, content='logs_raw', content_rowid='id');
    """)
    # Triggers
    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS logs_ai AFTER INSERT ON logs_raw BEGIN
          INSERT INTO logs_fts(rowid, content) VALUES (new.id, new.content);
        END;
    """)
    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS logs_ad AFTER DELETE ON logs_raw BEGIN
          INSERT INTO logs_fts(logs_fts, rowid, content) VALUES('delete', old.id, old.content);
        END;
    """)
    conn.commit()
    return conn

def import_file(filepath: str):
    if not os.path.exists(filepath):
        print(f"Error: File {filepath} not found.")
        return

    conn = init_db_sync()
    cursor = conn.cursor()
    
    print(f"🚀 Starting import for: {filepath}")
    
    # Faster writes
    cursor.execute("PRAGMA synchronous = OFF;")
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA cache_size = 10000;")

    imported_count = 0
    skipped_count = 0
    batch = []
    
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            # Using tqdm for progress if file size is known, otherwise simple iterator
            for line in tqdm(f, desc="Processing Lines", unit="lines"):
                line = line.strip()
                if not line:
                    continue
                
                line_hash = get_line_hash(line)
                batch.append((line, line_hash))
                
                if len(batch) >= CHUNK_SIZE:
                    try:
                        # INSERT OR IGNORE skips duplicates based on content_hash UNIQUE constraint
                        cursor.executemany(
                            "INSERT OR IGNORE INTO logs_raw (content, content_hash) VALUES (?, ?)", 
                            batch
                        )
                        conn.commit()
                        imported_count += len(batch) # Note: exact count of inserted vs ignored is hard with executemany
                        batch = []
                    except Exception as e:
                        print(f"Error writing batch: {e}")

            # Process remaining
            if batch:
                cursor.executemany(
                    "INSERT OR IGNORE INTO logs_raw (content, content_hash) VALUES (?, ?)", 
                    batch
                )
                conn.commit()
                imported_count += len(batch)

    except KeyboardInterrupt:
        print("\nStopping import...")
    finally:
        # Re-enable sync for safety
        cursor.execute("PRAGMA synchronous = NORMAL;")
        conn.close()
        print(f"\n✅ Import Finished. Processed chunk count: ~{imported_count}")
        print("Note: Actual inserts may be lower due to deduplication.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bulk Import Logs to Telegram Bot DB")
    parser.add_argument("file", help="Path to the .txt file")
    args = parser.parse_args()
    
    import_file(args.file)
