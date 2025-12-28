import aiosqlite
import logging
import os

DB_NAME = "logs.db"

async def init_db():
    """
    Initializes the database with a dual-table architecture:
    1. 'logs_raw': Standard table for storage and uniqueness (via hash).
    2. 'logs_fts': Virtual table for high-speed indexing.
    """
    async with aiosqlite.connect(DB_NAME) as db:
        # 1. Create the raw storage table with a Unique Hash to prevent duplicates
        await db.execute("""
            CREATE TABLE IF NOT EXISTS logs_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content TEXT NOT NULL,
                content_hash TEXT UNIQUE NOT NULL
            );
        """)

        # 2. Create the FTS5 virtual table (External Content Table)
        # This tells FTS5 to index the data in 'logs_raw' without duplicating the text storage.
        await db.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS logs_fts 
            USING fts5(content, content='logs_raw', content_rowid='id');
        """)

        # 3. Triggers to keep FTS index in sync with Raw Data automatically
        await db.execute("""
            CREATE TRIGGER IF NOT EXISTS logs_ai AFTER INSERT ON logs_raw BEGIN
              INSERT INTO logs_fts(rowid, content) VALUES (new.id, new.content);
            END;
        """)
        await db.execute("""
            CREATE TRIGGER IF NOT EXISTS logs_ad AFTER DELETE ON logs_raw BEGIN
              INSERT INTO logs_fts(logs_fts, rowid, content) VALUES('delete', old.id, old.content);
            END;
        """)
        
        await db.commit()
        logging.info("Database initialized with FTS5 architecture.")

async def search_logs(query_text: str, limit: int = 20) -> tuple[list[str], int]:
    """
    Performs a non-blocking FTS5 search.
    Returns: (list_of_results, total_count_estimate)
    """
    # Sanitize query for FTS5
    # We wrap the query in quotes to treat it as a phrase or split by spaces for AND logic
    # Here we treat spaces as AND operators for broader matching
    clean_query = query_text.replace('"', '""')
    
    # Logic: "word1 word2" -> matches rows containing BOTH word1 AND word2 (in any order)
    # If you want exact phrase match, wrap the whole string in quotes in the SQL.
    fts_query = f'"{clean_query}"' if " " in clean_query else f"{clean_query}*"

    async with aiosqlite.connect(DB_NAME) as db:
        # Perform the search
        # 'snippet' can be used for highlighting, but we want full lines here.
        cursor = await db.execute(f"""
            SELECT content 
            FROM logs_fts 
            WHERE logs_fts MATCH ? 
            ORDER BY rank 
            LIMIT ?
        """, (fts_query, limit))
        
        rows = await cursor.fetchall()
        results = [row[0] for row in rows]
        
        # Check if there are more results (simple optimization: check if we hit the limit)
        # For exact counts on FTS, it's expensive, so we infer.
        count_cursor = await db.execute("SELECT count(*) FROM logs_fts WHERE logs_fts MATCH ?", (fts_query,))
        count_row = await count_cursor.fetchone()
        total_count = count_row[0] if count_row else 0

        return results, total_count
