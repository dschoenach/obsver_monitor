import sqlite3
import argparse
from typing import List, Tuple

def inspect_db(db_path: str) -> None:
    """
    Connect to an SQLite database, list all tables, and print column names for each table.
    """
    print(f"Tables in {db_path} (via sqlite3):")
    try:
        with sqlite3.connect(db_path) as con:
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables: List[Tuple[str]] = cur.fetchall()
            for t in tables:
                table_name = t[0]
                print(f" - {table_name}")
                cur.execute(f"PRAGMA table_info({table_name});")
                columns = cur.fetchall()
                column_names = [col[1] for col in columns]
                print(f"    columns: {', '.join(column_names)}")
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")

def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect SQLite database schema.")
    parser.add_argument("--db", required=True, help="Path to the SQLite database file.")
    args = parser.parse_args()
    inspect_db(args.db)

if __name__ == "__main__":
    main()
