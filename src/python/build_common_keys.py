import argparse, os, sqlite3, duckdb, polars as pl

REQUIRED_COLUMNS = {"fcst_dttm","valid_dttm","SID","parameter","level","lon","lat"}

def find_sqlites(root, obstypevar):
    for dirpath, _, files in os.walk(root):
        for f in files:
            if f.startswith(f"OFCTABLE_{obstypevar}_") and f.endswith(".sqlite"):
                yield os.path.join(dirpath, f)

def inspect_sqlite(file_path):
    """
    Return (tables:list[str], columns_per_table:dict[str,set[str]])
    Safe fallback on errors.
    """
    tables = []
    cols = {}
    try:
        with sqlite3.connect(file_path) as con:
            cur = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [r[0] for r in cur.fetchall()]
            for t in tables:
                cur2 = con.execute(f"SELECT * FROM '{t}' LIMIT 0")
                cols[t] = {d[0] for d in cur2.description or []}
    except Exception:
        pass
    return tables, cols

def pick_table(tables, requested):
    """
    Decide which table to use.
    Only accept:
      - exact match
      - case-insensitive match
    Otherwise return None (skip).
    """
    if requested in tables:
        return requested
    low = {t.lower(): t for t in tables}
    rl = requested.lower()
    if rl in low:
        return low[rl]
    return None  # do NOT auto-substitute arbitrary single table (avoid wrong data)

def insert_keys_from_table(con, file_path, table_name, round_dec, start_date, end_date, debug=False):
    con.execute(f"ATTACH '{file_path}' AS db1 (TYPE SQLITE);")
    if debug:
        print(f"[debug] Inserting keys from {file_path} table {table_name}")

    # Build the WHERE clause for date filtering
    where_clauses = []
    if start_date:
        where_clauses.append(f"valid_dttm >= '{start_date}'")
    if end_date:
        where_clauses.append(f"valid_dttm <= '{end_date}'")
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    con.execute(f"""
        INSERT INTO work_keys
        SELECT DISTINCT
            CAST(hash(
                CAST(fcst_dttm AS BIGINT),
                CAST(valid_dttm AS BIGINT),
                SID,
                parameter,
                level,
                CAST(ROUND(lon * POW(10,{round_dec})) AS BIGINT),
                CAST(ROUND(lat * POW(10,{round_dec})) AS BIGINT)
            ) AS HUGEINT) AS obs_key
        FROM db1."{table_name}"
        {where_sql};
    """)
    con.execute("DETACH db1;")

def main():
    ap = argparse.ArgumentParser(description="Build common observation keys across experiments.")
    ap.add_argument("--obstypevar", required=True)
    ap.add_argument("--round-dec", type=int, default=2)
    ap.add_argument("--out", required=True)
    ap.add_argument("--exp", action="append", nargs=2, metavar=("EXP_NAME","DATA_ROOT"))
    ap.add_argument("--start", help="Start date (YYYY-MM-DDTHH:MM:SS)")
    ap.add_argument("--end", help="End date (YYYY-MM-DDTHH:MM:SS)")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--strict-missing", action="store_true",
                    help="Abort if any SQLite file lacks the requested table or required columns.")
    args = ap.parse_args()

    if not args.exp:
        raise SystemExit("Provide at least one --exp EXP_NAME DATA_ROOT pair")

    con = duckdb.connect()
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute("CREATE TEMP TABLE work_keys (obs_key HUGEINT);")

    exp_names = []
    for exp_name, root in args.exp:
        exp_names.append(exp_name)
        print(f"Collecting keys for {exp_name} ...")
        any_file = False
        used_files = 0
        skipped_no_table = 0
        skipped_cols = 0
        for f in find_sqlites(root, args.obstypevar):
            any_file = True
            tables, cols = inspect_sqlite(f)
            if not tables:
                skipped_no_table += 1
                if args.debug:
                    print(f"[debug] Skip {f}: no tables present")
                continue
            chosen = pick_table(tables, args.obstypevar)
            if not chosen:
                skipped_no_table += 1
                if args.debug:
                    print(f"[debug] Skip {f}: requested '{args.obstypevar}' not in {tables}")
                continue
            table_cols = cols.get(chosen, set())
            missing_cols = REQUIRED_COLUMNS - table_cols
            if missing_cols:
                skipped_cols += 1
                msg = f"[debug] Skip {f}: table '{chosen}' missing columns {sorted(missing_cols)}"
                if args.strict_missing:
                    raise SystemExit(f"Strict mode abort: {msg}")
                if args.debug:
                    print(msg)
                continue
            # Attach + insert
            try:
                insert_keys_from_table(con, f, chosen, args.round_dec, args.start, args.end, args.debug)
                used_files += 1
            except Exception as e:
                skipped_cols += 1
                if args.debug:
                    print(f"[debug] Error inserting from {f}: {e}")
        if not any_file:
            print(f"Warning: no SQLite files for {args.obstypevar} under {root}")
        if args.strict_missing and (skipped_no_table > 0 or skipped_cols > 0):
            raise SystemExit(f"Strict mode: skipped_no_table={skipped_no_table} skipped_bad_columns={skipped_cols}")
        # Deduplicate for this experiment
        con.execute("""
            CREATE TEMP TABLE dedup AS SELECT DISTINCT obs_key FROM work_keys;
            DELETE FROM work_keys;
            INSERT INTO work_keys SELECT * FROM dedup;
            DROP TABLE dedup;
        """)
        con.execute(f"CREATE TEMP TABLE ks_{exp_name} AS SELECT obs_key FROM work_keys; DELETE FROM work_keys;")
        print(f"{exp_name}: files used={used_files}, skipped_no_table={skipped_no_table}, skipped_bad_columns={skipped_cols}")

    exp_tables = [f"ks_{n}" for n in exp_names]
    if len(exp_tables) == 1:
        con.execute(f"CREATE TABLE common AS SELECT obs_key FROM {exp_tables[0]};")
    else:
        inter = " INTERSECT ".join([f"SELECT obs_key FROM {t}" for t in exp_tables])
        con.execute(f"CREATE TABLE common AS {inter};")

    df = con.execute("SELECT obs_key FROM common").pl()
    df.write_parquet(args.out)
    print(f"Wrote {len(df)} common keys to {args.out}")

if __name__ == "__main__":
    main()