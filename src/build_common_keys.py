import argparse, os, duckdb, polars as pl

def find_sqlites(root, obstypevar):
    for dirpath, _, files in os.walk(root):
        for f in files:
            if f.startswith(f"OFCTABLE_{obstypevar}_") and f.endswith(".sqlite"):
                yield os.path.join(dirpath, f)

KEY_SQL_TEMPLATE = """
INSTALL sqlite; LOAD sqlite;
ATTACH '{path}' AS db1 (TYPE SQLITE);
INSERT INTO keys
SELECT DISTINCT
    CAST(hash(
        CAST(fcst_dttm AS BIGINT),
        CAST(valid_dttm AS BIGINT),
        SID,
        parameter,
        level,
        CAST(ROUND(lon * POW(10,{rd})) AS BIGINT),
        CAST(ROUND(lat * POW(10,{rd})) AS BIGINT)
    ) AS HUGEINT) AS obs_key
FROM db1.{table};
DETACH db1;
"""

def main():
    ap = argparse.ArgumentParser(description="Build intersection of observation keys across experiments.")
    ap.add_argument("--obstypevar", required=True)
    ap.add_argument("--round-dec", type=int, default=2, help="Decimal places for lat/lon rounding.")
    ap.add_argument("--out", required=True, help="Output Parquet file for common keys.")
    ap.add_argument("--exp", action="append", nargs=2, metavar=("EXP_NAME","DATA_ROOT"),
                    help="Repeat: experiment name and its data root directory.")
    args = ap.parse_args()

    if not args.exp:
        raise SystemExit("Provide at least one --exp EXP_NAME DATA_ROOT pair")

    con = duckdb.connect()
    # Use BIGINT so DuckDB hash() 64-bit values fit (avoid UINT32 overflow)
    con.execute("CREATE TEMP TABLE keys (obs_key HUGEINT);")

    for exp_name, root in args.exp:
        print(f"Collecting keys for {exp_name} ...")
        for f in find_sqlites(root, args.obstypevar):
            con.execute(KEY_SQL_TEMPLATE.format(path=f, table=args.obstypevar, rd=args.round_dec))

        # De-duplicate after each experiment to keep table small
        con.execute("CREATE TEMP TABLE dedup AS SELECT DISTINCT obs_key FROM keys; DELETE FROM keys; INSERT INTO keys SELECT * FROM dedup; DROP TABLE dedup;")
        # Stash this experiment's keys
        con.execute(f"CREATE TEMP TABLE ks_{exp_name} AS SELECT obs_key FROM keys; DELETE FROM keys;")

    # Intersect all ks_ tables
    exp_tables = [f"ks_{e[0]}" for e in args.exp]
    intersect_sql = " INTERSECT ".join([f"SELECT obs_key FROM {t}" for t in exp_tables])
    con.execute(f"CREATE TABLE common AS {intersect_sql};")
    df = con.execute("SELECT obs_key FROM common").pl()
    df.write_parquet(args.out)
    print(f"Wrote {len(df)} common keys to {args.out}")

if __name__ == "__main__":
    main()