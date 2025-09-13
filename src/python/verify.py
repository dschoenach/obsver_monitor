import argparse
import duckdb
import polars as pl
from multiprocessing import Pool
import os
from typing import List, Tuple, Optional
import hashlib
import sqlite3   # added

def process_file(task_args: Tuple[str, str, str, str, bool, bool, Optional[int], Optional[str], int]) -> pl.DataFrame:
    """
    Execute the verification SQL against a single SQLite file and return a Polars DataFrame.
    """
    file_path, exp_name, obstypevar, parameter, by_lead, by_model, fcint, key_filter, round_dec = task_args
    try:
        con = duckdb.connect(database=":memory:", read_only=False)
        con.execute("INSTALL sqlite; LOAD sqlite;")
        con.execute(f"ATTACH '{file_path}' AS db1 (TYPE SQLITE);")
        sql = build_sql(by_lead=by_lead, by_model=by_model, obstypevar=obstypevar, fcint=fcint,
                        key_filter=key_filter, round_dec=round_dec, parameter=parameter)
        df = con.execute(sql).pl()
        df = df.with_columns([
            pl.lit(exp_name).alias("experiment"),
            pl.lit(obstypevar).alias("obstypevar"),
            pl.lit(os.path.basename(file_path)).alias("source")
        ])
        con.close()
        return df
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        try:
            con.close()
        except Exception:
            pass
        return pl.DataFrame()

def build_sql(by_lead: bool, by_model: bool, obstypevar: str, fcint: Optional[int],
              key_filter: Optional[str], round_dec: int, parameter: Optional[str] = None) -> str:
    """
    Build the SQL string for metrics grouped by pressure brackets and time, optionally lead/model.
    Adds cycle_hour (forecast cycle hour extracted from fcst_dttm).
    """
    # Base grouping & selection (add cycle_hour for both parameter modes)
    if parameter == "tb":
        group_by_cols = ["vt_hour", "cycle_hour", "channel"]
        select_cols = [
            "strptime(CAST(valid_dttm AS VARCHAR), '%Y%m%d%H') AS vt_hour",
            "(CAST(fcst_dttm AS BIGINT) % 100) AS cycle_hour",
            "level AS channel",
        ]
    else:
        group_by_cols = ["vt_hour", "cycle_hour", "pressure_bracket"]
        select_cols = [
            "strptime(CAST(valid_dttm AS VARCHAR), '%Y%m%d%H') AS vt_hour",
            "(CAST(fcst_dttm AS BIGINT) % 100) AS cycle_hour",
            """
            CASE
                WHEN level >= 95000 THEN '1050-950'
                WHEN level >= 85000 THEN '950-850'
                WHEN level >= 75000 THEN '850-750'
                WHEN level >= 65000 THEN '750-650'
                WHEN level >= 55000 THEN '650-550'
                WHEN level >= 45000 THEN '550-450'
                WHEN level >= 35000 THEN '450-350'
                WHEN level >= 25000 THEN '350-250'
                WHEN level >= 15000 THEN '250-150'
                ELSE '150-0'
            END AS pressure_bracket
            """,
        ]

    if by_lead:
        group_by_cols.append("lead_time")
        select_cols.append("lead_time")
    if by_model:
        group_by_cols.append("fcst_model")
        select_cols.append("fcst_model")

    select_str = ", ".join(select_cols)

    # WHERE clause (single construction; removed earlier duplicate)
    where_clauses = []
    if fcint is not None and fcint > 0:
        if 24 % fcint != 0:
            raise ValueError(f"fcint ({fcint}) must divide 24 evenly.")
        allowed_hours = ",".join(str(h) for h in range(0, 24, fcint))
        where_clauses.append(f"(CAST(fcst_dttm AS BIGINT) % 100) IN ({allowed_hours})")

    key_join = ""
    if key_filter:
        select_str += f""",
        CAST(hash(
            CAST(fcst_dttm AS BIGINT),
            CAST(valid_dttm AS BIGINT),
            SID,
            parameter,
            level,
            CAST(ROUND(lon * POW(10,{round_dec})) AS BIGINT),
            CAST(ROUND(lat * POW(10,{round_dec})) AS BIGINT)
        ) AS HUGEINT) AS obs_key
        """
        key_join = f"INNER JOIN read_parquet('{key_filter}') mk USING (obs_key)"

    where_str = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    group_cols_no_obskey = [c for c in group_by_cols if c != 'obs_key']
    group_list = ", ".join(group_cols_no_obskey)

    return f"""
        WITH base AS (
            SELECT {select_str}, fcst, obs
            FROM db1.{obstypevar}
            {where_str}
        )
        SELECT
            {group_list},
            COUNT(*) AS n,
            AVG(fcst - obs) AS bias,
            AVG(ABS(fcst - obs)) AS mae,
            SQRT(AVG(POW(fcst - obs, 2))) AS rmse
        FROM base
        {key_join}
        GROUP BY {group_list}
    """

def find_input_files(root: str, obstypevar: str) -> List[str]:
    """
    Recursively find all SQLite files matching the expected naming pattern for an obstypevar.
    """
    matches: List[str] = []
    for dirpath, _, files in os.walk(root):
        for f in files:
            if f.startswith(f"OFCTABLE_{obstypevar}_") and f.endswith(".sqlite"):
                matches.append(os.path.join(dirpath, f))
    matches.sort()
    return matches

def sqlite_has_table(file_path: str, table: str) -> bool:
    """Return True if SQLite file contains the given table."""
    try:
        with sqlite3.connect(file_path) as con:
            cur = con.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table,)
            )
            return cur.fetchone() is not None
    except Exception:
        return False

def main() -> None:
    parser = argparse.ArgumentParser(description="Run parallel verification.")
    parser.add_argument("--exp-name", required=True, help="Experiment name.")
    parser.add_argument("--data-root", required=True, help="Root directory for data.")
    parser.add_argument("--obstypevar", required=True, help="Observation type variable (table name).")
    parser.add_argument("--parameter", help="Parameter type (e.g. tb)")
    parser.add_argument("--start", required=True, help="Start date (YYYYMMDDHH).")
    parser.add_argument("--end", required=True, help="End date (YYYYMMDDHH).")
    parser.add_argument("--out", required=True, help="Output Parquet file path.")
    parser.add_argument("--jobs", type=int, default=4, help="Number of parallel jobs.")
    parser.add_argument("--by-lead", action="store_true", help="Group by lead time.")
    parser.add_argument("--by-model", action="store_true", help="Group by forecast model.")
    parser.add_argument("--fcint", type=int, help="Forecast start time interval in hours (e.g., 12 for 00Z, 12Z).")
    parser.add_argument("--key-filter", help="Parquet file with column obs_key to restrict to common observations.")
    parser.add_argument("--round-dec", type=int, default=2, help="Rounding decimals for lat/lon (must match key file).")
    parser.add_argument("--strict-missing", action="store_true",
                        help="Fail if any input file is missing the required table.")
    args = parser.parse_args()

    files = find_input_files(args.data_root, args.obstypevar)
    if not files:
        print("No matching SQLite files found.")
        pl.DataFrame().write_parquet(args.out)
        print(f"Verification metrics saved to {args.out}")
        return

    # Pre-scan for table presence
    present = []
    missing = []
    for f in files:
        if sqlite_has_table(f, args.obstypevar):
            present.append(f)
        else:
            missing.append(f)

    if missing:
        print(f"[verify] {len(missing)} of {len(files)} files missing table '{args.obstypevar}':")
        for m in missing[:8]:
            print(f"  MISSING: {m}")
        if len(missing) > 8:
            print(f"  ... ({len(missing)-8} more)")
        if args.strict_missing:
            print("Strict mode: aborting due to missing tables.")
            pl.DataFrame().write_parquet(args.out)
            print(f"Verification metrics saved to {args.out}")
            return

    if not present:
        print("All files missing required table; nothing to process.")
        pl.DataFrame().write_parquet(args.out)
        print(f"Verification metrics saved to {args.out}")
        return

    print(f"[verify] Using {len(present)} files with table '{args.obstypevar}' "
          f"(skipped {len(missing)}).")

    pool_args = [(f, args.exp_name, args.obstypevar, args.parameter, args.by_lead, args.by_model,
                  args.fcint, args.key_filter, args.round_dec) for f in present]

    with Pool(args.jobs) as p:
        results = p.map(process_file, pool_args)

    non_empty = [df for df in results if not df.is_empty()]
    if not non_empty:
        print("All queries returned empty; writing empty metrics file.")
        pl.DataFrame().write_parquet(args.out)
        print(f"Verification metrics saved to {args.out}")
        return

    final_df = pl.concat(non_empty, how="vertical_relaxed")
    final_df.write_parquet(args.out)
    print(f"Verification metrics saved to {args.out} (rows={final_df.height})")
    # --- NEW: Save to SQLite ---
    sqlite_path = os.path.join(os.path.dirname(args.out), "metrics.sqlite")
    table_name = f"{args.exp_name}_{args.obstypevar}"
    
    # Use pandas for easier SQLite writing with Polars
    final_df.to_pandas().to_sql(table_name, f"sqlite:///{sqlite_path}", if_exists="replace", index=False)
    print(f"Metrics also saved to SQLite table '{table_name}' in {sqlite_path}")

if __name__ == "__main__":
    main()