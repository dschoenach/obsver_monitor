import argparse
import os
import glob
import math
import polars as pl
import numpy as np
import os
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from sklearn.preprocessing import minmax_scale
import json
import matplotlib.gridspec as gridspec

def _load_var_labels() -> dict:
    """Load variable name labels from var_names.json next to this file."""
    try:
        here = os.path.dirname(__file__)
        path = os.path.join(here, 'var_names.json')
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _parse_env_list(name: str) -> list[str]:
    val = os.environ.get(name, "").strip()
    return [tok for tok in val.split() if tok]


def _order_variables_for_monitor(title: str, vars_found: list[str]) -> list[str]:
    t = (title or "").lower()
    order_env = None
    if "surface" in t:
        order_env = _parse_env_list("SURFPAR_MONITOR")
    elif "temp" in t:
        order_env = _parse_env_list("TEMPPAR_MONITOR")
    if not order_env:
        return sorted(vars_found)
    seen = set()
    ordered = [v for v in order_env if v in vars_found and not (v in seen or seen.add(v))]
    rest = sorted([v for v in vars_found if v not in set(ordered)])
    return ordered + rest


def plot_scorecard(df: pl.DataFrame, outdir: str, title: str, exp_names: list[str],
                   display_names: list[str], start_date: str, end_date: str,
                   fcint: int | None) -> None:
    if len(exp_names) != 2:
        print("Need exactly two experiments.")
        return
    df = df.filter(pl.col("experiment").is_in(exp_names))
    if df.is_empty():
        print("No data for experiments.")
        return
    needed = {"lead_time", "rmse", "obstypevar"}
    missing = needed - set(df.columns)
    if missing:
        print(f"Missing columns: {missing}")
        return

    has_vt = "vt_hour" in df.columns

    if has_vt:
        collapse = ["obstypevar", "lead_time", "vt_hour", "experiment"]
        per_vt = (df.group_by([c for c in collapse if c in df.columns])
                    .agg(pl.mean("rmse").alias("rmse")))
        wide = per_vt.pivot(index=["obstypevar", "lead_time", "vt_hour"],
                            on="experiment",
                            values="rmse")
        for e in exp_names:
            if e not in wide.columns:
                wide = wide.with_columns(pl.lit(np.nan).alias(e))
        wide = wide.drop_nulls(subset=exp_names)
        wide = wide.with_columns((pl.col(exp_names[0]) - pl.col(exp_names[1])).alias("pair_diff"))
        stats = (wide.group_by(["obstypevar", "lead_time"])
                      .agg([
                          pl.mean("pair_diff").alias("rmse_diff"),
                          pl.std("pair_diff", ddof=1).alias("diff_std"),
                          pl.len().alias("n_samples")
                      ]))
        stats = stats.with_columns(
            pl.when(
                (pl.col("n_samples").is_null()) | (pl.col("n_samples") < 2) |
                (pl.col("diff_std").is_null()) | (pl.col("diff_std") == 0)
            ).then(pl.lit(0.0))
            .otherwise(
                abs(pl.col("rmse_diff")) / (pl.col("diff_std") / pl.col("n_samples").sqrt())
            ).alias("z_score")
        )
        stats = stats.with_columns(
            pl.col("z_score").map_elements(
                lambda z: max(0.0, min(1.0, 1 - math.erfc(z / math.sqrt(2)))),
                return_dtype=pl.Float64
            ).alias("significance")
        )
        work_df = stats
    else:
        agg = (df.group_by(["obstypevar", "lead_time", "experiment"])
                 .agg(pl.mean("rmse").alias("rmse")))
        pivot_df = agg.pivot(index=["obstypevar", "lead_time"],
                             on="experiment",
                             values="rmse")
        for e in exp_names:
            if e not in pivot_df.columns:
                pivot_df = pivot_df.with_columns(pl.lit(np.nan).alias(e))
        work_df = pivot_df.with_columns(
            (pl.col(exp_names[0]) - pl.col(exp_names[1])).alias("rmse_diff"),
            pl.lit(0.0).alias("significance"),
            pl.lit(0).alias("n_samples"),
            pl.lit(0.0).alias("z_score")
        )

    if not work_df.is_empty():
        print("\n--- Z-Score and Significance Data ---")
        output_df = work_df.select([
            "obstypevar", "lead_time", "rmse_diff", "z_score", "significance", "n_samples"
        ])
        print(output_df)
        
        # --- CSV Writing ---
        zscore_out_path = os.path.join(outdir, f"{title}_zscore_data.csv")
        try:
            output_df.write_csv(zscore_out_path)
            print(f"Saved z-score data to: {zscore_out_path}\n")
        except Exception as e:
            print(f"Failed to write z-score data: {e}\n")
        
        # --- SQLite Writing (Correct Location) ---
        sqlite_path = os.path.join(outdir, "metrics.sqlite")
        try:
            output_df.to_pandas().to_sql(
                "scorecard_zscores", 
                f"sqlite:///{sqlite_path}", 
                if_exists="replace", 
                index=False
            )
            print(f"Scorecard data also saved to SQLite table 'scorecard_zscores' in {sqlite_path}")
        except Exception as e:
            print(f"Failed to write scorecard data to SQLite: {e}\n")


    if work_df.is_empty():
        print("No diff data.")
        return

    # --- Start of new plotting logic from scorecard2.py ---
    df_plot = work_df.to_pandas()

    # --- 2. Pre-process the data for plotting ---

    # Category 1: Positive or Negative direction
    df_plot['direction'] = np.where(df_plot['rmse_diff'] > 0, 'Positive', 'Negative')

    # Category 2: Is it significant?
    df_plot['is_significant'] = df_plot['significance'] > 0.95

    # Property 1: Alpha (transparency)
    df_plot['plot_alpha'] = 0.0
    significant_mask = df_plot['is_significant']

    # Scale alpha per variable
    for var in df_plot['obstypevar'].unique():
        var_significant_mask = (df_plot['obstypevar'] == var) & significant_mask
        if var_significant_mask.any():
            values_to_scale = df_plot.loc[var_significant_mask, 'rmse_diff'].abs()
            if not values_to_scale.empty:
                # If all values are the same, minmax_scale produces NaN. Handle this.
                if values_to_scale.min() == values_to_scale.max():
                    df_plot.loc[var_significant_mask, 'plot_alpha'] = 0.75  # Assign a mid-range alpha
                else:
                    df_plot.loc[var_significant_mask, 'plot_alpha'] = minmax_scale(
                        values_to_scale, feature_range=(0.5, 1.0)
                    )

    # Assign fixed, low alpha for all non-significant values
    df_plot.loc[~significant_mask, 'plot_alpha'] = 0.25

    # Ensure no NaN values are left in plot_alpha as a safeguard
    df_plot['plot_alpha'] = df_plot['plot_alpha'].fillna(0.25)

    # Clip values to handle potential floating point inaccuracies from scaling
    df_plot['plot_alpha'] = df_plot['plot_alpha'].clip(0.0, 1.0)

    # Property 2: Border color
    df_plot['border_color'] = np.where(df_plot['is_significant'], 'black', 'none')

    # --- 3. Set up the plot ---
    n_vars = len(set(df_plot['obstypevar']))
    fig_w = 12.0

    # Top strip fixed to 3 cm; bottom height scales with number of variables
    top_h_in = 3.0 / 2.54
    per_row_h = 0.35
    base_plot_h = 2.6
    bottom_h_in = max(4.0, base_plot_h + per_row_h * n_vars)

    fig_h = top_h_in + bottom_h_in
    fig = plt.figure(figsize=(fig_w, fig_h))

    # Wider left margin for long variable names; keep some right padding
    gs = gridspec.GridSpec(
        nrows=2, ncols=2,
        height_ratios=[top_h_in, bottom_h_in],
        width_ratios=[1.35, 0.65],            # give legend panel enough space
        left=0.26, right=0.97, top=0.98, bottom=0.10,
        wspace=0.20, hspace=0.08
    )

    ax_title = fig.add_subplot(gs[0, 0])
    ax_legend = fig.add_subplot(gs[0, 1])
    ax = fig.add_subplot(gs[1, :])  # main scorecard axis spans both columns
    ax_title.set_axis_off()
    ax_legend.set_axis_off()

    # Map variable names to y-axis coordinates for plotting
    variables = _order_variables_for_monitor(title, list(df_plot['obstypevar'].unique()))
    y_coords = {var: i for i, var in enumerate(variables)}

    # --- 4. Loop through the data and draw each tile ---
    for _, row in df_plot.iterrows():
        face_color = 'steelblue' if row['direction'] == 'Positive' else '#b2182b'
        base_height = 0.9
        if row['is_significant']:
            rect_width = 1.0
            rect_height = base_height
        else:
            rect_width = 0.8
            rect_height = 0.8 * base_height

        x_pos = row['lead_time'] - rect_width / 2
        y_pos = y_coords[row['obstypevar']] - rect_height / 2

        rect = patches.Rectangle(
            (x_pos, y_pos),
            width=rect_width,
            height=rect_height,
            facecolor=face_color,
            alpha=row['plot_alpha'],
            edgecolor=row['border_color'],
            linewidth=1.5
        )
        ax.add_patch(rect)

    # --- 5. Finalize and style the plot ---
    # Do not enforce a fixed aspect ratio; allow width to remain
    # constant while height adapts to the number of variables.
    ax.set_xlim(df_plot['lead_time'].min() - 1, df_plot['lead_time'].max() + 1)
    ax.set_ylim(-1, len(variables))
    ax.set_yticks(list(y_coords.values()))

    # Use friendly variable labels for monitor only, based on title hint
    t_lower_for_labels = (title or "").lower()
    labels_map = _load_var_labels() if ('monitor' in t_lower_for_labels) else {}
    if 'surface' in t_lower_for_labels:
        group_key = 'surface'
    elif 'temp' in t_lower_for_labels:
        group_key = 'upper_air'
    else:
        group_key = None

    if labels_map and group_key and group_key in labels_map:
        def _label_for(code: str) -> str:
            entry = labels_map[group_key].get(code)
            if isinstance(entry, dict):
                return entry.get('label', code)
            elif isinstance(entry, str):
                return entry
            return code
        ytick_labels = [ _label_for(v) for v in y_coords.keys() ]
    else:
        ytick_labels = list(y_coords.keys())
    ax.set_yticklabels(ytick_labels)
    # OLD:
    # ax.set_xticks(sorted(df_plot['lead_time'].unique()))
    # NEW: show only every 3rd hour
    all_leads = sorted(df_plot['lead_time'].unique())
    try:
        # Treat as integers if possible
        lead_array = np.array(all_leads)
        if np.issubdtype(lead_array.dtype, np.integer):
            xticks = [lt for lt in all_leads if lt % 3 == 0]
        else:
            # Fallback: pick every 3rd unique value by position
            xticks = all_leads[::3]
    except Exception:
        xticks = all_leads[::3]
    if not xticks:
        xticks = all_leads
    ax.set_xticks(xticks)
    ax.set_xlabel('Lead Time (hours/days)', fontsize=12)
    #ax.set_ylabel('Variable', fontsize=12)

    # Enforce consistent tick label sizes across domains
    ax.tick_params(axis='both', which='major', labelsize=11)
    ax.tick_params(axis='both', which='minor', labelsize=10)

    # --- 6. Title + Legend using dedicated top subplots ---

    # Build multi-line title (left-aligned) in the title axis
    t_lower = (title or "").lower()
    if "surface" in t_lower:
        domain_label = "Surface"
    elif "temp" in t_lower:
        domain_label = "Upper Air"
    else:
        domain_label = None

    if domain_label:
        title_line1 = f"{domain_label}: {display_names[1]} vs {display_names[0]}"
    else:
        title_line1 = f"{display_names[1]} vs {display_names[0]}"

    lines = [title_line1]
    if start_date and end_date:
        lines.append(f"{start_date} - {end_date}")
    if fcint:
        lines.append(f"00, {fcint} UTC")

    ax_title.text(
        0.0, 1.0, "\n".join(lines),
        ha='left', va='top',
        fontsize=16, transform=ax_title.transAxes, linespacing=1.15
    )

    # Legend in the right-top panel (fully visible, not clipped)
    legend_elements = [
        patches.Patch(facecolor='steelblue', edgecolor='black', alpha=0.8,
                      label='Positive (Significant)', linewidth=1.5),
        patches.Patch(facecolor='steelblue', edgecolor='none', alpha=0.2,
                      label='Positive (Not Significant)'),
        patches.Patch(facecolor='#b2182b', edgecolor='none', alpha=0.2,
                      label='Negative (Not Significant)'),
        patches.Patch(facecolor='#b2182b', edgecolor='black', alpha=0.8,
                      label='Negative (Significant)', linewidth=1.5),
    ]
    ax_legend.legend(
        handles=legend_elements,
        title=f"{display_names[1]} > {display_names[0]}",
        loc='upper left',
        frameon=True, borderaxespad=0.0,
        prop={'size': 10}, title_fontsize=11,
        handlelength=1.6, borderpad=0.6, labelspacing=0.5
    )

    os.makedirs(outdir, exist_ok=True)
    out_path = os.path.join(outdir, f"{title}_scorecard.png")
    # Save with a consistent pixel size so on-screen font sizes appear
    # uniform across domains; avoid automatic "tight" shrinking which can
    # change the output pixel size per plot content.
    fig.savefig(out_path, dpi=170)
    plt.close(fig)
    print(f"Saved plot: {out_path}")
    # --- End of new plotting logic ---


def _expand_metrics(paths: list[str]) -> list[str]:
    files: list[str] = []
    for p in paths:
        if os.path.isdir(p):
            files.extend(sorted(glob.glob(os.path.join(p, "*_metrics.parquet"))))
        elif "*" in p or "?" in p or "[" in p:
            files.extend(sorted(glob.glob(p)))
        else:
            files.append(p)
    seen = set()
    uniq = []
    for f in files:
        if f not in seen:
            seen.add(f)
            uniq.append(f)
    return uniq

def main():
    parser = argparse.ArgumentParser(description="Generate scorecard plots.")
    parser.add_argument("--exp-a", required=True)
    parser.add_argument("--exp-b", required=True)
    parser.add_argument("--exp-a-name", help="Short name for experiment A for display.")
    parser.add_argument("--exp-b-name", help="Short name for experiment B for display.")
    parser.add_argument("--metrics", nargs="+", required=True,
                        help="Metrics parquet files or directories (auto-glob *_metrics.parquet).")
    parser.add_argument("--outdir", required=True, help="Directory to save plots.")
    parser.add_argument("--title", required=True, help="Scorecard title.")
    parser.add_argument("--monitor-temp-cycles", type=int, help="Filter vt_hour by multiples of this cycle.")
    parser.add_argument("--fcint", type=int, help="Forecast interval in hours to display in title.")
    args = parser.parse_args()

    exp_names = [args.exp_a, args.exp_b]
    display_names = [args.exp_a_name or args.exp_a, args.exp_b_name or args.exp_b]

    metric_files = _expand_metrics(args.metrics)
    if not metric_files:
        print("No metrics files found.")
        return

    dfs = []
    for m in metric_files:
        if not os.path.exists(m):
            print(f"Missing metrics file: {m}")
            continue
        if not m.endswith(".parquet"):
            continue
        try:
            df = pl.read_parquet(m)
        except Exception as e:
            print(f"Failed reading {m}: {e}")
            continue
        if df.is_empty():
            continue
        rename_map = {}
        if "channel" in df.columns:
            rename_map["channel"] = "level_bracket"
        if "pressure_bracket" in df.columns:
            rename_map["pressure_bracket"] = "level_bracket"
        if rename_map:
            df = df.rename(rename_map)
        dfs.append(df)

    if not dfs:
        print("No valid metrics loaded; aborting.")
        return

    all_df = pl.concat(dfs, how="vertical_relaxed")

    # Filter by monitor temp cycles if provided
    if args.monitor_temp_cycles and 'vt_hour' in all_df.columns:
        print(f"Filtering by vt_hour cycle: {args.monitor_temp_cycles}")
        all_df = all_df.filter((pl.col("vt_hour") % 100) % args.monitor_temp_cycles == 0)

    start_date = all_df["vt_hour"].min()
    end_date = all_df["vt_hour"].max()

    plot_scorecard(all_df, args.outdir, args.title, exp_names, display_names, start_date, end_date, args.fcint)
    
    # REMOVED the incorrect SQLite logic from here

if __name__ == "__main__":
    main()
