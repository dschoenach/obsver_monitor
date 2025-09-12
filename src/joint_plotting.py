import argparse, os
import polars as pl
import matplotlib.pyplot as plt
from typing import Dict

BRACKET_MIDPOINTS: Dict[str, int] = {
    "1050-950": 1000, "950-850": 900, "850-750": 800, "750-650": 700,
    "650-550": 600, "550-450": 500, "450-350": 400, "350-250": 300,
    "250-150": 200, "150-0": 75
}

METRIC_STYLES = {
    "rmse": {"linestyle": "--", "label": "RMSE"},
    "bias": {"linestyle": "-", "label": "Bias"}
}

def _aggregate_profile(df: pl.DataFrame) -> pl.DataFrame:
    return (df.group_by(["experiment", "pressure_bracket"])
              .agg([
                  pl.mean("bias").alias("bias"),
                  pl.mean("rmse").alias("rmse"),
                  pl.sum("n").alias("n_sum")
              ]))

def _aggregate_tb_profile(df: pl.DataFrame) -> pl.DataFrame:
    return (df.group_by(["experiment", "channel"])
              .agg([
                  pl.mean("bias").alias("bias"),
                  pl.mean("rmse").alias("rmse"),
                  pl.sum("n").alias("n_sum")
              ]))

def _aggregate_timeseries(df: pl.DataFrame) -> pl.DataFrame:
    return (df.group_by(["experiment", "vt_hour"])
              .agg([
                  pl.mean("bias").alias("bias"),
                  pl.mean("rmse").alias("rmse"),
                  pl.sum("n").alias("n_sum")
              ]))

def plot_combined_tb_profiles(df: pl.DataFrame, outdir: str, title_prefix: str, exp_colors: Dict[str, str], lead_time_str: str) -> None:
    agg = _aggregate_tb_profile(df)
    counts = (agg.group_by("channel")
                .agg(pl.sum("n_sum").alias("n_all"))
                .sort("channel"))

    fig, ax = plt.subplots(figsize=(12.0, 10.0))
    ax2 = ax.twiny()
    ax2.barh(counts["channel"], counts["n_all"], color="gray", alpha=0.15, height=0.8)
    ax2.set_xlabel("Count")

    line_handles = []
    exps_in_order = list(exp_colors.keys())

    for exp in exps_in_order:
        sub = agg.filter(pl.col("experiment")==exp).sort("channel")
        h_rmse, = ax.plot(sub["rmse"], sub["channel"],
                          color=exp_colors[exp], linestyle=METRIC_STYLES["rmse"]["linestyle"],
                          marker="o", label=f"RMSE {exp}")
        line_handles.append(h_rmse)

    for exp in exps_in_order:
        sub = agg.filter(pl.col("experiment")==exp).sort("channel")
        h_bias, = ax.plot(sub["bias"], sub["channel"],
                          color=exp_colors[exp], linestyle=METRIC_STYLES["bias"]["linestyle"],
                          marker="s", label=f"Bias {exp}")
        line_handles.append(h_bias)

    ax.axvline(0, color='black', linestyle='-', linewidth=1)
    ax.set_title(f"{title_prefix} - Vertical Profiles")
    ax.set_xlabel("Value")
    ax.set_ylabel("Channel")
    ax.set_yticks(counts["channel"].to_list())
    ax.grid(True, which="both", linestyle="--", linewidth=0.5, alpha=0.7)
    ax.legend(handles=line_handles, loc='center left', bbox_to_anchor=(1.05, 0.5), frameon=False)
    fig.tight_layout()
    path = os.path.join(outdir, f"{title_prefix}_profile{lead_time_str}.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved plot: {path}")

def plot_combined_profiles(df: pl.DataFrame, outdir: str, title_prefix: str, exp_colors: Dict[str, str], lead_time_str: str) -> None:
    mapping_df = pl.DataFrame({
        "pressure_bracket": list(BRACKET_MIDPOINTS.keys()),
        "pressure_midpoint": list(BRACKET_MIDPOINTS.values())
    })
    agg = _aggregate_profile(df).join(mapping_df, on="pressure_bracket", how="left")
    counts = (agg.group_by("pressure_midpoint")
                .agg(pl.sum("n_sum").alias("n_all"))
                .sort("pressure_midpoint"))

    fig, ax = plt.subplots(figsize=(12.0, 10.0))
    ax2 = ax.twiny()
    ax2.barh(counts["pressure_midpoint"], counts["n_all"], color="gray", alpha=0.15, height=60)
    ax2.set_xlabel("Count")

    line_handles = []
    exps_in_order = list(exp_colors.keys())

    for exp in exps_in_order:
        sub = agg.filter(pl.col("experiment")==exp).sort("pressure_midpoint")
        h_rmse, = ax.plot(sub["rmse"], sub["pressure_midpoint"],
                          color=exp_colors[exp], linestyle=METRIC_STYLES["rmse"]["linestyle"],
                          marker="o", label=f"RMSE {exp}")
        line_handles.append(h_rmse)

    for exp in exps_in_order:
        sub = agg.filter(pl.col("experiment")==exp).sort("pressure_midpoint")
        h_bias, = ax.plot(sub["bias"], sub["pressure_midpoint"],
                          color=exp_colors[exp], linestyle=METRIC_STYLES["bias"]["linestyle"],
                          marker="s", label=f"Bias {exp}")
        line_handles.append(h_bias)

    ax.axvline(0, color='black', linestyle='-', linewidth=1)
    ax.set_title(f"{title_prefix} - Vertical Profiles")
    ax.set_xlabel("Value")
    ax.set_ylabel("Pressure (hPa)")
    ax.set_yticks(list(BRACKET_MIDPOINTS.values()))
    ax.set_yticklabels(list(BRACKET_MIDPOINTS.keys()))
    ax.invert_yaxis()
    ax.grid(True, which="both", linestyle="--", linewidth=0.5, alpha=0.7)
    ax.legend(handles=line_handles, loc='center left', bbox_to_anchor=(1.05, 0.5), frameon=False)
    fig.tight_layout()
    path = os.path.join(outdir, f"{title_prefix}_profile{lead_time_str}.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved plot: {path}")

def plot_combined_timeseries(df: pl.DataFrame, outdir: str, title_prefix: str, exp_colors: Dict[str, str], lead_time: str) -> None:
    agg = _aggregate_timeseries(df).sort("vt_hour")
    counts = (agg.group_by("vt_hour")
                .agg(pl.sum("n_sum").alias("n_all"))
                .sort("vt_hour"))

    fig, ax = plt.subplots(figsize=(12.0, 10.0))
    ax2 = ax.twinx()
    ax2.bar(counts["vt_hour"], counts["n_all"], color="gray", alpha=0.12, width=0.8)
    ax2.set_ylabel("Count")

    line_handles = []
    exps_in_order = list(exp_colors.keys())

    for exp in exps_in_order:
        sub = agg.filter(pl.col("experiment")==exp).sort("vt_hour")
        h_rmse, = ax.plot(sub["vt_hour"], sub["rmse"], color=exp_colors[exp],
                          linestyle=METRIC_STYLES["rmse"]["linestyle"], marker="o",
                          label=f"RMSE {exp}")
        line_handles.append(h_rmse)

    for exp in exps_in_order:
        sub = agg.filter(pl.col("experiment")==exp).sort("vt_hour")
        h_bias, = ax.plot(sub["vt_hour"], sub["bias"], color=exp_colors[exp],
                          linestyle=METRIC_STYLES["bias"]["linestyle"], marker="s",
                          label=f"Bias {exp}")
        line_handles.append(h_bias)

    ax.axhline(0, color='black', linestyle='-', linewidth=1)
    ax.set_title(f"{title_prefix} - Time Series")
    ax.set_xlabel("Valid Time")
    ax.set_ylabel("Value")
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)
    ax.legend(handles=line_handles, loc='center left', bbox_to_anchor=(1.05, 0.5), frameon=False)
    fig.tight_layout()
    path = os.path.join(outdir, f"{title_prefix}_timeseries{lead_time}.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved plot: {path}")

def main() -> None:
    parser = argparse.ArgumentParser(description="Joint plotting for multiple experiments.")
    parser.add_argument("--metrics", nargs="+", required=True, help="Metrics parquet files (one per experiment).")
    parser.add_argument("--outdir", required=True, help="Output directory for plots.")
    parser.add_argument("--title-prefix", required=True, help="Title / filename prefix.")
    parser.add_argument("--lead-time", type=int, help="Select specific lead time.")
    parser.add_argument("--exp-color", action="append",
                        help="Experiment color mapping EXP=COLOR (repeatable).")
    args = parser.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    # Set global font sizes for matplotlib
    plt.rcParams.update({
        'font.size': 18,
        'axes.titlesize': 18,
        'axes.labelsize': 16,
        'xtick.labelsize': 16,
        'ytick.labelsize': 16,
        'legend.fontsize': 16,
        'figure.titlesize': 20
    })

    dfs = []
    for m in args.metrics:
        if not os.path.exists(m):
            print(f"Missing metrics file: {m}")
            continue
        df = pl.read_parquet(m)
        if df.is_empty():
            print(f"Empty metrics file: {m}")
            continue
        dfs.append(df)
    if not dfs:
        print("No valid metrics loaded; aborting.")
        return
    all_df = pl.concat(dfs, how="vertical_relaxed")

    if args.lead_time is not None:
        all_df = all_df.filter(pl.col("lead_time") == args.lead_time)

    # Build color map
    exp_names = sorted(all_df.select(pl.col("experiment").unique()).to_series().to_list())
    exp_colors: Dict[str, str] = {}
    if args.exp_color:
        for spec in args.exp_color:
            if "=" not in spec:
                print(f"Ignoring malformed --exp-color '{spec}' (need EXP=COLOR).")
                continue
            k, v = spec.split("=", 1)
            exp_colors[k] = v
    # Assign any missing colors from default cycle
    import itertools
    default_cycle = itertools.cycle(plt.rcParams['axes.prop_cycle'].by_key()['color'])
    for e in exp_names:
        if e not in exp_colors:
            exp_colors[e] = next(default_cycle)

    lead_time_str = f"_lt_{args.lead_time}" if args.lead_time is not None else ""

    if "channel" in all_df.columns:
        plot_combined_tb_profiles(all_df, args.outdir, args.title_prefix, exp_colors, lead_time_str)
    else:
        plot_combined_profiles(all_df, args.outdir, args.title_prefix, exp_colors, lead_time_str)
    plot_combined_timeseries(all_df, args.outdir, args.title_prefix, exp_colors, lead_time_str)

if __name__ == "__main__":
    main()