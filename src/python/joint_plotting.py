import argparse
import os
import itertools
from typing import Dict, List, Optional, Iterable, Tuple

import polars as pl
import matplotlib.pyplot as plt

BRACKET_MIDPOINTS: Dict[str, int] = {
    "1050-950": 1000, "950-850": 900, "850-750": 800, "750-650": 700,
    "650-550": 600, "550-450": 500, "450-350": 400, "350-250": 300,
    "250-150": 200, "150-0": 75
}

METRIC_STYLES = {
    "rmse": {"linestyle": "--", "label": "RMSE", "marker": "o"},
    "bias": {"linestyle": "-", "label": "Bias", "marker": "s"},
}

# ---------------- Aggregations ---------------- #

def _aggregate(df: pl.DataFrame, group_cols: List[str]) -> pl.DataFrame:
    needed = {"experiment", *group_cols, "bias", "rmse", "n"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns for aggregation: {missing}")
    return (
        df.group_by(["experiment", *group_cols])
          .agg([
              pl.mean("bias").alias("bias"),
              pl.mean("rmse").alias("rmse"),
              pl.sum("n").alias("n_sum"),
          ])
    )

# ---------------- Utilities ---------------- #

def _parse_mapping(specs: Optional[Iterable[str]], label: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not specs:
        return out
    for spec in specs:
        if "=" not in spec:
            print(f"Ignoring malformed --{label} '{spec}' (need A=B).")
            continue
        k, v = spec.split("=", 1)
        out[k] = v
    return out

def _build_title(prefix: str,
                 plot_kind: str,
                 start_date: Optional[str],
                 end_date: Optional[str],
                 cycle_hours: List[int],
                 hours: Optional[List[str]]) -> str:
    title = f"{prefix} - {plot_kind}"
    if start_date and end_date:
        title += f"\n{start_date} - {end_date}"
    if cycle_hours:
        cyc = ", ".join(f"{h:02d}" for h in cycle_hours)
        line3 = f"{cyc} UTC"
        if hours:
            hrs = ", ".join(hours)
            line3 += f" + {{{hrs}}}"
        title += f"\n{line3}"
    return title

def _ensure_colors(experiments: List[str], user: Dict[str, str]) -> Dict[str, str]:
    colors = dict(user)
    default_cycle = itertools.cycle(plt.rcParams['axes.prop_cycle'].by_key()['color'])
    for e in experiments:
        if e not in colors:
            colors[e] = next(default_cycle)
    return colors

def _plot_metric_lines(ax, agg: pl.DataFrame,
                       x_col: str,
                       y_col: str,
                       experiments: List[str],
                       exp_colors: Dict[str, str],
                       exp_names: Dict[str, str],
                       invert_y: bool = False) -> List:
    handles = []
    for metric in ("rmse", "bias"):
        for exp in experiments:
            sub = agg.filter(pl.col("experiment") == exp).sort(y_col)
            if sub.is_empty():
                continue
            disp = exp_names.get(exp, exp)
            style = METRIC_STYLES[metric]
            (h,) = ax.plot(
                sub[metric],
                sub[y_col],
                color=exp_colors[exp],
                linestyle=style["linestyle"],
                marker=style["marker"],
                label=f"{style['label']} {disp}",
            )
            handles.append(h)
    if invert_y:
        ax.invert_yaxis()
    return handles

# ---------------- Plotters ---------------- #

def plot_profiles_pressure(df: pl.DataFrame,
                           outdir: str,
                           prefix: str,
                           exp_colors: Dict[str, str],
                           exp_names: Dict[str, str],
                           lead_time_tag: str,
                           start_date: Optional[str],
                           end_date: Optional[str],
                           cycle_hours: List[int],
                           hours: Optional[List[str]]) -> None:
    if "pressure_bracket" not in df.columns:
        return
    mapping_df = pl.DataFrame({
        "pressure_bracket": list(BRACKET_MIDPOINTS.keys()),
        "pressure_midpoint": list(BRACKET_MIDPOINTS.values())
    })
    agg = _aggregate(df, ["pressure_bracket"]).join(mapping_df, on="pressure_bracket", how="left")
    counts = (
        agg.group_by("pressure_midpoint")
           .agg(pl.sum("n_sum").alias("n_all"))
           .sort("pressure_midpoint")
    )
    fig, ax = plt.subplots(figsize=(12, 8))
    plt.subplots_adjust(right=0.72)
    ax2 = ax.twiny()
    ax2.barh(counts["pressure_midpoint"], counts["n_all"], color="gray", alpha=0.15, height=60)
    ax2.set_xlabel("Count")
    experiments = sorted(agg["experiment"].unique())
    handles = _plot_metric_lines(ax, agg, x_col="rmse", y_col="pressure_midpoint",
                                 experiments=experiments,
                                 exp_colors=exp_colors,
                                 exp_names=exp_names,
                                 invert_y=True)
    ax.axvline(0, color="black", linewidth=1)
    ax.set_title(_build_title(prefix, "Vertical Profiles", start_date, end_date, cycle_hours, hours))
    ax.set_xlabel("Value")
    ax.set_ylabel("Pressure (hPa)")
    ax.set_yticks(list(BRACKET_MIDPOINTS.values()))
    ax.set_yticklabels(list(BRACKET_MIDPOINTS.keys()))
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)
    ax.legend(handles=handles, loc="center left", bbox_to_anchor=(1.02, 0.5), frameon=True)
    out_path = os.path.join(outdir, f"{prefix}_profile{lead_time_tag}.png")
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved plot: {out_path}")

def plot_profiles_channel(df: pl.DataFrame,
                          outdir: str,
                          prefix: str,
                          exp_colors: Dict[str, str],
                          exp_names: Dict[str, str],
                          lead_time_tag: str,
                          start_date: Optional[str],
                          end_date: Optional[str],
                          cycle_hours: List[int],
                          hours: Optional[List[str]]) -> None:
    if "channel" not in df.columns:
        return
    agg = _aggregate(df, ["channel"])
    counts = (
        agg.group_by("channel")
           .agg(pl.sum("n_sum").alias("n_all"))
           .sort("channel")
    )
    fig, ax = plt.subplots(figsize=(12, 8))
    plt.subplots_adjust(right=0.72)
    ax2 = ax.twiny()
    ax2.barh(counts["channel"], counts["n_all"], color="gray", alpha=0.15, height=0.8)
    ax2.set_xlabel("Count")
    experiments = sorted(agg["experiment"].unique())
    handles = _plot_metric_lines(ax, agg, x_col="rmse", y_col="channel",
                                 experiments=experiments,
                                 exp_colors=exp_colors,
                                 exp_names=exp_names)
    ax.axvline(0, color="black", linewidth=1)
    ax.set_title(_build_title(prefix, "Vertical Profiles", start_date, end_date, cycle_hours, hours))
    ax.set_xlabel("Value")
    ax.set_ylabel("Channel")
    ax.set_yticks(counts["channel"].to_list())
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)
    ax.legend(handles=handles, loc="center left", bbox_to_anchor=(1.02, 0.5), frameon=True)
    out_path = os.path.join(outdir, f"{prefix}_profile{lead_time_tag}.png")
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved plot: {out_path}")

def plot_timeseries(df: pl.DataFrame,
                    outdir: str,
                    prefix: str,
                    exp_colors: Dict[str, str],
                    exp_names: Dict[str, str],
                    lead_time_tag: str,
                    start_date: Optional[str],
                    end_date: Optional[str],
                    cycle_hours: List[int],
                    hours: Optional[List[str]]) -> None:
    if "vt_hour" not in df.columns:
        return
    agg = _aggregate(df, ["vt_hour"]).sort("vt_hour")
    counts = (
        agg.group_by("vt_hour")
           .agg(pl.sum("n_sum").alias("n_all"))
           .sort("vt_hour")
    )
    fig, ax = plt.subplots(figsize=(14, 8))
    plt.subplots_adjust(right=0.72)
    ax2 = ax.twinx()
    ax2.bar(counts["vt_hour"], counts["n_all"], color="gray", alpha=0.12, width=0.8)
    ax2.set_ylabel("Count")
    experiments = sorted(agg["experiment"].unique())
    handles: List = []
    for metric in ("rmse", "bias"):
        style = METRIC_STYLES[metric]
        for exp in experiments:
            sub = agg.filter(pl.col("experiment") == exp).sort("vt_hour")
            if sub.is_empty():
                continue
            disp = exp_names.get(exp, exp)
            (h,) = ax.plot(
                sub["vt_hour"],
                sub[metric],
                color=exp_colors[exp],
                linestyle=style["linestyle"],
                marker=style["marker"],
                label=f"{style['label']} {disp}",
            )
            handles.append(h)
    ax.axhline(0, color="black", linewidth=1)
    ax.set_title(_build_title(prefix, "Time Series", start_date, end_date, cycle_hours, hours))
    ax.set_xlabel("Valid Time")
    ax.set_ylabel("Value")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)
    ax.legend(handles=handles, loc="center left", bbox_to_anchor=(1.02, 0.5), frameon=True)
    out_path = os.path.join(outdir, f"{prefix}_timeseries{lead_time_tag}.png")
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved plot: {out_path}")

# ---------------- Main ---------------- #

def main() -> None:
    parser = argparse.ArgumentParser(description="Joint plotting for multiple experiments.")
    parser.add_argument("--metrics", nargs="+", required=True,
                        help="Metrics parquet files (one or more, any experiments).")
    parser.add_argument("--outdir", required=True, help="Output directory.")
    parser.add_argument("--title-prefix", required=True, help="Title / filename prefix.")
    parser.add_argument("--lead-time", type=int, help="Filter: specific lead_time.")
    parser.add_argument("--exp-color", action="append",
                        help="Experiment color mapping EXP=COLOR (repeatable).")
    parser.add_argument("--exp-name", action="append",
                        help="Experiment display name mapping LONG=SHORT (repeatable).")
    parser.add_argument("--start-date", help="Override start date for title.")
    parser.add_argument("--end-date", help="Override end date for title.")
    parser.add_argument("--fcint", type=int,
                        help="(Deprecated) Forecast cycle interval; ignored.")
    parser.add_argument("--hours", nargs="+",
                        help="Filter: list of valid-time hours (integers).")
    args = parser.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    if args.fcint is not None:
        print("NOTE: --fcint is deprecated and ignored (no effect on plots).")

    plt.rcParams.update({
        "font.size": 16,
        "axes.titlesize": 18,
        "axes.labelsize": 15,
        "xtick.labelsize": 13,
        "ytick.labelsize": 13,
        "legend.fontsize": 13,
        "figure.titlesize": 20,
    })

    # Load
    dfs: List[pl.DataFrame] = []
    for path in args.metrics:
        if not os.path.exists(path):
            print(f"Missing metrics file: {path}")
            continue
        try:
            df = pl.read_parquet(path)
        except Exception as e:
            print(f"Failed reading {path}: {e}")
            continue
        if df.is_empty():
            print(f"Empty metrics file: {path}")
            continue
        dfs.append(df)
    if not dfs:
        print("No valid metrics loaded; aborting.")
        return
    all_df = pl.concat(dfs, how="vertical_relaxed")

    # Optional filtering
    if args.lead_time is not None and "lead_time" in all_df.columns:
        all_df = all_df.filter(pl.col("lead_time") == args.lead_time)

    if args.hours and "vt_hour" in all_df.columns and all_df["vt_hour"].dtype.is_temporal():
        want_hours = {int(h) for h in args.hours}
        all_df = all_df.filter(pl.col("vt_hour").dt.hour().is_in(sorted(want_hours)))

    if all_df.is_empty():
        print("All data removed after filtering; aborting.")
        return

    # Title date range (from data unless overridden)
    auto_start = str(all_df["vt_hour"].min()) if "vt_hour" in all_df.columns else None
    auto_end = str(all_df["vt_hour"].max()) if "vt_hour" in all_df.columns else None
    start_date = args.start_date or auto_start
    end_date = args.end_date or auto_end

    # Cycle hours if available
    if "cycle_hour" in all_df.columns:
        cycle_hours = sorted(all_df["cycle_hour"].unique().to_list())
    else:
        cycle_hours = []

    # Experiment sets
    experiments = sorted(all_df["experiment"].unique().to_list())
    exp_names_map = _parse_mapping(args.exp_name, "exp-name")
    exp_color_map = _ensure_colors(experiments, _parse_mapping(args.exp_color, "exp-color"))

    lead_time_tag = f"_lt_{args.lead_time}" if args.lead_time is not None else ""

    # Plot (channel vs pressure profile selection)
    if "channel" in all_df.columns:
        plot_profiles_channel(all_df, args.outdir, args.title_prefix,
                              exp_color_map, exp_names_map, lead_time_tag,
                              start_date, end_date, cycle_hours, args.hours)
    elif "pressure_bracket" in all_df.columns:
        plot_profiles_pressure(all_df, args.outdir, args.title_prefix,
                               exp_color_map, exp_names_map, lead_time_tag,
                               start_date, end_date, cycle_hours, args.hours)
    else:
        print("No profile dimension (channel/pressure_bracket) found: skipping profile plot.")

    plot_timeseries(all_df, args.outdir, args.title_prefix,
                    exp_color_map, exp_names_map, lead_time_tag,
                    start_date, end_date, cycle_hours, args.hours)

if __name__ == "__main__":
    main()
