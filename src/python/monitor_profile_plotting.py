import argparse, os
import json
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from typing import Dict, Optional, List

METRIC_STYLES = {
    "rmse": {"linestyle": "--", "label": "RMSE"},
    "bias": {"linestyle": "-", "label": "Bias"}
}

def _load_var_names():
    """Load variable name mapping JSON; return safe default on failure."""
    var_name_path = os.path.join(os.path.dirname(__file__), "var_names.json")
    var_names = {"surface": {}, "upper_air": {}}
    if os.path.exists(var_name_path):
        try:
            import json as _json
            with open(var_name_path, "r", encoding="utf-8") as f:
                data = _json.load(f)
                if isinstance(data, dict):
                    var_names.update({k: v for k, v in data.items() if isinstance(v, dict)})
        except Exception:
            pass
    return var_names

def _aggregate_profile(df: pl.DataFrame) -> pl.DataFrame:
    return (df.group_by(["experiment", "pressure_level", "obstypevar"])
              .agg([
                  pl.mean("bias").alias("bias"),
                  pl.mean("rmse").alias("rmse"),
                  pl.sum("n_samples").alias("n_sum")
              ]))

def plot_temp_profiles(df: pl.DataFrame, outdir: str, exp_colors: Dict[str, str], exp_names: Dict[str, str], start_date: str, end_date: str, fcint: Optional[int], monitor_temp_cycles: Optional[int], cycles: Optional[List[int]]) -> None: 
    agg = _aggregate_profile(df)
    var_names = _load_var_names()
    variables = agg["obstypevar"].unique().to_list()

    for var in variables:
        var_df = agg.filter(pl.col("obstypevar") == var)
        counts = (var_df.group_by("pressure_level")
                      .agg(pl.sum("n_sum").alias("n_all"))
                      .sort("pressure_level", descending=True))

        fig, ax = plt.subplots(figsize=(15.0, 10.0))
        plt.subplots_adjust(right=0.75)
        ax2 = ax.twiny()
        ax2.barh(counts["pressure_level"], counts["n_all"], color="gray", alpha=0.15, height=10)
        ax2.set_xlabel("Count")

        line_handles = []
        exps_in_order = list(exp_colors.keys())

        for exp in exps_in_order:
            sub = var_df.filter(pl.col("experiment") == exp).sort("pressure_level", descending=True)
            if sub.is_empty(): continue

            disp_name = exp_names.get(exp, exp)
            h_rmse, = ax.plot(sub["rmse"], sub["pressure_level"],
                              color=exp_colors[exp], linestyle=METRIC_STYLES["rmse"]["linestyle"],
                              marker="o", label=f"RMSE {disp_name}")
            line_handles.append(h_rmse)

        for exp in exps_in_order:
            sub = var_df.filter(pl.col("experiment") == exp).sort("pressure_level", descending=True)
            if sub.is_empty(): continue

            disp_name = exp_names.get(exp, exp)
            h_bias, = ax.plot(sub["bias"], sub["pressure_level"],
                              color=exp_colors[exp], linestyle=METRIC_STYLES["bias"]["linestyle"],
                              marker="s", label=f"Bias {disp_name}")
            line_handles.append(h_bias)

        ax.axvline(0, color='black', linestyle='-', linewidth=1)
        entry = var_names.get("upper_air", {}).get(var, var)
        if isinstance(entry, dict):
            label = entry.get("label", var)
            unit = entry.get("unit")
        else:
            label, unit = entry, None
        base_title = f"{label} [{unit}]" if unit else label
        title = base_title
        if start_date and end_date:
            title += f"\n{start_date} - {end_date}"
        
        title_line3 = ""
        if fcint is not None:
            fcint_hours = ", ".join(f"{h:02d}" for h in range(0, 24, fcint))
            title_line3 = f"{fcint_hours} UTC"

        if cycles is not None:
            cycles_str = ", ".join(f"{c:02d}" for c in cycles)
            if title_line3:
                title_line3 += f" + {{{cycles_str}}}"
            else:
                title_line3 = f"{{{cycles_str}}}"

        if title_line3:
            title += f"\n{title_line3}"

        ax.set_title(title)
        # Use variable unit on x-axis for profile plots
        ax.set_xlabel(unit if unit else "Value")
        ax.set_ylabel("Pressure (hPa)")
        ax.set_yticks(counts["pressure_level"].to_list())
        ax.invert_yaxis()
        ax.grid(True, which="both", linestyle="--", linewidth=0.5, alpha=0.7)
        ax.legend(handles=line_handles, loc='center left', bbox_to_anchor=(1, 0.5), frameon=True)

        plot_dir = os.path.join(outdir, f"temp_{var}")
        os.makedirs(plot_dir, exist_ok=True)
        path = os.path.join(plot_dir, f"temp_{var}_profile.png")
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved plot: {path}")

def plot_series(df: pl.DataFrame, outdir: str, exp_colors: Dict[str, str], exp_names: Dict[str, str], x_axis: str, start_date: str, end_date: str, fcint: Optional[int], monitor_temp_cycles: Optional[int], cycles: Optional[List[int]]) -> None: 
    if x_axis == "lead_time":
        agg = df.group_by(["experiment", "lead_time", "obstypevar"]).agg(pl.mean("bias"), pl.mean("rmse"), pl.sum("n_samples").alias("n_sum")).sort("lead_time")
        x_label = "Lead Time (h)"
    elif x_axis == "vt_hour":
        agg = df.group_by(["experiment", "vt_hour", "obstypevar"]).agg(pl.mean("bias"), pl.mean("rmse"), pl.sum("n_samples").alias("n_sum")).sort("vt_hour").with_columns(pl.col('vt_hour').cast(str))
        x_label = "Valid Time"
    else:
        raise ValueError(f"Unknown x_axis: {x_axis}")

    var_names = _load_var_names()
    variables = agg["obstypevar"].unique().to_list()

    for var in variables:
        var_df = agg.filter(pl.col("obstypevar") == var)
        counts = (var_df.group_by(x_axis).agg(pl.sum("n_sum").alias("n_all")).sort(x_axis))

        fig, ax = plt.subplots(figsize=(15.0, 10.0))
        plt.subplots_adjust(right=0.75)
        ax2 = ax.twinx()
        ax2.bar(counts[x_axis], counts["n_all"], color="gray", alpha=0.12, width=0.8)
        ax2.set_ylabel("Count")

        line_handles = []
        exps_in_order = list(exp_colors.keys())

        for exp in exps_in_order:
            sub = var_df.filter(pl.col("experiment") == exp).sort(x_axis)
            if sub.is_empty(): continue
            disp_name = exp_names.get(exp, exp)
            h_rmse, = ax.plot(sub[x_axis], sub["rmse"], color=exp_colors[exp], linestyle="--", marker="o", label=f"RMSE {disp_name}")
            line_handles.append(h_rmse)

        for exp in exps_in_order:
            sub = var_df.filter(pl.col("experiment") == exp).sort(x_axis)
            if sub.is_empty(): continue
            disp_name = exp_names.get(exp, exp)
            h_bias, = ax.plot(sub[x_axis], sub["bias"], color=exp_colors[exp], linestyle="-", marker="s", label=f"Bias {disp_name}")
            line_handles.append(h_bias)

        ax.axhline(0, color='black', linestyle='-', linewidth=1)
        entry = var_names.get("upper_air", {}).get(var, var)
        if isinstance(entry, dict):
            label = entry.get("label", var)
            unit = entry.get("unit")
        else:
            label, unit = entry, None
        base_title = f"{label} [{unit}]" if unit else label
        title = base_title
        if start_date and end_date:
            title += f"\n{start_date} - {end_date}"

        title_line3 = ""
        if fcint is not None:
            fcint_hours = ", ".join(f"{h:02d}" for h in range(0, 24, fcint))
            title_line3 = f"{fcint_hours} UTC"

        if cycles is not None:
            cycles_str = ", ".join(f"{c:02d}" for c in cycles)
            if title_line3:
                title_line3 += f" + {{{cycles_str}}}"
            else:
                title_line3 = f"{{{cycles_str}}}"

        if title_line3:
            title += f"\n{title_line3}"

        ax.set_title(title)
        ax.set_xlabel(x_label)
        # Use variable unit on y-axis when available for series
        ax.set_ylabel(unit if unit else "Value")
        # Match monitor_plotting.py x-axis tick density behavior
        if x_axis == "vt_hour":
            ax.xaxis.set_major_locator(mticker.MaxNLocator(nbins=10))
        elif x_axis == "lead_time":
            max_lead_time = df["lead_time"].max()
            if max_lead_time is not None:
                ax.set_xticks(range(0, max_lead_time + 1, 3))
        ax.tick_params(axis='x', rotation=45)
        ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)
        ax.legend(handles=line_handles, loc='center left', bbox_to_anchor=(1.14, 0.5), frameon=True)

        plot_dir = os.path.join(outdir, f"temp_{var}")
        os.makedirs(plot_dir, exist_ok=True)
        path = os.path.join(plot_dir, f"temp_{var}_{x_axis}_series.png")
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved plot: {path}")

def main() -> None: 
    parser = argparse.ArgumentParser(description="Plotting for temperature profile metrics.")
    parser.add_argument("--metrics", required=True, help="Metrics parquet file.")
    parser.add_argument("--outdir", required=True, help="Output directory for plots.")
    parser.add_argument("--exp-color", action="append",
                        help="Experiment color mapping EXP=COLOR (repeatable).")
    parser.add_argument("--exp-name", action="append",
                        help="Experiment name mapping LONG_NAME=SHORT_NAME (repeatable).")
    parser.add_argument("--fcint", type=int, help="Forecast interval in hours.")
    parser.add_argument("--monitor-temp-cycles", type=int, help="Cycle interval for temperature plots.")
    args = parser.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    plt.rcParams.update({
        'font.size': 18, 'axes.titlesize': 18, 'axes.labelsize': 16,
        'xtick.labelsize': 12, 'ytick.labelsize': 16, 'legend.fontsize': 16,
        'figure.titlesize': 20
    })

    df = pl.read_parquet(args.metrics)
    if df.is_empty():
        print("Empty metrics file; aborting.")
        return

    cycles = None
    if args.monitor_temp_cycles:
        max_lead_time = df["lead_time"].max()
        if max_lead_time is not None:
            cycles = range(args.monitor_temp_cycles, max_lead_time + 1, args.monitor_temp_cycles)
            df = df.filter(pl.col("lead_time").is_in(cycles))

    start_date = df["vt_hour"].min()
    end_date = df["vt_hour"].max()

    exp_names_map: Dict[str, str] = {}
    if args.exp_name:
        for spec in args.exp_name:
            if "=" not in spec: 
                print(f"Ignoring malformed --exp-name '{spec}' (need LONG_NAME=SHORT_NAME).")
                continue
            k, v = spec.split("=", 1)
            exp_names_map[k] = v

    exp_names = sorted(df.select(pl.col("experiment").unique()).to_series().to_list())
    exp_colors: Dict[str, str] = {}
    if args.exp_color:
        for spec in args.exp_color:
            if "=" not in spec: continue
            k, v = spec.split("=", 1)
            exp_colors[k] = v

    import itertools
    default_cycle = itertools.cycle(plt.rcParams['axes.prop_cycle'].by_key()['color'])
    for e in exp_names:
        if e not in exp_colors:
            exp_colors[e] = next(default_cycle)

    plot_temp_profiles(df, args.outdir, exp_colors, exp_names_map, start_date, end_date, args.fcint, args.monitor_temp_cycles, cycles)
    plot_series(df, args.outdir, exp_colors, exp_names_map, "lead_time", start_date, end_date, args.fcint, args.monitor_temp_cycles, cycles)
    plot_series(df, args.outdir, exp_colors, exp_names_map, "vt_hour", start_date, end_date, args.fcint, args.monitor_temp_cycles, cycles)

if __name__ == "__main__":
    main()
