import argparse, os
import polars as pl
import matplotlib.pyplot as plt
from typing import Dict

METRIC_STYLES = {
    "rmse": {"linestyle": "--", "label": "RMSE"},
    "bias": {"linestyle": "-", "label": "Bias"}
}

def _aggregate_profile(df: pl.DataFrame) -> pl.DataFrame:
    return (df.group_by(["experiment", "pressure_level", "obstypevar"])
              .agg([
                  pl.mean("bias").alias("bias"),
                  pl.mean("rmse").alias("rmse"),
                  pl.sum("n_samples").alias("n_sum")
              ]))

def plot_temp_profiles(df: pl.DataFrame, outdir: str, exp_colors: Dict[str, str]) -> None:
    agg = _aggregate_profile(df)
    variables = agg["obstypevar"].unique().to_list()

    for var in variables:
        var_df = agg.filter(pl.col("obstypevar") == var)
        counts = (var_df.group_by("pressure_level")
                      .agg(pl.sum("n_sum").alias("n_all"))
                      .sort("pressure_level", descending=True))

        fig, ax = plt.subplots(figsize=(12.0, 10.0))
        ax2 = ax.twiny()
        ax2.barh(counts["pressure_level"], counts["n_all"], color="gray", alpha=0.15, height=10)
        ax2.set_xlabel("Count")

        line_handles = []
        exps_in_order = list(exp_colors.keys())
        xmax = None
        xmin = None

        for exp in exps_in_order:
            sub = var_df.filter(pl.col("experiment") == exp).sort("pressure_level", descending=True)
            if sub.is_empty(): continue

            h_rmse, = ax.plot(sub["rmse"], sub["pressure_level"],
                              color=exp_colors[exp], linestyle=METRIC_STYLES["rmse"]["linestyle"],
                              marker="o", label=f"RMSE {exp}")
            line_handles.append(h_rmse)

        for exp in exps_in_order:
            sub = var_df.filter(pl.col("experiment") == exp).sort("pressure_level", descending=True)
            if sub.is_empty(): continue

            h_bias, = ax.plot(sub["bias"], sub["pressure_level"],
                              color=exp_colors[exp], linestyle=METRIC_STYLES["bias"]["linestyle"],
                              marker="s", label=f"Bias {exp}")
            line_handles.append(h_bias)
            vals = list(sub["rmse"]) + list(sub["bias"])
            if vals:
                vmax = max(vals); vmin = min(vals)
                xmax = vmax if xmax is None else max(xmax, vmax)
                xmin = vmin if xmin is None else min(xmin, vmin)

        if xmin is not None and xmax is not None:
            span = xmax - xmin if xmax != xmin else (abs(xmax) if xmax != 0 else 1.0)
            ax.set_xlim(xmin - 0.05*span, xmax + 0.18*span)

        ax.axvline(0, color='black', linestyle='-', linewidth=1)
        ax.set_title(f"Temp Profile - {var}")
        ax.set_xlabel("Value")
        ax.set_ylabel("Pressure (hPa)")
        ax.set_yticks(counts["pressure_level"].to_list())
        ax.invert_yaxis()
        ax.grid(True, which="both", linestyle="--", linewidth=0.5, alpha=0.7)
        ax.legend(handles=line_handles, loc="upper right", framealpha=0.85, ncol=1, frameon=False)
        fig.tight_layout()

        plot_dir = os.path.join(outdir, f"temp_{var}")
        os.makedirs(plot_dir, exist_ok=True)
        path = os.path.join(plot_dir, f"temp_{var}_profile.png")
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved plot: {path}")

def plot_series(df: pl.DataFrame, outdir: str, exp_colors: Dict[str, str], x_axis: str) -> None:
    if x_axis == "lead_time":
        agg = df.group_by(["experiment", "lead_time", "obstypevar"]).agg(pl.mean("bias"), pl.mean("rmse"), pl.sum("n_samples").alias("n_sum")).sort("lead_time")
        x_label = "Lead Time (h)"
    elif x_axis == "vt_hour":
        agg = df.group_by(["experiment", "vt_hour", "obstypevar"]).agg(pl.mean("bias"), pl.mean("rmse"), pl.sum("n_samples").alias("n_sum")).sort("vt_hour").with_columns(pl.col('vt_hour').cast(str))
        x_label = "Valid Time"
    else:
        raise ValueError(f"Unknown x_axis: {x_axis}")

    variables = agg["obstypevar"].unique().to_list()

    for var in variables:
        var_df = agg.filter(pl.col("obstypevar") == var)
        counts = (var_df.group_by(x_axis).agg(pl.sum("n_sum").alias("n_all")).sort(x_axis))

        fig, ax = plt.subplots(figsize=(12.0, 10.0))
        ax2 = ax.twinx()
        ax2.bar(counts[x_axis], counts["n_all"], color="gray", alpha=0.12, width=0.8)
        ax2.set_ylabel("Count")

        line_handles = []
        exps_in_order = list(exp_colors.keys())
        ymax, ymin = None, None

        for exp in exps_in_order:
            sub = var_df.filter(pl.col("experiment") == exp).sort(x_axis)
            if sub.is_empty(): continue
            h_rmse, = ax.plot(sub[x_axis], sub["rmse"], color=exp_colors[exp], linestyle="--", marker="o", label=f"RMSE {exp}")
            line_handles.append(h_rmse)

        for exp in exps_in_order:
            sub = var_df.filter(pl.col("experiment") == exp).sort(x_axis)
            if sub.is_empty(): continue
            h_bias, = ax.plot(sub[x_axis], sub["bias"], color=exp_colors[exp], linestyle="-", marker="s", label=f"Bias {exp}")
            line_handles.append(h_bias)
            vals = list(sub["rmse"]) + list(sub["bias"])
            if vals:
                vmax, vmin = max(vals), min(vals)
                ymax = vmax if ymax is None else max(ymax, vmax)
                ymin = vmin if ymin is None else min(ymin, vmin)

        if ymin is not None and ymax is not None:
            span = ymax - ymin if ymax != ymin else (abs(ymax) if ymax != 0 else 1.0)
            ax.set_ylim(ymin - 0.05 * span, ymax + 0.18 * span)

        ax.axhline(0, color='black', linestyle='-', linewidth=1)
        ax.set_title(f"Temp Series - {var} - {x_label}")
        ax.set_xlabel(x_label)
        ax.set_ylabel("Value")
        ax.tick_params(axis='x', rotation=45)
        ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)
        ax.legend(handles=line_handles, loc="upper right", framealpha=0.85, frameon=False)
        fig.tight_layout()

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

    plot_temp_profiles(df, args.outdir, exp_colors)
    plot_series(df, args.outdir, exp_colors, "lead_time")
    plot_series(df, args.outdir, exp_colors, "vt_hour")

if __name__ == "__main__":
    main()