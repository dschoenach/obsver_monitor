import argparse
import polars as pl
import matplotlib.pyplot as plt
import os
from typing import Dict

BRACKET_MIDPOINTS: Dict[str, int] = {
    "1050-950": 1000,
    "950-850": 900,
    "850-750": 800,
    "750-650": 700,
    "650-550": 600,
    "550-450": 500,
    "450-350": 400,
    "350-250": 300,
    "250-150": 200,
    "150-0": 75
}

def plot_profiles(df: pl.DataFrame, outdir: str, title: str) -> None:
    mapping_df = pl.DataFrame({"pressure_bracket": list(BRACKET_MIDPOINTS.keys()), "pressure_midpoint": list(BRACKET_MIDPOINTS.values())})
    for metric in ["bias", "mae", "rmse"]:
        fig, ax = plt.subplots(figsize=(6.2, 8.0))
        profile_data = (df.group_by("pressure_bracket")
                          .agg(pl.mean(metric).alias(metric))
                          .join(mapping_df, on="pressure_bracket", how="inner")
                          .sort("pressure_midpoint"))
        ax.plot(profile_data[metric], profile_data["pressure_midpoint"], marker="o", label=metric.upper())
        # Add headroom on x-axis to reduce legend overlap
        xmin = float(profile_data[metric].min())
        xmax = float(profile_data[metric].max())
        span = xmax - xmin if xmax != xmin else (abs(xmax) if xmax != 0 else 1.0)
        ax.set_xlim(xmin - 0.05 * span, xmax + 0.15 * span)
        ax.set_title(f"{title} - {metric.upper()} Profile")
        ax.set_xlabel(metric.upper())
        ax.set_ylabel("Pressure (hPa)")
        ax.set_yticks(list(BRACKET_MIDPOINTS.values()))
        ax.set_yticklabels(list(BRACKET_MIDPOINTS.keys()))
        ax.invert_yaxis()
        ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)
        ax.legend(loc="upper right", framealpha=0.85)
        fig.tight_layout()
        path = os.path.join(outdir, f"{metric}_profile.png")
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved plot: {path}")

def plot_timeseries(df: pl.DataFrame, outdir: str, title: str) -> None:
    for metric in ["bias", "mae", "rmse"]:
        fig, ax = plt.subplots(figsize=(9.5, 4.6))
        ts = (df.group_by("vt_hour")
                .agg(pl.mean(metric).alias(metric))
                .sort("vt_hour"))
        x = ts["vt_hour"].to_list()
        y = ts[metric].to_list()
        ax.plot(x, y, marker="o", label=metric.upper())
        # Add y headroom for legend
        if y:
            ymin = min(y)
            ymax = max(y)
            span = ymax - ymin if ymax != ymin else (abs(ymax) if ymax != 0 else 1.0)
            ax.set_ylim(ymin - 0.05 * span, ymax + 0.15 * span)
        ax.set_title(f"{title} - {metric.upper()} Time Series")
        ax.set_xlabel("Valid Time")
        ax.set_ylabel(metric.upper())
        ax.tick_params(axis='x', rotation=45)
        ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)
        ax.legend(loc="upper right", framealpha=0.85)
        fig.tight_layout()
        path = os.path.join(outdir, f"{metric}_timeseries.png")
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved plot: {path}")

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate verification plots.")
    parser.add_argument("--metrics", required=True, help="Path to the metrics Parquet file.")
    parser.add_argument("--outdir", required=True, help="Directory to save plots.")
    parser.add_argument("--title", required=True, help="Plot title prefix.")
    args = parser.parse_args()
    try:
        df = pl.read_parquet(args.metrics)
    except Exception as e:
        print(f"Error reading {args.metrics}: {e}")
        return
    os.makedirs(args.outdir, exist_ok=True)
    plot_profiles(df, args.outdir, args.title)
    plot_timeseries(df, args.outdir, args.title)

if __name__ == "__main__":
    main()
