#!/bin/bash
set -e

# --- Configuration ---
# Use environment variables from master script, or provide defaults
BASE_OUTDIR="${MONITOR_OUTPUT:-out/verification_run_monitor}"
EXP1="${MONITOR_EXP_A:-meps2_preop_rednmc06mbr000}"
EXP2="${MONITOR_EXP_B:-meps2_preop_rednmc06_t2h2mbr000}"
START="${START:-2025070200}"
END="${END:-2025073121}"
FCINT="${FCINT:-12}"

# Define paths
RBASE=$(pwd)
PROJECTNAME="monitor"
WORKDIR="${BASE_OUTDIR}/work"
PLOTS="${BASE_OUTDIR}/plots"
VFLD_ROOT="$RBASE/data/monitor/vfld"
VOBS_ROOT="$RBASE/data/monitor/vobs"

# --- Setup ---
echo "Running Monitor Verification"
echo "Experiments: $EXP1, $EXP2"
echo "Output: ${BASE_OUTDIR}"
echo "--------------------------------------------------"
mkdir -p "$WORKDIR" "$PLOTS"

# --- Run C++ Verification ---
echo "Running C++ verification..."
# The C++ executable is expected to be at src/verify_cpp_parallel
if [ ! -f "src/verify_cpp_parallel" ]; then
    echo "C++ executable not found at src/verify_cpp_parallel."
    echo "Please compile it first, e.g., with:"
    echo "g++ -std=c++17 -O3 -fopenmp src/verify_cpp_parallel.cpp -o src/verify_cpp_parallel"
    exit 1
fi
cp src/verify_cpp_parallel "$WORKDIR/"

cd "$WORKDIR"
chmod +x verify_cpp_parallel

./verify_cpp_parallel "$START" "$END" "$FCINT" \
  "$(readlink -f ${VOBS_ROOT}/vobs_meps)" \
  "$(readlink -f ${VFLD_ROOT}/${EXP1})" \
  "$(readlink -f ${VFLD_ROOT}/${EXP2})"

cd "$RBASE"
# Convert CSV output to Parquet for the scorecard script
METRICS_CSV="${WORKDIR}/surface_metrics.csv"
if [[ -f "$METRICS_CSV" ]]; then
    echo "Converting CSV metrics to Parquet..."
    python3 -c "import polars as pl; pl.read_csv('${METRICS_CSV}').write_parquet('${WORKDIR}/surface_metrics.parquet')"
else
    echo "WARNING: C++ verification did not produce surface_metrics.csv"
fi


# --- Run Scorecard for Surface Metrics ---
METRICS_FILE="${WORKDIR}/surface_metrics.parquet"
if [[ -f "$METRICS_FILE" ]]; then
  echo "Building scorecard for monitor surface metrics..."
  python3 -m src.scorecard \
    --exp-a "$EXP1" \
    --exp-b "$EXP2" \
    --metrics "$METRICS_FILE" \
    --outdir "$PLOTS" \
    --title "${PROJECTNAME}_surface"
else
  echo "WARNING: ${METRICS_FILE} not found. Skipping scorecard."
fi

echo "Monitor Verification Complete."
