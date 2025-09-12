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
EXP_COLORS_STR="${EXP_COLORS:-#1f77b4 #d62728}"

# Define paths
RBASE=$(pwd)
PROJECTNAME="monitor"
WORKDIR="${BASE_OUTDIR}/work"
PLOTS="${BASE_OUTDIR}/plots"
VFLD_ROOT="$RBASE/data/monitor/vfld"
VOBS_ROOT="$RBASE/data/monitor/vobs"
METRICS_FILE="${WORKDIR}/surface_metrics.parquet"
TEMP_METRICS_FILE="${WORKDIR}/temp_metrics.parquet"

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
# Convert CSV output to Parquet
if [[ -f "${WORKDIR}/surface_metrics.csv" ]]; then
    echo "Converting surface CSV metrics to Parquet..."
    python3 -c "import polars as pl; pl.read_csv('${WORKDIR}/surface_metrics.csv').write_parquet('${METRICS_FILE}')"
else
    echo "WARNING: C++ verification did not produce surface_metrics.csv"
fi
if [[ -f "${WORKDIR}/temp_metrics.csv" ]]; then
    echo "Converting temp CSV metrics to Parquet..."
    python3 -c "import polars as pl; pl.read_csv('${WORKDIR}/temp_metrics.csv').write_parquet('${TEMP_METRICS_FILE}')"
else
    echo "WARNING: C++ verification did not produce temp_metrics.csv"
fi


# --- Run Scorecard for Surface Metrics ---
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

# --- Run Scorecard for Temp Profiles ---
if [[ -f "$TEMP_METRICS_FILE" ]]; then
  echo "Building scorecard for monitor temp profiles..."
  python3 -m src.scorecard \
    --exp-a "$EXP1" \
    --exp-b "$EXP2" \
    --metrics "$TEMP_METRICS_FILE" \
    --outdir "$PLOTS" \
    --title "${PROJECTNAME}_temp"
else
  echo "WARNING: ${TEMP_METRICS_FILE} not found. Skipping temp scorecard."
fi

# --- Run Plotting for Surface Metrics ---
if [[ -f "$METRICS_FILE" ]]; then
  echo "Building timeseries and lead time plots for monitor surface metrics..."
  EXPS=("$EXP1" "$EXP2")
  read -r -a EXP_COLORS <<< "$EXP_COLORS_STR"

  COLOR_ARGS=()
  for i in "${!EXPS[@]}"; do
    if [[ -n "${EXP_COLORS[$i]:-}" ]]; then
      COLOR_ARGS+=(--exp-color "${EXPS[$i]}"="${EXP_COLORS[$i]}")
    fi
  done

  python3 -m src.monitor_plotting \
    --metrics "$METRICS_FILE" \
    --outdir "$PLOTS" \
    --title-prefix "${PROJECTNAME}_surface" \
    "${COLOR_ARGS[@]}"
else
  echo "WARNING: ${METRICS_FILE} not found. Skipping surface metric plots."
fi

# --- Run Plotting for Temp Profiles ---
if [[ -f "$TEMP_METRICS_FILE" ]]; then
  echo "Building temp profile plots for monitor..."
  EXPS=("$EXP1" "$EXP2")
  read -r -a EXP_COLORS <<< "$EXP_COLORS_STR"

  COLOR_ARGS=()
  for i in "${!EXPS[@]}"; do
    if [[ -n "${EXP_COLORS[$i]:-}" ]]; then
      COLOR_ARGS+=(--exp-color "${EXPS[$i]}"="${EXP_COLORS[$i]}")
    fi
  done

  python3 -m src.monitor_profile_plotting \
    --metrics "$TEMP_METRICS_FILE" \
    --outdir "$PLOTS" \
    "${COLOR_ARGS[@]}"
else
  echo "WARNING: ${TEMP_METRICS_FILE} not found. Skipping temp profile plots."
fi

echo "Monitor Verification Complete."
