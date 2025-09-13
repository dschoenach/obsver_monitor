#!/bin/bash
set -e

# --- Configuration ---
# Use environment variables from master script, or provide defaults
BASE_OUTDIR="${MONITOR_OUTPUT:-out/verification_run_monitor}"
read -r -a EXPS <<< "${MONITOR_EXP_BASES:-meps2_preop_rednmc06mbr000 meps2_preop_rednmc04mbr000}"
read -r -a EXP_NAMES <<< "${MONITOR_EXP_NAMES:-REF rednmc04}"
START="${START_MONITOR:-2025070200}"
END="${END_MONITOR:-2025073121}"
FCINT="${FCINT_MONITOR:-12}"
EXP_COLORS_STR="${EXP_COLORS_MONITOR:-#1f77b4 #d62728}"

# NEW: Common key restriction controls
RESTRICT_COMMON_KEYS="${RESTRICT_COMMON_KEYS:-1}"   # 1=enable, 0=disable
ROUND_DEC="${ROUND_DEC:-2}"

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
echo "Experiments: ${EXPS[*]}"
echo "Display Names: ${EXP_NAMES[*]}"
echo "Output: ${BASE_OUTDIR}"
echo "--------------------------------------------------"
mkdir -p "$WORKDIR" "$PLOTS"

# --- Run C++ Verification ---
echo "Running C++ verification..."
# The C++ executable is expected to be at src/cpp/verify_cpp_parallel
if [ ! -f "src/cpp/verify_cpp_parallel" ]; then
    echo "C++ executable not found at src/cpp/verify_cpp_parallel."
    echo "Please compile it first, e.g., with:"
    echo "g++ -std=c++17 -O3 -fopenmp src/cpp/verify_cpp_parallel.cpp -o src/cpp/verify_cpp_parallel"
    exit 1
fi
cp src/cpp/verify_cpp_parallel "$WORKDIR/"

cd "$WORKDIR"
chmod +x verify_cpp_parallel

CPP_ARGS=()
for EXP in "${EXPS[@]}"; do
  CPP_ARGS+=("$(readlink -f "${VFLD_ROOT}/${EXP}")")
done

time ./verify_cpp_parallel "$START" "$END" "$FCINT" \
  "$(readlink -f "${VOBS_ROOT}/vobs_meps")" \
  "${CPP_ARGS[@]}"


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


# --- Run Scorecards ---
if [[ -f "$METRICS_FILE" && ${#EXPS[@]} -gt 1 ]]; then
  echo "Building scorecards for monitor surface metrics..."
  for i in $(seq 1 $((${#EXPS[@]} - 1))); do
    python3 -m src.python.scorecard \
      --exp-a "${EXPS[0]}" \
      --exp-b "${EXPS[$i]}" \
      --exp-a-name "${EXP_NAMES[0]}" \
      --exp-b-name "${EXP_NAMES[$i]}" \
      --metrics "$METRICS_FILE" \
      --outdir "$PLOTS" \
      --title "${PROJECTNAME}_surface_${EXP_NAMES[0]}_vs_${EXP_NAMES[$i]}"
  done
else
  echo "WARNING: Not enough experiments for scorecard or metrics file not found."
fi

if [[ -f "$TEMP_METRICS_FILE" && ${#EXPS[@]} -gt 1 ]]; then
  echo "Building scorecards for monitor temp profiles..."
  for i in $(seq 1 $((${#EXPS[@]} - 1))); do
    python3 -m src.python.scorecard \
      --exp-a "${EXPS[0]}" \
      --exp-b "${EXPS[$i]}" \
      --exp-a-name "${EXP_NAMES[0]}" \
      --exp-b-name "${EXP_NAMES[$i]}" \
      --metrics "$TEMP_METRICS_FILE" \
      --outdir "$PLOTS" \
      --title "${PROJECTNAME}_temp_${EXP_NAMES[0]}_vs_${EXP_NAMES[$i]}"
  done
else
  echo "WARNING: Not enough experiments for temp scorecard or temp metrics file not found."
fi


# --- Run Plotting for Surface Metrics ---
if [[ -f "$METRICS_FILE" ]]; then
  echo "Building timeseries and lead time plots for monitor surface metrics..."
  read -r -a EXP_COLORS <<< "$EXP_COLORS_STR"

  COLOR_ARGS=()
  NAME_ARGS=()
  for i in "${!EXPS[@]}"; do
    if [[ -n "${EXP_COLORS[$i]:-}" ]]; then
      COLOR_ARGS+=(--exp-color "${EXPS[$i]}"="${EXP_COLORS[$i]}")
    fi
    NAME_ARGS+=(--exp-name "${EXPS[$i]}"="${EXP_NAMES[$i]}")
  done

  python3 -m src.python.monitor_plotting \
    --metrics "$METRICS_FILE" \
    --outdir "$PLOTS" \
    --title-prefix "${PROJECTNAME}_surface" \
    --fcint "$FCINT" \
    "${COLOR_ARGS[@]}" \
    "${NAME_ARGS[@]}" 
else
  echo "WARNING: ${METRICS_FILE} not found. Skipping surface metric plots."
fi

# --- Run Plotting for Temp Profiles ---
if [[ -f "$TEMP_METRICS_FILE" ]]; then
  echo "Building temp profile plots for monitor..."
  read -r -a EXP_COLORS <<< "$EXP_COLORS_STR"

  COLOR_ARGS=()
  NAME_ARGS=()
  for i in "${!EXPS[@]}"; do
    if [[ -n "${EXP_COLORS[$i]:-}" ]]; then
      COLOR_ARGS+=(--exp-color "${EXPS[$i]}"="${EXP_COLORS[$i]}")
    fi
    NAME_ARGS+=(--exp-name "${EXPS[$i]}"="${EXP_NAMES[$i]}")
  done

  TEMP_CYCLES_ARG=()
  if [[ -n "${MONITOR_TEMP_CYCLES:-}" ]]; then
    TEMP_CYCLES_ARG+=(--monitor-temp-cycles "${MONITOR_TEMP_CYCLES}")
  fi

  python3 -m src.python.monitor_profile_plotting \
    --metrics "$TEMP_METRICS_FILE" \
    --outdir "$PLOTS" \
    --fcint "$FCINT" \
    "${TEMP_CYCLES_ARG[@]}" \
    "${COLOR_ARGS[@]}" \
    "${NAME_ARGS[@]}" 
else
  echo "WARNING: ${TEMP_METRICS_FILE} not found. Skipping temp profile plots."
fi

wait

echo "Monitor Verification Complete."
