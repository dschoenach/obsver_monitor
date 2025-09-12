#!/usr/bin/env bash
set -euo pipefail

# --- Configuration ---
# Use environment variables from master script, or provide defaults
EXPS=("${OBSVER_EXP_A:-meps2_preop_rednmc06}" "${OBSVER_EXP_B:-meps2_preop_rednmc06_t2h2}")
EXP_NAMES=("${EXP_A_NAME:-${EXPS[0]}}" "${EXP_B_NAME:-${EXPS[1]}}")
EXPPATHS=("${OBSVER_PATH_A:-data/obsver/meps2_preop_rednmc06mbr000}" "${OBSVER_PATH_B:-data/obsver/meps2_preop_rednmc06_t2h2mbr000}")
OBSVARS_STR="${OBSVARS:-atms_tb}"
# No eval needed here, can be read directly into an array
read -r -a OBSVARS <<< "$OBSVARS_STR"

USE_COMMON_KEYS="${USE_COMMON_KEYS:-0}"
START="${START:-2025070200}"
END="${END:-2025080121}"
FCINT="${FCINT:-12}"
ROUND_DEC="${ROUND_DEC:-2}"
EXP_COLORS_STR="${EXP_COLORS:-#1f77b4 #d62728}"
# No eval needed here, can be read directly into an array
read -r -a EXP_COLORS <<< "$EXP_COLORS_STR"
GENERATE_LEADTIME_PLOTS="${GENERATE_LEADTIME_PLOTS:-1}"

# --- Paths ---
OUTDIR="${OBSVER_OUTPUT:-out/obsver_run/obsver}"
PLOTS="${OUTDIR}/plots"
mkdir -p "${OUTDIR}" "${PLOTS}"

echo "Experiments: ${EXPS[*]}"
echo "Display Names: ${EXP_NAMES[*]}"
echo "Variables: ${OBSVARS[*]}"
echo "Output: ${OUTDIR}"

# --- Per-variable common key generation + verification ---
for OV in "${OBSVARS[@]}"; do
  echo "--------------------------------------------------"
  OBSTYPEVAR="$OV"
  PARAMETER=""
  if [[ $OV == *_tb ]]; then
    PARAMETER="tb"
  fi
  echo "PARAMETER is $PARAMETER"

  if [[ "${USE_COMMON_KEYS}" -eq 1 ]]; then
    echo "Building common observation keys for ${OBSTYPEVAR}"
    KEYFILE="${OUTDIR}/common_${OBSTYPEVAR}_keys.parquet"

    BUILD_ARGS=()
    for i in "${!EXPS[@]}"; do
      BUILD_ARGS+=(--exp "${EXPS[$i]}" "${EXPPATHS[$i]}")
    done

    python3 -m src.build_common_keys \
      --obstypevar "${OBSTYPEVAR}" \
      --round-dec "${ROUND_DEC}" \
      --out "${KEYFILE}" \
      "${BUILD_ARGS[@]}"
  else
    KEYFILE=""
    echo "Skipping common key build for ${OBSTYPEVAR} (USE_COMMON_KEYS=0)"
  fi

  echo "Running verification for ${OBSTYPEVAR}"
  for i in "${!EXPS[@]}"; do
    EXP="${EXPS[$i]}"
    ROOT="${EXPPATHS[$i]}"
    OUT_MET="${OUTDIR}/${EXP}_${OBSTYPEVAR}_metrics.parquet"
    echo "  ${EXP} -> ${OUT_MET}"
    CMD=(
      python3 -m src.verify
      --exp-name "${EXP}"
      --data-root "${ROOT}"
      --obstypevar "${OBSTYPEVAR}"
      --start "${START}"
      --end "${END}"
      --out "${OUT_MET}"
      --jobs 8
      --by-model
      --fcint "${FCINT}"
      --round-dec "${ROUND_DEC}"
      --by-lead
    )
    if [[ -n "${PARAMETER}" ]]; then
      CMD+=(--parameter "${PARAMETER}")
    fi
    if [[ -n "${KEYFILE}" ]]; then
      CMD+=(--key-filter "${KEYFILE}")
    fi
    "${CMD[@]}"
  done
done

# --- Plotting (keeps EXPS order) ---
ALL_LEAD_TIMES_PER_VAR=()
for OV in "${OBSVARS[@]}"; do
  OBSTYPEVAR="$OV"
  METRICS_FILES=()
  for EXP in "${EXPS[@]}"; do
    METRICS_FILES+=("${OUTDIR}/${EXP}_${OBSTYPEVAR}_metrics.parquet")
  done
  OV_PLOT_DIR="${PLOTS}/${OBSTYPEVAR}"
  mkdir -p "${OV_PLOT_DIR}"
  echo "Plotting combined profiles & timeseries for ${OBSTYPEVAR}"

  COLOR_ARGS=()
  NAME_ARGS=()
  for i in "${!EXPS[@]}"; do
    if [[ -n "${EXP_COLORS[$i]:-}" ]]; then
      COLOR_ARGS+=(--exp-color "${EXPS[$i]}"="${EXP_COLORS[$i]}")
    fi
    NAME_ARGS+=(--exp-name "${EXPS[$i]}"="${EXP_NAMES[$i]}")
  done

  # Extract unique lead times from the first experiment's metrics file (sorted).
  # Falls back to empty string if column missing or file unreadable.
  LTS=$(python3 - <<PY
import polars as pl, sys
path = "${METRICS_FILES[0]}"
try:
    df = pl.read_parquet(path)
except Exception:
    print("")
    raise SystemExit
if "lead_time" not in df.columns or df.is_empty():
    print("")
else:
    lts = sorted(df["lead_time"].unique().to_list())
    print(" ".join(str(x) for x in lts))
PY
)

  ALL_LEAD_TIMES_PER_VAR+=("$LTS")

  python3 -m src.joint_plotting \
    --metrics "${METRICS_FILES[@]}" \
    --outdir "${OV_PLOT_DIR}" \
    --title-prefix "${OBSTYPEVAR}" \
    --start-date "$START" \
    --end-date "$END" \
    "${COLOR_ARGS[@]}" \
    "${NAME_ARGS[@]}" &

  # Per-lead-time plots
  if [[ "${GENERATE_LEADTIME_PLOTS}" -eq 1 ]]; then
    if [[ -n "${LTS}" ]]; then
      for LT in ${LTS}; do
        python3 -m src.joint_plotting \
          --metrics "${METRICS_FILES[@]}" \
          --outdir "${OV_PLOT_DIR}" \
          --title-prefix "${OBSTYPEVAR}" \
          --lead-time "${LT}" \
          --start-date "$START" \
          --end-date "$END" \
          "${COLOR_ARGS[@]}" \
          "${NAME_ARGS[@]}" &
      done
    fi
  else
    echo "Skipping per-lead-time plots for ${OBSTYPEVAR} (GENERATE_LEADTIME_PLOTS != 1)"
  fi
done

wait

# --- Scorecard (explicit order for title/legend) ---
# Collect only metric files corresponding to variables in OBSVARS (and that actually exist)
METRICS_FILES=()
for OV in "${OBSVARS[@]}"; do
  for EXP in "${EXPS[@]}"; do
    f="${OUTDIR}/${EXP}_${OV}_metrics.parquet"
    [[ -f "$f" ]] && METRICS_FILES+=("$f")
  done
done

if [[ ${#METRICS_FILES[@]} -gt 0 ]]; then
  # Fixed unmatched brace in echo (removed stray })
  echo "Building scorecard from ${#METRICS_FILES[@]} metric files (vars: ${OBSVARS[*]})"
  python3 -m src.scorecard \
    --exp-a "${EXPS[0]}" \
    --exp-b "${EXPS[1]}" \
    --exp-a-name "${EXP_NAMES[0]}" \
    --exp-b-name "${EXP_NAMES[1]}" \
    --metrics "${METRICS_FILES[@]}" \
    --outdir "${PLOTS}" \
    --title "Scorecard" \
    --start-date "$START" \
    --end-date "$END"
else
  echo "No metric files found for listed variables; skipping scorecard."
fi
