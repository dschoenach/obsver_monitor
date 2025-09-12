#!/bin/bash
set -e

# --- Master Configuration ---
# All results will go into this single directory
export MASTER_OUTPUT="webapp/out/unified_verification"

# --- Experiment Configuration ---
# Base names for experiments
export EXP_A_BASE="meps2_preop_rednmc06"
export EXP_B_BASE="meps2_preop_rednmc06_t2h2"

# Names for Obsver (used in file names and python scripts)
export OBSVER_EXP_A="${EXP_A_BASE}"
export OBSVER_EXP_B="${EXP_B_BASE}"

# Names for Monitor (often has 'mbr000' suffix)
export MONITOR_EXP_A="${EXP_A_BASE}mbr000"
export MONITOR_EXP_B="${EXP_B_BASE}mbr000"

# --- Path Configuration ---
# Paths for Obsver data
export OBSVER_PATH_A="data/obsver/${MONITOR_EXP_A}"
export OBSVER_PATH_B="data/obsver/${MONITOR_EXP_B}"

# --- Date/Time Configuration ---
export START="2025070200"
export END="2025070321" #80121"
export FCINT=12

# --- Obsver Specific Configuration ---
export OBSVARS="atms_tb" # "amsua_tb" "mhs_tb" etc.
export USE_COMMON_KEYS=1
export ROUND_DEC=2
export EXP_COLORS="#1f77b4 #d62728"
export GENERATE_LEADTIME_PLOTS=0 # Set to 0 to disable per-lead-time plots in obsver


# ==================================================
#         STARTING COMPLETE VERIFICATION SUITE
# ==================================================
echo "Cleaning and creating master output at: ${MASTER_OUTPUT}"
rm -rf "$MASTER_OUTPUT"
mkdir -p "$MASTER_OUTPUT"

# --- Run Monitor Verification ---
echo -e "\n----- Running Monitor Workflow -----"
# We set the environment variable that run_all_monitor.sh will now use
export MONITOR_OUTPUT="${MASTER_OUTPUT}/monitor"
bash scr/run_all_monitor.sh

# --- Run Obsver Verification ---
echo -e "\n----- Running Obsver Workflow -----"
# We set the environment variable that run_all_obsver.sh will now use
export OBSVER_OUTPUT="${MASTER_OUTPUT}/obsver"
bash scr/run_all_obsver.sh


echo -e "\n=================================================="
echo "    COMPLETE VERIFICATION SUITE FINISHED"
echo "    Results are in: ${MASTER_OUTPUT}"
echo "=================================================="