#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Define the directory where the script is located
export SCRIPTS_FOLDER=$(dirname "$(readlink -f "$0")")

# Define the log directory and file
export LOGDIR="${SCRIPTS_FOLDER}/logs"
export LOG_FILE="${LOGDIR}/WebScraping_Graham_$(date +"%Y%m%d%H%M%S").log"

# Ensure the log directory exists
mkdir -p "${LOGDIR}"

# Set up logging to file
exec > >(tee "${LOG_FILE}") 2>&1

# Print start time
echo "Starting the Python script at $(date)"

# Activate the virtual environment
source "${SCRIPTS_FOLDER}/venv/bin/activate"

# Run the Python script
python3 "${SCRIPTS_FOLDER}/WebScraping_Graham.py"

# Print completion time
echo "Python script completed successfully at $(date)"

# Deactivate the virtual environment
deactivate

# Print location of the log file
echo "Log file created at: ${LOG_FILE}"
