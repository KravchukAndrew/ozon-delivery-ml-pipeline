#!/usr/bin/env bash

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

if [ ! -f ".airflow-venv/bin/activate" ]; then
  echo "ERROR: .airflow-venv not found."
  echo "Create it and install Airflow first."
  exit 1
fi

source .airflow-venv/bin/activate

export AIRFLOW_HOME="$PROJECT_DIR/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_DIR/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True

mkdir -p "$AIRFLOW_HOME"

echo "============================================================"
echo "Starting local Airflow"
echo "============================================================"
echo "Project dir:   $PROJECT_DIR"
echo "AIRFLOW_HOME:  $AIRFLOW_HOME"
echo "DAGS_FOLDER:   $AIRFLOW__CORE__DAGS_FOLDER"
echo
echo "Airflow UI:"
echo "http://localhost:8080"
echo
echo "Login: admin"
echo "Password file:"
echo "$AIRFLOW_HOME/standalone_admin_password.txt"
echo "============================================================"

airflow standalone
