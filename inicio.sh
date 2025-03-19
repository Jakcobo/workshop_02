export AIRFLOW_HOME="$(pwd)/airflow"
echo "AIRFLOW_HOME establecido en ${AIRFLOW_HOME}"

airflow standalone
