mkdir -p ./dags ./logs ./plugins ./config
echo "AIRFLOW_UID=$(id -u)" > .env
docker-compose up
