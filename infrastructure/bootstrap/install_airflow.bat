@echo off
echo Installing Apache Airflow 2.7.3...
pip install "apache-airflow==2.7.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.8.txt"

echo Installing additional libraries (requests, pandas)...
pip install requests pandas

echo Installation complete!
pause
