Write-Host "Installing Apache Airflow 2.7.3..."
pip install "apache-airflow==2.7.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.8.txt"

Write-Host "Installing additional libraries (requests, pandas)..."
pip install requests pandas

Write-Host "Installation complete!"
