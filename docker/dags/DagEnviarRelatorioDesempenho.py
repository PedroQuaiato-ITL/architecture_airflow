from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from DAG_relatorios_desempenho_revendas.RelatoriosRevendas import RelatoriosRevendas


with DAG(
    "DAG_relatorio_revendas_diario",
    description="Gera relatório por gerente, envia e salva no Elasticsearch",
    schedule_interval="0 9 * * *",  # roda todo dia às 6h
    start_date=datetime(2025, 6, 23),
    catchup=False,
    tags=["relatorio", "revendas"],
) as dag:

    task_executing_query_dataset = PythonOperator(
        task_id="executing_query_dataset",
        python_callable=RelatoriosRevendas.executing_query_dataset,
    )

    task_organized_data = PythonOperator(
        task_id="organized_data",
        python_callable=RelatoriosRevendas.organized_data,
    )

    task_separed_data = PythonOperator(
        task_id="separed_data",
        python_callable=RelatoriosRevendas.separed_data,
    )

    task_creating_csv = PythonOperator(
        task_id="creating_csv",
        python_callable=RelatoriosRevendas.creating_csv,
    )

    task_enviar_para_elasticsearch = PythonOperator(
        task_id="enviar_para_elasticsearch",
        python_callable=RelatoriosRevendas.enviar_para_elasticsearch,
    )

    task_enviar_email_csvs = PythonOperator(
        task_id="enviar_email_csvs",
        python_callable=RelatoriosRevendas.enviar_email_csvs,
    )

    # Definindo a ordem de execução
    task_executing_query_dataset >> task_organized_data >> task_separed_data
    task_separed_data >> [task_creating_csv, task_enviar_para_elasticsearch, task_enviar_email_csvs]
