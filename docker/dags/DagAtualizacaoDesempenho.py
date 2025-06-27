from airflow import DAG
from pendulum import timezone
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from DAG_atualizacao_desempenho_revendas.AutualizacaoDesempenho import DagAtualizacaoDesempenho

with DAG(
    dag_id='DAG_atualizacao_desemenho_revenda',
    start_date=datetime(2025, 6, 17, tzinfo=timezone('America/Sao_Paulo')),
    schedule_interval='0 1 * * *',
    catchup=False,
    tags=['Comercial', 'Desempenho']
) as dag:

    task_gnio = PythonOperator(
        task_id='executing_query_gnio',
        python_callable=DagAtualizacaoDesempenho.executing_querys_gnio
    )

    task_dataset = PythonOperator(
        task_id='executing_query_dataset',
        python_callable=DagAtualizacaoDesempenho.executing_querys_dataset
    )

    task_organize = PythonOperator(
        task_id='organized_results_query',
        python_callable=DagAtualizacaoDesempenho.organized_results_query
    )

    task_calculos = PythonOperator(
        task_id='calculos_results',
        python_callable=DagAtualizacaoDesempenho.calculos_results
    )

    task_insert = PythonOperator(
        task_id='insert_data',
        python_callable=DagAtualizacaoDesempenho.insert_data
    )

    [task_gnio, task_dataset] >> task_organize >> task_calculos >> task_insert
