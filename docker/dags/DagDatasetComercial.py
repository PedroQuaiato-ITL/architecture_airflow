from airflow import DAG
from pendulum import timezone
from datetime import datetime
from airflow.operators.python import PythonOperator
from DAG_atualizacao_cadastral_revendas.DatasetLoadedDataMining import DatasetLoadedDataMining

class DagDatasetComercial:
    @staticmethod
    def dag_comercial_configuration():
        try:
            with DAG(
                dag_id="DAG_atualizacao_cadastral_revendas",
                start_date=datetime(2025, 6, 17, tzinfo=timezone('America/Sao_Paulo')),
                schedule_interval="30 1 * * *",
                catchup=False,
                tags=["Comercial", "Cadastral"]
            ) as dag:

                task1 = PythonOperator(
                    task_id="executing_query_gnio",
                    python_callable=DatasetLoadedDataMining.executing_query_gnio,
                    provide_context=True
                )

                task2 = PythonOperator(
                    task_id="organized_response_database",
                    python_callable=DatasetLoadedDataMining.organized_response_database,
                    provide_context=True
                )

                task3 = PythonOperator(
                    task_id="insert_database",
                    python_callable=DatasetLoadedDataMining.insert_database,
                    provide_context=True
                )

                task1 >> task2 >> task3

                return dag

        except Exception as erro:
            raise RuntimeError("Erro ao configurar a DAG comercial: ") from erro

dag = DagDatasetComercial.dag_comercial_configuration()