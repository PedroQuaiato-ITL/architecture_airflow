from airflow import DAG
from pendulum import timezone
from datetime import datetime
from airflow.operators.python import PythonOperator
from DAG_atualizacao_desempenho_revendas.AnaliseDesempenhoRevendas import AnaliseDesempenhoRevendas

with DAG(
    dag_id='DAG_analise_desempenho_revendas',
    start_date=datetime(2025, 7, 11, tzinfo=timezone('America/Sao_Paulo')),
    schedule_interval='0 1 * * *',
    catchup=False,
    tags=['Desempenho', 'Revendas', 'Comercial']
) as dag:

    executar_query_gnio = PythonOperator(
        task_id='executar_query_gnio',
        python_callable=AnaliseDesempenhoRevendas.executar_query_gnio
    )

    tratar_dataframe_gnio = PythonOperator(
        task_id='tratar_dataframe_gnio',
        python_callable=AnaliseDesempenhoRevendas.tratar_dataframe_gnio
    )

    executar_query_dataset = PythonOperator(
        task_id='executar_query_dataset',
        python_callable=AnaliseDesempenhoRevendas.executar_query_dataset
    )

    juntar_dataframes = PythonOperator(
        task_id='juntar_dataframes',
        python_callable=AnaliseDesempenhoRevendas.juntar_dataframes
    )

    realizar_calculos_desempenho = PythonOperator(
        task_id='realizar_calculos_desempenho',
        python_callable=AnaliseDesempenhoRevendas.realizar_calculos_desempenho
    )

    realizar_tratamento_dados = PythonOperator(
        task_id='tratamento_dataframe',
        python_callable=AnaliseDesempenhoRevendas.tratamento_pre_insercao
    )

    inserir_banco_dados = PythonOperator(
        task_id='inserir_banco_dados',
        python_callable=AnaliseDesempenhoRevendas.inserir_banco_dados
    )

    (
        executar_query_gnio
        >> tratar_dataframe_gnio
        >> executar_query_dataset
        >> juntar_dataframes
        >> realizar_calculos_desempenho
        >> realizar_tratamento_dados
        >> inserir_banco_dados
    )
