import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from Modules.DatabaseConfigurationConnection import DatabaseConfigurationConnection as HookConnection


class AnaliseDesempenhoRevendas(LoggingMixin):

    @staticmethod
    def executar_query_gnio(**kwargs):
        try:
            connection_database, _ = HookConnection.databaseConfigurationGnio()
            sql = Variable.get('QUERY_REVENDAS_DESEMPENHO_GNIO')
            df = pd.read_sql(sql, connection_database)

            json_data = df.to_json(orient='records')
            kwargs['ti'].xcom_push(key='df_json_gnio', value=json_data)

        except Exception as erro:
            AnaliseDesempenhoRevendas().log.error(f"Erro ao executar query GNIO: {erro}")
            raise

    @staticmethod
    def tratar_dataframe_gnio(**kwargs):
        try:
            ti = kwargs['ti']
            json_data = ti.xcom_pull(key='df_json_gnio', task_ids='executar_query_gnio')
            df = pd.read_json(json_data, orient='records')

            # Limpeza e renomeio
            df.drop(columns=['tiposeq', 'tipo', 'idrevenda', 'idgerente'], inplace=True, errors='ignore')

            rename_map = {
                'nomerevenda': 'revenda',
                'qtdfaturado': 'contratos_faturados_gnio',
                'qtddegustacao': 'degustacoes_gnio',
                'valorfaturado': 'recorrencia_projetada_gnio',
                'valordegustacao': 'valor_degustacoes_gnio'
            }
            df.rename(columns=rename_map, inplace=True)

            json_data = df.to_json(orient='records')
            ti.xcom_push(key='df_json_gnio_tratado', value=json_data)

        except Exception as erro:
            AnaliseDesempenhoRevendas().log.error(f"Erro ao tratar GNIO: {erro}")
            raise

    @staticmethod
    def executar_query_dataset(**kwargs):
        try:
            connection_database, _ = HookConnection.databaseConfigurationDataset()
            sql = Variable.get('QUERY_REVENDAS_DESEMPENHO_RELATORIOS')
            df = pd.read_sql(sql, connection_database)

            df = df.sort_values(by='revenda')

            json_data = df.to_json(orient='records')
            kwargs['ti'].xcom_push(key='df_json_dataset', value=json_data)

        except Exception as erro:
            AnaliseDesempenhoRevendas().log.error(f"Erro ao executar query Dataset: {erro}")
            raise

    @staticmethod
    def juntar_dataframes(**kwargs):
        try:
            ti = kwargs['ti']
            json_gnio = ti.xcom_pull(key='df_json_gnio_tratado', task_ids='tratar_dataframe_gnio')
            df_gnio = pd.read_json(json_gnio, orient='records')

            json_dataset = ti.xcom_pull(key='df_json_dataset', task_ids='executar_query_dataset')
            df_dataset = pd.read_json(json_dataset, orient='records')

            df_join = pd.merge(df_dataset, df_gnio, on='revenda', how='left')

            revendas_sem_match = df_join[df_join['contratos_faturados_gnio'].isnull()]['revenda'].unique()
            if len(revendas_sem_match) > 0:
                AnaliseDesempenhoRevendas().log.warning(f"Revendas sem match no GNIO: {revendas_sem_match}")

            json_data = df_join.to_json(orient='records')
            ti.xcom_push(key='df_json_join', value=json_data)

        except Exception as erro:
            AnaliseDesempenhoRevendas().log.error(f"Erro ao juntar DataFrames: {erro}")
            raise

    @staticmethod
    def realizar_calculos_desempenho(**kwargs):
        try:
            ti = kwargs['ti']
            json_join = ti.xcom_pull(key='df_json_join', task_ids='juntar_dataframes')
            df = pd.read_json(json_join, orient='records')

            # Corrigir NULL onde faz sentido
            df['contratos_faturados_gnio'] = df['contratos_faturados_gnio'].fillna(0)
            df['recorrencia_projetada_gnio'] = df['recorrencia_projetada_gnio'].fillna(0)

            # Cálculos corretos
            df['crescimento_contratos'] = df['contratos_faturados_gnio'] - df['contratos_faturados']
            df['ativacoes'] = np.where(df['crescimento_contratos'] > 0, df['crescimento_contratos'], 0)
            df['cancelamentos'] = np.where(df['crescimento_contratos'] < 0, -df['crescimento_contratos'], 0)
            df['saldo'] = df['ativacoes'] - df['cancelamentos']
            df['diferenca_recorrencia'] = df['recorrencia_projetada_gnio'] - df['recorrencia_projetada']

            json_data = df.to_json(orient='records')
            ti.xcom_push(key='df_json_calculado', value=json_data)

        except Exception as erro:
            AnaliseDesempenhoRevendas().log.error(f"Erro ao realizar cálculos: {erro}")
            raise
    
    @staticmethod
    def tratamento_pre_insercao(**kwargs):
        try:

            ti = kwargs['ti']
            json_data = ti.xcom_pull(key='df_json_calculado', task_ids='realizar_calculos_desempenho')
            df = pd.read_json(json_data, orient='records')

            # retirar colunas desnecessarias

            colunas_retirar = ['contratos_faturados', 'recorrencia_projetada', 'degustacoes', 'valor_degustacoes', 'crescimento_contratos', 'data_registro']
            for col in colunas_retirar:
                df = df.drop(columns=col)

            # adicionar uma coluna 

            hoje = datetime.now()
            ontem = hoje - timedelta(days=1)
            # mudar essa porah
            df['data_registro'] = ontem.strftime("%Y-%m-%d")

            # colunas renomear

            rename_map = {
                'contratos_faturados_gnio': 'contratos_faturados',
                'degustacoes_gnio': 'degustacoes',
                'recorrencia_projetada_gnio': 'recorrencia_projetada',
                'valor_degustacoes_gnio': 'valor_degustacoes',
            }
            df.rename(columns=rename_map, inplace=True)

            df['degustacoes'] = df['degustacoes'].fillna(0).astype(int)

            # reordenar colunas
            colunas_ordem = ['uuid', 'revenda', 'contratos_faturados', 'recorrencia_projetada', 'diferenca_recorrencia', 'degustacoes', 'valor_degustacoes', 'ativacoes', 'cancelamentos', 'saldo', 'gerente', 'data_registro']
            df = df.reindex(columns=colunas_ordem)

            json_data = df.to_json(orient='records')
            ti.xcom_push(key='df_json_calculado_tradutor', value=json_data)

        except Exception as erro:
            AnaliseDesempenhoRevendas().log.error(f"Erro ao realizar o pré tratamento de dados: {erro}")
            raise


    @staticmethod
    def inserir_banco_dados(**kwargs):
        try:
            ti = kwargs['ti']
            json_data = ti.xcom_pull(key='df_json_calculado_tradutor', task_ids='tratamento_dataframe')
            df = pd.read_json(json_data, orient='records')

            if df.empty:
                print("Nenhum dado encontrado para inserção. Pulando etapa.")
                return

            query_insert = """
                INSERT INTO public.registro_historico_desempenho_revendas (
                    uuid, revenda, contratos_faturados, recorrencia_projetada,
                    diferenca_recorrencia, degustacoes, valor_degustacoes,
                    ativacoes, cancelamentos, saldo, data_registro
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            conn, cursor = HookConnection.databaseConfigurationDataset()

            registros_inseridos = 0

            for _, row in df.iterrows():
                cursor.execute(
                    query_insert,
                    (
                        row.get('uuid'),
                        row.get('revenda'),
                        row.get('contratos_faturados'),
                        row.get('recorrencia_projetada'),
                        row.get('diferenca_recorrencia'),
                        row.get('degustacoes'),
                        row.get('valor_degustacoes'),
                        row.get('ativacoes'),
                        row.get('cancelamentos'),
                        row.get('saldo'),
                        row.get('data_registro')
                    )
                )
                registros_inseridos += 1

            conn.commit()
            cursor.close()
            conn.close()
            print(f"{registros_inseridos} registros inseridos com sucesso.")

        except Exception as erro:
            AnaliseDesempenhoRevendas().log.error(f"Erro ao realizar a inserção dentro do banco de dados: {erro}")
            raise