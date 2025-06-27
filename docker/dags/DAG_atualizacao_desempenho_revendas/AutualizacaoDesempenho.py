from Modules.DatabaseConfigurationConnection import DatabaseConfigurationConnection as hook
from datetime import datetime, timedelta
from airflow.models import Variable

class DagAtualizacaoDesempenho:

    @staticmethod
    def executing_querys_gnio(ti):
        try:
            query = Variable.get("QUERY_REVENDAS_DESEMPENHO_GNIO")
            conn, cursor = hook.databaseConfigurationGnio()
            cursor.execute(query)
            resultado = cursor.fetchall()
            ti.xcom_push(key="resultado_query_gnio", value=resultado)
            cursor.close()
            conn.close()
        except Exception as erro:
            print("Erro ao executar as querys com banco GNIO:", erro)

    @staticmethod
    def executing_querys_dataset(ti):
        try:
            query = "SELECT uuid, revenda FROM public.registro_cadastral_revendas"
            conn, cursor = hook.databaseConfigurationDataset()
            cursor.execute(query)
            resultado = cursor.fetchall()
            ti.xcom_push(key="resultado_query_dataset", value=resultado)
            cursor.close()
            conn.close()
        except Exception as erro:
            print("Erro ao executar as querys com banco Dataset:", erro)

    @staticmethod
    def organized_results_query(ti):
        try:
            dataset = ti.xcom_pull(task_ids="executing_query_dataset", key="resultado_query_dataset")
            gnio = ti.xcom_pull(task_ids="executing_query_gnio", key="resultado_query_gnio")
            ontem = datetime.now() - timedelta(days=1)
            data_ontem_str = ontem.strftime('%Y-%m-%d')

            mapa_dataset = {
                i[1].strip().lower(): i[0]
                for i in dataset
            }

            dados_organizados = []
            for i in gnio:
                nome_revenda = i[4].strip().lower()
                uuid = mapa_dataset.get(nome_revenda)

                if uuid:
                    dados_organizados.append({
                        'uuid': uuid,
                        'revenda': i[4],
                        'contratos_faturados': i[5],
                        'degustacoes': i[6],
                        'recorrencia': i[7],
                        'valor_degustacoes': i[8],
                        'data_registro': data_ontem_str
                    })

            ti.xcom_push(key="dados_organizados", value=dados_organizados)
        except Exception as erro:
            print("Erro ao organizar os resultados:", erro)

    @staticmethod
    def calculos_results(ti):
        try:
            dados_atuais = ti.xcom_pull(task_ids="organized_results_query", key="dados_organizados")
            data_registro_atual = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')  # 👈 CORRIGIDO AQUI

            query = """
                SELECT * FROM public.registro_historico_desempenho_revendas
                WHERE data_registro = (
                    SELECT MAX(data_registro)
                    FROM public.registro_historico_desempenho_revendas
                )
                ORDER BY revenda ASC;
            """
            conn, cursor = hook.databaseConfigurationDataset()
            cursor.execute(query)
            historico = cursor.fetchall()
            cursor.close()
            conn.close()

            ultima_data_registro = (
                historico[0][-1].strftime('%Y-%m-%d') if historico and historico[0][-1] else None
            )
            historico_por_uuid = {i[0]: i for i in historico}

            dados_finais = []

            for atual in dados_atuais:
                uuid = atual['uuid']
                revenda = atual['revenda']
                contratos_faturados = atual['contratos_faturados'] or 0
                degustacoes = atual['degustacoes'] or 0
                recorrencia = atual['recorrencia'] or 0.0
                valor_degustacoes = atual['valor_degustacoes'] or 0.0

                anterior = historico_por_uuid.get(uuid)
                if anterior:
                    contratos_anteriores = anterior[2] or 0
                    recorrencia_anterior = anterior[3] or 0.0

                    ativacoes = max(contratos_faturados - contratos_anteriores, 0)
                    cancelamentos = max(contratos_anteriores - contratos_faturados, 0)
                    saldo = ativacoes - cancelamentos
                    diferenca_recorrencia = recorrencia - recorrencia_anterior
                else:
                    ativacoes = cancelamentos = saldo = diferenca_recorrencia = 0.0

                dados_finais.append((
                    uuid, revenda, contratos_faturados, recorrencia,
                    diferenca_recorrencia, degustacoes, valor_degustacoes,
                    ativacoes, cancelamentos, saldo, data_registro_atual  # 👈 ESTA É A DATA INSERIDA
                ))

            ti.xcom_push(key="dados_finais", value=dados_finais or [])

        except Exception as erro:
            print("Erro ao realizar os cálculos:", erro)
            ti.xcom_push(key="dados_finais", value=[])

    @staticmethod
    def insert_data(ti):
        try:
            dados = ti.xcom_pull(task_ids="calculos_results", key="dados_finais")

            if not dados:
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

            conn, cursor = hook.databaseConfigurationDataset()
            cursor.executemany(query_insert, dados)
            conn.commit()
            cursor.close()
            conn.close()
            print(f"{len(dados)} registros inseridos com sucesso.")

        except Exception as erro:
            print("Erro ao inserir dados no banco:", erro)
