import os
import smtplib
import pandas as pd 
from airflow.models import Variable
from collections import defaultdict
from elasticsearch import Elasticsearch, exceptions
from elasticsearch.helpers import bulk
from email.message import EmailMessage
from Modules.DatabaseConfigurationConnection import DatabaseConfigurationConnection as hook


class RelatoriosRevendas:
    @staticmethod
    def executing_query_dataset(ti):
        try:
            query = Variable.get("QUERY_REVENDAS_DESEMPENHO_RELATORIOS")
            conn, cursor = hook.databaseConfigurationDataset()
            cursor.execute(query)
            resultado = cursor.fetchall()
            ti.xcom_push(key="resultado_query_dataset", value=resultado)
            cursor.close()
            conn.close()
        except Exception as erro:
            print("Erro ao executar as query do dataset: ", erro)

    @staticmethod
    def organized_data(ti):
        try:
            dataset_result = ti.xcom_pull(task_ids="executing_query_dataset", key="resultado_query_dataset")
            dados_organizados = []
            for dado in dataset_result:
                dados_organizados.append({
                    'revenda': dado[1],
                    'contratos_faturados': dado[2],
                    'recorrencia_projetada': dado[3],
                    'diferenca_recorrencia': dado[4],
                    'degustacoes': dado[5],
                    'valor_degustacoes': dado[6],
                    'ativacoes': dado[7],
                    'cancelamentos': dado[8],
                    'saldo': dado[9],
                    'data_registro': dado[10],
                    'gerente': dado[11]
                })
            ti.xcom_push(key="organized_data", value=dados_organizados)
        except Exception as erro:
            print("Erro ao separar os dados de forma correta: ", erro)

    @staticmethod
    def separed_data(ti):
        try:
            dados_organizados = ti.xcom_pull(task_ids="organized_data", key="organized_data")
            dados_por_gerente = defaultdict(list)
            for dado in dados_organizados:
                gerente = dado['gerente']
                dados_por_gerente[gerente].append(dado)
            ti.xcom_push(key="separed_data", value=dados_por_gerente)
        except Exception as erro:
            print("Erro ao separar os dados por gerente: ", erro)

    @staticmethod
    def creating_csv(ti):
        try:
            dados_por_gerente = ti.xcom_pull(task_ids="separed_data", key="separed_data")
            colunas = [
                'revenda',
                'contratos_faturados',
                'recorrencia_projetada',
                'diferenca_recorrencia',
                'degustacoes',
                'valor_degustacoes',
                'ativacoes',
                'cancelamentos',
                'saldo',
                'data_registro',
                'gerente'
            ]
            for gerente, registros in dados_por_gerente.items():
                df = pd.DataFrame(registros, columns=colunas)

                # 🪄 FORMATANDO MONETÁRIOS
                for coluna_moeda in ['recorrencia_projetada', 'diferenca_recorrencia', 'valor_degustacoes']:
                    df[coluna_moeda] = df[coluna_moeda].astype(float).map(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

                nome_arquivo = f"{gerente.replace(' ', '_')}_dados.csv"
                df.to_csv(nome_arquivo, index=False, sep=';', encoding='utf-8')

        except Exception as erro:
            print("Erro ao gerar o csv: ", erro)

    @staticmethod
    def enviar_para_elasticsearch(ti):
        try:
            dados_por_gerente = ti.xcom_pull(task_ids="separed_data", key="separed_data")
            es = Elasticsearch(
                "http://10.1.10.52:9200",
                basic_auth=("elastic", "admin123"),
                verify_certs=False,
                timeout=30
            )
            if not es.ping():
                print("Erro: não conseguiu conectar no Elasticsearch!")
                return
            print(f"Conectado no Elasticsearch, indexando dados...")
            for gerente, registros in dados_por_gerente.items():
                print(f"Enviando dados para o gerente: {gerente} (total: {len(registros)})")
                acoes = [
                    {
                        "_index": "relatorio-revendas",
                        "_source": registro
                    }
                    for registro in registros
                ]
                try:
                    success, errors = bulk(es, acoes)
                    if errors:
                        print(f"Erros no bulk para o gerente {gerente}: {errors}")
                    else:
                        print(f"Sucesso no bulk para o gerente {gerente}. Documentos enviados: {success}")
                except exceptions.ElasticsearchException as bulk_erro:
                    print(f"Erro no bulk para o gerente {gerente}: {bulk_erro}")
        except Exception as erro:
            print(f"Erro geral ao enviar dados para Elasticsearch: {erro}")

    @staticmethod
    def enviar_email_csvs(ti):
        try:
            dados_por_gerente = ti.xcom_pull(task_ids="separed_data", key="separed_data")
            EMAIL_ORIGEM = "do-not-reply@intelidata.inf.br"  # Troque pelo seu email real configurado
            SENHA = "Ad8oxD!aYi9KsgYj*g67"

            emails_mapeados = {
                "sabrina": "sabrina.lima@intelidata.inf.br",
                "leticia": "leticia.borges@intelidata.inf.br",
                "emerson": "emerson.mello@intelidata.inf.br",
                "barbara": "barbara.goedert@intelidata.inf.br",
            }

            sufixos_ignorados = {"neto", "filho", "junior", "jr", "sobrinho"}

            for gerente, registros in dados_por_gerente.items():
                colunas = [
                    'revenda',
                    'contratos_faturados',
                    'recorrencia_projetada',
                    'diferenca_recorrencia',
                    'degustacoes',
                    'valor_degustacoes',
                    'ativacoes',
                    'cancelamentos',
                    'saldo',
                    'data_registro',
                    'gerente'
                ]
                df = pd.DataFrame(registros, columns=colunas)
                nome_arquivo = f"{gerente.replace(' ', '_')}_dados.csv"
                df.to_csv(nome_arquivo, index=False, sep=',', encoding='utf-8')

                nomes = gerente.lower().split()
                while nomes and nomes[-1] in sufixos_ignorados:
                    nomes.pop()

                primeiro_nome = nomes[0] if nomes else ""

                email_gerente = emails_mapeados.get(primeiro_nome)
                if not email_gerente:
                    if len(nomes) >= 2:
                        email_gerente = f"{nomes[0]}.{nomes[-1]}@intelidata.inf.br"
                    elif nomes:
                        email_gerente = f"{nomes[0]}@intelidata.inf.br"
                    else:
                        email_gerente = "destino@intelidata.inf.br"

                msg = EmailMessage()
                msg["Subject"] = "Relatório de desempenho"
                msg["From"] = EMAIL_ORIGEM
                msg["To"] = email_gerente
                msg.set_content("Segue em anexo o relatório.")

                with open(nome_arquivo, "rb") as f:
                    msg.add_attachment(f.read(), maintype="application", subtype="octet-stream", filename=nome_arquivo)

                try:
                    with smtplib.SMTP("smtp-mail.outlook.com", 587) as smtp:
                        smtp.ehlo()
                        smtp.starttls()
                        smtp.ehlo()
                        smtp.login(EMAIL_ORIGEM, SENHA)
                        smtp.send_message(msg)
                    print(f"Email enviado para {email_gerente}")
                except Exception as e:
                    print(f"Erro ao enviar email para {email_gerente}: {e}")

                os.remove(nome_arquivo)

        except Exception as erro:
            print("Erro ao enviar os e-mails:", erro)
