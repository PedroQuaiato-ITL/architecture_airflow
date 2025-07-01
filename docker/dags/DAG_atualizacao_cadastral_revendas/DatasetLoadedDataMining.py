from airflow.models import Variable
from Modules.DatabaseConfigurationConnection import DatabaseConfigurationConnection as hook
import uuid

class DatasetLoadedDataMining:
    @staticmethod
    def executing_query_gnio(ti):
        conn = None
        cursor = None
        try:
            query = Variable.get("QUERY_REVENDAS_CADASTRAL_GNIO")
            conn, cursor = hook.databaseConfigurationGnio()
            cursor.execute(query)
            resultados = cursor.fetchall()
            ti.xcom_push(key="executing_query_gnio", value=resultados)
        except Exception as erro:
            raise RuntimeError("Erro ao executar a query no banco GNIO") from erro
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @staticmethod
    def organized_response_database(ti):
        try:
            organized_data = []
            response = ti.xcom_pull(task_ids="executing_query_gnio", key="executing_query_gnio")
            if not response:
                raise ValueError("Nenhum dado retornado da task executing_query_gnio")

            for revenda in response:
                organized_data.append({
                    'uuid':         str(uuid.uuid4()),
                    'revenda':      revenda[1],
                    'razao_social': revenda[0],
                    'cnpj':         revenda[2],
                    'bairro':       revenda[3],
                    'cep':          revenda[4],
                    'cidade':       revenda[5],
                    'estado':       revenda[6],
                    'email':        revenda[7],
                    'gerente':      revenda[8],
                    'categoria':    revenda[9]
                })
                print(f'Revenda {revenda[1]} organizada com sucesso !')

            ti.xcom_push(key="organized_response_database", value=organized_data)

        except Exception as erro:
            raise RuntimeError("Erro ao organizar os dados retornados") from erro

    @staticmethod
    def insert_database(ti):
        conn = None
        cursor = None
        try:
            response_organized = ti.xcom_pull(task_ids="organized_response_database", key="organized_response_database")
            if not response_organized:
                raise ValueError("Nenhum dado organizado disponível para inserção")

            conn, cursor = hook.databaseConfigurationDataset()
            query_verify = Variable.get("QUERY_VERIFICADORA_INSERCAO_CADASTRAL")
            query_insert = Variable.get("QUERY_INSERT_DATABASE_CADASTRAL")
            query_update = Variable.get("QUERY_REVENDAS_CADASTRAL_UPDATE")

            for revenda in response_organized:
                cursor.execute(query_verify, (revenda['revenda'],))
                verify = cursor.fetchone()
                count = verify[0] if verify else 0

                if count > 0:
                    cursor.execute(query_update, (
                        revenda['razao_social'],
                        revenda['cnpj'],
                        revenda['bairro'],
                        revenda['cep'],
                        revenda['cidade'],
                        revenda['estado'],
                        revenda['email'],
                        revenda['gerente'],
                        revenda['categoria'],
                        revenda['revenda'],
                    ))

                    print(f"Revenda {revenda['revenda']} Atualizada com sucesso !")
                else:
                    cursor.execute(query_insert, (
                        revenda['uuid'],
                        revenda['revenda'],
                        revenda['razao_social'],
                        revenda['cnpj'],
                        revenda['bairro'],
                        revenda['cep'],
                        revenda['cidade'],
                        revenda['estado'],
                        revenda['email'],
                        revenda['gerente'],
                        revenda['categoria'],
                    ))

                    print(f"Revenda {revenda['revenda']} Adicionada com sucesso !")

            conn.commit()
        except Exception as erro:
            raise RuntimeError("Erro ao inserir os dados no banco dataset") from erro
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()