from airflow.providers.postgres.hooks.postgres import PostgresHook

class DatabaseConfigurationConnection:
    @staticmethod
    def databaseConfigurationGnio():
        try:
            hook = PostgresHook(postgres_conn_id='gnio')
            conn = hook.get_conn()
            cursor = conn.cursor()
            return conn, cursor
        except Exception as erro:
            print("Erro ao configurar a conexão com a database gnio: ", erro)
            raise

    @staticmethod
    def databaseConfigurationDataset():
        try:
            hook = PostgresHook(postgres_conn_id='dataset_comercial_database')
            conn = hook.get_conn()
            cursor = conn.cursor()
            return conn, cursor
        except Exception as erro:
            print("Erro ao configurar a conexão com a database dataset: ", erro)
            raise
