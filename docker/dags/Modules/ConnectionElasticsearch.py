from elasticsearch import Elasticsearch, helpers
import pandas as pd
import base64
import os

class ConnectionElasticsearch:
    @staticmethod
    def connectionClient():
        try:
            es = Elasticsearch(
                "http://127.0.0.1:9200",
                basic_auth=("elastic", "admin123"),
                verify_certs=False
            )
            if es.ping():
                print("🔌 Conectado ao Elasticsearch!")
                return es
            else:
                raise Exception("❌ Elasticsearch não respondeu ao ping.")
        except Exception as erro:
            print("Erro ao conectar com o Elastic:", erro)
            return None

    @staticmethod
    def createIndex(es, index_name):
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name)
            print(f"📦 Índice '{index_name}' criado.")
        else:
            print(f"📦 Índice '{index_name}' já existe.")

    @staticmethod
    def insertCSV(es, index_name, csv_path):
        try:
            df = pd.read_csv(csv_path)
            records = df.to_dict(orient="records")

            actions = [
                {
                    "_index": index_name,
                    "_source": record
                }
                for record in records
            ]

            helpers.bulk(es, actions)
            print(f"✅ Inserido {len(records)} documentos do CSV no índice '{index_name}'.")
        except Exception as erro:
            print("Erro ao inserir CSV:", erro)

    @staticmethod
    def insertFile(es, index_name, file_path):
        try:
            with open(file_path, "rb") as file:
                encoded = base64.b64encode(file.read()).decode("utf-8")

            filename = os.path.basename(file_path)
            doc = {
                "filename": filename,
                "content": encoded
            }

            es.index(index=index_name, document=doc)
            print(f"📄 Arquivo '{filename}' inserido no índice '{index_name}'.")
        except Exception as erro:
            print("Erro ao inserir arquivo:", erro)
