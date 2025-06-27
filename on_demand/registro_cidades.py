import requests as rq
import uuid
import psycopg2
import time

# Conexão com o banco
conn = psycopg2.connect(
    host="rds.intelidata.inf.br",
    port="5432",
    database="dataset_comercial",
    user="dashboardcomercial",
    password="@dT6dMf915^tG*M"
)
cur = conn.cursor()

# API IBGE
api = 'https://servicodados.ibge.gov.br/api/v1/localidades/municipios'
response = rq.get(api)
dados = response.json()

# Função para buscar lat/lon pela cidade
def buscar_lat_lon(cidade, estado):
    try:
        url = f"https://nominatim.openstreetmap.org/search"
        params = {
            'city': cidade,
            'state': estado,
            'country': 'Brasil',
            'format': 'json'
        }
        headers = {
            'User-Agent': 'pedro.bot'
        }
        resp = rq.get(url, params=params, headers=headers)
        resp.raise_for_status()
        resultado = resp.json()
        if resultado:
            return float(resultado[0]['lat']), float(resultado[0]['lon'])
    except Exception as e:
        print(f"⚠️ Erro ao buscar lat/lon de {cidade}, {estado}: {e}")
    return None, None

for cidade in dados:
    microrregiao = cidade.get('microrregiao')
    if microrregiao and microrregiao.get('mesorregiao') and microrregiao['mesorregiao'].get('UF'):
        estado = microrregiao['mesorregiao']['UF']['nome']
        nome_cidade = cidade['nome']

        # Buscar coordenadas
        lat, lon = buscar_lat_lon(nome_cidade, estado)
        time.sleep(1)  # evita bloqueio por flood (respeita o rate limit)

        data = {
            'uuid': str(uuid.uuid4()),
            'id_cidade': cidade['id'],
            'cidade': nome_cidade,
            'id_microrregiao': microrregiao['id'],
            'microrregiao': microrregiao['nome'],
            'id_mesorregiao': microrregiao['mesorregiao']['id'],
            'mesorregiao': microrregiao['mesorregiao']['nome'],
            'id_estado': microrregiao['mesorregiao']['UF']['id'],
            'sigla_estado': microrregiao['mesorregiao']['UF']['sigla'],
            'estado': estado,
            'id_regiao': microrregiao['mesorregiao']['UF']['regiao']['id'],
            'sigla_regiao': microrregiao['mesorregiao']['UF']['regiao']['sigla'],
            'regiao': microrregiao['mesorregiao']['UF']['regiao']['nome'],
            'latitude': lat,
            'longitude': lon
        }

        try:
            cur.execute("""
                INSERT INTO municipios_ibge (
                    uuid, id_cidade, cidade,
                    id_microrregiao, microrregiao,
                    id_mesorregiao, mesorregiao,
                    id_estado, sigla_estado, estado,
                    id_regiao, sigla_regiao, regiao,
                    latitude, longitude
                ) VALUES (
                    %(uuid)s, %(id_cidade)s, %(cidade)s,
                    %(id_microrregiao)s, %(microrregiao)s,
                    %(id_mesorregiao)s, %(mesorregiao)s,
                    %(id_estado)s, %(sigla_estado)s, %(estado)s,
                    %(id_regiao)s, %(sigla_regiao)s, %(regiao)s,
                    %(latitude)s, %(longitude)s
                )
                ON CONFLICT (uuid) DO NOTHING;
            """, data)
            conn.commit()
            print(f"✅ Inserido: {nome_cidade} - Lat/Lon: {lat}, {lon}")
        except Exception as e:
            print(f"❌ Erro ao inserir {nome_cidade}: {e}")
            conn.rollback()


# Fecha conexão
cur.close()
conn.close()
print("🎯 Tudo pronto!")