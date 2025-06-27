import requests

class Functions:
    @staticmethod
    def get_geolocalization(cep: str):
        try:
            response = requests.get(f"https://viacep.com.br/ws/{cep}/json/")
            data = response.json()

            if "erro" in data:
                return {'longitude': None, 'latitude': None}

            endereco = f"{data.get('logradouro', '')}, {data.get('bairro', '')}, {data.get('localidade', '')} - {data.get('uf', '')}, Brasil"

            nominatim_url = "https://nominatim.openstreetmap.org/search"
            params = {
                'q': endereco,
                'format': 'json',
                'limit': 1,
                'addressdetails': 0
            }
            geo_response = requests.get(nominatim_url, params=params, headers={'User-Agent': 'cep_geocoder_airflow'})
            geo_data = geo_response.json()

            if geo_data:
                return {
                    'longitude': float(geo_data[0]['lon']),
                    'latitude': float(geo_data[0]['lat'])
                }
            else:
                return {'longitude': None, 'latitude': None}

        except Exception as e:
            print(f"Erro no get_geolocalization: {e}")
            return {'longitude': None, 'latitude': None}
