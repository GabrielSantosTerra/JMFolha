import requests
import asyncio
import os
import sys
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from api.config import UNIDADES, API_HEADERS
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db.tb_variaveis import get_variaveis

# URL e cabeçalhos da API
url = "https://api.pontomais.com.br/external_api/v1/reports/missing_days"

# Array global para armazenar o DataFrame
faltas_data = []

async def enviar_requisicoes():
    _, date_ranges = await get_variaveis()
    resultados = []

    for dt_ini, dt_fim in date_ranges:
        payload = {
            "report": {
                "start_date": dt_ini,
                "end_date": dt_fim,
                "group_by": "team",
                "row_filters": "with_inactives,has_time_cards",
                "columns": "employee_id,name,registration_number,missing_motive,date",
                "format": "json",
                "business_unit_id": UNIDADES["DJ"]
            }
        }

        try:
            response = requests.post(url, headers=API_HEADERS, json=payload)
            response.raise_for_status()
            data = response.json()

            if 'data' in data and isinstance(data['data'], list) and len(data['data']) > 0:
                faltas = data['data'][0][0].get('data', []) if isinstance(data['data'][0], list) else []
                
                if faltas:
                    resultados.extend(faltas)
                else:
                    print("A lista de faltas está vazia ou não foi encontrada.")
            else:
                print("A estrutura de dados esperada não foi encontrada.")

        except requests.exceptions.HTTPError as http_err:
            print(f"Erro HTTP para {dt_ini} a {dt_fim}: {http_err}")
        except Exception as err:
            print(f"Erro para {dt_ini} a {dt_fim}: {err}")

    # Converte os resultados em um DataFrame e armazena no array global
    df = pd.DataFrame(resultados)
    faltas_data.append(df)

def obter_faltas():
    asyncio.run(enviar_requisicoes())
    return faltas_data[-1] if faltas_data else pd.DataFrame()

if __name__ == "__main__":
    df = obter_faltas()
    if df.empty:
        print("Nenhum dado foi encontrado.")
    else:
        print("Dados de Faltas:")
        print(df)
