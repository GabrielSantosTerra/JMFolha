import requests
import asyncio
import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.tb_variaveis import get_variaveis
from config import UNIDADES


url = "https://api.pontomais.com.br/external_api/v1/reports/time_cards"
headers = {
    'access-token': '$2a$12$m4d0mFdph8dVAFPKJ8SBHOLCPHrIBqKx/3GESBSbrgniaJ0r7G9qW',
    'Content-Type': 'application/json'
}

async def enviar_requisicoes():
    _, date_ranges = await get_variaveis()
    resultados = []

    for dt_ini, dt_fim in date_ranges:
        payload = {
            "report": {
                "start_date": dt_ini,
                "end_date": dt_fim,
                "group_by": "team",
                "row_filters": "",
                "columns": "employee_id,employee_name,registration_number,date,time,time_card_index",
                "format": "json",
                "business_unit_id": UNIDADES["DJ"]
            }
        }

        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            if 'data' in data:
                resultados.extend(data['data'])
        except requests.exceptions.HTTPError as http_err:
            print(f"Erro HTTP para {dt_ini} a {dt_fim}: {http_err}")
        except Exception as err:
            print(f"Erro para {dt_ini} a {dt_fim}: {err}")

    df = pd.DataFrame(resultados)
    df.to_csv("time_cards_report.csv", index=False, encoding='utf-8')
    print("Arquivo CSV gerado: time_cards_report.csv")

if __name__ == "__main__":
    asyncio.run(enviar_requisicoes())
