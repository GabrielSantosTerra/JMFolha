import requests
import asyncio
import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.tb_variaveis import get_variaveis  # Importa a função que retorna o DataFrame e o array de datas
from config import UNIDADES

# URL do endpoint da API
url = "https://api.pontomais.com.br/external_api/v1/reports/exemptions"  # Substitua pela URL real da API


# Token e cabeçalhos de autenticação
headers = {
    'access-token': '$2a$12$m4d0mFdph8dVAFPKJ8SBHOLCPHrIBqKx/3GESBSbrgniaJ0r7G9qW',
    'Content-Type': 'application/json'
}

async def enviar_requisicoes():
    # Obter o array de intervalos de datas de `db/tb_variaveis`
    _, date_ranges = await get_variaveis()  # Chama a função e obtém apenas `date_ranges`

    # Lista para armazenar todas as respostas da API
    resultados = []

    for dt_ini, dt_fim in date_ranges:
        # Estrutura do payload para cada intervalo de datas
        payload = {
            "report": {
                "start_date": dt_ini,
                "end_date": dt_fim,
                "group_by": "team",
                "row_filters": "with_inactives,has_time_cards,file",
                "columns": "employee_id,employee_name,registration_number,date,motive",
                "format": "json",
                "business_unit_id": UNIDADES["DJ"]
            }
        }

        try:
            # Envia a requisição POST para cada intervalo de datas
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()  # Verifica se houve erro na requisição
            
            # Armazena a resposta no DataFrame para cada intervalo de datas
            data = response.json()
            if 'data' in data:  # Verifica se a resposta contém a chave 'data'
                resultados.extend(data['data'])  # Adiciona cada entrada da resposta à lista de resultados

        except requests.exceptions.HTTPError as http_err:
            print(f"Erro HTTP para {dt_ini} a {dt_fim}: {http_err}")
            print("Detalhes:", response.text)  # Exibe detalhes do erro
        except Exception as err:
            print(f"Erro para {dt_ini} a {dt_fim}: {err}")

    # Converte a lista de resultados em um DataFrame
    df = pd.DataFrame(resultados)

    # Salva o DataFrame em um arquivo CSV
    df.to_csv("saida_relatorio.csv", index=False, encoding='utf-8')
    print("Arquivo CSV gerado: saida_relatorio.csv")

# Chama a função de requisições
if __name__ == "__main__":
    asyncio.run(enviar_requisicoes())
