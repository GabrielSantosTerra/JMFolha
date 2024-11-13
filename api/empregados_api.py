import os
import sys
import requests
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from api.config import UNIDADES, API_HEADERS

# Configuração da URL e cabeçalhos da API
url = "https://api.pontomais.com.br/external_api/v1/reports/employees"

# Payload da requisição
payload = {
    "report": {
        "group_by": "",
        "row_filters": "with_inactives,has_time_cards",
        "columns": "name,pis,registration_number,job_title,team,cpf,cost_center,admission_date",
        "format": "json",
        "filter_by": "business_unit_id",
        "business_unit_id": UNIDADES["DJ"]
    }
}

def obter_empregados():
    """Faz a requisição à API e retorna um DataFrame com os dados de empregados."""
    try:
        response = requests.post(url, headers=API_HEADERS, json=payload)
        response.raise_for_status()
        data = response.json()

        # Exibe a estrutura da resposta completa para depuração
        print("Estrutura da resposta da API:", data)
        
        # Verifica a estrutura da resposta e extrai os dados dos empregados
        if 'data' in data and isinstance(data['data'], list) and len(data['data']) > 0:
            # Acessa o campo 'data' dentro do primeiro item da lista, caso exista
            empregados = data['data'][0][0].get('data', []) if isinstance(data['data'][0], list) else []
            
            if empregados:
                # Converte para DataFrame se houver dados
                df = pd.DataFrame(empregados)
                print("Dados de Empregados:")
                print(df)  # Exibe o DataFrame no terminal
                return df
            else:
                print("A lista de empregados está vazia ou não foi encontrada.")
                return None
        else:
            print("A estrutura de dados esperada não foi encontrada.")
            return None

    except requests.exceptions.HTTPError as http_err:
        print(f"Erro HTTP: {http_err}")
    except Exception as err:
        print(f"Erro: {err}")
    
    return None

# Executa a função e exibe o DataFrame se o script for executado diretamente
if __name__ == "__main__":
    obter_empregados()
