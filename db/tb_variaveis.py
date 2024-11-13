# tb_variaveis.py
import asyncio
import asyncpg
import pandas as pd
import importlib.util
from decimal import Decimal
from datetime import date

spec = importlib.util.spec_from_file_location("db", "C:/Projetos/JMfolha/db/db.py")
db_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(db_module)
DB_CONFIG = db_module.DB_CONFIG

def convert_data_types(record):
    """Converte valores Decimal para float e date para string em um dicionário."""
    for key, value in record.items():
        if isinstance(value, Decimal):
            record[key] = float(value)
        elif isinstance(value, date):
            record[key] = value.isoformat()  # Converte a data para string no formato YYYY-MM-DD
    return record

async def get_variaveis():
    try:
        # Conecte-se ao banco de dados usando as configurações do db.py
        connection = await asyncpg.connect(
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"]
        )
        
        print("Conexão com o banco de dados estabelecida com sucesso!")

        # Execute a consulta SELECT
        query = "SELECT * FROM tb_variaveis"
        records = await connection.fetch(query)

        # Converte os registros para uma lista de dicionários e ajusta valores Decimal e date
        records_json = [convert_data_types(dict(record)) for record in records]
        
        # Fecha a conexão após a consulta
        await connection.close()
        print("Conexão encerrada com sucesso.")
        
        # Converte a lista de dicionários para um DataFrame
        df = pd.DataFrame(records_json)
        
        # Extrai as colunas 'dt_ini' e 'dt_fim' como uma lista de tuplas
        date_ranges = list(zip(df['dt_ini'], df['dt_fim']))
        
        # Retorna o DataFrame e a lista de tuplas
        return df, date_ranges

    except Exception as e:
        print(f"Erro ao conectar ou consultar o banco de dados: {e}")
        return None, None

# Execute a função assíncrona e exibe o DataFrame e o array com 'dt_ini' e 'dt_fim'
if __name__ == "__main__":
    df, date_ranges = asyncio.run(get_variaveis())
    if df is not None:
        print("Dados em DataFrame:\n", df)
    if date_ranges is not None:
        print("Array de intervalos de datas (dt_ini, dt_fim):\n", date_ranges)
