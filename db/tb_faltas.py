import asyncio
import asyncpg
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from api.faltas_api import obter_faltas  # Chama para preencher o array global
from db import DB_CONFIG

async def processar_faltas_no_banco(df):
    conn = await asyncpg.connect(
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"]
    )
    
    print("Conexão com o banco de dados estabelecida.")

    try:
        delete_query = "DELETE FROM tb_faltas"
        await conn.execute(delete_query)
        print("Dados antigos excluídos da tabela tb_faltas.")
        
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO tb_faltas (id, nome, matricula, motivo, data_ocorrencia)
            VALUES ($1, $2, $3, $4, $5)
            """
            await conn.execute(insert_query, row['employee_id'], row['name'], row['registration_number'], row['missing_motive'], row['date'])
        
        print("Dados novos inseridos com sucesso na tabela tb_faltas.")
    except Exception as e:
        print(f"Erro ao processar dados no banco: {e}")
    finally:
        await conn.close()
        print("Conexão com o banco de dados encerrada.")

if __name__ == "__main__":
    # Executa a função para preencher faltas_data com o DataFrame
    df_faltas = obter_faltas()
    
    if df_faltas.empty:
        print("Nenhum dado de faltas encontrado para inserir.")
    else:
        asyncio.run(processar_faltas_no_banco(df_faltas))
