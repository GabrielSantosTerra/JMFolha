import sys
import os
import asyncio
import asyncpg
import pandas as pd
from db import DB_CONFIG

# Importa a função para obter empregados
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from api.empregados_api import obter_empregados

async def mostrar_empregados():
    """Conecta ao banco, exclui registros antigos e insere novos dados de empregados."""
    try:
        # Conecta ao banco de dados usando asyncpg e DB_CONFIG
        conn = await asyncpg.connect(
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"]
        )
        print("Conexão com o banco de dados estabelecida com sucesso!")

        # Exclui todos os registros da tabela tb_empregado
        await conn.execute("DELETE FROM tb_empregado;")
        print("Todos os registros da tabela tb_empregado foram excluídos.")

        # Obtém o DataFrame de empregados
        df = obter_empregados()
        if df is not None and not df.empty:
            # Remove o dia da semana (os cinco primeiros caracteres) de 'admission_date' e converte
            df['admission_date'] = df['admission_date'].str[5:]  # Remove o dia da semana (e a vírgula)
            
            # Converte a coluna 'admission_date' para o formato de data compatível com o banco
            df['admission_date'] = pd.to_datetime(df['admission_date'], format='%d/%m/%Y', errors='coerce')
            df['ano_mes_admissao'] = df['admission_date'].dt.strftime('%Y-%m')  # Extrai ano e mês para 'ano_mes_admissao'

            # Remove registros com datas inválidas
            df = df.dropna(subset=['admission_date'])

            print("Dados de Empregados prontos para inserção:")
            print(df.to_string())  # Exibe o DataFrame completo no terminal

            # Insere os dados do DataFrame na tabela tb_empregado
            for _, row in df.iterrows():
                await conn.execute(
                    """
                    INSERT INTO tb_empregado (nome, pis, matricula, cargo, equipe, unidade, cpf, dt_admissao, ano_mes_admissao)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
                    """,
                    row['name'],                    # Mapeado para 'nome'
                    row['pis'],                     # Mapeado para 'pis'
                    row['registration_number'],     # Mapeado para 'matricula'
                    row['job_title'],               # Mapeado para 'cargo'
                    row['team'],                    # Mapeado para 'equipe'
                    row['cost_center'],             # Mapeado para 'unidade'
                    row['cpf'],                     # Mapeado para 'cpf'
                    row['admission_date'],          # Objeto datetime para 'dt_admissao'
                    row['ano_mes_admissao']         # Mapeado para 'ano_mes_admissao' (YYYY-MM)
                )
            print("Dados inseridos na tabela tb_empregado com sucesso.")
        else:
            print("Não foi possível obter os dados de empregados ou o DataFrame está vazio.")

        await conn.close()  # Fecha a conexão com o banco
    except Exception as e:
        print(f"Erro ao conectar ou manipular o banco de dados: {e}")

# Executa a função assíncrona
if __name__ == "__main__":
    asyncio.run(mostrar_empregados())
