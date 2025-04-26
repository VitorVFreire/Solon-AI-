from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
import pandas as pd
import sqlite3

# Carrega variáveis de ambiente
load_dotenv()

# Dados de conexão
URI = "bolt://localhost:7687"
USER = os.getenv("USER_NEO4J")
PASSWORD = os.getenv("PASSWORD_NEO4J")

relacao_atividades = pd.read_excel('database/resultados_consolidados.xlsx')

query_empresas = """
SELECT *
FROM empresas
WHERE razao_social LIKE '%CONPAR CON%' LIMIT 50
"""

query_cnaes = "SELECT * FROM cnaes LIMIT 5;"

# Conectando ao banco de dados para checar os dados
with sqlite3.connect("../data_scraper/cnpj.db") as conn:
    df_empresas = pd.read_sql_query(query_empresas, conn)
    df_cnaes = pd.read_sql_query(query_cnaes, conn)

# Exibir alguns dados das tabelas
print("Empresas:")
print(df_empresas.head())

print("\nCNAEs:")
print(df_cnaes.head())

# Criar driver de conexão
driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

def test_connection():
    with driver.session() as session:
        result = session.run("RETURN 'Conexão com Neo4j bem-sucedida!' AS mensagem")
        print(result.single()["mensagem"])

# Executar teste
test_connection()

# Fechar o driver
driver.close()
