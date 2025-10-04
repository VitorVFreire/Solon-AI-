import os
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
import logging
from typing import List
from neo4j import GraphDatabase
import uuid

ROOT_DIR = Path(__file__).parent
load_dotenv(override=True, dotenv_path=os.path.join(Path(__file__).parent.parent, '.env'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Neo4jConnection:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_query(self, query: str, params: dict = None):
        with self.driver.session() as session:
            result = session.run(query, params or {})
            return [dict(r) for r in result]

if __name__ == "__main__":
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "senha")
    neo = Neo4jConnection(neo4j_uri, neo4j_user, neo4j_password)
    
    df = pd.read_csv('/home/vitor/Solon-AI-/web_search/database/b3_companys.csv')
    print(df.head(5))

    nodes = neo.execute_query("MATCH (n:Company) RETURN n.company_name as company")
    for node in nodes:
        new_id = str(uuid.uuid4())
        nome = node['company']
        sigla = df.loc[df['full_name']==nome]['symbol']
        logger.info(f'\n\nEmpresa e Sigla\n\n\n{nome}: {sigla}\n\n\n')
        neo.execute_query("""
        MATCH (n:Company)
        WHERE n.company_name = $company
        SET n.symbol = $sigla
        """, {"company": nome, "sigla": sigla})

    neo.close()
    logger.info("Processo finalizado.")
