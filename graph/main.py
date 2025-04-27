import os
from pathlib import Path
from utils import *
from src import *

# Define o caminho da pasta
ROOT_DIR = Path(__file__).parent.parent
DATABASE_DIR = os.path.join(ROOT_DIR, 'web_search', 'output')
DATABASE_PATH = Path(DATABASE_DIR)

def main():
    try:
        folders = listar_arquivos_por_pasta(DATABASE_PATH)
        
        conn = Neo4jConnection(os.getenv('URL_NEO4J'), os.getenv('USER_NEO4J'), os.getenv('PASSWORD_NEO4J'))
        try:
            companies = Companies(
                conn=conn,
                folders=folders
            )
            
            companies = Companies(conn, folders)
            companies.create_nodes()
            companies.create_relationships()
        finally:
            conn.close()
    except Exception as e:
        print(f"Erro ao processar os arquivos: {e}")

if __name__ == "__main__":
    main()