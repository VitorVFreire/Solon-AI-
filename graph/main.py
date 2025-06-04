import os
from pathlib import Path
from utils import *
from src import *

# Define o caminho da pasta
ROOT_DIR = Path(__file__).parent.parent
DATABASE_DIR = os.path.join(ROOT_DIR, 'web_search', 'output')
DATABASE_PATH = Path(DATABASE_DIR)

def select():
    try:
        
        conn = Neo4jConnection(os.getenv('NEO4J_URI'), os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASSWORD'))

        logger.info("\n--- Análise Focada na Empresa: Amazon.com Inc BDR ---")
        company_analysis_results = conn.get_company_focused_analysis(nome_empresa_principal="Amazon.com Inc BDR")
        if company_analysis_results:
            for record in company_analysis_results:
                logger.info(record)
        else:
            logger.info("Nenhum resultado para a análise focada na empresa.")

        logger.info("\n--- Análise Focada no Setor: Comércio Varejista ---")
        sector_analysis_results = conn.get_sector_focused_analysis(nome_atividade_principal="Comércio Varejista", peso_atividade_direta=0.6)
        if sector_analysis_results:
            for record in sector_analysis_results:
                logger.info(record)
        else:
            logger.info("Nenhum resultado para a análise focada no setor.")

    finally:
        if 'conn' in locals() and conn.driver:
            conn.close()
    pass

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
    select()
    #main()