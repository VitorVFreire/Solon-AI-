import os
import logging
import json
from dotenv import load_dotenv
from typing import Set # Adicionado para tipagem

from src.neo4j_connection import Neo4jConnection
from src.ai_client import AIClient, setup_client
from src.new_scrapper import ReadNews
from src.news_processor import NewsProcessor
from utils import clean_filename 
from pathlib import Path

ROOT_DIR = Path(__file__).parent

load_dotenv(override=True, dotenv_path=os.path.join(Path(__file__).parent.parent,'.env'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(), 
        logging.FileHandler("app_activity.log", mode='a', encoding='utf-8') 
    ]
)
logger = logging.getLogger(__name__)

PROMPT_PATHS = {
    "system_entity_identification": os.path.join(ROOT_DIR, 'prompts/system_entity_identification.md'),
    "human_entity_identification": os.path.join(ROOT_DIR, 'prompts/human_entity_identification.md'),
    "system_impact_analysis": os.path.join(ROOT_DIR, 'prompts/system_impact_analysis.md'),
    "human_impact_analysis": os.path.join(ROOT_DIR, 'prompts/human_impact_analysis.md')
}
OUTPUT_DIR = os.path.join(ROOT_DIR, "output/analysis_results")

def check_prompt_files():
    for key, path in PROMPT_PATHS.items():
        if not os.path.exists(path):
            logger.error(f"Arquivo de prompt não encontrado: {path} (para chave '{key}')")
            return False
    logger.info("Todos os arquivos de prompt foram encontrados.")
    return True

def main():
    logger.info("Iniciando aplicação de análise de notícias.")

    if not check_prompt_files():
        logger.error("Encerrando devido à falta de arquivos de prompt.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    neo4j_conn = None
    try:
        neo4j_uri = os.getenv("NEO4J_URI") 
        neo4j_user = os.getenv("NEO4J_USER")
        neo4j_password = os.getenv("NEO4J_PASSWORD")
        
        if neo4j_uri and neo4j_user and neo4j_password:
            try:
                neo4j_conn = Neo4jConnection(neo4j_uri, neo4j_user, neo4j_password)
            except Exception as e:
                logger.error(f"Falha ao conectar ao Neo4j: {e}. Prosseguindo sem conexão Neo4j.")
                neo4j_conn = None
        else:
            logger.warning("Variáveis de ambiente Neo4j não configuradas completamente. Prosseguindo sem conexão Neo4j.")

        list_news_url = ['https://www.infomoney.com.br/mercados/', 'https://www.infomoney.com.br/economia/', 'https://www.infomoney.com.br/tudo-sobre/trader/']#os.getenv('URL_NEWS')
        base_url_news = os.getenv('BASE_URL_NEWS') 
        text_to_replace = os.getenv('TEXT_REPLACE')

        if not all([list_news_url, base_url_news]):
            logger.error("URL_NEWS ou BASE_URL_NEWS não definidas nas variáveis de ambiente. Não é possível buscar notícias.")
            return

        logger.info("Inicializando leitor de notícias...")
        reader = ReadNews(
            urls=list_news_url,
            base_url=base_url_news,
            text_replace=text_to_replace if text_to_replace else "",
            headless=os.getenv("SELENIUM_HEADLESS", "true").lower() == "true"
        )
        reader.fetch_articles() 

        if not reader.articles_list:
            logger.warning("Nenhuma notícia foi carregada. Encerrando.")
            return
        logger.info(f"{len(reader.articles_list)} notícias carregadas para processamento.")

        ai_service = os.getenv("AI_SERVICE", "openAI") 
        logger.info(f"Configurando cliente AI para o serviço: {ai_service}")
        ai_config = setup_client(api_service=ai_service) # type: ignore
        llm_client = AIClient(ai_config)
        logger.info(f"Cliente LLM inicializado usando modelo: {ai_config.get('model')}")

        processor = NewsProcessor(
            llm_client=llm_client,
            neo4j_conn=neo4j_conn,
            prompt_paths=PROMPT_PATHS,
            output_dir=OUTPUT_DIR
        )
        
        logger.info(f"Iniciando processamento de {len(reader.articles_list)} notícias.")
        batch_results = processor.process_news_batch(reader.articles_list)
        
        logger.info("Processamento de notícias concluído.")
        successful_analyses = 0
        if batch_results:
            for i, result_item in enumerate(batch_results):
                if result_item:
                    logger.info(f"--- Resultado da análise para notícia {i+1} (Título: {result_item.get('news_title', 'N/A')[:50]}...) ---")
                    
                    for profile, analysis in result_item.get("analysis_by_profile", {}).items():
                        if "error" not in analysis and isinstance(analysis, dict):
                            logger.info(f"  Perfil {profile}: Pessoal={analysis.get('personal_score', 'N/A')}, Setorial={analysis.get('sector_score', 'N/A')}")
                            logger.info(f"    Just. Pessoal: {analysis.get('justification', {}).get('personal', 'N/A')}")
                            logger.info(f"    Just. Setorial: {analysis.get('justification', {}).get('sector', 'N/A')}")
                        else:
                            logger.warning(f"  Perfil {profile}: Erro na análise - {analysis.get('error', 'Detalhe do erro não disponível')}")
                    successful_analyses += 1
                else:
                    logger.info(f"Notícia {i+1} foi pulada (provavelmente duplicata ou erro no processamento inicial).")
            logger.info(f"{successful_analyses}/{len(reader.articles_list)} notícias efetivamente processadas e resultaram em uma análise.")
        else:
            logger.info("Nenhum resultado de análise foi retornado.")


    except FileNotFoundError as e:
        logger.critical(f"Arquivo crítico não encontrado: {e}", exc_info=True)
    except ValueError as e:
        logger.critical(f"Erro de valor (configuração?): {e}", exc_info=True)
    except Exception as e:
        logger.critical(f"Erro inesperado na execução principal: {e}", exc_info=True)
    finally:
        if neo4j_conn:
            neo4j_conn.close()
        logger.info("Aplicação de análise de notícias finalizada.")

if __name__ == "__main__":
    main()