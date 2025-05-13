from src import *
from utils import *
import os
import logging
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SYSTEM_PROMPT_PATH = 'prompts/analysis/system_analysis.md'
HUMAN_PROMPT_PATH = 'prompts/analysis/human_analysis.md'

def main():
    try:
        # Verificar prompts
        if not os.path.exists(SYSTEM_PROMPT_PATH):
            raise FileNotFoundError(f"Prompt de sistema não encontrado: {SYSTEM_PROMPT_PATH}")
        if not os.path.exists(HUMAN_PROMPT_PATH):
            raise FileNotFoundError(f"Prompt humano não encontrado: {HUMAN_PROMPT_PATH}")

        reader = ReadNews(
            url=os.getenv('URL_NEWS'),
            base_url=os.getenv('BASE_URL'),
            text_replace=os.getenv('TEXT_REPLACE')
        )
        
        reader.load_article()
                
        if not reader.articles:
            logger.warning("Nenhuma notícia encontrada")
            return 0
        
        logger.info("Iniciando processamento de análise de notícias")
        
        config = setup_client('openAI')
        llm_client = AIClient(config)
        
        output_dir = 'output/companies'
        os.makedirs(output_dir, exist_ok=True)
        
        processor = NewsProcessor( 
            llm_client=llm_client, 
            system_prompt_file=SYSTEM_PROMPT_PATH, 
            human_prompt_file=HUMAN_PROMPT_PATH,
            output_dir=output_dir
        )
        
        # Processar notícias
        logger.info(f"Iniciando processamento de {len(reader.article_title)} notícias")
        results = processor.process_news_batch(reader.articles)
        
        # Exibir resultados
        logger.info("Análise concluída")
        for result in results:
            if "formatted_result" in result:
                company_name = result["formatted_result"].get("company_name", "N/A")
                economic_activity = result["formatted_result"].get("economic_activity", "N/A")
                output_file = os.path.join(output_dir, f"{clean_filename(company_name or economic_activity)}_analysis.json")
                
                print(f"\nResultado da análise para {company_name or economic_activity}:")
                print(result["formatted_result"])
                print(f"\nO resultado completo foi salvo em: {output_file}")
            else:
                logger.warning("Resultado não formatado corretamente.")
                print("Resultado não formatado corretamente. Verifique o arquivo de saída.")
            
    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado: {str(e)}")
        print(f"Erro: {str(e)}")
    except Exception as e:
        logger.error(f"Erro na execução: {str(e)}", exc_info=True)
        print(f"Erro na execução: {str(e)}")

if __name__ == "__main__":
    main()